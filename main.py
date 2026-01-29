import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
from decimal import Decimal

import asyncpg
import redis.asyncio as redis
import sentry_sdk
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from celery import Celery
from celery.schedules import crontab
from cryptography.fernet import Fernet
from fastapi import FastAPI, Request, HTTPException
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    bot_token: str
    webhook_host: str
    webhook_path: str
    webhook_secret: str
    database_url: str
    db_pool_min: int = 5
    db_pool_max: int = 20
    redis_url: str
    redis_cache_ttl: int = 300
    celery_broker_url: str
    celery_result_backend: str
    encryption_key: str
    jwt_secret: str
    rate_limit_per_minute: int = 30
    sentry_dsn: Optional[str] = None
    prometheus_port: int = 9090
    metabolism_interval: int = 30
    global_event_interval: int = 604800
    backup_interval: int = 21600

    class Config:
        env_file = ".env"


settings = Settings()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if settings.sentry_dsn:
    sentry_sdk.init(dsn=settings.sentry_dsn)

app = FastAPI()
bot = Bot(token=settings.bot_token, session=AiohttpSession())
dp = Dispatcher()
router = Router()

fernet = Fernet(settings.encryption_key.encode())

redis_client = redis.from_url(settings.redis_url, decode_responses=True)

# Global database pool - will be initialized on startup
db_pool: Optional[asyncpg.Pool] = None

celery_app = Celery(
    "cellular_empire",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.beat_schedule = {
    "metabolism-every-30s": {
        "task": "main.process_metabolism",
        "schedule": timedelta(seconds=settings.metabolism_interval),
    },
    "global-events-weekly": {
        "task": "main.trigger_global_event",
        "schedule": timedelta(seconds=settings.global_event_interval),
    },
    "backup-every-6h": {
        "task": "main.backup_database",
        "schedule": timedelta(seconds=settings.backup_interval),
    },
}

REQUESTS_COUNT = Counter("bot_requests_total", "Total requests")
ACTIVE_PLAYERS = Gauge("active_players", "Active players")
COLONY_SIZE = Histogram("colony_size_cells", "Colony size in cells")

# Locks: created lazily to avoid binding to the wrong event loop (e.g. Celery tasks)
_db_pool_init_lock: Optional[asyncio.Lock] = None
_locks_guard: Optional[asyncio.Lock] = None
_player_locks: Dict[int, asyncio.Lock] = {}
_symbiosis_locks: Dict[Tuple[int, int], asyncio.Lock] = {}


def _ensure_locks_initialized() -> None:
    """Ensure global asyncio locks are initialized.

    Locks are initialized lazily to avoid issues with event-loop binding when the
    module is imported in different runtimes.
    """
    global _db_pool_init_lock, _locks_guard
    if _db_pool_init_lock is None:
        _db_pool_init_lock = asyncio.Lock()
    if _locks_guard is None:
        _locks_guard = asyncio.Lock()


def validate_telegram_id(telegram_id: int) -> None:
    """Validate Telegram user ID.

    Telegram IDs are positive 64-bit integers.
    """
    if not isinstance(telegram_id, int):
        raise ValueError("Telegram ID должен быть целым числом")
    if telegram_id <= 0 or telegram_id >= 2**63:
        raise ValueError("Telegram ID имеет неверное значение")


def player_cache_key(telegram_id: int) -> str:
    """Redis key for player cache (keyed by telegram_id)."""
    return f"player:{telegram_id}"


def colony_cache_key(player_id: int) -> str:
    """Redis key for colony cache (keyed by internal player_id)."""
    return f"colony:{player_id}"


def parse_json_field(value: Any, default: Any) -> Any:
    """Parse a JSON/JSONB field from asyncpg.

    asyncpg can return JSONB columns as Python objects (dict/list) or as strings
    depending on codecs/settings. This helper handles both.
    """
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return default


async def _get_player_lock(telegram_id: int) -> asyncio.Lock:
    """Get a per-telegram_id lock to prevent race conditions."""
    _ensure_locks_initialized()
    assert _locks_guard is not None

    async with _locks_guard:
        lock = _player_locks.get(telegram_id)
        if lock is None:
            lock = asyncio.Lock()
            _player_locks[telegram_id] = lock
        return lock


async def _get_symbiosis_lock(player_id_a: int, player_id_b: int) -> asyncio.Lock:
    """Get a per-pair lock to serialize symbiosis creation."""
    _ensure_locks_initialized()
    assert _locks_guard is not None

    pair = tuple(sorted((player_id_a, player_id_b)))
    async with _locks_guard:
        lock = _symbiosis_locks.get(pair)
        if lock is None:
            lock = asyncio.Lock()
            _symbiosis_locks[pair] = lock
        return lock


async def invalidate_cache_keys(*keys: str) -> None:
    """Best-effort cache invalidation."""
    if not keys:
        return
    try:
        await redis_client.delete(*keys)
    except Exception as e:
        logger.warning(f"Failed to invalidate cache keys {keys}: {e}", exc_info=True)


async def invalidate_player_cache(telegram_id: int) -> None:
    """Invalidate cached player data for a Telegram user."""
    await invalidate_cache_keys(player_cache_key(telegram_id))


async def invalidate_colony_cache(player_id: int) -> None:
    """Invalidate cached colony data for an internal player id."""
    await invalidate_cache_keys(colony_cache_key(player_id))


async def invalidate_player_and_colony_cache(telegram_id: int, player_id: int) -> None:
    """Invalidate both player and colony cache entries."""
    await invalidate_cache_keys(player_cache_key(telegram_id), colony_cache_key(player_id))


class EvolutionPhase(str, Enum):
    INIT = "INIT"
    SINGLE_CELL = "SINGLE_CELL"
    COLONY = "COLONY"
    MULTICELLULAR = "MULTICELLULAR"
    ECOSYSTEM = "ECOSYSTEM"
    SENTIENT_BIOMASS = "SENTIENT_BIOMASS"


class GeneRarity(str, Enum):
    COMMON = "Common"
    RARE = "Rare"
    EPIC = "Epic"
    LEGENDARY = "Legendary"
    MYTHIC = "Mythic"


class EventType(str, Enum):
    VIRUS = "VIRUS"
    ICE_AGE = "ICE_AGE"
    RADIATION = "RADIATION"
    SYMBIOSIS_REQUEST = "SYMBIOSIS_REQUEST"
    MUTATION_AVAILABLE = "MUTATION_AVAILABLE"


class SymbiosisType(str, Enum):
    ENDOSYMBIOSIS = "ENDOSYMBIOSIS"
    CONSORTIUM = "CONSORTIUM"


class GameStates(StatesGroup):
    menu = State()
    evolution = State()
    lab = State()
    symbiosis = State()
    environment = State()
    mutation_select = State()
    symbiosis_request = State()


@dataclass
class Gene:
    id: str
    name: str
    rarity: GeneRarity
    slot: str
    bonuses: Dict[str, float]
    synergy_bonus: float = 1.0


@dataclass
class ColonyStats:
    cell_count: int
    energy: Decimal
    biomass: float
    phase: EvolutionPhase
    pandemic_resistance: float
    organelles: Dict[str, int]
    mutations: List[Gene]


class PlayerCreate(BaseModel):
    telegram_id: int
    username: Optional[str] = None

class ColonyUpdate(BaseModel):
    cell_count: int = Field(..., ge=0)
    energy: Decimal = Field(..., ge=0)
    biomass: float = Field(..., ge=0)
    pandemic_resistance: float = Field(..., ge=0, le=1)

class MutationData(BaseModel):
    gene_id: str
    slot: str = Field(..., pattern=r"^(offensive|defensive|utility)$")

class SymbiosisRequest(BaseModel):
    target_player_id: int
    symbiosis_type: SymbiosisType
    resource_exchange_rate: float = Field(..., ge=0, le=1)


# Global constants for validation
VALID_ENVIRONMENTS = {"ocean", "surface", "deep", "volcanic", "ice"}
ENVIRONMENT_NAMES = {
    "ocean": "Океан",
    "surface": "Поверхность", 
    "deep": "Глубины",
    "volcanic": "Гидротермальные источники",
    "ice": "Ледяной покров"
}

gene_pool = {
    "offensive": [
        Gene("toxin_1", "Базовый токсин", GeneRarity.COMMON, "offensive", {"damage": 1.2}),
        Gene("toxin_2", "Усиленный токсин", GeneRarity.RARE, "offensive", {"damage": 1.5}),
        Gene("acid_1", "Кислотное выделение", GeneRarity.COMMON, "offensive", {"damage": 1.3, "corrosion": 1.1}),
        Gene("virus_1", "Вирусный фаг", GeneRarity.EPIC, "offensive", {"damage": 2.0, "spread": 1.3}),
        Gene("predator_1", "Хищнические тенденции", GeneRarity.RARE, "offensive", {"damage": 1.8, "consumption": 1.2}),
        Gene("quantum_1", "Квантовый паразитизм", GeneRarity.LEGENDARY, "offensive", {"damage": 2.5, "reality_bend": 1.1}),
        Gene("bio_weapon_1", "Биологическое оружие", GeneRarity.MYTHIC, "offensive", {"damage": 5.0, "extinction": 1.5}),
    ],
    "defensive": [
        Gene("membrane_1", "Укрепленная мембрана", GeneRarity.COMMON, "defensive", {"defense": 1.2}),
        Gene("membrane_2", "Жесткая оболочка", GeneRarity.RARE, "defensive", {"defense": 1.5}),
        Gene("regen_1", "Регенерация", GeneRarity.COMMON, "defensive", {"regen": 1.1, "healing": 1.1}),
        Gene("antitoxin_1", "Антитоксин", GeneRarity.COMMON, "defensive", {"toxin_resist": 1.3}),
        Gene("immunity_1", "Иммунная система", GeneRarity.EPIC, "defensive", {"virus_resist": 2.0, "all_resist": 1.2}),
        Gene("immortality_1", "Частичная бессмертность", GeneRarity.LEGENDARY, "defensive", {"death_resist": 0.5, "age_resist": 0.3}),
        Gene("quantum_shield", "Квантовый щит", GeneRarity.MYTHIC, "defensive", {"all_resist": 3.0, "reality_anchor": 1.5}),
    ],
    "utility": [
        Gene("photosynth_1", "Фотосинтез", GeneRarity.COMMON, "utility", {"energy_gen": 1.2, "sun_bonus": 1.1}),
        Gene("chemosynth_1", "Хемосинтез", GeneRarity.COMMON, "utility", {"energy_gen": 1.1, "mineral_bonus": 1.2}),
        Gene("mitochondria_1", "Митохондрии", GeneRarity.RARE, "utility", {"energy_eff": 1.5, "power_bonus": 1.2}),
        Gene("division_1", "Ускоренное деление", GeneRarity.RARE, "utility", {"growth_rate": 1.3}),
        Gene("adaptation_1", "Адаптация", GeneRarity.EPIC, "utility", {"env_resist": 1.4, "mutation_rate": 1.2}),
        Gene("intelligence_1", "Проблеск интеллекта", GeneRarity.LEGENDARY, "utility", {"research_bonus": 2.0, "coordination": 1.5}),
        Gene("ascension_1", "Склонность к вознесению", GeneRarity.MYTHIC, "utility", {"ascension_bonus": 5.0, "reality_perception": 2.0}),
    ],
}


def get_phase_by_cell_count(cell_count: int) -> EvolutionPhase:
    if cell_count >= 10**9:
        return EvolutionPhase.SENTIENT_BIOMASS
    elif cell_count >= 10**6:
        return EvolutionPhase.ECOSYSTEM
    elif cell_count >= 10**4:
        return EvolutionPhase.MULTICELLULAR
    elif cell_count >= 100:
        return EvolutionPhase.COLONY
    elif cell_count >= 1:
        return EvolutionPhase.SINGLE_CELL
    return EvolutionPhase.INIT


def calculate_synergy_bonus(genes: List[Gene]) -> float:
    """Calculate synergy bonus.

    Синергия учитывает:
    1) Дубликаты генов (по gene.id): каждая дополнительная копия даёт +10% к
       множителю (мультипликативно), максимум x1.5 на один тип гена.
    2) Комбинации слотов: наличие хотя бы одного гена в каждом из трёх слотов
       (offensive/defensive/utility) даёт +10%.
    3) Разнообразие раритетов: наличие хотя бы 3 разных раритетов даёт +5%.
    """
    if not genes:
        return 1.0

    from collections import Counter

    counts = Counter(g.id for g in genes)
    bonus = 1.0

    for count in counts.values():
        if count >= 2:
            per_gene_bonus = min(1.0 + 0.10 * (count - 1), 1.5)
            bonus *= per_gene_bonus

    slots = {g.slot for g in genes}
    if len(slots) >= 3:
        bonus *= 1.10

    rarities = {g.rarity if isinstance(g.rarity, GeneRarity) else GeneRarity(g.rarity) for g in genes}
    if len(rarities) >= 3:
        bonus *= 1.05

    return bonus


def select_random_gene(slot: str) -> Gene:
    genes = gene_pool[slot]
    weights = {
        GeneRarity.COMMON: 0.699,
        GeneRarity.RARE: 0.200,
        GeneRarity.EPIC: 0.070,
        GeneRarity.LEGENDARY: 0.029,
        GeneRarity.MYTHIC: 0.002,
    }
    # Use random.choices for better handling of edge cases
    weighted_genes = [(g, weights[g.rarity]) for g in genes]
    total_weight = sum(w for _, w in weighted_genes)
    
    if total_weight <= 0:
        return random.choice(genes)
    
    # Use random.choices with proper normalization
    selected = random.choices(
        population=weighted_genes,
        weights=[w for _, w in weighted_genes],
        k=1
    )[0]
    
    return selected[0]


async def get_db_pool() -> asyncpg.Pool:
    """Get or create the global database pool.

    Protected by an asyncio.Lock to avoid double-initialization under concurrency.
    """
    global db_pool
    _ensure_locks_initialized()
    assert _db_pool_init_lock is not None

    if db_pool is not None:
        return db_pool

    async with _db_pool_init_lock:
        if db_pool is None:
            db_pool = await asyncpg.create_pool(
                settings.database_url,
                min_size=settings.db_pool_min,
                max_size=settings.db_pool_max,
            )

    assert db_pool is not None
    return db_pool


@celery_app.task
def process_metabolism():
    asyncio.run(_process_metabolism_async())


async def _process_metabolism_async():
    """Process metabolism for all colonies."""
    pool = None
    task_redis: Optional[redis.Redis] = None
    try:
        # Celery tasks use asyncio.run(), so create a Redis client bound to this loop.
        task_redis = redis.from_url(settings.redis_url, decode_responses=True)

        pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
        )
        async with pool.acquire() as conn:
            colonies = await conn.fetch("""
                SELECT c.id,
                       c.player_id,
                       p.telegram_id,
                       c.cell_count,
                       c.energy,
                       c.organelles,
                       c.environment
                FROM colonies c
                JOIN players p ON c.player_id = p.id
                WHERE c.last_calc_at < NOW() - INTERVAL '30 seconds'
            """)

            # Batch processing to avoid long transactions
            batch_size = 100
            processed = 0

            for i in range(0, len(colonies), batch_size):
                batch = colonies[i:i + batch_size]
                async with conn.transaction():
                    for colony in batch:
                        try:
                            cell_count = int(colony["cell_count"])
                            energy = Decimal(str(colony["energy"]))

                            organelles_raw = parse_json_field(colony["organelles"], {})
                            organelles = {
                                k: int(v)
                                for k, v in organelles_raw.items()
                                if v is not None
                            }

                            environment = (colony["environment"] or "ocean").strip()
                            if environment not in VALID_ENVIRONMENTS:
                                environment = "ocean"

                            sun_factor = Decimal("1.0") if environment == "surface" else Decimal("0.3")
                            mineral_factor = Decimal("1.0") if environment in {"deep", "volcanic"} else Decimal("0.5")

                            photosynthesis = (
                                Decimal(str(organelles.get("photosynthesis", 0)))
                                * Decimal("0.1")
                                * sun_factor
                            )
                            chemosynthesis = (
                                Decimal(str(organelles.get("chemosynthesis", 0)))
                                * Decimal("0.05")
                                * mineral_factor
                            )

                            base_metabolism = Decimal(str(cell_count)) * Decimal("0.01")
                            organelle_upkeep = Decimal(str(sum(organelles.values()))) * Decimal("0.02")

                            delta_e = photosynthesis + chemosynthesis - base_metabolism - organelle_upkeep
                            new_energy = max(Decimal(0), energy + delta_e)

                            if new_energy < Decimal("0.1") * Decimal(str(cell_count)):
                                cell_loss = int(cell_count * 0.1)
                                new_cell_count = max(1, cell_count - cell_loss)
                                await conn.execute(
                                    """
                                    UPDATE colonies
                                    SET cell_count = $1, energy = $2, last_calc_at = NOW()
                                    WHERE id = $3
                                    """,
                                    new_cell_count,
                                    new_energy,
                                    colony["id"],
                                )
                            else:
                                new_cell_count = cell_count
                                await conn.execute(
                                    """
                                    UPDATE colonies
                                    SET energy = $1, last_calc_at = NOW()
                                    WHERE id = $2
                                    """,
                                    new_energy,
                                    colony["id"],
                                )

                            phase = get_phase_by_cell_count(new_cell_count)
                            await conn.execute(
                                """
                                UPDATE players
                                SET current_phase = $1
                                WHERE id = $2
                                """,
                                phase.value,
                                colony["player_id"],
                            )

                            # Invalidate caches for this player (colony stats + player profile)
                            try:
                                await task_redis.delete(
                                    player_cache_key(int(colony["telegram_id"])),
                                    colony_cache_key(int(colony["player_id"])),
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to invalidate cache for player {colony['player_id']}: {e}",
                                    exc_info=True,
                                )

                            processed += 1
                        except Exception as e:
                            logger.error(f"Error processing colony {colony['id']}: {e}", exc_info=True)
                            continue

            logger.info(f"Metabolism processed {processed} colonies")
    except Exception as e:
        logger.error(f"Error in metabolism processing: {e}", exc_info=True)
    finally:
        if pool is not None:
            await pool.close()
        if task_redis is not None:
            try:
                await task_redis.close()
            except Exception as e:
                logger.warning(f"Error closing task Redis client: {e}", exc_info=True)


@celery_app.task
def trigger_global_event():
    asyncio.run(_trigger_global_event_async())


async def _trigger_global_event_async():
    """Trigger a global event affecting random colonies."""
    pool = None
    task_redis: Optional[redis.Redis] = None
    try:
        task_redis = redis.from_url(settings.redis_url, decode_responses=True)

        pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
        )
        async with pool.acquire() as conn:
            event_type = random.choice([EventType.VIRUS, EventType.ICE_AGE, EventType.RADIATION])
            severity = random.random()

            affected_rows = await conn.fetch(
                """
                INSERT INTO events (type, target_colony_id, params, expires_at)
                SELECT $1, id, $2, NOW() + INTERVAL '24 hours'
                FROM colonies
                WHERE random() < $3
                RETURNING target_colony_id
                """,
                event_type.value,
                json.dumps({"severity": severity}),
                0.3,
            )

            colony_ids = [int(r["target_colony_id"]) for r in affected_rows]
            if not colony_ids:
                logger.info(f"Triggered global event: {event_type.value} with severity {severity} (0 colonies)")
                return

            if event_type == EventType.VIRUS:
                await conn.execute(
                    """
                    UPDATE colonies
                    SET cell_count = GREATEST(
                        1,
                        (cell_count * (1 - $1 * (1 - pandemic_resistance)))::bigint
                    )
                    WHERE id = ANY($2::int[])
                    """,
                    severity,
                    colony_ids,
                )
            elif event_type == EventType.RADIATION:
                await conn.execute(
                    """
                    UPDATE colonies
                    SET mutation_tree = jsonb_set(
                        mutation_tree,
                        '{radiation_mutations}',
                        to_jsonb(COALESCE((mutation_tree->>'radiation_mutations')::int, 0) + 1)
                    )
                    WHERE id = ANY($1::int[])
                    """,
                    colony_ids,
                )
            elif event_type == EventType.ICE_AGE:
                # Energy decreases due to harsher conditions
                await conn.execute(
                    """
                    UPDATE colonies
                    SET energy = GREATEST(0, energy - (energy * ($1::numeric * 0.2)))
                    WHERE id = ANY($2::int[])
                    """,
                    severity,
                    colony_ids,
                )

            # Invalidate caches for affected colonies
            try:
                affected_players = await conn.fetch(
                    """
                    SELECT c.player_id, p.telegram_id
                    FROM colonies c
                    JOIN players p ON c.player_id = p.id
                    WHERE c.id = ANY($1::int[])
                    """,
                    colony_ids,
                )
                keys: List[str] = []
                for row in affected_players:
                    keys.append(player_cache_key(int(row["telegram_id"])))
                    keys.append(colony_cache_key(int(row["player_id"])))
                if keys:
                    await task_redis.delete(*keys)
            except Exception as e:
                logger.warning(f"Failed to invalidate caches after global event: {e}", exc_info=True)

            logger.info(
                f"Triggered global event: {event_type.value} with severity {severity} ({len(colony_ids)} colonies)"
            )
    except Exception as e:
        logger.error(f"Error triggering global event: {e}", exc_info=True)
    finally:
        if pool is not None:
            await pool.close()
        if task_redis is not None:
            try:
                await task_redis.close()
            except Exception as e:
                logger.warning(f"Error closing task Redis client: {e}", exc_info=True)


@celery_app.task
def backup_database():
    logger.info("Starting database backup")


async def check_rate_limit(telegram_id: int) -> bool:
    """Check rate limit for a user using atomic Redis operation."""
    try:
        validate_telegram_id(telegram_id)
    except ValueError:
        return True

    key = f"rate_limit:{telegram_id}"
    
    # Lua script to atomically check and increment rate limit
    lua_script = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])
    
    local current = redis.call('GET', key)
    if current == false then
        redis.call('SETEX', key, ttl, 1)
        return 1
    end
    
    current = tonumber(current)
    if current >= limit then
        return 0
    end
    
    redis.call('INCR', key)
    return 1
    """
    
    try:
        result = await redis_client.eval(
            lua_script,
            1,
            key,
            settings.rate_limit_per_minute,
            60
        )
        return bool(result)
    except Exception as e:
        logger.error(f"Rate limit check error: {e}", exc_info=True)
        return True  # Fail open on Redis errors


async def get_or_create_player(telegram_id: int, username: Optional[str] = None) -> Dict:
    """Get or create a player by Telegram ID.

    Uses a per-user asyncio.Lock and an UPSERT to prevent race conditions.
    """
    validate_telegram_id(telegram_id)

    lock = await _get_player_lock(telegram_id)
    async with lock:
        cache_key = player_cache_key(telegram_id)
        cached = await redis_client.get(cache_key)
        if cached:
            try:
                data = json.loads(cached)
                cached_at = data.get("cached_at")
                if cached_at:
                    try:
                        cached_dt = datetime.fromisoformat(cached_at)
                        if datetime.utcnow() - cached_dt > timedelta(seconds=settings.redis_cache_ttl * 2):
                            raise ValueError("stale cache")
                    except Exception:
                        pass
                if isinstance(data, dict) and data.get("telegram_id") == telegram_id and "id" in data:
                    return data
            except Exception as e:
                logger.warning(f"Cache data corrupted for player {telegram_id}: {e}", exc_info=True)
                await redis_client.delete(cache_key)

        try:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    player = await conn.fetchrow(
                        """
                        INSERT INTO players (telegram_id, username, current_phase)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (telegram_id) DO UPDATE
                        SET username = COALESCE(EXCLUDED.username, players.username),
                            last_activity = NOW()
                        RETURNING *
                        """,
                        telegram_id,
                        username,
                        EvolutionPhase.INIT.value,
                    )

                    if not player:
                        raise RuntimeError("Не удалось получить данные игрока")

                    # Ensure colony exists for this player
                    await conn.execute(
                        """
                        INSERT INTO colonies (
                            player_id, cell_count, energy, biomass,
                            mutation_tree, organelles, environment, pandemic_resistance
                        )
                        SELECT $1, 1, 100.0, 1.0, '{}'::jsonb, '{}'::jsonb, 'ocean', 0.1
                        WHERE NOT EXISTS (SELECT 1 FROM colonies WHERE player_id = $1)
                        """,
                        player["id"],
                    )

                result = dict(player)
                result["cached_at"] = datetime.utcnow().isoformat()

                try:
                    await redis_client.setex(cache_key, settings.redis_cache_ttl, json.dumps(result, default=str))
                except Exception as e:
                    logger.warning(f"Failed to cache player data: {e}", exc_info=True)

                return result
        except Exception as e:
            logger.error(f"Error in get_or_create_player: {e}", exc_info=True)
            raise


async def check_player_exists(telegram_id: int) -> Optional[Dict]:
    """Check if a player exists without creating one."""
    try:
        validate_telegram_id(telegram_id)
    except ValueError:
        return None

    cache_key = player_cache_key(telegram_id)
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            data = json.loads(cached)
            if isinstance(data, dict) and data.get("telegram_id") == telegram_id and "id" in data:
                return data
        except Exception as e:
            logger.warning(f"Cache data corrupted for player {telegram_id}: {e}", exc_info=True)
            await redis_client.delete(cache_key)

    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            player = await conn.fetchrow(
                """
                SELECT * FROM players WHERE telegram_id = $1
                """,
                telegram_id,
            )

            if player:
                result = dict(player)
                result["cached_at"] = datetime.utcnow().isoformat()
                try:
                    await redis_client.setex(cache_key, settings.redis_cache_ttl, json.dumps(result, default=str))
                except Exception as e:
                    logger.warning(f"Failed to cache player data: {e}", exc_info=True)
                return result
            return None
    except Exception as e:
        logger.error(f"Error in check_player_exists: {e}", exc_info=True)
        return None


async def get_colony_stats(player_id: int) -> ColonyStats:
    """Get colony statistics for a player."""
    cache_key = colony_cache_key(player_id)
    cached = await redis_client.get(cache_key)
    if cached:
        try:
            data = json.loads(cached)

            cached_at = data.get("cached_at")
            if cached_at:
                try:
                    cached_dt = datetime.fromisoformat(cached_at)
                    if datetime.utcnow() - cached_dt > timedelta(seconds=settings.redis_cache_ttl * 2):
                        raise ValueError("stale cache")
                except Exception:
                    pass

            phase_raw = data.get("phase", EvolutionPhase.INIT.value)
            phase = phase_raw if isinstance(phase_raw, EvolutionPhase) else EvolutionPhase(str(phase_raw))

            organelles_raw = data.get("organelles") or {}
            if not isinstance(organelles_raw, dict):
                organelles_raw = {}
            organelles = {k: int(v) for k, v in organelles_raw.items() if v is not None}

            mutations_raw = data.get("mutations") or []
            mutations: List[Gene] = []
            if isinstance(mutations_raw, list):
                for g in mutations_raw:
                    try:
                        mutations.append(
                            Gene(
                                id=g["id"],
                                name=g["name"],
                                rarity=GeneRarity(g["rarity"]) if isinstance(g.get("rarity"), str) else g["rarity"],
                                slot=g["slot"],
                                bonuses=g.get("bonuses", {}),
                            )
                        )
                    except Exception:
                        continue

            return ColonyStats(
                cell_count=int(data["cell_count"]),
                energy=Decimal(str(data["energy"])),
                biomass=float(data["biomass"]),
                phase=phase,
                pandemic_resistance=float(data.get("pandemic_resistance", 0.1)),
                organelles=organelles,
                mutations=mutations,
            )
        except Exception as e:
            logger.warning(f"Cache data corrupted for player {player_id}: {e}", exc_info=True)
            await redis_client.delete(cache_key)

    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT c.*, p.current_phase as phase
                FROM colonies c
                JOIN players p ON c.player_id = p.id
                WHERE p.id = $1
                """,
                player_id,
            )

            if not row:
                raise ValueError("Colony not found")

            mutations_rows = await conn.fetch(
                """
                SELECT gene_id FROM mutation_tree WHERE colony_id = $1
                """,
                row["id"],
            )

            gene_pool_map = {
                gene.id: gene
                for slot_genes in gene_pool.values()
                for gene in slot_genes
            }

            genes = [
                gene_pool_map[m["gene_id"]]
                for m in mutations_rows
                if m["gene_id"] in gene_pool_map
            ]

            organelles_data = parse_json_field(row["organelles"], {})
            if not isinstance(organelles_data, dict):
                organelles_data = {}
            organelles_data = {k: int(v) for k, v in organelles_data.items() if v is not None}

            stats = ColonyStats(
                cell_count=int(row["cell_count"]),
                energy=Decimal(str(row["energy"])),
                biomass=float(row["biomass"]),
                phase=EvolutionPhase(row["phase"]),
                pandemic_resistance=float(row["pandemic_resistance"]),
                organelles=organelles_data,
                mutations=genes,
            )

            try:
                await redis_client.setex(
                    cache_key,
                    settings.redis_cache_ttl,
                    json.dumps(
                        {
                            "cell_count": stats.cell_count,
                            "energy": str(stats.energy),
                            "biomass": stats.biomass,
                            "phase": stats.phase.value,
                            "pandemic_resistance": stats.pandemic_resistance,
                            "organelles": stats.organelles,
                            "mutations": [
                                {
                                    "id": g.id,
                                    "name": g.name,
                                    "rarity": g.rarity.value,
                                    "slot": g.slot,
                                    "bonuses": g.bonuses,
                                }
                                for g in genes
                            ],
                            "cached_at": datetime.utcnow().isoformat(),
                        },
                        default=str,
                    ),
                )
            except Exception as e:
                logger.warning(f"Failed to cache colony stats: {e}", exc_info=True)

            return stats
    except Exception as e:
        logger.error(f"Error in get_colony_stats: {e}", exc_info=True)
        raise


def create_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🧬 Эволюция"), KeyboardButton(text="⚡ Метаболизм")],
            [KeyboardButton(text="🌍 Среда"), KeyboardButton(text="🤝 Симбиоз")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🔬 Лаборатория")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )


@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    """Handle /start command."""
    try:
        REQUESTS_COUNT.inc()
        
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id, message.from_user.username)
        stats = await get_colony_stats(player["id"])
        
        welcome_text = f"""
🧫 <b>Добро пожаловать в Клеточную Империю!</b>

Ваша колония:
• <b>Этап:</b> {stats.phase.value}
• <b>Клеток:</b> {stats.cell_count:,}
• <b>Энергия:</b> {stats.energy:.2f}
• <b>Биомасса:</b> {stats.biomass:.2f}
• <b>Устойчивость:</b> {stats.pandemic_resistance:.1%}

Цель: достичь <b>Планетарного разума</b> (10¹⁸ клеток)
"""
        
        await message.answer(welcome_text, reply_markup=create_main_menu(), parse_mode="HTML")
        await state.set_state(GameStates.menu)
    except Exception as e:
        logger.error(f"Error in cmd_start: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")


@router.message(F.text == "📊 Статистика")
async def show_stats(message: types.Message):
    """Show player statistics."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id)
        stats = await get_colony_stats(player["id"])
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            leaderboard = await conn.fetch("""
                SELECT p.username, c.cell_count, c.biomass,
                       RANK() OVER (ORDER BY c.cell_count DESC) as rank
                FROM players p
                JOIN colonies c ON p.id = c.player_id
                ORDER BY c.cell_count DESC
                LIMIT 10
            """)
        
        rank_info = ""
        for i, row in enumerate(leaderboard[:5], 1):
            rank_info += f"{i}. <b>{row['username'] or 'Unknown'}</b>: {row['cell_count']:,} клеток\n"
        
        stats_text = f"""
📊 <b>Ваша статистика</b>

🧫 <b>Колония</b>
• Клеток: {stats.cell_count:,}
• Энергия: {stats.energy:.2f}
• Биомасса: {stats.biomass:.2f}
• Этап: {stats.phase.value}

🛡️ <b>Защита</b>
• Устойчивость к пандемиям: {stats.pandemic_resistance:.1%}
• Органелл: {sum(stats.organelles.values())}
• Мутаций: {len(stats.mutations)}

🏆 <b>Топ-5 игроков</b>
{rank_info}

🎯 <b>Прогресс к Планетарному разуму</b>
{(stats.cell_count / 10**18) * 100:.10f}%
"""
        
        await message.answer(stats_text, parse_mode="HTML", reply_markup=create_main_menu())
    except Exception as e:
        logger.error(f"Error in show_stats: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении статистики.")


@router.message(F.text == "🧬 Эволюция")
async def show_evolution(message: types.Message, state: FSMContext):
    """Show evolution tree and progress."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id)
        stats = await get_colony_stats(player["id"])
        
        next_phase = None
        next_threshold = None
        
        if stats.phase == EvolutionPhase.INIT:
            next_phase = EvolutionPhase.SINGLE_CELL
            next_threshold = 1
        elif stats.phase == EvolutionPhase.SINGLE_CELL:
            next_phase = EvolutionPhase.COLONY
            next_threshold = 100
        elif stats.phase == EvolutionPhase.COLONY:
            next_phase = EvolutionPhase.MULTICELLULAR
            next_threshold = 10_000
        elif stats.phase == EvolutionPhase.MULTICELLULAR:
            next_phase = EvolutionPhase.ECOSYSTEM
            next_threshold = 1_000_000
        elif stats.phase == EvolutionPhase.ECOSYSTEM:
            next_phase = EvolutionPhase.SENTIENT_BIOMASS
            next_threshold = 1_000_000_000
        
        evolution_text = f"""
🧬 <b>Древо эволюции</b>

<b>Текущий этап:</b> {stats.phase.value}
<b>Клеток:</b> {stats.cell_count:,}

"""
        
        if next_phase:
            progress = (stats.cell_count / next_threshold) * 100
            progress_bar = min(20, int(progress / 5))
            evolution_text += f"""
<b>Следующий этап:</b> {next_phase.value}
<b>Требуется:</b> {next_threshold:,} клеток
<b>Прогресс:</b> {progress:.1f}%

{"▓" * progress_bar}{"░" * (20 - progress_bar)}
"""
        else:
            evolution_text += "\n<b>🏆 Вы достигли максимального этапа!</b>"
        
        buttons = []
        if stats.cell_count >= 1000:
            buttons.append(InlineKeyboardButton(text="🔬 Исследовать мутацию", callback_data="research_mutation"))
        if stats.cell_count >= 10000:
            buttons.append(InlineKeyboardButton(text="🧪 Горизонтальный перенос", callback_data="horizontal_transfer"))
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[i:i + 1] for i in range(0, len(buttons), 1)])
        
        await message.answer(evolution_text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(GameStates.evolution)
    except Exception as e:
        logger.error(f"Error in show_evolution: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении эволюции.")


@router.callback_query(F.data == "research_mutation")
async def research_mutation(callback: CallbackQuery, state: FSMContext):
    """Research a new mutation."""
    try:
        await callback.answer()
        
        if not await check_rate_limit(callback.from_user.id):
            await callback.message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(callback.from_user.id)
        stats = await get_colony_stats(player["id"])
        
        current_slots = {g.slot for g in stats.mutations}
        if len(current_slots) >= 3:
            await callback.message.edit_text("❌ У вас уже максимум мутаций! Удалите старую для новой.")
            return

        available_slots = [
            slot for slot in ("offensive", "defensive", "utility")
            if slot not in current_slots
        ]
        if not available_slots:
            await callback.message.edit_text("❌ Нет доступных слотов для мутации.")
            return

        selected_slot = random.choice(available_slots)
        new_gene = select_random_gene(selected_slot)

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                colony_id = await conn.fetchval(
                    "SELECT id FROM colonies WHERE player_id = $1",
                    player["id"],
                )
                if not colony_id:
                    await callback.message.edit_text("❌ Колония не найдена!")
                    return

                try:
                    await conn.execute(
                        """
                        INSERT INTO mutation_tree (colony_id, gene_id, slot, rarity, bonuses)
                        VALUES ($1, $2, $3, $4, $5)
                        """,
                        colony_id,
                        new_gene.id,
                        selected_slot,
                        new_gene.rarity.value,
                        json.dumps(new_gene.bonuses),
                    )
                except asyncpg.UniqueViolationError:
                    await callback.message.edit_text("❌ У вас уже есть этот ген! Попробуйте еще раз.")
                    return

        await invalidate_player_and_colony_cache(callback.from_user.id, player["id"])
        
        await callback.message.edit_text(
            f"""✨ <b>Новая мутация!</b>

<b>Ген:</b> {new_gene.name}
<b>Слот:</b> {selected_slot}
<b>Раритет:</b> {new_gene.rarity.value}

<b>Бонусы:</b>
""" + "\n".join([f"• {k}: +{v:.1f}%" for k, v in new_gene.bonuses.items()]),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error in research_mutation: {e}", exc_info=True)
        await callback.message.answer("❌ Произошла ошибка при исследовании мутации.")


@router.message(F.text == "⚡ Метаболизм")
async def show_metabolism(message: types.Message):
    """Show metabolism information."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id)
        stats = await get_colony_stats(player["id"])

        consumption_per_sec = Decimal(stats.cell_count) * Decimal("0.01")
        generation_per_sec = (
            Decimal(str(stats.organelles.get("photosynthesis", 0))) * Decimal("0.1")
            + Decimal(str(stats.organelles.get("chemosynthesis", 0))) * Decimal("0.05")
        )
        low_energy_threshold = Decimal(stats.cell_count) * Decimal("0.1")
        energy_status = "⚠️ <b>Низкая энергия!</b>" if stats.energy < low_energy_threshold else "✅ Энергия стабильна"

        metabolism_text = f"""
⚡ <b>Метаболизм колонии</b>

<b>Текущая энергия:</b> {stats.energy:.2f}
<b>Потребление:</b> {consumption_per_sec:.2f}/сек
<b>Генерация:</b> {generation_per_sec:.2f}/сек

<b>Органеллы:</b>
• Фотосинтез: {stats.organelles.get('photosynthesis', 0)}
• Хемосинтез: {stats.organelles.get('chemosynthesis', 0)}
• Митохондрии: {stats.organelles.get('mitochondria', 0)}

{energy_status}
"""
        
        buttons = [
            InlineKeyboardButton(text="🌱 Добавить фотосинтез", callback_data="add_photosynthesis"),
            InlineKeyboardButton(text="💎 Добавить хемосинтез", callback_data="add_chemosynthesis"),
            InlineKeyboardButton(text="⚡ Добавить митохондрии", callback_data="add_mitochondria"),
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[i:i + 1] for i in range(0, len(buttons), 1)])
        
        await message.answer(metabolism_text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in show_metabolism: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении информации о метаболизме.")


@router.callback_query(F.data.startswith("add_"))
async def add_organelle(callback: CallbackQuery):
    """Add an organelle to the colony."""
    try:
        await callback.answer()
        
        organelle_type = callback.data.replace("add_", "")
        player = await get_or_create_player(callback.from_user.id)
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Use SELECT FOR UPDATE to prevent race conditions
            async with conn.transaction():
                colony = await conn.fetchrow("""
                    SELECT c.* FROM colonies c
                    JOIN players p ON c.player_id = p.id
                    WHERE p.id = $1
                    FOR UPDATE
                """, player["id"])
                
                if not colony:
                    await callback.message.edit_text("❌ Колония не найдена!")
                    return
                
                organelles = parse_json_field(colony["organelles"], {})
                if not isinstance(organelles, dict):
                    organelles = {}
                organelles = {k: int(v) for k, v in organelles.items() if v is not None}

                current_count = int(organelles.get(organelle_type, 0) or 0)
                cost = 50 * (current_count + 1)
                current_energy = Decimal(str(colony["energy"]))
                
                if current_energy < Decimal(str(cost)):
                    await callback.message.edit_text("❌ Недостаточно энергии!")
                    return
                
                organelles[organelle_type] = current_count + 1
                
                await conn.execute("""
                    UPDATE colonies 
                    SET organelles = $1, energy = energy - $2, last_calc_at = NOW()
                    WHERE id = $3
                """, json.dumps(organelles), cost, colony["id"])
        
        await invalidate_player_and_colony_cache(callback.from_user.id, player["id"])
        
        organelle_names = {
            "photosynthesis": "Фотосинтез",
            "chemosynthesis": "Хемосинтез", 
            "mitochondria": "Митохондрии"
        }
        
        await callback.message.edit_text(f"✅ Добавлено: {organelle_names.get(organelle_type, organelle_type)} (+1)")
    except Exception as e:
        logger.error(f"Error in add_organelle: {e}", exc_info=True)
        await callback.message.answer("❌ Произошла ошибка при добавлении органеллы.")


@router.message(F.text == "🤝 Симбиоз")
async def show_symbiosis(message: types.Message):
    """Show symbiosis relationships."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id)
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Fixed SQL logic to correctly identify partner
            symbioses = await conn.fetch("""
                SELECT sc.*,
                       CASE 
                           WHEN sc.host_id = $1 THEN p2.username
                           ELSE p1.username
                       END as partner_name
                FROM symbiosis_contracts sc
                JOIN players p1 ON sc.host_id = p1.id
                JOIN players p2 ON sc.symbiont_id = p2.id
                WHERE sc.host_id = $1 OR sc.symbiont_id = $1
            """, player["id"])
        
        symbiosis_text = f"""
🤝 <b>Симбиоз и консорциумы</b>

<b>Ваши симбиотические связи:</b> {len(symbioses)}

"""
        
        for sym in symbioses:
            symbiosis_text += f"• <b>{sym['partner_name']}</b> - {sym['contract_type']} ({sym['resource_exchange_rate']:.1%})\n"
        
        buttons = [
            InlineKeyboardButton(text="🌿 Предложить симбиоз", callback_data="request_symbiosis"),
            InlineKeyboardButton(text="💌 Отправить споры", callback_data="send_spores"),
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[i:i + 1] for i in range(0, len(buttons), 1)])
        
        await message.answer(symbiosis_text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in show_symbiosis: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении информации о симбиозе.")


@router.callback_query(F.data == "request_symbiosis")
async def request_symbiosis(callback: CallbackQuery, state: FSMContext):
    """Ask user for a target Telegram ID to create a symbiosis contract."""
    try:
        await callback.answer()

        if not await check_rate_limit(callback.from_user.id):
            await callback.message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return

        await callback.message.edit_text("Введите Telegram ID игрока для симбиоза:")
        await state.set_state(GameStates.symbiosis_request)
    except Exception as e:
        logger.error(f"Error in request_symbiosis: {e}", exc_info=True)
        await callback.message.answer("❌ Произошла ошибка. Попробуйте позже.")


@router.message(GameStates.symbiosis_request)
async def process_symbiosis_request(message: types.Message, state: FSMContext):
    """Process symbiosis request."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return

        try:
            target_id = int((message.text or "").strip())
            validate_telegram_id(target_id)
        except (ValueError, TypeError):
            await message.answer("❌ Неверный Telegram ID!")
            return
        
        if target_id == message.from_user.id:
            await message.answer("❌ Нельзя отправить запрос самому себе!")
            return
        
        player = await get_or_create_player(message.from_user.id)
        
        # Check if target player exists without creating
        target = await check_player_exists(target_id)
        if not target:
            await message.answer("❌ Игрок не найден!")
            return
        
        pair_lock = await _get_symbiosis_lock(player["id"], target["id"])
        async with pair_lock:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Lock both colonies to make the comparison + insert consistent
                    player_colony = await conn.fetchrow(
                        "SELECT cell_count FROM colonies WHERE player_id = $1 FOR UPDATE",
                        player["id"],
                    )
                    target_colony = await conn.fetchrow(
                        "SELECT cell_count FROM colonies WHERE player_id = $1 FOR UPDATE",
                        target["id"],
                    )

                    if not player_colony or not target_colony:
                        await message.answer("❌ Ошибка: одна из колоний не найдена!")
                        return

                    if player_colony["cell_count"] > target_colony["cell_count"]:
                        contract_type = SymbiosisType.ENDOSYMBIOSIS
                    else:
                        contract_type = SymbiosisType.CONSORTIUM

                    try:
                        await conn.execute(
                            """
                            INSERT INTO symbiosis_contracts (
                                host_id, symbiont_id, contract_type, resource_exchange_rate
                            )
                            VALUES ($1, $2, $3, $4)
                            """,
                            player["id"],
                            target["id"],
                            contract_type.value,
                            0.1,
                        )
                    except asyncpg.UniqueViolationError:
                        await message.answer("❌ Симбиоз между этими игроками уже существует!")
                        return
        
        sender_name = message.from_user.username or message.from_user.full_name or "игрок"
        try:
            await bot.send_message(
                target_id,
                f"🤝 Игрок {sender_name} предлагает симбиоз ({contract_type.value})!\n\nКолония получит +10% к росту."
            )
        except Exception as e:
            logger.warning(f"Could not send symbiosis message to {target_id}: {e}", exc_info=True)
        
        await message.answer(f"✅ Предложение симбиоза отправлено!")
        await state.set_state(GameStates.menu)
    except Exception as e:
        logger.error(f"Error in process_symbiosis_request: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при отправке запроса.")


@router.message(F.text == "🌍 Среда")
async def show_environment(message: types.Message):
    """Show environment information."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id)
        
        # Get current environment from colony, not organelles
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            colony = await conn.fetchrow("""
                SELECT environment FROM colonies WHERE player_id = $1
            """, player["id"])
        
        # Check if colony exists before using it
        if not colony:
            await message.answer("❌ Колония не найдена!")
            return
        
        current_environment = colony["environment"] or "ocean"
        
        environments = {
            "ocean": {"name": "Океан", "energy": "⭐", "danger": "🛡️"},
            "surface": {"name": "Поверхность", "energy": "⭐⭐⭐", "danger": "⚠️"},
            "deep": {"name": "Глубины", "energy": "⭐⭐", "danger": "⚠️⚠️"},
            "volcanic": {"name": "Гидротермальные источники", "energy": "⭐⭐⭐⭐", "danger": "⚠️⚠️⚠️"},
            "ice": {"name": "Ледяной покров", "energy": "⭐", "danger": "🛡️🛡️"},
        }
        
        current_env = environments.get(current_environment, environments["ocean"])
        
        env_text = f"""
🌍 <b>Среда обитания</b>

<b>Текущая среда:</b> {current_env['name']}
<b>Энергия:</b> {current_env['energy']}
<b>Опасность:</b> {current_env['danger']}

<b>Доступные среды:</b>
"""
        
        for key, env in environments.items():
            env_text += f"\n<b>{env['name']}</b> - Энергия: {env['energy']}, Опасность: {env['danger']}"
        
        buttons = []
        for key in environments.keys():
            if key != current_environment:
                buttons.append(InlineKeyboardButton(text=f"Переместиться в {environments[key]['name']}", callback_data=f"move_{key}"))
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[i:i + 1] for i in range(0, len(buttons), 1)])
        
        await message.answer(env_text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in show_environment: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении информации о среде.")


@router.callback_query(F.data.startswith("move_"))
async def move_environment(callback: CallbackQuery):
    """Move colony to a different environment."""
    try:
        await callback.answer()

        if not await check_rate_limit(callback.from_user.id):
            await callback.message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return

        new_env = (callback.data or "").replace("move_", "").strip()

        if new_env not in VALID_ENVIRONMENTS:
            logger.warning(f"Invalid environment requested: {new_env}")
            await callback.message.edit_text("❌ Неверная среда обитания!")
            return

        player = await get_or_create_player(callback.from_user.id)

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                colony_id = await conn.fetchval(
                    "SELECT id FROM colonies WHERE player_id = $1 FOR UPDATE",
                    player["id"],
                )
                if not colony_id:
                    await callback.message.edit_text("❌ Колония не найдена!")
                    return

                await conn.execute(
                    """
                    UPDATE colonies
                    SET environment = $1
                    WHERE id = $2
                    """,
                    new_env,
                    colony_id,
                )

        await invalidate_colony_cache(player["id"])
        await callback.message.edit_text(f"✅ Колония перемещена в {ENVIRONMENT_NAMES.get(new_env, new_env)}!")
    except Exception as e:
        logger.error(f"Error in move_environment: {e}", exc_info=True)
        await callback.message.answer("❌ Произошла ошибка при смене среды.")


@router.message(F.text == "🔬 Лаборатория")
async def show_lab(message: types.Message):
    """Show genetic laboratory."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        player = await get_or_create_player(message.from_user.id)
        stats = await get_colony_stats(player["id"])
        
        lab_text = f"""
🔬 <b>Генетическая лаборатория</b>

<b>Активные мутации:</b>
"""
        
        for i, gene in enumerate(stats.mutations, 1):
            lab_text += f"\n{i}. <b>{gene.name}</b> ({gene.rarity.value}) - {gene.slot}"
            lab_text += "\n" + "\n".join([f"   • {k}: +{v:.1f}%" for k, v in gene.bonuses.items()])
        
        if not stats.mutations:
            lab_text += "\n<i>Мутации не обнаружены</i>"
        
        synergy = calculate_synergy_bonus(stats.mutations)
        if synergy > 1.0:
            lab_text += f"\n\n✨ <b>Синергия:</b> x{synergy:.1f}"
        
        buttons = []
        if len(stats.mutations) > 0:
            buttons.append(InlineKeyboardButton(text="🗑️ Удалить мутацию", callback_data="remove_mutation"))
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[i:i + 1] for i in range(0, len(buttons), 1)])
        
        await message.answer(lab_text, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in show_lab: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении информации о лаборатории.")


@router.callback_query(F.data == "remove_mutation")
async def remove_mutation_menu(callback: CallbackQuery):
    """Show a menu to remove an existing mutation."""
    try:
        await callback.answer()

        if not await check_rate_limit(callback.from_user.id):
            await callback.message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return

        player = await get_or_create_player(callback.from_user.id)
        stats = await get_colony_stats(player["id"])

        if not stats.mutations:
            await callback.message.edit_text("❌ У вас нет мутаций для удаления.")
            return

        buttons: List[InlineKeyboardButton] = []
        for i, gene in enumerate(stats.mutations, 1):
            buttons.append(
                InlineKeyboardButton(
                    text=f"{i}. {gene.name}",
                    callback_data=f"remove_gene_{gene.id}",
                )
            )

        keyboard = InlineKeyboardMarkup(inline_keyboard=[[b] for b in buttons])
        await callback.message.edit_text("Выберите мутацию для удаления:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in remove_mutation_menu: {e}", exc_info=True)
        await callback.message.answer("❌ Произошла ошибка. Попробуйте позже.")


@router.callback_query(F.data.startswith("remove_gene_"))
async def remove_gene(callback: CallbackQuery):
    """Remove a mutation from the player's colony."""
    try:
        await callback.answer()

        if not await check_rate_limit(callback.from_user.id):
            await callback.message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return

        gene_id = (callback.data or "").replace("remove_gene_", "").strip()
        if not gene_id:
            await callback.message.answer("❌ Неверный идентификатор мутации.")
            return

        player = await get_or_create_player(callback.from_user.id)

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                colony_id = await conn.fetchval(
                    "SELECT id FROM colonies WHERE player_id = $1",
                    player["id"],
                )
                if not colony_id:
                    await callback.message.edit_text("❌ Колония не найдена!")
                    return

                await conn.execute(
                    """
                    DELETE FROM mutation_tree
                    WHERE colony_id = $1 AND gene_id = $2
                    """,
                    colony_id,
                    gene_id,
                )

        await invalidate_player_and_colony_cache(callback.from_user.id, player["id"])
        await callback.message.edit_text("🗑️ Мутация удалена!")
    except Exception as e:
        logger.error(f"Error in remove_gene: {e}", exc_info=True)
        await callback.message.answer("❌ Произошла ошибка при удалении мутации.")


@router.message(Command("leaderboard"))
async def cmd_leaderboard(message: types.Message):
    """Show top players leaderboard."""
    try:
        if not await check_rate_limit(message.from_user.id):
            await message.answer("⏳ Превышен лимит запросов. Подождите минуту.")
            return
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            top_players = await conn.fetch("""
                SELECT p.username, c.cell_count, c.biomass, p.current_phase,
                       RANK() OVER (ORDER BY c.cell_count DESC) as rank
                FROM players p
                JOIN colonies c ON p.id = c.player_id
                ORDER BY c.cell_count DESC
                LIMIT 20
            """)
        
        board_text = "🏆 <b>Топ-20 игроков</b>\n\n"
        for player in top_players:
            board_text += f"{player['rank']}. <b>{player['username'] or 'Unknown'}</b>\n"
            board_text += f"   {player['cell_count']:,} клеток | {player['biomass']:.1f} биомассы | {player['current_phase']}\n\n"
        
        await message.answer(board_text, parse_mode="HTML", reply_markup=create_main_menu())
    except Exception as e:
        logger.error(f"Error in cmd_leaderboard: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при получении таблицы лидеров.")


@router.message(Command("help"))
async def cmd_help(message: types.Message):
    """Show help information."""
    try:
        help_text = """
📖 <b>Помощь по Клеточной Империи</b>

<b>Основные команды:</b>
• /start - Начать игру
• /stats - Ваша статистика
• /leaderboard - Топ игроков
• /help - Эта помощь

<b>Механики:</b>
• <b>Эволюция</b> - развивайте колонию через 6 этапов
• <b>Мутации</b> - 12 генов с 5 ранками раритета
• <b>Симбиоз</b> - объединяйтесь с другими игроками
• <b>Метаболизм</b> - управляйте энергией и органеллами
• <b>Среда</b> - адаптируйтесь к разным условиям
• <b>Пандемии</b> - выживайте в глобальных катастрофах

<b>Ваши цели:</b>
1. Растите колонию до 10¹⁸ клеток
2. Исследуйте мутации и способности
3. Достижите Планетарного разума
4. Станьте лучшим игроком!

<b>Советы:</b>
• Следите за энергией, иначе начнется автолиз
• Комбинируйте 3 одинаковых гена для синергии
• Адаптируйтесь к среде для бонусов
• Сотрудничайте через симбиоз
"""
        
        await message.answer(help_text, parse_mode="HTML", reply_markup=create_main_menu())
    except Exception as e:
        logger.error(f"Error in cmd_help: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка.")


@app.post(settings.webhook_path)
async def webhook_handler(request: Request):
    """Handle incoming webhook requests from Telegram."""
    try:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret != settings.webhook_secret:
            raise HTTPException(status_code=403, detail="Invalid secret")
        
        try:
            update = await request.json()
        except Exception as e:
            logger.error(f"Invalid JSON in webhook: {e}")
            raise HTTPException(status_code=400, detail="Invalid JSON")
        
        try:
            telegram_update = types.Update(**update)
        except Exception as e:
            logger.error(f"Invalid Update object: {e}")
            raise HTTPException(status_code=400, detail="Invalid update format")
        
        await dp.feed_update(bot, telegram_update)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in webhook handler: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# Register router
dp.include_router(router)


@app.on_event("startup")
async def on_startup():
    """Initialize application on startup."""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                username VARCHAR(255),
                current_phase VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                last_activity TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS colonies (
                id SERIAL PRIMARY KEY,
                player_id INT REFERENCES players(id) ON DELETE CASCADE,
                cell_count BIGINT NOT NULL DEFAULT 1,
                energy DECIMAL NOT NULL DEFAULT 100.0,
                biomass FLOAT NOT NULL DEFAULT 1.0,
                mutation_tree JSONB DEFAULT '{}',
                organelles JSONB DEFAULT '{}',
                environment VARCHAR(50) DEFAULT 'ocean' CHECK (environment IN ('ocean', 'surface', 'deep', 'volcanic', 'ice')),
                last_calc_at TIMESTAMP DEFAULT NOW(),
                pandemic_resistance FLOAT DEFAULT 0.1
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS evolution_branches (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                parent_id INT REFERENCES evolution_branches(id),
                unlock_cell_count BIGINT NOT NULL,
                bonuses JSONB,
                visual_emoji VARCHAR(10)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS mutation_tree (
                id SERIAL PRIMARY KEY,
                colony_id INT REFERENCES colonies(id) ON DELETE CASCADE,
                gene_id VARCHAR(100) NOT NULL,
                slot VARCHAR(50) NOT NULL,
                rarity VARCHAR(50) NOT NULL CHECK (rarity IN ('Common', 'Rare', 'Epic', 'Legendary', 'Mythic')),
                bonuses JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Add unique constraint to prevent duplicate genes in same colony
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_mutation_tree_unique 
            ON mutation_tree(colony_id, gene_id)
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS symbiosis_contracts (
                id SERIAL PRIMARY KEY,
                host_id INT REFERENCES players(id) ON DELETE CASCADE,
                symbiont_id INT REFERENCES players(id) ON DELETE CASCADE,
                contract_type VARCHAR(50) NOT NULL,
                resource_exchange_rate FLOAT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Prevent duplicates regardless of direction (A<->B)
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_symbiosis_contracts_unique_pair
            ON symbiosis_contracts (LEAST(host_id, symbiont_id), GREATEST(host_id, symbiont_id))
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                type VARCHAR(50) NOT NULL,
                target_colony_id INT REFERENCES colonies(id) ON DELETE CASCADE,
                params JSONB,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_players_telegram ON players(telegram_id);
            CREATE INDEX IF NOT EXISTS idx_colonies_player ON colonies(player_id);
            CREATE INDEX IF NOT EXISTS idx_events_target ON events(target_colony_id);
            CREATE INDEX IF NOT EXISTS idx_events_expires ON events(expires_at);
            CREATE INDEX IF NOT EXISTS idx_mutation_tree_colony ON mutation_tree(colony_id);
            CREATE INDEX IF NOT EXISTS idx_mutation_tree_gene ON mutation_tree(gene_id);
        """)
    
    await bot.set_webhook(
        url=f"{settings.webhook_host}{settings.webhook_path}",
        secret_token=settings.webhook_secret
    )
    
    start_http_server(settings.prometheus_port)
    logger.info("Bot started with webhook")


@app.on_event("shutdown")
async def on_shutdown():
    """Gracefully shutdown the application."""
    global db_pool
    
    try:
        logger.info("Shutting down bot...")
        await bot.delete_webhook()
    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")
    
    try:
        if dp.storage is not None:
            await dp.storage.close()
    except Exception as e:
        logger.error(f"Error closing storage: {e}")
    
    try:
        if bot.session is not None:
            await bot.session.close()
    except Exception as e:
        logger.error(f"Error closing bot session: {e}")
    
    try:
        if db_pool is not None:
            await db_pool.close()
            logger.info("Database pool closed")
    except Exception as e:
        logger.error(f"Error closing database pool: {e}")
    
    try:
        await redis_client.close()
        logger.info("Redis connection closed")
    except Exception as e:
        logger.error(f"Error closing Redis connection: {e}")
    
    logger.info("Shutdown complete")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)