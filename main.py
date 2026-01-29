import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Union
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
    from collections import Counter
    slots = [g.slot for g in genes]
    counts = Counter(slots)
    bonus = 1.0
    for count in counts.values():
        if count >= 3:
            bonus *= 1.5
    return bonus


def select_random_gene(slot: str) -> Gene:
    genes = gene_pool[slot]
    weights = {
        GeneRarity.COMMON: 0.70,
        GeneRarity.RARE: 0.20,
        GeneRarity.EPIC: 0.07,
        GeneRarity.LEGENDARY: 0.029,
        GeneRarity.MYTHIC: 0.001,
    }
    weighted_genes = [(g, weights[g.rarity]) for g in genes]
    total_weight = sum(w for _, w in weighted_genes)
    r = random.random() * total_weight
    cum_weight = 0
    for gene, weight in weighted_genes:
        cum_weight += weight
        if r <= cum_weight:
            return gene
    return genes[0]


async def get_db_pool() -> asyncpg.Pool:
    """Get or create the global database pool."""
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
        )
    return db_pool


@celery_app.task
def process_metabolism():
    asyncio.run(_process_metabolism_async())


async def _process_metabolism_async():
    """Process metabolism for all colonies."""
    pool = None
    try:
        pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
        )
        async with pool.acquire() as conn:
            async with conn.transaction():
                colonies = await conn.fetch("""
                    SELECT id, cell_count, energy, biomass, organelles, environment, pandemic_resistance
                    FROM colonies
                    WHERE last_calc_at < NOW() - INTERVAL '30 seconds'
                """)
                
                for colony in colonies:
                    try:
                        cell_count = colony["cell_count"]
                        energy = Decimal(colony["energy"])
                        organelles = json.loads(colony["organelles"] or "{}")
                        environment = colony["environment"] or "ocean"
                        
                        sun_factor = Decimal("1.0") if environment in ["surface", "shallow"] else Decimal("0.3")
                        mineral_factor = Decimal("1.0") if environment in ["deep", "volcanic"] else Decimal("0.5")
                        
                        photosynthesis = Decimal(str(organelles.get("photosynthesis", 0))) * Decimal("0.1") * sun_factor
                        chemosynthesis = Decimal(str(organelles.get("chemosynthesis", 0))) * Decimal("0.05") * mineral_factor
                        base_metabolism = Decimal(str(cell_count)) * Decimal("0.01")
                        organelle_upkeep = Decimal(str(sum(organelles.values()))) * Decimal("0.02")
                        
                        delta_e = photosynthesis + chemosynthesis - base_metabolism - organelle_upkeep
                        new_energy = max(Decimal(0), energy + delta_e)
                        
                        if new_energy < Decimal("0.1") * Decimal(str(cell_count)):
                            cell_loss = int(cell_count * 0.1)
                            new_cell_count = max(1, cell_count - cell_loss)
                            await conn.execute("""
                                UPDATE colonies 
                                SET cell_count = $1, energy = $2, last_calc_at = NOW()
                                WHERE id = $3
                            """, new_cell_count, new_energy, colony["id"])
                        else:
                            new_cell_count = cell_count
                            await conn.execute("""
                                UPDATE colonies 
                                SET energy = $1, last_calc_at = NOW()
                                WHERE id = $2
                            """, new_energy, colony["id"])
                        
                        phase = get_phase_by_cell_count(new_cell_count)
                        await conn.execute("""
                            UPDATE players SET current_phase = $1 WHERE id = (
                                SELECT player_id FROM colonies WHERE id = $2
                            )
                        """, phase.value, colony["id"])
                    except Exception as e:
                        logger.error(f"Error processing colony {colony['id']}: {e}")
    except Exception as e:
        logger.error(f"Error in metabolism processing: {e}")
    finally:
        if pool is not None:
            await pool.close()


@celery_app.task
def trigger_global_event():
    asyncio.run(_trigger_global_event_async())


async def _trigger_global_event_async():
    """Trigger a global event affecting random colonies."""
    pool = None
    try:
        pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=settings.db_pool_min,
            max_size=settings.db_pool_max,
        )
        async with pool.acquire() as conn:
            event_type = random.choice([EventType.VIRUS, EventType.ICE_AGE, EventType.RADIATION])
            severity = random.random()
            
            await conn.execute("""
                INSERT INTO events (type, target_colony_id, params, expires_at)
                SELECT $1, id, $2, NOW() + INTERVAL '24 hours'
                FROM colonies
                WHERE random() < $3
            """, event_type.value, json.dumps({"severity": severity}), 0.3)
            
            if event_type == EventType.VIRUS:
                await conn.execute("""
                    UPDATE colonies 
                    SET cell_count = GREATEST(1, cell_count * (1 - $1 * (1 - pandemic_resistance)))
                    WHERE id IN (SELECT target_colony_id FROM events WHERE type = $2)
                """, severity, event_type.value)
            elif event_type == EventType.RADIATION:
                await conn.execute("""
                    UPDATE colonies 
                    SET mutation_tree = mutation_tree || jsonb_build_object('radiation_mutations', 
                        (mutation_tree->>'radiation_mutations' OR '0')::int + 1)
                    WHERE id IN (SELECT target_colony_id FROM events WHERE type = $2)
                """, event_type.value)
            logger.info(f"Triggered global event: {event_type.value} with severity {severity}")
    except Exception as e:
        logger.error(f"Error triggering global event: {e}")
    finally:
        if pool is not None:
            await pool.close()


@celery_app.task
def backup_database():
    logger.info("Starting database backup")


async def check_rate_limit(telegram_id: int) -> bool:
    """Check rate limit for a user using atomic Redis operation."""
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
        logger.error(f"Rate limit check error: {e}")
        return True  # Fail open on Redis errors


async def get_or_create_player(telegram_id: int, username: Optional[str] = None) -> Dict:
    """Get or create a player by Telegram ID."""
    cache_key = f"player:{telegram_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            player = await conn.fetchrow("""
                SELECT * FROM players WHERE telegram_id = $1
            """, telegram_id)
            
            if not player:
                async with conn.transaction():
                    player_id = await conn.fetchval("""
                        INSERT INTO players (telegram_id, username, current_phase)
                        VALUES ($1, $2, $3)
                        RETURNING id
                    """, telegram_id, username, EvolutionPhase.INIT.value)
                    
                    await conn.execute("""
                        INSERT INTO colonies (player_id, cell_count, energy, biomass, 
                                            mutation_tree, organelles, environment, pandemic_resistance)
                        VALUES ($1, 1, 100.0, 1.0, '{}', '{}', 'ocean', 0.1)
                    """, player_id)
                    
                    player = await conn.fetchrow("""
                        SELECT * FROM players WHERE id = $1
                    """, player_id)
            
            result = dict(player)
            await redis_client.setex(cache_key, settings.redis_cache_ttl, json.dumps(result, default=str))
            return result
    except Exception as e:
        logger.error(f"Error in get_or_create_player: {e}")
        raise


async def check_player_exists(telegram_id: int) -> Optional[Dict]:
    """Check if a player exists without creating one."""
    cache_key = f"player:{telegram_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            player = await conn.fetchrow("""
                SELECT * FROM players WHERE telegram_id = $1
            """, telegram_id)
            
            if player:
                result = dict(player)
                await redis_client.setex(cache_key, settings.redis_cache_ttl, json.dumps(result, default=str))
                return result
            return None
    except Exception as e:
        logger.error(f"Error in check_player_exists: {e}")
        return None


async def get_colony_stats(player_id: int) -> ColonyStats:
    """Get colony statistics for a player."""
    cache_key = f"colony:{player_id}"
    cached = await redis_client.get(cache_key)
    if cached:
        data = json.loads(cached)
        # Check if phase is already EvolutionPhase or string
        phase = data["phase"]
        if isinstance(phase, str):
            phase = EvolutionPhase(phase)
        
        return ColonyStats(
            cell_count=data["cell_count"],
            energy=Decimal(data["energy"]),
            biomass=data["biomass"],
            phase=phase,
            pandemic_resistance=data["pandemic_resistance"],
            organelles=data["organelles"],
            mutations=[Gene(
                id=g["id"],
                name=g["name"],
                rarity=GeneRarity(g["rarity"]) if isinstance(g["rarity"], str) else g["rarity"],
                slot=g["slot"],
                bonuses=g["bonuses"]
            ) for g in data["mutations"]]
        )
    
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT c.*, p.current_phase as phase
                FROM colonies c
                JOIN players p ON c.player_id = p.id
                WHERE p.id = $1
            """, player_id)
            
            if not row:
                raise ValueError("Colony not found")
            
            mutations = await conn.fetch("""
                SELECT * FROM mutation_tree WHERE colony_id = $1
            """, row["id"])
            
            # Optimize N+1 problem with dict comprehension
            gene_pool_map = {
                gene.id: gene
                for slot_genes in gene_pool.values()
                for gene in slot_genes
            }
            
            genes = [
                gene_pool_map[m["gene_id"]]
                for m in mutations
                if m["gene_id"] in gene_pool_map
            ]
            
            stats = ColonyStats(
                cell_count=row["cell_count"],
                energy=Decimal(row["energy"]),
                biomass=float(row["biomass"]),
                phase=EvolutionPhase(row["phase"]),
                pandemic_resistance=float(row["pandemic_resistance"]),
                organelles=json.loads(row["organelles"] or "{}"),
                mutations=genes
            )
            
            await redis_client.setex(cache_key, settings.redis_cache_ttl, json.dumps({
                "cell_count": stats.cell_count,
                "energy": str(stats.energy),
                "biomass": stats.biomass,
                "phase": stats.phase.value,
                "pandemic_resistance": stats.pandemic_resistance,
                "organelles": stats.organelles,
                "mutations": [{"id": g.id, "name": g.name, "rarity": g.rarity.value, "slot": g.slot, "bonuses": g.bonuses} for g in genes]
            }))
            
            return stats
    except Exception as e:
        logger.error(f"Error in get_colony_stats: {e}")
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
            evolution_text += f"""
<b>Следующий этап:</b> {next_phase.value}
<b>Требуется:</b> {next_threshold:,} клеток
<b>Прогресс:</b> {progress:.1f}%

{"▓" * int(progress / 5)}{"░" * (20 - int(progress / 5))}
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
        
        available_slots = ["offensive", "defensive", "utility"]
        current_slots = {g.slot for g in stats.mutations}
        if len(current_slots) >= 3:
            await callback.message.edit_text("❌ У вас уже максимум мутаций! Удалите старую для новой.")
            return
        
        selected_slot = random.choice(available_slots)
        new_gene = select_random_gene(selected_slot)
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            colony_id = await conn.fetchval("SELECT id FROM colonies WHERE player_id = $1", player["id"])
            await conn.execute("""
                INSERT INTO mutation_tree (colony_id, gene_id, slot, rarity, bonuses)
                VALUES ($1, $2, $3, $4, $5)
            """, colony_id, new_gene.id, selected_slot, new_gene.rarity.value, json.dumps(new_gene.bonuses))
        
        await redis_client.delete(f"player:{player['id']}")
        await redis_client.delete(f"colony:{player['id']}")
        
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
        
        metabolism_text = f"""
⚡ <b>Метаболизм колонии</b>

<b>Текущая энергия:</b> {stats.energy:.2f}
<b>Потребление:</b> {stats.cell_count * 0.01:.2f}/сек
<b>Генерация:</b> {(stats.organelles.get('photosynthesis', 0) * 0.1 + stats.organelles.get('chemosynthesis', 0) * 0.05):.2f}/сек

<b>Органеллы:</b>
• Фотосинтез: {stats.organelles.get('photosynthesis', 0)}
• Хемосинтез: {stats.organelles.get('chemosynthesis', 0)}
• Митохондрии: {stats.organelles.get('mitochondria', 0)}

{"⚠️ <b>Низкая энергия!</b>" if stats.energy < stats.cell_count * 0.1 else "✅ Энергия стабильна"}
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
            colony = await conn.fetchrow("""
                SELECT c.* FROM colonies c
                JOIN players p ON c.player_id = p.id
                WHERE p.id = $1
            """, player["id"])
            
            organelles = json.loads(colony["organelles"] or "{}")
            current_count = organelles.get(organelle_type, 0)
            
            cost = 50 * (current_count + 1)
            if Decimal(colony["energy"]) < Decimal(str(cost)):
                await callback.message.edit_text("❌ Недостаточно энергии!")
                return
            
            organelles[organelle_type] = current_count + 1
            
            await conn.execute("""
                UPDATE colonies 
                SET organelles = $1, energy = energy - $2, last_calc_at = NOW()
                WHERE id = $3
            """, json.dumps(organelles), cost, colony["id"])
        
        await redis_client.delete(f"player:{player['id']}")
        await redis_client.delete(f"colony:{player['id']}")
        
        await callback.message.edit_text(f"✅ Добавлено: {organelle_type} (+1)")
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
        stats = await get_colony_stats(player["id"])
        
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
    await callback.answer()
    await callback.message.edit_text("Введите Telegram ID игрока для симбиоза:")
    await state.set_state(GameStates.symbiosis_request)


@router.message(GameStates.symbiosis_request)
async def process_symbiosis_request(message: types.Message, state: FSMContext):
    """Process symbiosis request."""
    try:
        try:
            target_id = int(message.text)
        except ValueError:
            await message.answer("❌ Неверный формат ID!")
            return
        
        # Validate player ID
        if target_id <= 0:
            await message.answer("❌ Неверный ID игрока!")
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
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            player_colony = await conn.fetchrow("SELECT cell_count FROM colonies WHERE player_id = $1", player["id"])
            target_colony = await conn.fetchrow("SELECT cell_count FROM colonies WHERE player_id = $1", target["id"])
            
            if player_colony["cell_count"] > target_colony["cell_count"]:
                contract_type = SymbiosisType.ENDOSYMBIOSIS
            else:
                contract_type = SymbiosisType.CONSORTIUM
            
            await conn.execute("""
                INSERT INTO symbiosis_contracts (host_id, symbiont_id, contract_type, resource_exchange_rate)
                VALUES ($1, $2, $3, 0.1)
            """, player["id"], target["id"], contract_type.value)
        
        try:
            await bot.send_message(
                target_id,
                f"🤝 Игрок {message.from_user.username} предлагает симбиоз ({contract_type.value})!\n\nКолония получит +10% к росту."
            )
        except Exception as e:
            logger.warning(f"Could not send symbiosis message to {target_id}: {e}")
        
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
        
        current_environment = colony["environment"] if colony else "ocean"
        
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
    await callback.answer()
    
    new_env = callback.data.replace("move_", "")
    player = await get_or_create_player(callback.from_user.id)
    
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        colony_id = await conn.fetchval("SELECT id FROM colonies WHERE player_id = $1", player["id"])
        await conn.execute("""
            UPDATE colonies SET environment = $1 WHERE id = $2
        """, new_env, colony_id)
    
    await redis_client.delete(f"colony:{player['id']}")
    
    environments = {
        "ocean": "Океан",
        "surface": "Поверхность",
        "deep": "Глубины",
        "volcanic": "Гидротермальные источники",
        "ice": "Ледяной покров",
    }
    
    await callback.message.edit_text(f"✅ Колония перемещена в {environments[new_env]}!")


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
    await callback.answer()
    
    player = await get_or_create_player(callback.from_user.id)
    stats = await get_colony_stats(player["id"])
    
    buttons = []
    for i, gene in enumerate(stats.mutations, 1):
        buttons.append(InlineKeyboardButton(
            text=f"{i}. {gene.name}",
            callback_data=f"remove_gene_{gene.id}"
        ))
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons[i:i + 1] for i in range(0, len(buttons), 1)])
    await callback.message.edit_text("Выберите мутацию для удаления:", reply_markup=keyboard)


@router.callback_query(F.data.startswith("remove_gene_"))
async def remove_gene(callback: CallbackQuery):
    await callback.answer()
    
    gene_id = callback.data.replace("remove_gene_", "")
    player = await get_or_create_player(callback.from_user.id)
    
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        colony_id = await conn.fetchval("SELECT id FROM colonies WHERE player_id = $1", player["id"])
        await conn.execute("""
            DELETE FROM mutation_tree WHERE colony_id = $1 AND gene_id = $2
        """, colony_id, gene_id)
    
    await redis_client.delete(f"player:{player['id']}")
    await redis_client.delete(f"colony:{player['id']}")
    
    await callback.message.edit_text("🗑️ Мутация удалена!")


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
                environment VARCHAR(50) DEFAULT 'ocean',
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
                rarity VARCHAR(50) NOT NULL,
                bonuses JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
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