from fastapi import FastAPI, HTTPException, Request, Header
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
from pydantic import ConfigDict
import databases
import sqlalchemy
from sqlalchemy import select, func, or_, text
import os
import json
import asyncio
import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./domo.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

ADMIN_TOKEN      = os.getenv("ADMIN_TOKEN", "changeme")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
GOOGLE_CLIENT_ID   = os.getenv("GOOGLE_CLIENT_ID", "")

database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

listings_table = sqlalchemy.Table(
    "listings", metadata,
    sqlalchemy.Column("id",               sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("title",            sqlalchemy.String(300), nullable=False),
    sqlalchemy.Column("description",      sqlalchemy.Text),
    sqlalchemy.Column("price",            sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("price_period",     sqlalchemy.String(20), default="month"),
    sqlalchemy.Column("deal_type",        sqlalchemy.String(20), default="rent"),
    sqlalchemy.Column("property_type",    sqlalchemy.String(30), default="apartment"),
    sqlalchemy.Column("rooms",            sqlalchemy.String(20)),
    sqlalchemy.Column("area",             sqlalchemy.Float),
    sqlalchemy.Column("floor",            sqlalchemy.Integer),
    sqlalchemy.Column("floors_total",     sqlalchemy.Integer),
    sqlalchemy.Column("house_type",       sqlalchemy.String(20)),
    sqlalchemy.Column("address",          sqlalchemy.String(300), nullable=False),
    sqlalchemy.Column("city",             sqlalchemy.String(100), nullable=False),
    sqlalchemy.Column("district",         sqlalchemy.String(100)),
    sqlalchemy.Column("lat",              sqlalchemy.Float),
    sqlalchemy.Column("lng",              sqlalchemy.Float),
    sqlalchemy.Column("photos",           sqlalchemy.Text, default="[]"),
    sqlalchemy.Column("amenities",        sqlalchemy.Text, default="[]"),
    sqlalchemy.Column("pets_allowed",     sqlalchemy.Boolean, default=False),
    sqlalchemy.Column("children_allowed", sqlalchemy.Boolean, default=True),
    sqlalchemy.Column("condition",        sqlalchemy.String(20)),
    sqlalchemy.Column("contact_name",     sqlalchemy.String(100)),
    sqlalchemy.Column("phone",            sqlalchemy.String(50)),
    sqlalchemy.Column("telegram",         sqlalchemy.String(100)),
    sqlalchemy.Column("is_urgent",        sqlalchemy.Boolean, default=False),
    sqlalchemy.Column("is_hot",           sqlalchemy.Boolean, default=False),
    sqlalchemy.Column("is_verified",      sqlalchemy.Boolean, default=False),
    sqlalchemy.Column("views",            sqlalchemy.Integer, default=0),
    sqlalchemy.Column("approved",         sqlalchemy.Boolean, default=True),
    sqlalchemy.Column("created_at",       sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("metro_station",    sqlalchemy.String(100)),
    sqlalchemy.Column("metro_minutes",    sqlalchemy.Integer),
    sqlalchemy.Column("payment_status",   sqlalchemy.String(20), default="free"),
)

settings_table = sqlalchemy.Table(
    "settings", metadata,
    sqlalchemy.Column("key",   sqlalchemy.String(100), primary_key=True),
    sqlalchemy.Column("value", sqlalchemy.Text),
)

payments_table = sqlalchemy.Table(
    "payments", metadata,
    sqlalchemy.Column("id",                   sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("listing_id",           sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("payme_transaction_id", sqlalchemy.String(100)),
    sqlalchemy.Column("amount",               sqlalchemy.BigInteger, nullable=False),
    sqlalchemy.Column("state",                sqlalchemy.Integer, default=1),
    sqlalchemy.Column("create_time",          sqlalchemy.BigInteger, default=0),
    sqlalchemy.Column("perform_time",         sqlalchemy.BigInteger, default=0),
    sqlalchemy.Column("cancel_time",          sqlalchemy.BigInteger, default=0),
    sqlalchemy.Column("reason",               sqlalchemy.Integer),
)

users_table = sqlalchemy.Table(
    "users", metadata,
    sqlalchemy.Column("id",          sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("provider",    sqlalchemy.String(20), nullable=False),   # 'telegram' | 'google'
    sqlalchemy.Column("provider_id", sqlalchemy.String(100), nullable=False),  # telegram_id or google sub
    sqlalchemy.Column("name",        sqlalchemy.String(200)),
    sqlalchemy.Column("username",    sqlalchemy.String(100)),                   # telegram @username
    sqlalchemy.Column("avatar_url",  sqlalchemy.Text),
    sqlalchemy.Column("created_at",  sqlalchemy.DateTime, default=datetime.utcnow),
)

sessions_table = sqlalchemy.Table(
    "sessions", metadata,
    sqlalchemy.Column("token",      sqlalchemy.String(64), primary_key=True),
    sqlalchemy.Column("user_id",    sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("created_at", sqlalchemy.DateTime, default=datetime.utcnow),
    sqlalchemy.Column("expires_at", sqlalchemy.DateTime, nullable=False),
)

connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = sqlalchemy.create_engine(DATABASE_URL, connect_args=connect_args)

# Run each migration in its own transaction so a failed ALTER doesn't abort the rest
def _run_migration(sql: str):
    try:
        with engine.begin() as _conn:
            _conn.execute(text(sql))
    except Exception:
        pass

with engine.begin() as conn:
    metadata.create_all(conn)

# DDL migrations — each in its own transaction
_is_pg = "postgresql" in DATABASE_URL
for _col_sql in [
    # Use IF NOT EXISTS (PostgreSQL 9.6+) to avoid "column already exists" errors
    "ALTER TABLE listings ADD COLUMN IF NOT EXISTS metro_station VARCHAR(100)",
    "ALTER TABLE listings ADD COLUMN IF NOT EXISTS metro_minutes INTEGER",
    "ALTER TABLE listings ADD COLUMN IF NOT EXISTS payment_status VARCHAR(20) DEFAULT 'free'",
    "ALTER TABLE listings ADD COLUMN IF NOT EXISTS user_id INTEGER",
    # SQLite fallback variants (no IF NOT EXISTS support for ALTER TABLE)
    *([] if _is_pg else [
        "ALTER TABLE listings ADD COLUMN metro_station VARCHAR(100)",
        "ALTER TABLE listings ADD COLUMN metro_minutes INTEGER",
        "ALTER TABLE listings ADD COLUMN payment_status VARCHAR(20)",
        "ALTER TABLE listings ADD COLUMN user_id INTEGER",
    ]),
]:
    _run_migration(_col_sql)

# Default settings — each in its own transaction
_defaults = [
    ("moderation_enabled", "false"),
    ("listing_ttl_days",   "0"),
    ("paid_mode",          "false"),
    ("payme_kassa_id",     ""),
    ("payme_secret_key",   ""),
    ("listing_price_uzs",  "0"),
]
for _key, _val in _defaults:
    _run_migration(
        f"INSERT INTO settings (key, value) VALUES ('{_key}', '{_val}') "
        "ON CONFLICT (key) DO NOTHING"
    )

app = FastAPI(title="Домо API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Pydantic ────────────────────────────────────────────────────────────────────

class ListingCreate(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    title: str
    description: Optional[str] = None
    price: int
    price_period: str = "month"
    deal_type: str = "rent"
    property_type: str = "apartment"
    rooms: Optional[str] = None
    area: Optional[float] = None
    floor: Optional[int] = None
    floors_total: Optional[int] = None
    house_type: Optional[str] = None
    address: str
    city: str
    district: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    photos: List[str] = []
    amenities: List[str] = []
    pets_allowed: bool = False
    children_allowed: bool = True
    condition: Optional[str] = None
    contact_name: Optional[str] = None
    phone: Optional[str] = None
    telegram: Optional[str] = None
    is_urgent: bool = False
    metro_station: Optional[str] = None
    metro_minutes: Optional[int] = None

class ListingUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    price: Optional[int] = None
    price_period: Optional[str] = None
    deal_type: Optional[str] = None
    property_type: Optional[str] = None
    rooms: Optional[str] = None
    area: Optional[float] = None
    floor: Optional[int] = None
    floors_total: Optional[int] = None
    house_type: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    condition: Optional[str] = None
    contact_name: Optional[str] = None
    phone: Optional[str] = None
    telegram: Optional[str] = None
    is_urgent: Optional[bool] = None
    pets_allowed: Optional[bool] = None
    children_allowed: Optional[bool] = None
    metro_station: Optional[str] = None
    metro_minutes: Optional[int] = None
    amenities: Optional[List[str]] = None
    photos: Optional[List[str]] = None
    lat: Optional[float] = None
    lng: Optional[float] = None

class AdminSettings(BaseModel):
    moderation_enabled: bool
    listing_ttl_days: int
    paid_mode: bool = False
    payme_kassa_id: str = ""
    payme_secret_key: str = ""
    listing_price_uzs: int = 0

class BatchRequest(BaseModel):
    ids: List[int]


# ── Helpers ─────────────────────────────────────────────────────────────────────

def row_to_dict(row) -> dict:
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    for field in ("photos", "amenities"):
        if isinstance(d.get(field), str):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                d[field] = []
    return d

def check_admin(token: str):
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Нет доступа")


# ── Auth helpers ─────────────────────────────────────────────────────────────────

def _make_token() -> str:
    return secrets.token_hex(32)

async def _upsert_user(provider: str, provider_id: str, name: str,
                       username: str = None, avatar_url: str = None) -> int:
    row = await database.fetch_one(
        users_table.select().where(
            (users_table.c.provider == provider) &
            (users_table.c.provider_id == str(provider_id))
        )
    )
    if row:
        await database.execute(
            users_table.update()
            .where(users_table.c.id == row["id"])
            .values(name=name, username=username, avatar_url=avatar_url)
        )
        return row["id"]
    return await database.execute(
        users_table.insert().values(
            provider=provider, provider_id=str(provider_id),
            name=name, username=username, avatar_url=avatar_url,
            created_at=datetime.utcnow()
        )
    )

async def _create_session(user_id: int) -> str:
    token = _make_token()
    expires = datetime.utcnow() + timedelta(days=365)
    await database.execute(
        sessions_table.insert().values(
            token=token, user_id=user_id,
            created_at=datetime.utcnow(), expires_at=expires
        )
    )
    return token

async def _get_user_by_token(token: str) -> dict | None:
    if not token:
        return None
    session = await database.fetch_one(
        sessions_table.select().where(
            (sessions_table.c.token == token) &
            (sessions_table.c.expires_at > datetime.utcnow())
        )
    )
    if not session:
        return None
    user = await database.fetch_one(
        users_table.select().where(users_table.c.id == session["user_id"])
    )
    return dict(user) if user else None

def _verify_telegram_hash(data: dict, bot_token: str) -> bool:
    """Проверяет подпись от Telegram Login Widget."""
    check_hash = data.pop("hash", "")
    data_check = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret = hashlib.sha256(bot_token.encode()).digest()
    expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, check_hash)


# ── Auth Pydantic models ──────────────────────────────────────────────────────────

class TelegramAuthData(BaseModel):
    id: int
    first_name: str
    last_name: Optional[str] = None
    username: Optional[str] = None
    photo_url: Optional[str] = None
    auth_date: int
    hash: str

class GoogleAuthData(BaseModel):
    id_token: str

async def get_setting(key: str, default: str = "") -> str:
    try:
        row = await database.fetch_one(
            settings_table.select().where(settings_table.c.key == key)
        )
        return row["value"] if row else default
    except Exception:
        return default

async def set_setting(key: str, value: str):
    try:
        existing = await database.fetch_one(
            settings_table.select().where(settings_table.c.key == key)
        )
        if existing:
            await database.execute(
                settings_table.update().where(settings_table.c.key == key).values(value=value)
            )
        else:
            await database.execute(settings_table.insert().values(key=key, value=value))
    except Exception:
        pass

# ── Payme helpers ───────────────────────────────────────────────────────────────

async def verify_payme_auth(request: Request) -> bool:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(auth[6:]).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception:
        return False
    if username != "Paycom":
        return False
    secret = await get_setting("payme_secret_key")
    return bool(secret) and password == secret

async def make_payme_url(listing_id: int, amount_tiyn: int) -> str:
    kassa_id = await get_setting("payme_kassa_id")
    params = f"m={kassa_id};ac.listing_id={listing_id};a={amount_tiyn}"
    encoded = base64.b64encode(params.encode("utf-8")).decode("utf-8")
    return f"https://checkout.paycom.uz/{encoded}"

def payme_ok(req_id, result: dict):
    return JSONResponse({"id": req_id, "result": result})

def payme_err(req_id, code: int, message: str, data: str = None):
    err = {"code": code, "message": {"ru": message, "uz": message, "en": message}}
    if data:
        err["data"] = data
    return JSONResponse({"id": req_id, "error": err})


async def cleanup_old_listings():
    while True:
        try:
            ttl = await get_setting("listing_ttl_days")
            days = int(ttl) if ttl and ttl.isdigit() else 0
            if days > 0:
                cutoff = datetime.utcnow() - timedelta(days=days)
                await database.execute(
                    listings_table.delete().where(listings_table.c.created_at < cutoff)
                )
        except Exception:
            pass
        await asyncio.sleep(3600)  # каждый час


# ── Startup / shutdown ──────────────────────────────────────────────────────────

async def _run_auth_bot():
    """Запускает Telegram бота авторизации в фоне."""
    if not TELEGRAM_BOT_TOKEN:
        return
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp  = Dispatcher()

    @dp.message(CommandStart())
    async def cmd_start(message: types.Message):
        args = message.text.split()
        user = message.from_user
        if len(args) < 2:
            await message.answer(
                "👋 Привет! Я бот приложения <b>Домо</b>.\n\n"
                "Открой приложение и нажми «Войти через Telegram» — я помогу тебе войти.",
                parse_mode="HTML"
            )
            return
        code = args[1]
        user_data = {
            "id": user.id, "first_name": user.first_name or "",
            "last_name": user.last_name, "username": user.username,
            "photo_url": None, "auth_date": 0, "hash": "",
        }
        try:
            result = await telegram_confirm(code, TelegramAuthData(**user_data))
            await message.answer(
                f"✅ <b>Добро пожаловать, {user.first_name}!</b>\n\n"
                "Вы успешно вошли в приложение <b>Домо</b>.\n"
                "Вернитесь в приложение — оно уже авторизовано 🎉",
                parse_mode="HTML"
            )
        except HTTPException:
            await message.answer("❌ Код авторизации не найден или истёк.\n\nПопробуйте ещё раз в приложении.")
        except Exception as e:
            await message.answer("⚠️ Ошибка подключения. Попробуйте позже.")

    await dp.start_polling(bot)

@app.on_event("startup")
async def startup():
    await database.connect()
    asyncio.create_task(cleanup_old_listings())
    asyncio.create_task(_run_auth_bot())

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


# ── Auth endpoints ───────────────────────────────────────────────────────────────

# Хранилище pending Telegram auth кодов: {code: {expires_at, confirmed, user_id, token}}
_tg_pending: dict = {}

DOMO_BOT_USERNAME = os.getenv("DOMO_BOT_USERNAME", "domo_auth_bot")

@app.post("/auth/telegram/init")
async def telegram_init():
    """Flutter вызывает это перед открытием бота — получает код."""
    code = secrets.token_hex(8)
    _tg_pending[code] = {
        "expires_at": datetime.utcnow() + timedelta(minutes=10),
        "confirmed": False,
        "user_id": None,
        "token": None,
    }
    return {"code": code, "bot_username": DOMO_BOT_USERNAME}


@app.post("/auth/telegram/confirm/{code}")
async def telegram_confirm(code: str, data: TelegramAuthData):
    """Бот вызывает это когда пользователь нажал START."""
    if code not in _tg_pending:
        raise HTTPException(status_code=404, detail="Код не найден или истёк")
    entry = _tg_pending[code]
    if entry["confirmed"] or datetime.utcnow() > entry["expires_at"]:
        raise HTTPException(status_code=400, detail="Код недействителен")

    name = data.first_name
    if data.last_name:
        name += f" {data.last_name}"
    user_id = await _upsert_user(
        provider="telegram",
        provider_id=str(data.id),
        name=name,
        username=data.username,
        avatar_url=data.photo_url,
    )
    token = await _create_session(user_id)
    entry["confirmed"] = True
    entry["user_id"]   = user_id
    entry["token"]     = token
    return {"ok": True}


@app.get("/auth/telegram/poll/{code}")
async def telegram_poll(code: str):
    """Flutter опрашивает это каждые 3 секунды."""
    entry = _tg_pending.get(code)
    if not entry:
        return {"confirmed": False}
    if entry["confirmed"]:
        user = await database.fetch_one(
            users_table.select().where(users_table.c.id == entry["user_id"])
        )
        del _tg_pending[code]
        return {"confirmed": True, "token": entry["token"], "user": dict(user)}
    if datetime.utcnow() > entry["expires_at"]:
        del _tg_pending[code]
        return {"confirmed": False, "expired": True}
    return {"confirmed": False}


@app.post("/auth/telegram")
async def auth_telegram(data: TelegramAuthData):
    d = data.model_dump()
    if TELEGRAM_BOT_TOKEN and not _verify_telegram_hash(d.copy(), TELEGRAM_BOT_TOKEN):
        raise HTTPException(status_code=401, detail="Неверная подпись Telegram")
    name = data.first_name
    if data.last_name:
        name += f" {data.last_name}"
    user_id = await _upsert_user(
        provider="telegram",
        provider_id=str(data.id),
        name=name,
        username=data.username,
        avatar_url=data.photo_url,
    )
    token = await _create_session(user_id)
    user = await database.fetch_one(users_table.select().where(users_table.c.id == user_id))
    return {"token": token, "user": dict(user)}


@app.post("/auth/google")
async def auth_google(data: GoogleAuthData):
    import urllib.request, urllib.error
    try:
        url = f"https://oauth2.googleapis.com/tokeninfo?id_token={data.id_token}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            info = json.loads(resp.read())
    except Exception:
        raise HTTPException(status_code=401, detail="Неверный Google токен")
    if GOOGLE_CLIENT_ID and info.get("aud") != GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=401, detail="Неверный client_id")
    user_id = await _upsert_user(
        provider="google",
        provider_id=info["sub"],
        name=info.get("name", ""),
        username=None,
        avatar_url=info.get("picture"),
    )
    token = await _create_session(user_id)
    user = await database.fetch_one(users_table.select().where(users_table.c.id == user_id))
    return {"token": token, "user": dict(user)}


@app.get("/auth/me")
async def auth_me(authorization: Optional[str] = Header(None)):
    token = (authorization or "").replace("Bearer ", "")
    user = await _get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Не авторизован")
    return user


@app.delete("/auth/logout")
async def auth_logout(authorization: Optional[str] = Header(None)):
    token = (authorization or "").replace("Bearer ", "")
    await database.execute(sessions_table.delete().where(sessions_table.c.token == token))
    return {"ok": True}


# ── Public endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "app": "Домо"}

@app.get("/admin")
async def admin_panel():
    return FileResponse(
        "static/admin.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )

@app.get("/listings")
async def get_listings(
    city:             Optional[str]  = None,
    deal_type:        Optional[str]  = None,
    property_type:    Optional[str]  = None,
    rooms:            Optional[str]  = None,
    price_min:        Optional[int]  = None,
    price_max:        Optional[int]  = None,
    price_period:     Optional[str]  = None,
    area_min:         Optional[float]= None,
    area_max:         Optional[float]= None,
    floor_min:        Optional[int]  = None,
    floor_max:        Optional[int]  = None,
    house_type:       Optional[str]  = None,
    amenities:        Optional[str]  = None,
    condition:        Optional[str]  = None,
    pets_allowed:     Optional[bool] = None,
    children_allowed: Optional[bool] = None,
    search:           Optional[str]  = None,
    sort:             Optional[str]  = None,
    page:             int = 1,
    limit:            int = 20,
):
    q = listings_table.select().where(listings_table.c.approved == True)

    if city:          q = q.where(listings_table.c.city.ilike(f"%{city}%"))
    if deal_type:     q = q.where(listings_table.c.deal_type == deal_type)
    if property_type: q = q.where(listings_table.c.property_type == property_type)
    if rooms:         q = q.where(listings_table.c.rooms == rooms)
    if price_min is not None: q = q.where(listings_table.c.price >= price_min)
    if price_max is not None: q = q.where(listings_table.c.price <= price_max)
    if price_period:  q = q.where(listings_table.c.price_period == price_period)
    if area_min is not None:  q = q.where(listings_table.c.area >= area_min)
    if area_max is not None:  q = q.where(listings_table.c.area <= area_max)
    if floor_min is not None: q = q.where(listings_table.c.floor >= floor_min)
    if floor_max is not None: q = q.where(listings_table.c.floor <= floor_max)
    if house_type:    q = q.where(listings_table.c.house_type == house_type)
    if condition:     q = q.where(listings_table.c.condition == condition)
    if pets_allowed is not None:     q = q.where(listings_table.c.pets_allowed == pets_allowed)
    if children_allowed is not None: q = q.where(listings_table.c.children_allowed == children_allowed)
    if search:
        term = f"%{search}%"
        q = q.where(or_(
            listings_table.c.title.ilike(term),
            listings_table.c.description.ilike(term),
            listings_table.c.address.ilike(term),
            listings_table.c.phone.ilike(term),
        ))

    if sort == 'price_asc':
        q = q.order_by(listings_table.c.is_hot.desc(), listings_table.c.price.asc())
    elif sort == 'price_desc':
        q = q.order_by(listings_table.c.is_hot.desc(), listings_table.c.price.desc())
    elif sort == 'views':
        q = q.order_by(listings_table.c.is_hot.desc(), listings_table.c.views.desc())
    else:  # newest — strictly by date, no hot priority
        q = q.order_by(listings_table.c.created_at.desc())
    q = q.offset((page - 1) * limit).limit(limit)

    rows = await database.fetch_all(q)
    result = [row_to_dict(r) for r in rows]

    if amenities:
        required = [a.strip() for a in amenities.split(",") if a.strip()]
        if required:
            result = [
                item for item in result
                if all(a in (item.get("amenities") or []) for a in required)
            ]

    return result


@app.get("/listings/{listing_id}")
async def get_listing(listing_id: int):
    row = await database.fetch_one(
        listings_table.select()
        .where(listings_table.c.id == listing_id)
        .where(listings_table.c.approved == True)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    return row_to_dict(row)


@app.post("/listings", status_code=201)
async def create_listing(data: ListingCreate, authorization: Optional[str] = Header(None)):
    if not data.title.strip():
        raise HTTPException(status_code=400, detail="Заголовок обязателен")
    if not data.address.strip():
        raise HTTPException(status_code=400, detail="Адрес обязателен")
    if not data.city.strip():
        raise HTTPException(status_code=400, detail="Город обязателен")

    token = (authorization or "").replace("Bearer ", "")
    current_user = await _get_user_by_token(token)

    moderation   = await get_setting("moderation_enabled")
    paid_mode    = await get_setting("paid_mode")
    price_uzs    = await get_setting("listing_price_uzs")

    is_paid_mode = paid_mode == "true"
    # In paid mode listing is hidden until payment confirmed; otherwise follow moderation setting
    auto_approve = not is_paid_mode and (moderation != "true")

    listing_id = await database.execute(listings_table.insert().values(
        title=data.title.strip(),
        description=data.description,
        price=data.price,
        price_period=data.price_period,
        deal_type=data.deal_type,
        property_type=data.property_type,
        rooms=data.rooms,
        area=data.area,
        floor=data.floor,
        floors_total=data.floors_total,
        house_type=data.house_type,
        address=data.address.strip(),
        city=data.city.strip(),
        district=data.district,
        lat=data.lat,
        lng=data.lng,
        photos=json.dumps(data.photos),
        amenities=json.dumps(data.amenities),
        pets_allowed=data.pets_allowed,
        children_allowed=data.children_allowed,
        condition=data.condition,
        contact_name=data.contact_name,
        phone=data.phone,
        telegram=data.telegram,
        is_urgent=data.is_urgent,
        metro_station=data.metro_station,
        metro_minutes=data.metro_minutes,
        is_hot=False,
        is_verified=False,
        views=0,
        approved=auto_approve,
        payment_status="pending" if is_paid_mode else "free",
        user_id=current_user["id"] if current_user else None,
        created_at=datetime.utcnow(),
    ))

    if is_paid_mode:
        price_int  = int(price_uzs) if price_uzs and price_uzs.isdigit() else 0
        amount_tiyn = price_int * 100  # 1 UZS = 100 tiyin
        payment_url = await make_payme_url(listing_id, amount_tiyn)
        return {
            "id":            listing_id,
            "needs_payment": True,
            "payment_url":   payment_url,
            "amount_uzs":    price_int,
            "message":       "Объявление будет опубликовано после оплаты",
        }

    msg = "Объявление опубликовано" if auto_approve else "Объявление отправлено на модерацию"
    return {"id": listing_id, "message": msg, "approved": auto_approve, "needs_payment": False}


@app.post("/listings/{listing_id}/view")
async def increment_views(listing_id: int):
    row = await database.fetch_one(
        listings_table.select().where(listings_table.c.id == listing_id)
    )
    if row:
        await database.execute(
            listings_table.update()
            .where(listings_table.c.id == listing_id)
            .values(views=(row["views"] or 0) + 1)
        )
    return {"ok": True}


@app.post("/listings/batch")
async def get_listings_batch(data: BatchRequest):
    if not data.ids:
        return []
    rows = await database.fetch_all(
        listings_table.select()
        .where(listings_table.c.id.in_(data.ids))
        .where(listings_table.c.approved == True)
    )
    return [row_to_dict(r) for r in rows]


@app.get("/my-listings")
async def my_listings(authorization: Optional[str] = Header(None)):
    token = (authorization or "").replace("Bearer ", "")
    user = await _get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Не авторизован")
    rows = await database.fetch_all(
        listings_table.select()
        .where(listings_table.c.user_id == user["id"])
        .order_by(listings_table.c.created_at.desc())
    )
    return [row_to_dict(r) for r in rows]


@app.patch("/listings/{listing_id}")
async def update_my_listing(listing_id: int, data: ListingUpdate,
                             authorization: Optional[str] = Header(None)):
    token = (authorization or "").replace("Bearer ", "")
    user = await _get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Не авторизован")
    row = await database.fetch_one(
        listings_table.select().where(listings_table.c.id == listing_id)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Объявление не найдено")
    if row["user_id"] != user["id"]:
        raise HTTPException(status_code=403, detail="Нет доступа")
    values = {k: v for k, v in data.model_dump().items() if v is not None}
    if "amenities" in values:
        values["amenities"] = json.dumps(values["amenities"])
    if "photos" in values:
        values["photos"] = json.dumps(values["photos"])
    # Булевые поля могут быть False — добавляем отдельно
    for bool_field in ("is_urgent", "pets_allowed", "children_allowed"):
        field_val = getattr(data, bool_field)
        if field_val is not None:
            values[bool_field] = field_val
    if values:
        await database.execute(
            listings_table.update()
            .where(listings_table.c.id == listing_id)
            .values(**values)
        )
    return row_to_dict(await database.fetch_one(
        listings_table.select().where(listings_table.c.id == listing_id)
    ))


@app.delete("/listings/{listing_id}")
async def delete_my_listing(listing_id: int, authorization: Optional[str] = Header(None)):
    token = (authorization or "").replace("Bearer ", "")
    user = await _get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Не авторизован")
    row = await database.fetch_one(
        listings_table.select().where(listings_table.c.id == listing_id)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Объявление не найдено")
    if row["user_id"] != user["id"]:
        raise HTTPException(status_code=403, detail="Нет доступа")
    await database.execute(listings_table.delete().where(listings_table.c.id == listing_id))
    return {"ok": True}


@app.get("/app-settings")
async def app_settings():
    moderation  = await get_setting("moderation_enabled")
    paid_mode   = await get_setting("paid_mode")
    price_uzs   = await get_setting("listing_price_uzs")
    return {
        "moderation_enabled": moderation == "true",
        "paid_mode":          paid_mode == "true",
        "listing_price_uzs":  int(price_uzs) if price_uzs and price_uzs.isdigit() else 0,
        "max_photos":         10,
    }


@app.get("/listings/{listing_id}/payment-status")
async def get_payment_status(listing_id: int):
    row = await database.fetch_one(
        listings_table.select().where(listings_table.c.id == listing_id)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    return {
        "listing_id":     listing_id,
        "payment_status": row["payment_status"] or "free",
        "approved":       bool(row["approved"]),
    }


# ── Payme merchant webhook ───────────────────────────────────────────────────────

@app.post("/payme/webhook")
async def payme_webhook(request: Request):
    if not await verify_payme_auth(request):
        body = await request.json()
        return payme_err(body.get("id"), -32504, "Ошибка авторизации")

    body    = await request.json()
    method  = body.get("method", "")
    params  = body.get("params", {})
    req_id  = body.get("id")

    if method == "CheckPerformTransaction":
        return await _payme_check_perform(req_id, params)
    elif method == "CreateTransaction":
        return await _payme_create(req_id, params)
    elif method == "PerformTransaction":
        return await _payme_perform(req_id, params)
    elif method == "CancelTransaction":
        return await _payme_cancel(req_id, params)
    elif method == "CheckTransaction":
        return await _payme_check_transaction(req_id, params)
    elif method == "GetStatement":
        return await _payme_statement(req_id, params)
    else:
        return payme_err(req_id, -32601, "Метод не найден")


async def _payme_check_perform(req_id, params):
    listing_id = params.get("account", {}).get("listing_id")
    amount     = params.get("amount", 0)
    if not listing_id:
        return payme_err(req_id, -31050, "Объявление не найдено", "listing_id")
    row = await database.fetch_one(
        listings_table.select().where(listings_table.c.id == int(listing_id))
    )
    if not row:
        return payme_err(req_id, -31050, "Объявление не найдено", str(listing_id))
    price_uzs  = await get_setting("listing_price_uzs")
    price_tiyn = int(price_uzs) * 100 if price_uzs and price_uzs.isdigit() else 0
    if price_tiyn > 0 and amount != price_tiyn:
        return payme_err(req_id, -31001, "Неверная сумма", "amount")
    return payme_ok(req_id, {"allow": True})


async def _payme_create(req_id, params):
    txn_id     = params.get("id")
    listing_id = params.get("account", {}).get("listing_id")
    amount     = params.get("amount", 0)
    t_time     = params.get("time", 0)
    if not listing_id:
        return payme_err(req_id, -31050, "Объявление не найдено", "listing_id")
    # Check if transaction already exists (idempotent)
    existing = await database.fetch_one(
        payments_table.select().where(payments_table.c.payme_transaction_id == txn_id)
    )
    if existing:
        if existing["state"] not in (1,):
            return payme_err(req_id, -31008, "Транзакция уже завершена или отменена")
        return payme_ok(req_id, {
            "create_time": existing["create_time"],
            "transaction": str(existing["id"]),
            "state":       existing["state"],
        })
    # Create new transaction
    int_id = await database.execute(payments_table.insert().values(
        listing_id=int(listing_id),
        payme_transaction_id=txn_id,
        amount=amount,
        state=1,
        create_time=t_time,
        perform_time=0,
        cancel_time=0,
    ))
    return payme_ok(req_id, {
        "create_time": t_time,
        "transaction": str(int_id),
        "state":       1,
    })


async def _payme_perform(req_id, params):
    txn_id = params.get("id")
    row = await database.fetch_one(
        payments_table.select().where(payments_table.c.payme_transaction_id == txn_id)
    )
    if not row:
        return payme_err(req_id, -31003, "Транзакция не найдена")
    if row["state"] == 2:
        return payme_ok(req_id, {
            "transaction":   str(row["id"]),
            "perform_time":  row["perform_time"],
            "state":         2,
        })
    if row["state"] != 1:
        return payme_err(req_id, -31008, "Невозможно выполнить транзакцию")
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    await database.execute(
        payments_table.update()
        .where(payments_table.c.payme_transaction_id == txn_id)
        .values(state=2, perform_time=now_ms)
    )
    # Activate listing
    await database.execute(
        listings_table.update()
        .where(listings_table.c.id == row["listing_id"])
        .values(approved=True, payment_status="paid")
    )
    return payme_ok(req_id, {
        "transaction":  str(row["id"]),
        "perform_time": now_ms,
        "state":        2,
    })


async def _payme_cancel(req_id, params):
    txn_id = params.get("id")
    reason = params.get("reason", 0)
    row = await database.fetch_one(
        payments_table.select().where(payments_table.c.payme_transaction_id == txn_id)
    )
    if not row:
        return payme_err(req_id, -31003, "Транзакция не найдена")
    now_ms    = int(datetime.utcnow().timestamp() * 1000)
    new_state = -2 if row["state"] == 2 else -1
    await database.execute(
        payments_table.update()
        .where(payments_table.c.payme_transaction_id == txn_id)
        .values(state=new_state, cancel_time=now_ms, reason=reason)
    )
    if new_state == -1:
        # Only revert to pending if cancelled before perform
        await database.execute(
            listings_table.update()
            .where(listings_table.c.id == row["listing_id"])
            .values(approved=False, payment_status="pending")
        )
    return payme_ok(req_id, {
        "transaction": str(row["id"]),
        "cancel_time": now_ms,
        "state":       new_state,
    })


async def _payme_check_transaction(req_id, params):
    txn_id = params.get("id")
    row = await database.fetch_one(
        payments_table.select().where(payments_table.c.payme_transaction_id == txn_id)
    )
    if not row:
        return payme_err(req_id, -31003, "Транзакция не найдена")
    return payme_ok(req_id, {
        "create_time":  row["create_time"],
        "perform_time": row["perform_time"] or 0,
        "cancel_time":  row["cancel_time"] or 0,
        "transaction":  str(row["id"]),
        "state":        row["state"],
        "reason":       row["reason"],
    })


async def _payme_statement(req_id, params):
    from_ms = params.get("from", 0)
    to_ms   = params.get("to", 0)
    rows = await database.fetch_all(
        payments_table.select()
        .where(payments_table.c.create_time >= from_ms)
        .where(payments_table.c.create_time <= to_ms)
    )
    txns = []
    for r in rows:
        listing_row = await database.fetch_one(
            listings_table.select().where(listings_table.c.id == r["listing_id"])
        )
        txns.append({
            "id":           r["payme_transaction_id"],
            "time":         r["create_time"],
            "amount":       r["amount"],
            "account":      {"listing_id": str(r["listing_id"])},
            "create_time":  r["create_time"],
            "perform_time": r["perform_time"] or 0,
            "cancel_time":  r["cancel_time"] or 0,
            "transaction":  str(r["id"]),
            "state":        r["state"],
            "reason":       r["reason"],
        })
    return payme_ok(req_id, {"transactions": txns})


# ── Admin endpoints ──────────────────────────────────────────────────────────────

@app.get("/admin/listings")
async def admin_listings(
    token: str,
    approved: Optional[bool] = None,
    page: int = 1,
    limit: int = 30,
    search: Optional[str] = None,
):
    check_admin(token)
    q = listings_table.select().order_by(listings_table.c.created_at.desc())
    if approved is not None:
        q = q.where(listings_table.c.approved == approved)
    if search:
        term = f"%{search}%"
        q = q.where(or_(
            listings_table.c.title.ilike(term),
            listings_table.c.city.ilike(term),
            listings_table.c.phone.ilike(term),
            listings_table.c.contact_name.ilike(term),
        ))
    total_q = select(func.count()).select_from(listings_table)
    if approved is not None:
        total_q = total_q.where(listings_table.c.approved == approved)
    if search:
        term = f"%{search}%"
        total_q = total_q.where(or_(
            listings_table.c.title.ilike(term),
            listings_table.c.city.ilike(term),
            listings_table.c.phone.ilike(term),
            listings_table.c.contact_name.ilike(term),
        ))
    total = await database.fetch_val(total_q)
    q = q.offset((page - 1) * limit).limit(limit)
    rows = await database.fetch_all(q)
    return {"items": [row_to_dict(r) for r in rows], "total": total, "page": page, "limit": limit}


@app.patch("/admin/listings/{lid}/approve")
async def approve_listing(lid: int, token: str):
    check_admin(token)
    await database.execute(
        listings_table.update().where(listings_table.c.id == lid).values(approved=True)
    )
    return {"message": "Одобрено"}


@app.patch("/admin/listings/{lid}/reject")
async def reject_listing(lid: int, token: str):
    check_admin(token)
    await database.execute(
        listings_table.update().where(listings_table.c.id == lid).values(approved=False)
    )
    return {"message": "Отклонено"}


@app.patch("/admin/listings/{lid}/hot")
async def set_hot(lid: int, token: str, value: bool = True):
    check_admin(token)
    await database.execute(
        listings_table.update().where(listings_table.c.id == lid).values(is_hot=value)
    )
    return {"message": "Обновлено"}


@app.patch("/admin/listings/{lid}/verified")
async def set_verified(lid: int, token: str, value: bool = True):
    check_admin(token)
    await database.execute(
        listings_table.update().where(listings_table.c.id == lid).values(is_verified=value)
    )
    return {"message": "Обновлено"}


@app.patch("/admin/listings/{lid}")
async def edit_listing(lid: int, token: str, data: ListingUpdate):
    check_admin(token)
    row = await database.fetch_one(
        listings_table.select().where(listings_table.c.id == lid)
    )
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    updates = {k: v for k, v in data.model_dump(exclude_unset=True).items() if v is not None}
    if "amenities" in updates:
        updates["amenities"] = json.dumps(updates["amenities"])
    if updates:
        await database.execute(
            listings_table.update().where(listings_table.c.id == lid).values(**updates)
        )
    return {"message": "Сохранено"}


@app.delete("/admin/listings/{lid}")
async def delete_listing(lid: int, token: str):
    check_admin(token)
    await database.execute(listings_table.delete().where(listings_table.c.id == lid))
    return {"message": "Удалено"}


@app.get("/admin/stats")
async def admin_stats(token: str):
    check_admin(token)
    total    = await database.fetch_val(select(func.count()).select_from(listings_table))
    approved = await database.fetch_val(select(func.count()).select_from(listings_table).where(listings_table.c.approved == True))
    pending  = await database.fetch_val(select(func.count()).select_from(listings_table).where(listings_table.c.approved == False))
    views    = await database.fetch_val(select(func.sum(listings_table.c.views)).select_from(listings_table)) or 0
    return {"total": total, "approved": approved, "pending": pending, "views": views}


@app.get("/admin/stats/cities")
async def stats_cities(token: str):
    check_admin(token)
    rows = await database.fetch_all(
        select(
            listings_table.c.city,
            func.count().label("count"),
            func.sum(listings_table.c.views).label("views"),
        )
        .select_from(listings_table)
        .where(listings_table.c.approved == True)
        .group_by(listings_table.c.city)
        .order_by(func.count().desc())
        .limit(20)
    )
    return [{"city": r["city"], "count": r["count"], "views": r["views"] or 0} for r in rows]


@app.get("/admin/settings")
async def get_admin_settings(token: str):
    check_admin(token)
    moderation  = await get_setting("moderation_enabled")
    ttl         = await get_setting("listing_ttl_days")
    paid_mode   = await get_setting("paid_mode")
    kassa_id    = await get_setting("payme_kassa_id")
    secret_key  = await get_setting("payme_secret_key")
    price_uzs   = await get_setting("listing_price_uzs")
    return {
        "moderation_enabled": moderation == "true",
        "listing_ttl_days":   int(ttl) if ttl and ttl.lstrip('-').isdigit() else 0,
        "paid_mode":          paid_mode == "true",
        "payme_kassa_id":     kassa_id or "",
        "payme_secret_key":   secret_key or "",
        "listing_price_uzs":  int(price_uzs) if price_uzs and price_uzs.isdigit() else 0,
    }


@app.patch("/admin/settings")
async def update_admin_settings(token: str, data: AdminSettings):
    check_admin(token)
    await set_setting("moderation_enabled", "true" if data.moderation_enabled else "false")
    await set_setting("listing_ttl_days",   str(max(0, data.listing_ttl_days)))
    await set_setting("paid_mode",          "true" if data.paid_mode else "false")
    await set_setting("payme_kassa_id",     data.payme_kassa_id.strip())
    await set_setting("payme_secret_key",   data.payme_secret_key.strip())
    await set_setting("listing_price_uzs",  str(max(0, data.listing_price_uzs)))
    return {"message": "Настройки сохранены"}
