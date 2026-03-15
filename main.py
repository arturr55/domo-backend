from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from pydantic import ConfigDict
import databases
import sqlalchemy
from sqlalchemy import select, func, or_, text
import os
import json
import asyncio
from datetime import datetime, timedelta

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./domo.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "changeme")

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
)

settings_table = sqlalchemy.Table(
    "settings", metadata,
    sqlalchemy.Column("key",   sqlalchemy.String(100), primary_key=True),
    sqlalchemy.Column("value", sqlalchemy.Text),
)

connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = sqlalchemy.create_engine(DATABASE_URL, connect_args=connect_args)

with engine.begin() as conn:
    metadata.create_all(conn)
    for col_sql in [
        "ALTER TABLE listings ADD COLUMN metro_station VARCHAR(100)",
        "ALTER TABLE listings ADD COLUMN metro_minutes INTEGER",
    ]:
        try:
            conn.execute(text(col_sql))
        except Exception:
            pass
    # Default settings
    for key, val in [("moderation_enabled", "false"), ("listing_ttl_days", "0")]:
        try:
            conn.execute(text(
                "INSERT INTO settings (key, value) VALUES (:k, :v)"
            ), {"k": key, "v": val})
        except Exception:
            pass

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
    metro_station: Optional[str] = None
    metro_minutes: Optional[int] = None
    amenities: Optional[List[str]] = None

class AdminSettings(BaseModel):
    moderation_enabled: bool
    listing_ttl_days: int

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

async def get_setting(key: str) -> str:
    row = await database.fetch_one(
        settings_table.select().where(settings_table.c.key == key)
    )
    return row["value"] if row else ""

async def set_setting(key: str, value: str):
    existing = await database.fetch_one(
        settings_table.select().where(settings_table.c.key == key)
    )
    if existing:
        await database.execute(
            settings_table.update().where(settings_table.c.key == key).values(value=value)
        )
    else:
        await database.execute(settings_table.insert().values(key=key, value=value))

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

@app.on_event("startup")
async def startup():
    await database.connect()
    asyncio.create_task(cleanup_old_listings())

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


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

    q = q.order_by(listings_table.c.is_hot.desc(), listings_table.c.created_at.desc())
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
async def create_listing(data: ListingCreate):
    if not data.title.strip():
        raise HTTPException(status_code=400, detail="Заголовок обязателен")
    if not data.address.strip():
        raise HTTPException(status_code=400, detail="Адрес обязателен")
    if not data.city.strip():
        raise HTTPException(status_code=400, detail="Город обязателен")

    moderation = await get_setting("moderation_enabled")
    auto_approve = moderation != "true"

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
        is_hot=False,
        is_verified=False,
        views=0,
        approved=auto_approve,
        created_at=datetime.utcnow(),
    ))
    msg = "Объявление опубликовано" if auto_approve else "Объявление отправлено на модерацию"
    return {"id": listing_id, "message": msg, "approved": auto_approve}


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


@app.get("/app-settings")
async def app_settings():
    moderation = await get_setting("moderation_enabled")
    return {
        "moderation_enabled": moderation == "true",
        "max_photos": 10,
    }


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
    moderation = await get_setting("moderation_enabled")
    ttl        = await get_setting("listing_ttl_days")
    return {
        "moderation_enabled": moderation == "true",
        "listing_ttl_days": int(ttl) if ttl and ttl.lstrip('-').isdigit() else 0,
    }


@app.patch("/admin/settings")
async def update_admin_settings(token: str, data: AdminSettings):
    check_admin(token)
    await set_setting("moderation_enabled", "true" if data.moderation_enabled else "false")
    await set_setting("listing_ttl_days", str(max(0, data.listing_ttl_days)))
    return {"message": "Настройки сохранены"}
