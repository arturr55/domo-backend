from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List
from pydantic import ConfigDict
import databases
import sqlalchemy
from sqlalchemy import select, func, or_, text
import os
import json
from datetime import datetime

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
    sqlalchemy.Column("price_period",     sqlalchemy.String(20), default="month"),   # month/day/hour
    sqlalchemy.Column("deal_type",        sqlalchemy.String(20), default="rent"),    # rent/sale
    sqlalchemy.Column("property_type",    sqlalchemy.String(30), default="apartment"),
    sqlalchemy.Column("rooms",            sqlalchemy.String(20)),                    # studio/1/2/3/4/5plus
    sqlalchemy.Column("area",             sqlalchemy.Float),
    sqlalchemy.Column("floor",            sqlalchemy.Integer),
    sqlalchemy.Column("floors_total",     sqlalchemy.Integer),
    sqlalchemy.Column("house_type",       sqlalchemy.String(20)),
    sqlalchemy.Column("address",          sqlalchemy.String(300), nullable=False),
    sqlalchemy.Column("city",             sqlalchemy.String(100), nullable=False),
    sqlalchemy.Column("district",         sqlalchemy.String(100)),
    sqlalchemy.Column("lat",              sqlalchemy.Float),
    sqlalchemy.Column("lng",              sqlalchemy.Float),
    sqlalchemy.Column("photos",           sqlalchemy.Text, default="[]"),   # JSON array of base64
    sqlalchemy.Column("amenities",        sqlalchemy.Text, default="[]"),   # JSON array
    sqlalchemy.Column("pets_allowed",     sqlalchemy.Boolean, default=False),
    sqlalchemy.Column("children_allowed", sqlalchemy.Boolean, default=True),
    sqlalchemy.Column("condition",        sqlalchemy.String(20)),            # euro/cosmetic/none
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

connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = sqlalchemy.create_engine(DATABASE_URL, connect_args=connect_args)

with engine.begin() as conn:
    metadata.create_all(conn)
    # migrations for new columns
    for col_sql in [
        "ALTER TABLE listings ADD COLUMN metro_station VARCHAR(100)",
        "ALTER TABLE listings ADD COLUMN metro_minutes INTEGER",
    ]:
        try:
            conn.execute(text(col_sql))
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

class BatchRequest(BaseModel):
    ids: List[int]


# ── Helpers ─────────────────────────────────────────────────────────────────────

def row_to_dict(row) -> dict:
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    # parse JSON fields
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


# ── Startup / shutdown ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


# ── Public endpoints ─────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "app": "Домо"}


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
    amenities:        Optional[str]  = None,   # comma-separated
    condition:        Optional[str]  = None,
    pets_allowed:     Optional[bool] = None,
    children_allowed: Optional[bool] = None,
    search:           Optional[str]  = None,
    page:             int = 1,
    limit:            int = 20,
):
    q = listings_table.select().where(listings_table.c.approved == True)

    if city:
        q = q.where(listings_table.c.city.ilike(f"%{city}%"))
    if deal_type:
        q = q.where(listings_table.c.deal_type == deal_type)
    if property_type:
        q = q.where(listings_table.c.property_type == property_type)
    if rooms:
        q = q.where(listings_table.c.rooms == rooms)
    if price_min is not None:
        q = q.where(listings_table.c.price >= price_min)
    if price_max is not None:
        q = q.where(listings_table.c.price <= price_max)
    if price_period:
        q = q.where(listings_table.c.price_period == price_period)
    if area_min is not None:
        q = q.where(listings_table.c.area >= area_min)
    if area_max is not None:
        q = q.where(listings_table.c.area <= area_max)
    if floor_min is not None:
        q = q.where(listings_table.c.floor >= floor_min)
    if floor_max is not None:
        q = q.where(listings_table.c.floor <= floor_max)
    if house_type:
        q = q.where(listings_table.c.house_type == house_type)
    if condition:
        q = q.where(listings_table.c.condition == condition)
    if pets_allowed is not None:
        q = q.where(listings_table.c.pets_allowed == pets_allowed)
    if children_allowed is not None:
        q = q.where(listings_table.c.children_allowed == children_allowed)
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

    # filter by amenities (in-memory, JSON field)
    if amenities:
        required = [a.strip() for a in amenities.split(",") if a.strip()]
        if required:
            filtered = []
            for item in result:
                item_amenities = item.get("amenities") or []
                if all(a in item_amenities for a in required):
                    filtered.append(item)
            result = filtered

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
        approved=True,
        created_at=datetime.utcnow(),
    ))
    return {"id": listing_id, "message": "Объявление опубликовано"}


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
    return {
        "moderation_enabled": False,
        "max_photos": 10,
    }


# ── Admin endpoints ──────────────────────────────────────────────────────────────

@app.get("/admin/listings")
async def admin_listings(token: str, approved: Optional[bool] = None):
    check_admin(token)
    q = listings_table.select().order_by(listings_table.c.created_at.desc())
    if approved is not None:
        q = q.where(listings_table.c.approved == approved)
    rows = await database.fetch_all(q)
    return [row_to_dict(r) for r in rows]


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
    return {"total": total, "approved": approved, "pending": pending}
