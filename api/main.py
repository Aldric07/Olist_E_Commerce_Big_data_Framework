from datetime import datetime, timedelta
from typing import Optional
import os
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel
import psycopg2, psycopg2.extras

SECRET_KEY = os.environ.get("JWT_SECRET", "olist-super-secret-key-2025")
ALGORITHM  = "HS256"

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "postgres"),
    "port":     int(os.environ.get("DB_PORT", "5432")),
    "dbname":   os.environ.get("DB_NAME",     "olist_dw"),
    "user":     os.environ.get("DB_USER",     "olist"),
    "password": os.environ.get("DB_PASS",     "olist123"),
}

# Mot de passe en clair - plus de bcrypt
FAKE_USERS = {
    "admin": {"username": "admin", "password": "secret"}
}

app = FastAPI(title="Olist Data Platform API", version="1.0.0")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

class Token(BaseModel):
    access_token: str
    token_type: str

class PagedResponse(BaseModel):
    total: int
    page: int
    page_size: int
    data: list

def authenticate_user(username, password):
    user = FAKE_USERS.get(username)
    if not user or user["password"] != password:
        return None
    return user

def create_token(data):
    to_encode = {**data, "exp": datetime.utcnow() + timedelta(minutes=60)}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user = FAKE_USERS.get(payload.get("sub"))
        if not user:
            raise HTTPException(status_code=401, detail="Token invalide")
        return user
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalide")

def query_paged(table, page, page_size, order_by="1", filters=""):
    offset = (page - 1) * page_size
    where  = "WHERE %s" % filters if filters else ""
    conn   = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM %s %s" % (table, where))
            total = cur.fetchone()["count"]
            cur.execute("SELECT * FROM %s %s ORDER BY %s LIMIT %d OFFSET %d" % (table, where, order_by, page_size, offset))
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return total, rows

@app.post("/token", response_model=Token, tags=["Auth"])
def login(form: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form.username, form.password)
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    return {"access_token": create_token({"sub": user["username"]}), "token_type": "bearer"}

@app.get("/datamarts/seller-performance", response_model=PagedResponse, tags=["Datamarts"])
def seller_performance(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100), _=Depends(get_current_user)):
    total, data = query_paged("dm_seller_performance", page, page_size, order_by="revenue_rank")
    return {"total": total, "page": page, "page_size": page_size, "data": data}

@app.get("/datamarts/monthly-revenue", response_model=PagedResponse, tags=["Datamarts"])
def monthly_revenue(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100), _=Depends(get_current_user)):
    total, data = query_paged("dm_monthly_revenue", page, page_size, order_by="order_month")
    return {"total": total, "page": page, "page_size": page_size, "data": data}

@app.get("/datamarts/order-status", response_model=PagedResponse, tags=["Datamarts"])
def order_status(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100), _=Depends(get_current_user)):
    total, data = query_paged("dm_order_status_performance", page, page_size, order_by="nb_orders DESC")
    return {"total": total, "page": page, "page_size": page_size, "data": data}

@app.get("/datamarts/review-distribution", response_model=PagedResponse, tags=["Datamarts"])
def review_distribution(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100), _=Depends(get_current_user)):
    total, data = query_paged("dm_review_distribution", page, page_size, order_by="review_score")
    return {"total": total, "page": page, "page_size": page_size, "data": data}

@app.get("/health", tags=["Monitoring"])
def health():
    try:
        psycopg2.connect(**DB_CONFIG).close()
        db = "ok"
    except Exception as e:
        db = str(e)
    return {"api": "ok", "db": db}