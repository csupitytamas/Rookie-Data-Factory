from fastapi import APIRouter, HTTPException, Depends, Request
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.models.auth_model import UserSession
from src.models.users_model import User
from src.schemas.users_schema import  UserResponse
from src.schemas.auth_schema import UserCreate,LoginRequest, LoginResponse
from passlib.context import CryptContext
from uuid import uuid4
from datetime import datetime, timedelta
import secrets
import os
from dotenv import load_dotenv

load_dotenv()
SECRET_KEY = os.getenv("SECRET_KEY", "fallback_key")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
router = APIRouter()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SESSION_EXPIRE_DAYS = 30

def create_token():
    return secrets.token_hex(32)


@router.post("/login")
def login(payload: LoginRequest, request: Request = None, db: Session = Depends(get_db)):
    email = payload.email
    password = payload.password
    stay_logged_in = payload.stay_logged_in

    user = db.query(User).filter_by(email=email).first()
    if not user or not pwd_context.verify(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Hibás email vagy jelszó.")

    token = create_token()
    session = UserSession(
        session_id=str(uuid4()),
        user_id=user.id,
        token=token,
        user_agent=request.headers.get("User-Agent"),
        ip_address=request.client.host,
        expires_at=datetime.utcnow() + timedelta(days=SESSION_EXPIRE_DAYS if stay_logged_in else 1)
    )
    db.add(session)
    db.commit()

    return {"token": token, "user_id": user.id, "stay_logged_in": stay_logged_in}

@router.post("/logout")
def logout(token: str, db: Session = Depends(get_db)):
    session = db.query(UserSession).filter_by(token=token, is_active=True).first()
    if not session:
        raise HTTPException(status_code=404, detail="Nincs ilyen aktív munkamenet.")

    session.is_active = False
    db.commit()
    return {"message": "Sikeres kijelentkezés."}

@router.post("/register", response_model=UserResponse)
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    existing = db.query(User).filter(User.email == user.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Ez az e-mail már regisztrálva van.")

    hashed_pw = pwd_context.hash(user.password)
    new_user = User(
        email=user.email,
        hashed_password=hashed_pw,
        save_path="~/Downloads",
        time_zone="Europe/Budapest",
        preferences={}
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user
