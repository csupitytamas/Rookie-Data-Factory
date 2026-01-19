from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from passlib.hash import bcrypt
from src.database.connection import get_db
from src.models.users_model import User
from src.schemas.users_schema import UserUpdate, UserResponse
from src.utils.auth_helper import validate_token
from src.schemas.auth_schema import TokenData
from dotenv import load_dotenv
import os

router = APIRouter()

load_dotenv()
SECRET_KEY = os.getenv("SECRET_KEY", "fallback_key")
ALGORITHM = os.getenv("ALGORITHM", "HS256")

@router.get("/me")
def get_profile(current_user: TokenData = Depends(validate_token)):
    return {"message": f"Üdv, {current_user.email}!"}


@router.get("/{email}", response_model=UserResponse)
def get_user_by_email(email: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Felhasználó nem található.")
    return user


@router.put("/update/{user_id}", response_model=UserResponse)
def update_user_settings(user_id: int, updates: UserUpdate, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Felhasználó nem található.")

    update_data = updates.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)

    db.commit()
    db.refresh(user)
    return user
