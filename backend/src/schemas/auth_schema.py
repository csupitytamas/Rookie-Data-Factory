from pydantic import BaseModel, EmailStr
from typing import Optional, Dict


class UserBase(BaseModel):
    email: EmailStr
    save_path: Optional[str] = None
    time_zone: Optional[str] = None
    preferences: Optional[Dict] = {}

class UserCreate(UserBase):
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str
    stay_logged_in: bool = False


class LoginResponse(BaseModel):
    token: str
    user_id: int


class TokenData(BaseModel):
    user_id: int
    email: str
    exp: Optional[int] = None  # ha timestamp-ben van az expiráció

class UserInfo(BaseModel):
    id: int
    email: str
    save_path: Optional[str] = None
    time_zone: Optional[str] = None