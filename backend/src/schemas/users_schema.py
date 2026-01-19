from pydantic import BaseModel, EmailStr
from typing import Optional, Dict
from datetime import datetime

class UserBase(BaseModel):
    email: EmailStr
    save_path: Optional[str] = None
    time_zone: Optional[str] = None
    preferences: Optional[Dict] = {}

class UserCreate(UserBase):
    password: str

class UserUpdate(BaseModel):
    save_path: Optional[str]
    time_zone: Optional[str]
    preferences: Optional[Dict]

class UserResponse(UserBase):
    id: int
    created_at: datetime
    last_login: Optional[datetime]
    is_active: bool

    model_config = {
        "from_attributes": True
    }