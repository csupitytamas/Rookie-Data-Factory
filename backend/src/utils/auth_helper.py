from fastapi import Header, HTTPException, Depends
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.models.auth_model import UserSession
from src.models.users_model import User
from src.schemas.auth_schema import TokenData
from datetime import datetime

def validate_token(
    authorization: str = Header(...),
    db: Session = Depends(get_db)
) -> TokenData:
    token = authorization.strip()

    # Lekérdezzük az aktív sessiont
    session = db.query(UserSession).filter_by(token=token, is_active=True).first()
    if not session or session.expires_at < datetime.utcnow():
        raise HTTPException(status_code=401, detail="Érvénytelen vagy lejárt munkamenet")

    # Lekérdezzük a felhasználót
    user = db.query(User).filter_by(id=session.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Felhasználó nem található")

    return TokenData(user_id=user.id, email=user.email)