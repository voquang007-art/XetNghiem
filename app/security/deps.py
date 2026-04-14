# -*- coding: utf-8 -*-
from fastapi import Request, HTTPException, status
from sqlalchemy.orm import Session

from ..database import SessionLocal
from ..models import Users, UserStatus, Roles, RoleCode, UserRoles
from ..security_deps import get_current_user as _legacy_get_current_user
from .matrix_scope import user_role_codes


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(request: Request, db: Session) -> Users | None:
    return _legacy_get_current_user(request, db)


def login_required(request: Request, db: Session) -> Users:
    user = get_current_user(request, db)
    if not user or user.status != UserStatus.ACTIVE:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Vui lòng đăng nhập.")
    return user


def user_has_any_role(user: Users, db: Session, role_codes: list[RoleCode]) -> bool:
    role_ids = [ur.role_id for ur in db.query(UserRoles).filter(UserRoles.user_id == user.id).all()]
    codes = set()
    for rid in role_ids:
        r = db.get(Roles, rid)
        if r:
            codes.add(r.code)
    return any(rc in codes for rc in role_codes)


def role_required(*allowed: RoleCode):
    allowed_codes = {str(getattr(r, "value", r)).upper() for r in allowed}

    def _checker(request: Request, db: Session) -> Users:
        user = login_required(request, db)
        codes = user_role_codes(db, user)
        if not (codes & allowed_codes):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền thực hiện chức năng này.")
        return user

    return _checker
