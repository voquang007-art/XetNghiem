# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime

# Bảo đảm import được package app khi chạy: python scripts\seed_admin.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Users, Roles, UserRoles, RoleCode, UserStatus

try:
    from passlib.context import CryptContext
except Exception as ex:
    raise RuntimeError(
        "Thiếu passlib[bcrypt]. Hãy chạy: pip install passlib[bcrypt]"
    ) from ex


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "HvgL@2025"
ADMIN_FULL_NAME = "Quản trị hệ thống"


def hash_password(raw_password: str) -> str:
    return pwd_context.hash(raw_password)


def get_role_code_value(value) -> str:
    raw = getattr(value, "value", value)
    return str(raw).strip().upper()


def ensure_admin_role(db: Session):
    wanted = "ROLE_ADMIN"

    role = None
    rows = db.query(Roles).all()
    for r in rows:
        code = get_role_code_value(getattr(r, "code", None))
        if code == wanted:
            role = r
            break

    if role:
        return role

    role = Roles()
    if hasattr(role, "id") and not getattr(role, "id", None):
        try:
            role.id = str(uuid.uuid4())
        except Exception:
            pass

    if hasattr(role, "code"):
        try:
            role.code = RoleCode.ROLE_ADMIN
        except Exception:
            role.code = wanted

    if hasattr(role, "name"):
        role.name = "Quản trị hệ thống"

    if hasattr(role, "description"):
        role.description = "Tài khoản quản trị hệ thống"

    db.add(role)
    db.flush()
    return role


def ensure_admin_user(db: Session):
    user = db.query(Users).filter(Users.username == ADMIN_USERNAME).first()

    hashed = hash_password(ADMIN_PASSWORD)
    now = datetime.utcnow()

    if not user:
        user = Users()

        if hasattr(user, "id") and not getattr(user, "id", None):
            try:
                user.id = str(uuid.uuid4())
            except Exception:
                pass

        if hasattr(user, "username"):
            user.username = ADMIN_USERNAME

    if hasattr(user, "full_name"):
        user.full_name = ADMIN_FULL_NAME

    if hasattr(user, "display_name") and not getattr(user, "display_name", None):
        user.display_name = ADMIN_FULL_NAME

    if hasattr(user, "email") and not getattr(user, "email", None):
        user.email = "admin@localhost"

    if hasattr(user, "phone") and not getattr(user, "phone", None):
        user.phone = ""

    if hasattr(user, "status"):
        try:
            user.status = UserStatus.ACTIVE
        except Exception:
            user.status = "ACTIVE"

    assigned_password = False

    if hasattr(user, "password_hash"):
        user.password_hash = hashed
        assigned_password = True
    elif hasattr(user, "hashed_password"):
        user.hashed_password = hashed
        assigned_password = True
    elif hasattr(user, "mat_khau_hash"):
        user.mat_khau_hash = hashed
        assigned_password = True
    elif hasattr(user, "password"):
        user.password = hashed
        assigned_password = True
    elif hasattr(user, "mat_khau"):
        user.mat_khau = hashed
        assigned_password = True

    if not assigned_password:
        raise RuntimeError(
            "Không tìm thấy field lưu mật khẩu trong model Users. "
            "Hãy kiểm tra lại models.py / auth.py."
        )

    if hasattr(user, "created_at") and not getattr(user, "created_at", None):
        user.created_at = now

    if hasattr(user, "updated_at"):
        user.updated_at = now

    if hasattr(user, "modified_at"):
        user.modified_at = now

    db.add(user)
    db.flush()
    return user


def ensure_user_role(db: Session, user, role):
    existed = (
        db.query(UserRoles)
        .filter(UserRoles.user_id == user.id, UserRoles.role_id == role.id)
        .first()
    )
    if existed:
        return existed

    ur = UserRoles()

    if hasattr(ur, "id") and not getattr(ur, "id", None):
        try:
            ur.id = str(uuid.uuid4())
        except Exception:
            pass

    ur.user_id = user.id
    ur.role_id = role.id

    if hasattr(ur, "created_at") and not getattr(ur, "created_at", None):
        ur.created_at = datetime.utcnow()

    db.add(ur)
    db.flush()
    return ur


def main():
    db = SessionLocal()
    try:
        role = ensure_admin_role(db)
        user = ensure_admin_user(db)
        ensure_user_role(db, user, role)

        db.commit()

        print("=== TẠO/CẬP NHẬT ADMIN THÀNH CÔNG ===")
        print(f"Username : {ADMIN_USERNAME}")
        print(f"Password : {ADMIN_PASSWORD}")
        print(f"User ID  : {getattr(user, 'id', '')}")
        print(f"Role ID  : {getattr(role, 'id', '')}")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()