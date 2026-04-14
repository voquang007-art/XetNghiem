# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime
from typing import Iterable, Optional

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import (
    Roles,
    RoleCode,
    UserRoles,
    Users,
    Units,
    ManagementScopes,
    ManagementScopeType,
    ScopePermissions,
    PermissionCode,
    ReportingLines,
    ReportingLineType,
)

DB_PATH = os.path.join(PROJECT_ROOT, "instance", "workxetnghiem.sqlite3")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

ROLE_SEED = [
    (RoleCode.ROLE_ADMIN, "Quản trị hệ thống"),
    (RoleCode.ROLE_LANH_DAO, "HĐTV"),
    (RoleCode.ROLE_BGD, "BGĐ"),
    (RoleCode.ROLE_TRUONG_KHOA, "Trưởng khoa"),
    (RoleCode.ROLE_PHO_TRUONG_KHOA, "Phó khoa"),
    (RoleCode.ROLE_KY_THUAT_VIEN_TRUONG, "Kỹ thuật viên trưởng"),
    (RoleCode.ROLE_QL_CHAT_LUONG, "Quản lý chất lượng"),
    (RoleCode.ROLE_QL_KY_THUAT, "Quản lý kỹ thuật"),
    (RoleCode.ROLE_QL_AN_TOAN, "Quản lý an toàn"),
    (RoleCode.ROLE_QL_VAT_TU, "Quản lý vật tư"),
    (RoleCode.ROLE_QL_TRANG_THIET_BI, "Quản lý trang thiết bị"),
    (RoleCode.ROLE_QL_MOI_TRUONG, "Quản lý môi trường"),
    (RoleCode.ROLE_QL_CNTT, "Quản lý CNTT"),
    (RoleCode.ROLE_TRUONG_NHOM, "Nhóm/Tổ trưởng"),
    (RoleCode.ROLE_PHO_NHOM, "Nhóm/Tổ phó"),
    (RoleCode.ROLE_NHAN_VIEN, "Nhân viên"),
    (RoleCode.ROLE_TRUONG_PHONG, "Tương thích ngược - Trưởng phòng"),
    (RoleCode.ROLE_PHO_PHONG, "Tương thích ngược - Phó phòng"),
    (RoleCode.ROLE_TO_TRUONG, "Tương thích ngược - Tổ trưởng"),
    (RoleCode.ROLE_PHO_TO, "Tương thích ngược - Tổ phó"),
]

USER_ROLE_ASSIGNMENTS = [
    # {"username": "admin", "role": RoleCode.ROLE_ADMIN},
    # {"username": "hdtv01", "role": RoleCode.ROLE_LANH_DAO},
    # {"username": "bgd01", "role": RoleCode.ROLE_BGD},
    # {"username": "truongkhoa", "role": RoleCode.ROLE_TRUONG_KHOA},
    # {"username": "phokhoa", "role": RoleCode.ROLE_PHO_TRUONG_KHOA},
    # {"username": "ktvtruong", "role": RoleCode.ROLE_KY_THUAT_VIEN_TRUONG},
    # {"username": "qlcl", "role": RoleCode.ROLE_QL_CHAT_LUONG},
    # {"username": "qlkt", "role": RoleCode.ROLE_QL_KY_THUAT},
    # {"username": "qlat", "role": RoleCode.ROLE_QL_AN_TOAN},
    # {"username": "qlvt", "role": RoleCode.ROLE_QL_VAT_TU},
    # {"username": "qlttb", "role": RoleCode.ROLE_QL_TRANG_THIET_BI},
    # {"username": "qlmt", "role": RoleCode.ROLE_QL_MOI_TRUONG},
    # {"username": "qlcntt", "role": RoleCode.ROLE_QL_CNTT},
    # {"username": "to_truong_vi_sinh", "role": RoleCode.ROLE_TRUONG_NHOM},
    # {"username": "to_pho_vi_sinh", "role": RoleCode.ROLE_PHO_NHOM},
]

MANAGEMENT_SCOPE_CONFIG = [
    # {
    #   "manager_username": "qlcl",
    #   "manager_unit_name": "Khoa Xét nghiệm",
    #   "target_unit_names": [
    #       "Nhóm Sinh hóa - Miễn dịch",
    #       "Nhóm Huyết học - Đông máu",
    #       "Nhóm Vi sinh",
    #       "Nhóm Sinh học phân tử",
    #       "Nhóm Elisa",
    #       "Nhóm Lấy máu - Gửi mẫu",
    #       "Nhóm Giải phẫu bệnh",
    #       "Nhóm Hỗ trợ sinh sản",
    #   ],
    #   "scope_type": ManagementScopeType.FULL_UNIT,
    #   "permissions": [
    #       PermissionCode.VIEW_WORK,
    #       PermissionCode.ASSIGN_WORK,
    #       PermissionCode.VIEW_PLANS,
    #       PermissionCode.VIEW_FILES,
    #       PermissionCode.REQUEST_COORDINATION,
    #   ],
    #   "notes": "Quản lý chất lượng quản lý chéo toàn khoa",
    # },
]

REPORTING_LINE_CONFIG = [
    # {"from_username": "bgd01", "to_username": "hdtv01", "line_type": ReportingLineType.ADMINISTRATIVE, "priority_no": 1, "notes": "BGĐ báo cáo HĐTV"},
    # {"from_username": "truongkhoa", "to_username": "bgd01", "line_type": ReportingLineType.ADMINISTRATIVE, "priority_no": 1, "notes": "Trưởng khoa báo cáo BGĐ"},
    # {"from_username": "phokhoa", "to_username": "truongkhoa", "line_type": ReportingLineType.ADMINISTRATIVE, "priority_no": 1, "notes": "Phó khoa báo cáo Trưởng khoa"},
    # {"from_username": "ktvtruong", "to_username": "truongkhoa", "line_type": ReportingLineType.TECHNICAL, "priority_no": 1, "notes": "KTV trưởng báo cáo Trưởng khoa"},
    # {"from_username": "qlcl", "to_username": "ktvtruong", "line_type": ReportingLineType.QUALITY, "priority_no": 1, "notes": "QLCL báo cáo KTV trưởng"},
    # {"from_username": "qlkt", "to_username": "ktvtruong", "line_type": ReportingLineType.TECHNICAL, "priority_no": 1, "notes": "QL kỹ thuật báo cáo KTV trưởng"},
    # {"from_username": "qlat", "to_username": "ktvtruong", "line_type": ReportingLineType.SAFETY, "priority_no": 1, "notes": "QL an toàn báo cáo KTV trưởng"},
    # {"from_username": "qlvt", "to_username": "ktvtruong", "line_type": ReportingLineType.OPERATIONS, "priority_no": 1, "notes": "QL vật tư báo cáo KTV trưởng"},
    # {"from_username": "qlttb", "to_username": "ktvtruong", "line_type": ReportingLineType.OPERATIONS, "priority_no": 1, "notes": "QL trang thiết bị báo cáo KTV trưởng"},
    # {"from_username": "qlmt", "to_username": "ktvtruong", "line_type": ReportingLineType.OPERATIONS, "priority_no": 1, "notes": "QL môi trường báo cáo KTV trưởng"},
    # {"from_username": "qlcntt", "to_username": "ktvtruong", "line_type": ReportingLineType.OPERATIONS, "priority_no": 1, "notes": "QL CNTT báo cáo KTV trưởng"},
    # {"from_username": "to_truong_vi_sinh", "to_username": "ktvtruong", "line_type": ReportingLineType.ADMINISTRATIVE, "priority_no": 1, "notes": "Tổ trưởng báo cáo KTV trưởng"},
]

def _norm(value) -> str:
    return str(getattr(value, "value", value)).strip().upper()

def get_user_by_username(db, username: str):
    return db.query(Users).filter(Users.username == username).first()

def get_unit_by_name(db, unit_name: str):
    return db.query(Units).filter(Units.ten_don_vi == unit_name).first()

def ensure_role(db, role_code: RoleCode, role_name: str):
    code_val = _norm(role_code)
    role = db.query(Roles).filter(Roles.code == role_code).first()
    if role:
        if role.name != role_name:
            role.name = role_name
            db.add(role)
            db.flush()
        print(f"[ROLE][OK] {code_val}")
        return role

    role = Roles(
        id=str(uuid.uuid4()),
        code=role_code,
        name=role_name,
    )
    db.add(role)
    db.flush()
    print(f"[ROLE][CREATE] {code_val}")
    return role

def ensure_user_role(db, username: str, role_code: RoleCode):
    user = get_user_by_username(db, username)
    if not user:
        print(f"[USER_ROLE][SKIP] Không tìm thấy username: {username}")
        return

    role = db.query(Roles).filter(Roles.code == role_code).first()
    if not role:
        print(f"[USER_ROLE][SKIP] Chưa có role: {_norm(role_code)}")
        return

    existed = (
        db.query(UserRoles)
        .filter(UserRoles.user_id == user.id, UserRoles.role_id == role.id)
        .first()
    )
    if existed:
        print(f"[USER_ROLE][OK] {username} -> {_norm(role_code)}")
        return

    row = UserRoles(
        id=str(uuid.uuid4()),
        user_id=user.id,
        role_id=role.id,
    )
    db.add(row)
    db.flush()
    print(f"[USER_ROLE][CREATE] {username} -> {_norm(role_code)}")

def ensure_scope_permission(db, scope_id: str, permission_code: PermissionCode):
    existed = (
        db.query(ScopePermissions)
        .filter(
            ScopePermissions.scope_id == scope_id,
            ScopePermissions.permission_code == permission_code,
        )
        .first()
    )
    if existed:
        return

    db.add(
        ScopePermissions(
            id=str(uuid.uuid4()),
            scope_id=scope_id,
            permission_code=permission_code,
        )
    )
    db.flush()

def ensure_management_scope(
    db,
    manager_username: str,
    manager_unit_name: Optional[str],
    target_unit_name: str,
    scope_type: ManagementScopeType,
    permissions: Iterable[PermissionCode],
    notes: Optional[str] = None,
):
    manager = get_user_by_username(db, manager_username)
    if not manager:
        print(f"[SCOPE][SKIP] Không tìm thấy manager_username: {manager_username}")
        return

    manager_unit = get_unit_by_name(db, manager_unit_name) if manager_unit_name else None
    target_unit = get_unit_by_name(db, target_unit_name)

    if not target_unit:
        print(f"[SCOPE][SKIP] Không tìm thấy target_unit_name: {target_unit_name}")
        return

    existed = (
        db.query(ManagementScopes)
        .filter(
            ManagementScopes.manager_user_id == manager.id,
            ManagementScopes.manager_unit_id == (manager_unit.id if manager_unit else None),
            ManagementScopes.target_unit_id == target_unit.id,
            ManagementScopes.scope_type == scope_type,
        )
        .first()
    )
    if existed:
        scope = existed
        print(f"[SCOPE][OK] {manager_username} -> {target_unit_name}")
    else:
        scope = ManagementScopes(
            id=str(uuid.uuid4()),
            manager_user_id=manager.id,
            manager_unit_id=manager_unit.id if manager_unit else None,
            target_unit_id=target_unit.id,
            scope_type=scope_type,
            is_active=True,
            notes=notes,
            created_at=datetime.utcnow(),
        )
        db.add(scope)
        db.flush()
        print(f"[SCOPE][CREATE] {manager_username} -> {target_unit_name}")

    for perm in permissions:
        ensure_scope_permission(db, scope.id, perm)

def ensure_reporting_line(
    db,
    from_username: str,
    to_username: str,
    line_type: ReportingLineType,
    priority_no: int = 1,
    notes: Optional[str] = None,
):
    from_user = get_user_by_username(db, from_username)
    to_user = get_user_by_username(db, to_username)

    if not from_user:
        print(f"[LINE][SKIP] Không tìm thấy from_username: {from_username}")
        return
    if not to_user:
        print(f"[LINE][SKIP] Không tìm thấy to_username: {to_username}")
        return

    existed = (
        db.query(ReportingLines)
        .filter(
            ReportingLines.from_user_id == from_user.id,
            ReportingLines.to_user_id == to_user.id,
            ReportingLines.line_type == line_type,
            ReportingLines.priority_no == priority_no,
        )
        .first()
    )
    if existed:
        print(f"[LINE][OK] {from_username} -> {to_username}")
        return

    db.add(
        ReportingLines(
            id=str(uuid.uuid4()),
            from_user_id=from_user.id,
            to_user_id=to_user.id,
            line_type=line_type,
            priority_no=priority_no,
            is_active=True,
            notes=notes,
            created_at=datetime.utcnow(),
        )
    )
    db.flush()
    print(f"[LINE][CREATE] {from_username} -> {to_username}")

def main():
    print(f"DB: {DB_PATH}")
    db = SessionLocal()
    try:
        print("\n=== SEED ROLES ===")
        for role_code, role_name in ROLE_SEED:
            ensure_role(db, role_code, role_name)

        print("\n=== SEED USER ROLES ===")
        for item in USER_ROLE_ASSIGNMENTS:
            ensure_user_role(db, item["username"], item["role"])

        print("\n=== SEED MANAGEMENT SCOPES ===")
        for cfg in MANAGEMENT_SCOPE_CONFIG:
            for target_unit_name in cfg.get("target_unit_names", []):
                ensure_management_scope(
                    db=db,
                    manager_username=cfg["manager_username"],
                    manager_unit_name=cfg.get("manager_unit_name"),
                    target_unit_name=target_unit_name,
                    scope_type=cfg.get("scope_type", ManagementScopeType.FULL_UNIT),
                    permissions=cfg.get("permissions", []),
                    notes=cfg.get("notes"),
                )

        print("\n=== SEED REPORTING LINES ===")
        for cfg in REPORTING_LINE_CONFIG:
            ensure_reporting_line(
                db=db,
                from_username=cfg["from_username"],
                to_username=cfg["to_username"],
                line_type=cfg.get("line_type", ReportingLineType.ADMINISTRATIVE),
                priority_no=int(cfg.get("priority_no", 1)),
                notes=cfg.get("notes"),
            )

        db.commit()
        print("\n=== HOÀN THÀNH ===")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
