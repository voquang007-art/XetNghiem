
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from app.database import SessionLocal
from app.models import Roles, RoleCode, ManagementScopes, ScopePermissions, PermissionCode, ReportingLines, ReportingLineType, Users, Units

ROLE_NAMES = {
    RoleCode.ROLE_ADMIN: "Quản trị hệ thống",
    RoleCode.ROLE_LANH_DAO: "HĐTV",
    RoleCode.ROLE_BGD: "BGĐ",
    RoleCode.ROLE_TRUONG_KHOA: "Trưởng khoa",
    RoleCode.ROLE_PHO_TRUONG_KHOA: "Phó khoa",
    RoleCode.ROLE_KY_THUAT_VIEN_TRUONG: "Kỹ thuật viên trưởng",
    RoleCode.ROLE_QL_CHAT_LUONG: "Quản lý chất lượng",
    RoleCode.ROLE_QL_KY_THUAT: "Quản lý kỹ thuật",
    RoleCode.ROLE_QL_AN_TOAN: "Quản lý an toàn",
    RoleCode.ROLE_QL_VAT_TU: "Quản lý vật tư",
    RoleCode.ROLE_QL_TRANG_THIET_BI: "Quản lý trang thiết bị",
    RoleCode.ROLE_QL_MOI_TRUONG: "Quản lý môi trường",
    RoleCode.ROLE_QL_CNTT: "Quản lý CNTT",
    RoleCode.ROLE_TRUONG_NHOM: "Nhóm/Tổ trưởng",
    RoleCode.ROLE_PHO_NHOM: "Nhóm/Tổ phó",
    RoleCode.ROLE_NHAN_VIEN: "Nhân viên",
}

# Cấu hình ví dụ: điền username thật trước khi chạy.
REPORTING_CONFIG = [
    # {"from_username": "bgd", "to_username": "hdtv", "line_type": ReportingLineType.ADMINISTRATIVE},
    # {"from_username": "truongkhoa", "to_username": "bgd", "line_type": ReportingLineType.ADMINISTRATIVE},
    # {"from_username": "ktvtruong", "to_username": "truongkhoa", "line_type": ReportingLineType.TECHNICAL},
]

SCOPE_CONFIG = [
    # {"manager_username": "ql_chat_luong", "target_unit_name": "Nhóm Elisa", "permissions": [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_FILES, PermissionCode.UPLOAD_FILES, PermissionCode.VIEW_PLANS]},
]

def ensure_roles(db):
    for code, name in ROLE_NAMES.items():
        row = db.query(Roles).filter(Roles.code == code).first()
        if not row:
            row = Roles(code=code, name=name)
            db.add(row)
        else:
            row.name = name
            db.add(row)
    db.commit()
    print('[OK] ensure roles')

def user_by_username(db, username):
    return db.query(Users).filter(Users.username == username).first()

def unit_by_name(db, name):
    return db.query(Units).filter(Units.ten_don_vi == name).first()

def ensure_reporting_lines(db):
    for cfg in REPORTING_CONFIG:
        fu = user_by_username(db, cfg['from_username'])
        tu = user_by_username(db, cfg['to_username'])
        if not fu or not tu:
            print('[SKIP reporting]', cfg)
            continue
        row = db.query(ReportingLines).filter(ReportingLines.from_user_id == fu.id, ReportingLines.to_user_id == tu.id).first()
        if not row:
            row = ReportingLines(from_user_id=fu.id, to_user_id=tu.id, line_type=cfg.get('line_type', ReportingLineType.ADMINISTRATIVE), priority_no=1, is_active=True)
            db.add(row)
        else:
            row.line_type = cfg.get('line_type', ReportingLineType.ADMINISTRATIVE)
            row.priority_no = 1
            row.is_active = True
            db.add(row)
    db.commit()
    print('[OK] ensure reporting lines')

def ensure_scopes(db):
    for cfg in SCOPE_CONFIG:
        mu = user_by_username(db, cfg['manager_username'])
        tu = unit_by_name(db, cfg['target_unit_name'])
        if not mu or not tu:
            print('[SKIP scope]', cfg)
            continue
        row = db.query(ManagementScopes).filter(ManagementScopes.manager_user_id == mu.id, ManagementScopes.target_unit_id == tu.id).first()
        if not row:
            row = ManagementScopes(manager_user_id=mu.id, target_unit_id=tu.id, is_active=True)
            db.add(row)
            db.flush()
        else:
            row.is_active = True
            db.add(row)
        perms = cfg.get('permissions') or []
        for perm in perms:
            sp = db.query(ScopePermissions).filter(ScopePermissions.scope_id == row.id, ScopePermissions.permission_code == perm).first()
            if not sp:
                db.add(ScopePermissions(scope_id=row.id, permission_code=perm))
    db.commit()
    print('[OK] ensure management scopes')

def main():
    db = SessionLocal()
    try:
        ensure_roles(db)
        ensure_reporting_lines(db)
        ensure_scopes(db)
    finally:
        db.close()

if __name__ == '__main__':
    main()
