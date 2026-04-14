# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import or_

from ..models import (
    Users,
    Units,
    Roles,
    UserRoles,
    UserUnitMemberships,
    VisibilityGrants,
    VisibilityMode,
    ManagementScopes,
    ScopePermissions,
    PermissionCode,
    RoleCode,
)


LAB_WIDE_ROLES = {
    RoleCode.ROLE_ADMIN.value,
    RoleCode.ROLE_LANH_DAO.value,
    RoleCode.ROLE_BGD.value,
    RoleCode.ROLE_TRUONG_KHOA.value,
    RoleCode.ROLE_PHO_TRUONG_KHOA.value,
}

FUNCTION_MANAGER_ROLES = {
    RoleCode.ROLE_QL_CHAT_LUONG.value,
    RoleCode.ROLE_QL_KY_THUAT.value,
    RoleCode.ROLE_QL_AN_TOAN.value,
}

OPERATIONS_MANAGER_ROLES = {
    RoleCode.ROLE_QL_VAT_TU.value,
    RoleCode.ROLE_QL_TRANG_THIET_BI.value,
    RoleCode.ROLE_QL_MOI_TRUONG.value,
    RoleCode.ROLE_QL_CNTT.value,
}

GROUP_LEAD_ROLES = {
    RoleCode.ROLE_TRUONG_NHOM.value,
    RoleCode.ROLE_PHO_NHOM.value,
    RoleCode.ROLE_TO_TRUONG.value,
    RoleCode.ROLE_PHO_TO.value,
}


def _normalize_role(value) -> str:
    return str(getattr(value, "value", value)).upper()


def user_role_codes(db: Session, user: Users) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user.id)
        .all()
    )
    return {_normalize_role(code) for (code,) in rows}


def _active_now(start_at, end_at) -> bool:
    now = datetime.utcnow()
    if start_at and start_at > now:
        return False
    if end_at and end_at < now:
        return False
    return True


def primary_unit_ids(db: Session, user: Users) -> set[str]:
    rows = db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id,
        UserUnitMemberships.is_active == True,
    ).all()
    return {r.unit_id for r in rows if r.unit_id}


def _descendant_unit_ids(db: Session, parent_ids: set[str]) -> set[str]:
    if not parent_ids:
        return set()
    all_units = db.query(Units).all()
    children_map: dict[str, list[str]] = {}
    for u in all_units:
        if u.parent_id:
            children_map.setdefault(u.parent_id, []).append(u.id)
    out = set(parent_ids)
    stack = list(parent_ids)
    while stack:
        cur = stack.pop()
        for child in children_map.get(cur, []):
            if child not in out:
                out.add(child)
                stack.append(child)
    return out


def managed_unit_ids(db: Session, user: Users) -> set[str]:
    roles = user_role_codes(db, user)
    base_units = primary_unit_ids(db, user)

    if roles & LAB_WIDE_ROLES:
        return {u.id for u in db.query(Units.id).all()}

    out = set(base_units)

    # quản lý chức năng/công việc/trưởng nhóm: thêm cây đơn vị trực thuộc của membership chính
    if roles & (FUNCTION_MANAGER_ROLES | OPERATIONS_MANAGER_ROLES | GROUP_LEAD_ROLES):
        out |= _descendant_unit_ids(db, base_units)

    scopes = db.query(ManagementScopes).filter(
        ManagementScopes.manager_user_id == user.id,
        ManagementScopes.is_active == True,
    ).all()
    for s in scopes:
        if not _active_now(getattr(s, "effective_from", None), getattr(s, "effective_to", None)):
            continue
        if s.target_unit_id:
            out.add(s.target_unit_id)
            if str(getattr(s.scope_type, "value", s.scope_type)) == "FULL_UNIT":
                out |= _descendant_unit_ids(db, {s.target_unit_id})
        if s.manager_unit_id:
            out.add(s.manager_unit_id)

    grants = db.query(VisibilityGrants).filter(VisibilityGrants.effective_to.is_(None)).all()
    for g in grants:
        if g.mode == VisibilityMode.VIEW_ALL:
            out.add(g.grantee_unit_id)

    return out


def visible_unit_ids(db: Session, user: Users) -> set[str]:
    roles = user_role_codes(db, user)
    if roles & LAB_WIDE_ROLES:
        return {u.id for u in db.query(Units.id).all()}
    return managed_unit_ids(db, user) | primary_unit_ids(db, user)


def visible_user_ids(db: Session, user: Users) -> set[str]:
    unit_ids = visible_unit_ids(db, user)
    if not unit_ids:
        return {user.id}
    rows = db.query(UserUnitMemberships.user_id).filter(
        UserUnitMemberships.unit_id.in_(unit_ids),
        UserUnitMemberships.is_active == True,
    ).all()
    out = {uid for (uid,) in rows if uid}

    scopes = db.query(ManagementScopes).filter(
        ManagementScopes.manager_user_id == user.id,
        ManagementScopes.is_active == True,
        ManagementScopes.target_user_id.is_not(None),
    ).all()
    for s in scopes:
        if _active_now(getattr(s, "effective_from", None), getattr(s, "effective_to", None)) and s.target_user_id:
            out.add(s.target_user_id)

    out.add(user.id)
    return out


def allowed_permission_codes(db: Session, user: Users) -> set[str]:
    roles = user_role_codes(db, user)
    if roles & LAB_WIDE_ROLES:
        return {p.value for p in PermissionCode}

    codes: set[str] = set()
    scopes = db.query(ManagementScopes).filter(
        ManagementScopes.manager_user_id == user.id,
        ManagementScopes.is_active == True,
    ).all()
    if not scopes:
        return codes
    scope_ids = [s.id for s in scopes if _active_now(getattr(s, "effective_from", None), getattr(s, "effective_to", None))]
    if not scope_ids:
        return codes
    rows = db.query(ScopePermissions.permission_code).filter(ScopePermissions.scope_id.in_(scope_ids)).all()
    for (code,) in rows:
        codes.add(str(getattr(code, "value", code)).upper())
    return codes


def can_view_unit(db: Session, user: Users, unit_id: str | None) -> bool:
    if not unit_id:
        return False
    return unit_id in visible_unit_ids(db, user)


def can_manage_unit(db: Session, user: Users, unit_id: str | None) -> bool:
    if not unit_id:
        return False
    return unit_id in managed_unit_ids(db, user)


def can_use_permission(db: Session, user: Users, permission_code: PermissionCode | str) -> bool:
    code = _normalize_role(permission_code)
    perms = allowed_permission_codes(db, user)
    if code in perms:
        return True
    roles = user_role_codes(db, user)
    if code in {p.value for p in PermissionCode} and roles & LAB_WIDE_ROLES:
        return True
    return False
