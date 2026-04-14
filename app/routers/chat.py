# -*- coding: utf-8 -*-
"""
app/routers/chat.py

Router giao diện cho module chat - giai đoạn 1.
Chỉ dựng khung màn hình:
- /chat
- /chat/{group_id}

Chưa triển khai sâu quyền đơn vị và WebSocket.
"""

from __future__ import annotations
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal, get_db
from app.security.deps import login_required
from app.chat.deps import get_display_name
from app.chat.realtime import manager
from app.chat.service import (
    enrich_groups_for_list,
    get_available_users_for_group,
    get_group_by_id,
    get_group_members,
    get_group_messages,
    get_user_groups,
    is_group_member,
    list_message_reactions,
    mark_group_as_read,
    get_group_pinned_items,
)

from starlette.templating import Jinja2Templates
import os

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))


def _company_name() -> str:
    return getattr(settings, "COMPANY_NAME", "") or "Bệnh viện Hùng Vương Gia Lai"


def _app_name() -> str:
    return getattr(settings, "APP_NAME", "") or "Ứng dụng quản lý, điều hành - Khoa Xét nghiệm"


def _ws_session_user_id(websocket: WebSocket) -> str | None:
    session = websocket.scope.get("session") or {}

    user_id = session.get("user_id")
    if user_id:
        return str(user_id)

    user_obj = session.get("user")
    if isinstance(user_obj, dict):
        uid = user_obj.get("id")
        if uid:
            return str(uid)

    uid = session.get("uid")
    if uid:
        return str(uid)

    return None
    
    
@router.get("/chat", response_class=HTMLResponse)
def chat_index(
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    groups = get_user_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)

    return request.app.state.templates.TemplateResponse(
        "chat/index.html",
        {
            "request": request,
            "company_name": _company_name(),
            "app_name": _app_name(),
            "current_user": current_user,
            "current_user_display_name": get_display_name(current_user),
            "groups": groups,
            "active_group": None,
            "messages": [],
            "chat_notice": "Khung phân hệ chat đã sẵn sàng. Chúc làm việc vui vẻ.",
        },
    )


@router.get("/chat/{group_id}", response_class=HTMLResponse)
def chat_room(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    group = get_group_by_id(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Không tìm thấy nhóm chat.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc nhóm chat này.")

    mark_group_as_read(db, group_id, current_user.id)

    groups = get_user_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)
    messages = get_group_messages(db, group_id, limit=100)
    group_members = get_group_members(db, group_id)
    available_users = get_available_users_for_group(db, group_id)
    pinned_items = get_group_pinned_items(db, group_id)

    reaction_map = list_message_reactions(db, [m.id for m in messages])

    for msg in messages:
        msg.reaction_counts = reaction_map.get(msg.id, {"like": 0, "heart": 0, "laugh": 0})

    for msg in messages:
        if getattr(msg, "created_at", None):
            msg.created_at_vn = msg.created_at + timedelta(hours=7)
        else:
            msg.created_at_vn = None

    return request.app.state.templates.TemplateResponse(
        "chat/room.html",
        {
            "request": request,
            "company_name": _company_name(),
            "app_name": _app_name(),
            "current_user": current_user,
            "current_user_display_name": get_display_name(current_user),
            "groups": groups,
            "active_group": group,
            "messages": messages,
            "group_members": group_members,
            "available_users": available_users,
            "pinned_items": pinned_items,
            "chat_notice": " Đây là giao diện phòng chat. Hãy gửi tin hoặc file để trao đổi công việc nhóm.",
        },
    )


@router.websocket("/ws/chat/groups/{group_id}")
async def websocket_chat_group(
    websocket: WebSocket,
    group_id: str,
):
    user_id = _ws_session_user_id(websocket)
    if not user_id:
        await websocket.close(code=1008)
        return

    db = SessionLocal()
    try:
        if not is_group_member(db, group_id, user_id):
            await websocket.close(code=1008)
            return
    finally:
        db.close()

    await manager.connect_group(group_id, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_group(group_id, websocket)
    except Exception:
        manager.disconnect_group(group_id, websocket)
        try:
            await websocket.close()
        except Exception:
            pass
            
@router.websocket("/ws/chat/notify")
async def websocket_chat_notify(
    websocket: WebSocket,
):
    user_id = _ws_session_user_id(websocket)
    if not user_id:
        await websocket.close(code=1008)
        return

    await manager.connect_notify(user_id, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_notify(user_id, websocket)
    except Exception:
        manager.disconnect_notify(user_id, websocket)
        try:
            await websocket.close()
        except Exception:
            pass            