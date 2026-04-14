# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import List, Optional, Set

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from sqlalchemy.orm import Session

from app.chat.deps import get_display_name
from app.chat.models import (
    ChatAttachments,
    ChatMessages,
    ChatMeetingAttendances,
    ChatMeetings,
    ChatMeetingSpeakerRequests,
)
from app.chat.realtime import manager
from app.chat.service import (
    add_member_to_group,
    approve_speaker_request,
    assign_meeting_secretary,
    create_group,
    create_meeting_session,
    create_message,
    create_speaker_request,
    enrich_groups_for_list,
    ensure_meeting_attendance_rows,
    get_available_users_for_group,
    get_group_by_id,
    get_group_member_user_ids,
    get_group_members,
    get_group_messages,
    get_meeting_attendance_rows,
    get_meeting_by_group_id,
    get_message_attachments,
    get_user_meeting_groups,
    is_group_member,
    list_speaker_requests,
    mark_meeting_absent,
    mark_meeting_checkin,
    move_speaker_request,
    save_message_attachment,
    set_meeting_presence,
    transition_meeting_status_if_needed,
    assign_meeting_host,
    auto_assign_meeting_host,
    remove_absent_members_from_live_meeting,
)
from app.config import settings
from app.database import get_db
from app.models import RoleCode, Roles, UserRoles, UserStatus, UserUnitMemberships, Users
from app.security.deps import login_required
from starlette.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))


def _company_name() -> str:
    return getattr(settings, "COMPANY_NAME", "") or "Bệnh viện Hùng Vương Gia Lai"


def _app_name() -> str:
    return getattr(settings, "APP_NAME", "") or "Ứng dụng quản lý, điều hành - Khoa Xét nghiệm"


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    result: Set[str] = set()
    for (code,) in rows:
        raw = getattr(code, "value", code)
        result.add(str(raw or "").strip().upper())
    return result


def _role_priority(role_codes: Set[str]) -> int:
    if "ROLE_TRUONG_KHOA" in role_codes:
        return 1
    if "ROLE_PHO_TRUONG_KHOA" in role_codes:
        return 2
    if "ROLE_KY_THUAT_VIEN_TRUONG" in role_codes:
        return 3
    if "ROLE_QL_CONG_VIEC" in role_codes:
        return 4
    functional = {
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
    }
    if functional & role_codes:
        return 5
    if {"ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"} & role_codes:
        return 6
    return 99


def _can_create_meeting(role_codes: Set[str]) -> bool:
    allowed = {
        "ROLE_HDTV",
        "ROLE_BGĐ",
        "ROLE_TRUONG_KHOA",
        "ROLE_PHO_TRUONG_KHOA",
        "ROLE_KY_THUAT_VIEN_TRUONG",
        "ROLE_QL_CONG_VIEC",
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
        "ROLE_TRUONG_NHOM",
        "ROLE_PHO_NHOM",
        "ROLE_TO_TRUONG",
        "ROLE_PHO_TO",
    }
    return bool(allowed & role_codes)


def _get_attendance_row_for_user(db: Session, meeting_id: str, user_id: str):
    rows = get_meeting_attendance_rows(db, meeting_id)
    for row in rows:
        if row.user_id == user_id:
            return row
    return None


def _can_manage_meeting_schedule(meeting, user_id: str) -> bool:
    if not meeting or not user_id:
        return False
    return user_id in {
        getattr(meeting, "host_user_id", None),
        getattr(meeting, "secretary_user_id", None),
        getattr(meeting, "designed_by_user_id", None),
    }

def _can_assign_meeting_host(meeting, user_id: str) -> bool:
    if not meeting or not user_id:
        return False
    return user_id in {
        getattr(meeting, "host_user_id", None),
        getattr(meeting, "designed_by_user_id", None),
    }


def _ensure_meeting_runtime_rules(db: Session, meeting):
    if not meeting:
        return None

    meeting = transition_meeting_status_if_needed(db, meeting)
    if not meeting:
        return None

    meeting = auto_assign_meeting_host(db, meeting.id) or meeting
    remove_absent_members_from_live_meeting(db, meeting.id)

    refreshed = db.get(ChatMeetings, meeting.id)
    return refreshed or meeting
    
    
def _to_datetime_local_value(dt_value) -> str:
    if not dt_value:
        return ""
    try:
        return (dt_value + timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return ""


def _consume_current_speaker_permission(db: Session, meeting_id: str, user_id: str) -> bool:
    speaker_rows = list_speaker_requests(db, meeting_id)
    for row in speaker_rows:
        if row.user_id != user_id:
            continue
        status = (getattr(row, "request_status", "") or "").upper()
        if status in {"APPROVED", "SPEAKING"}:
            row.request_status = "DONE"
            db.add(row)
            db.commit()
            return True
    return False


def _can_user_send_meeting_message(
    db: Session,
    meeting,
    user_id: str,
    attendance_row=None,
) -> bool:
    if not meeting or not user_id:
        return False

    if (meeting.meeting_status or "").upper() != "LIVE":
        return False

    if meeting.host_user_id == user_id:
        return True

    if attendance_row is None:
        attendance_row = _get_attendance_row_for_user(db, meeting.id, user_id)

    attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    if attendance_status == "ABSENT":
        return False

    speaker_rows = list_speaker_requests(db, meeting.id)
    for row in speaker_rows:
        if row.user_id != user_id:
            continue
        status = (getattr(row, "request_status", "") or "").upper()
        if status in {"APPROVED", "SPEAKING"}:
            return True

    return False
    
    
def _is_browser_previewable(filename: str) -> bool:
    name = (filename or "").strip().lower()
    return name.endswith(".pdf") or name.endswith(".png") or name.endswith(".jpg") or name.endswith(".jpeg") or name.endswith(".gif") or name.endswith(".webp") or name.endswith(".txt")


def _build_meeting_documents(messages: List[dict]) -> List[dict]:
    documents: List[dict] = []
    for msg in messages:
        if (msg.get("message_type") or "").upper() != "MEETING_DOC":
            continue
        for att in msg.get("attachments", []) or []:
            documents.append({
                "id": att.get("id"),
                "filename": att.get("filename") or "Tệp đính kèm",
                "path": att.get("path") or "#",
                "is_previewable": bool(att.get("is_previewable", False)),
                "sender_name": msg.get("sender_name") or "Người dùng",
                "created_at_text": msg.get("created_at_text") or "",
            })
    return documents


def _format_vn_dt_text(dt_value) -> str:
    if not dt_value:
        return "—"
    try:
        return (dt_value + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "—"


def _get_latest_meeting_conclusion_message(db: Session, group_id: str):
    if not group_id:
        return None

    return (
        db.query(ChatMessages)
        .filter(ChatMessages.group_id == group_id)
        .filter(ChatMessages.message_type == "MEETING_CONCLUSION")
        .order_by(ChatMessages.created_at.desc())
        .first()
    )


def _build_minutes_speaker_sections(messages: List[dict]) -> List[dict]:
    sections: List[dict] = []
    by_user: dict[str, dict] = {}

    for msg in messages or []:
        message_type = (msg.get("message_type") or "").strip().upper()
        if message_type not in {"TEXT", "FILE"}:
            continue

        sender_name = (msg.get("sender_name") or "Người dùng").strip() or "Người dùng"
        bucket = by_user.get(sender_name)
        if not bucket:
            bucket = {
                "sender_name": sender_name,
                "entries": [],
            }
            by_user[sender_name] = bucket
            sections.append(bucket)

        content = (msg.get("content") or "").strip()
        attachments = msg.get("attachments") or []

        parts: list[str] = []
        if content:
            parts.append(content)

        if attachments:
            file_names = []
            for att in attachments:
                file_name = (att.get("filename") or "").strip()
                if file_name:
                    file_names.append(file_name)
            if file_names:
                parts.append("Tệp trao đổi: " + ", ".join(file_names))

        if not parts:
            continue

        created_at_text = (msg.get("created_at_text") or "").strip()
        merged_text = " ".join(parts).strip()

        bucket["entries"].append({
            "created_at_text": created_at_text,
            "text": merged_text,
        })

    return sections


def _build_meeting_minutes_text(detail: dict) -> str:
    meeting = detail.get("meeting")
    host = detail.get("host")
    secretary = detail.get("secretary")
    designed_by = detail.get("designed_by")
    attendance_rows = detail.get("attendance_rows") or []
    messages = detail.get("messages") or []
    conclusion_text = (detail.get("conclusion_text") or "").strip()

    invited_count = len(detail.get("member_ids") or [])
    checked_in_count = int(detail.get("attendance_checked_in_count") or 0)
    absent_count = int(detail.get("attendance_absent_count") or 0)
    leave_count = 0

    sections = _build_minutes_speaker_sections(messages)

    lines: list[str] = []
    lines.append("BIÊN BẢN HỌP TRỰC TUYẾN")
    lines.append("")
    lines.append(f"Tên cuộc họp: {getattr(detail.get('group'), 'name', '') or '—'}")
    lines.append(f"Loại cuộc họp: {detail.get('scope_label') or '—'}")
    lines.append(f"Thời gian bắt đầu: {_format_vn_dt_text(getattr(meeting, 'scheduled_start_at', None))}")
    lines.append(f"Thời gian kết thúc: {_format_vn_dt_text(getattr(meeting, 'scheduled_end_at', None))}")
    lines.append(f"Chủ trì: {get_display_name(host) if host else '—'}")
    lines.append(f"Thư ký: {get_display_name(secretary) if secretary else 'Chưa chỉ định'}")
    lines.append(f"Người thiết kế: {get_display_name(designed_by) if designed_by else '—'}")
    lines.append(f"Số người mời: {invited_count}")
    lines.append(f"Số người có mặt: {checked_in_count}")
    lines.append(f"Số người báo vắng: {absent_count}")
    lines.append(f"Số người xin rời cuộc họp: {leave_count}")
    lines.append("")
    lines.append("I. THÀNH PHẦN THAM DỰ / HIỆN DIỆN")
    if attendance_rows:
        for idx, row in enumerate(attendance_rows, start=1):
            full_name = get_display_name(row.user) if getattr(row, "user", None) else (row.user_id or "Người dùng")
            attendance_label = getattr(row, "attendance_status_label", None) or _attendance_status_label(getattr(row, "attendance_status", None))
            presence_label = getattr(row, "presence_status_label", None) or ("Đang ở phòng" if (getattr(row, "presence_status", "") or "").upper() == "ONLINE" else "Ngoài phòng")
            absent_reason = (getattr(row, "absent_reason", "") or "").strip()

            extra = []
            if attendance_label:
                extra.append(attendance_label)
            if presence_label:
                extra.append(presence_label)
            if absent_reason:
                extra.append("Lý do: " + absent_reason)

            lines.append(f"{idx}. {full_name} - " + " - ".join(extra))
    else:
        lines.append("Không có dữ liệu thành phần tham dự.")
    lines.append("")
    lines.append("II. TỔNG HỢP Ý KIẾN TRAO ĐỔI THEO NGƯỜI PHÁT BIỂU")
    if sections:
        for idx, section in enumerate(sections, start=1):
            lines.append(f"{idx}. {section['sender_name']}:")
            for entry_idx, entry in enumerate(section["entries"], start=1):
                prefix = f"   {idx}.{entry_idx}"
                if entry.get("created_at_text"):
                    lines.append(f"{prefix} [{entry['created_at_text']}] {entry['text']}")
                else:
                    lines.append(f"{prefix} {entry['text']}")
    else:
        lines.append("Chưa có nội dung trao đổi được ghi nhận.")
    lines.append("")
    lines.append("III. KẾT LUẬN CỦA CHỦ TRÌ")
    if conclusion_text:
        for line in conclusion_text.splitlines():
            clean_line = line.rstrip()
            lines.append(clean_line if clean_line else "")
    else:
        lines.append("Chưa có kết luận cuộc họp.")
    lines.append("")

    return "\n".join(lines)

    
def _get_user_primary_unit(db: Session, user_id: str):
    membership = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(UserUnitMemberships.is_primary.desc())
        .first()
    )
    return membership.unit if membership else None


def _pick_host_and_secretary(
    db: Session,
    creator: Users,
    participant_ids: List[str],
    meeting_scope: str,
) -> tuple[Optional[str], Optional[str]]:
    scope = (meeting_scope or "TEAM").strip().upper()
    creator_roles = _load_role_codes_for_user(db, creator.id)
    participants = []
    for user_id in participant_ids:
        user = db.get(Users, user_id)
        if user:
            participants.append(user)

    if scope == "DEPARTMENT":
        truong_khoa: List[Users] = []
        pho_khoa: List[Users] = []
        for user in participants:
            codes = _load_role_codes_for_user(db, user.id)
            if "ROLE_TRUONG_KHOA" in codes:
                truong_khoa.append(user)
            elif "ROLE_PHO_TRUONG_KHOA" in codes:
                pho_khoa.append(user)

        if truong_khoa:
            return truong_khoa[0].id, creator.id
        if pho_khoa:
            return pho_khoa[0].id, creator.id
        return creator.id, creator.id

    if scope in {"TEAM", "FUNCTIONAL", "OPERATIONS"}:
        return creator.id, None

    if scope == "GENERAL":
        # ưu tiên người có vai trò cao nhất trong thành phần, nếu không có thì creator
        ranked = sorted(participants, key=lambda item: (_role_priority(_load_role_codes_for_user(db, item.id)), (item.full_name or item.username or "")))
        if ranked:
            return ranked[0].id, None
    return creator.id, None


def _meeting_status_label(value: str) -> str:
    return {
        "UPCOMING": "Sắp họp",
        "LIVE": "Đang họp",
        "ENDED": "Đã kết thúc",
    }.get((value or "").strip().upper(), value or "—")


def _attendance_status_label(value: str) -> str:
    return {
        "PENDING": "Chưa phản hồi",
        "ABSENT": "Báo vắng",
        "CHECKED_IN": "Đã điểm danh",
    }.get((value or "").strip().upper(), value or "—")


def _meeting_scope_label(value: str) -> str:
    return {
        "TEAM": "Họp nhóm",
        "FUNCTIONAL": "Họp QL công việc / QL chức năng",
        "DEPARTMENT": "Họp khoa",
        "GENERAL": "Hội ý",
    }.get((value or "").strip().upper(), value or "Cuộc họp")


def _build_message_vm(db: Session, message, current_user_id: str) -> dict:
    sender_name = get_display_name(message.sender) if getattr(message, "sender", None) else "Người dùng"
    attachments = []
    for att in get_message_attachments(db, message.id):
        if getattr(att, "deleted_by_owner", False):
            continue
        filename = getattr(att, "filename", "") or ""
        path = getattr(att, "path", "") or "#"
        attachments.append({
            "id": att.id,
            "filename": filename,
            "path": path,
            "is_previewable": _is_browser_previewable(filename),
        })
    created_text = ""
    if getattr(message, "created_at", None):
        created_text = (message.created_at + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")
    return {
        "id": message.id,
        "sender_name": sender_name,
        "content": message.content or "",
        "message_type": message.message_type,
        "created_at_text": created_text,
        "is_mine": message.sender_user_id == current_user_id,
        "attachments": attachments,
        "recalled": bool(getattr(message, "recalled", False)),
    }

def _can_delete_meeting(meeting, user_id: str) -> bool:
    if not meeting or not user_id:
        return False
    return user_id in {
        getattr(meeting, "designed_by_user_id", None),
        getattr(meeting, "host_user_id", None),
        getattr(meeting, "secretary_user_id", None),
    }


def _prepare_meeting_groups_for_sidebar(
    db: Session,
    groups: List,
    current_user_id: str,
) -> List:
    prepared: List = []
    for group in groups or []:
        meeting = get_meeting_by_group_id(db, group.id)
        if meeting:
            meeting = _ensure_meeting_runtime_rules(db, meeting)

        sort_dt = (
            getattr(meeting, "scheduled_start_at", None)
            or getattr(meeting, "created_at", None)
            or getattr(group, "created_at", None)
            or datetime.utcnow()
        )

        group.meeting_row = meeting
        group.meeting_sort_at = sort_dt
        group.list_status_label = _meeting_status_label(getattr(meeting, "meeting_status", "")) if meeting else "Cuộc họp"
        group.can_delete_meeting = _can_delete_meeting(meeting, current_user_id)
        prepared.append(group)

    prepared.sort(
        key=lambda item: getattr(item, "meeting_sort_at", None) or datetime.min,
        reverse=True,
    )
    return prepared


def _build_meeting_groups_by_month(
    groups: List,
    selected_id: str = "",
) -> List[dict]:
    buckets: List[dict] = []
    year_map: dict[str, dict] = {}

    for group in groups or []:
        sort_dt = getattr(group, "meeting_sort_at", None) or getattr(group, "created_at", None) or datetime.utcnow()
        year_key = f"{sort_dt.year:04d}"
        month_key = f"{sort_dt.year:04d}-{sort_dt.month:02d}"

        year_bucket = year_map.get(year_key)
        if not year_bucket:
            year_bucket = {
                "year_key": year_key,
                "year_label": f"Năm {sort_dt.year}",
                "months": [],
                "is_open": False,
            }
            year_map[year_key] = year_bucket
            buckets.append(year_bucket)

        month_bucket = None
        for item in year_bucket["months"]:
            if item["month_key"] == month_key:
                month_bucket = item
                break

        if not month_bucket:
            month_bucket = {
                "month_key": month_key,
                "month_label": f"Tháng {sort_dt.month:02d}",
                "groups": [],
                "count": 0,
                "is_open": False,
            }
            year_bucket["months"].append(month_bucket)

        month_bucket["groups"].append(group)
        month_bucket["count"] += 1

        if str(getattr(group, "id", "")) == str(selected_id or ""):
            month_bucket["is_open"] = True
            year_bucket["is_open"] = True

    if not selected_id and buckets:
        buckets[0]["is_open"] = True
        if buckets[0]["months"]:
            buckets[0]["months"][0]["is_open"] = True

    return buckets


def _remove_meeting_group_attachment_files(db: Session, group_id: str) -> None:
    rows = (
        db.query(ChatAttachments.path)
        .join(ChatMessages, ChatMessages.id == ChatAttachments.message_id)
        .filter(ChatMessages.group_id == group_id)
        .all()
    )

    for (path_value,) in rows:
        try:
            rel_path = str(path_value or "").lstrip("/").replace("/", os.sep)
            if not rel_path:
                continue
            abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", rel_path))
            if os.path.isfile(abs_path):
                os.remove(abs_path)
        except Exception:
            continue
            
@router.get("/meetings", response_class=HTMLResponse)
def meetings_index(
    request: Request,
    selected_id: str = "",
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    groups = get_user_meeting_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)
    groups = _prepare_meeting_groups_for_sidebar(db, groups, current_user.id)
    meeting_groups_by_month = _build_meeting_groups_by_month(groups, selected_id)

    selected_group = None
    selected_meeting = None
    if selected_id:
        selected_group = get_group_by_id(db, selected_id)
        if selected_group and selected_group.group_type == "MEETING" and is_group_member(db, selected_group.id, current_user.id):
            selected_meeting = get_meeting_by_group_id(db, selected_group.id)
            selected_meeting = _ensure_meeting_runtime_rules(db, selected_meeting)
            if not is_group_member(db, selected_group.id, current_user.id):
                selected_group = None
                selected_meeting = None
        else:
            selected_group = None
    elif groups:
        selected_group = groups[0]
        selected_meeting = get_meeting_by_group_id(db, selected_group.id)
        selected_meeting = _ensure_meeting_runtime_rules(db, selected_meeting)
        if not is_group_member(db, selected_group.id, current_user.id):
            selected_group = None
            selected_meeting = None

    current_user_role_codes = _load_role_codes_for_user(db, current_user.id)
    current_user_can_create_meeting = _can_create_meeting(current_user_role_codes)

    active_users = (
        db.query(Users)
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    
    detail = None
    if selected_group and selected_meeting:
        members = get_group_members(db, selected_group.id)
        attendance_rows = get_meeting_attendance_rows(db, selected_meeting.id)
        attendance_map = {row.user_id: row for row in attendance_rows}
        speaker_requests = list_speaker_requests(db, selected_meeting.id)
        all_messages = [
            _build_message_vm(db, msg, current_user.id)
            for msg in get_group_messages(db, selected_group.id, limit=150)
        ]
        messages = [
            msg for msg in all_messages
            if (msg.get("message_type") or "").upper() != "MEETING_DOC"
        ]
        member_ids = [m.user_id for m in members]
        available_secretaries = [m.user for m in members if getattr(m, "user", None)]
        available_hosts = []
        for m in members:
            user_obj = getattr(m, "user", None)
            if not user_obj:
                continue
            attendance_row = attendance_map.get(m.user_id)
            attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
            if attendance_status == "ABSENT":
                continue
            available_hosts.append(user_obj)

        current_attendance = attendance_map.get(current_user.id)
        host = db.get(Users, selected_meeting.host_user_id) if selected_meeting.host_user_id else None
        secretary = db.get(Users, selected_meeting.secretary_user_id) if selected_meeting.secretary_user_id else None
        designed_by = db.get(Users, selected_meeting.designed_by_user_id) if selected_meeting.designed_by_user_id else None

        for row in attendance_rows:
            row.attendance_status_label = _attendance_status_label(row.attendance_status)
            row.presence_status_label = "Đang ở phòng" if (row.presence_status or "").upper() == "ONLINE" else "Ngoài phòng"

        for row in speaker_requests:
            row.user_name = get_display_name(row.user) if getattr(row, "user", None) else row.user_id
            row.request_status_label = {
                "PENDING": "Đang chờ",
                "APPROVED": "Đã cho phép",
                "SPEAKING": "Đang phát biểu",
                "DONE": "Đã phát biểu",
            }.get((row.request_status or "").upper(), row.request_status or "—")

        current_attendance_status = (getattr(current_attendance, "attendance_status", "") or "PENDING").upper()
        host_attendance = attendance_map.get(selected_meeting.host_user_id) if selected_meeting.host_user_id else None
        host_attendance_status = (getattr(host_attendance, "attendance_status", "") or "PENDING").upper() if host_attendance else "PENDING"

        can_upload_documents = current_user.id in {
            selected_meeting.host_user_id,
            selected_meeting.secretary_user_id,
        }
        can_manage_schedule = _can_manage_meeting_schedule(selected_meeting, current_user.id)
        can_send_meeting_message = _can_user_send_meeting_message(
            db,
            selected_meeting,
            current_user.id,
            attendance_row=current_attendance,
        )
        can_register_speaker = (
            (selected_meeting.meeting_status or "").upper() == "LIVE"
            and current_attendance_status != "ABSENT"
            and not can_send_meeting_message
        )

        attendance_pending_count = 0
        attendance_absent_count = 0
        attendance_checked_in_count = 0
        for row in attendance_rows:
            status = (getattr(row, "attendance_status", "") or "PENDING").upper()
            if status == "ABSENT":
                attendance_absent_count += 1
            elif status == "CHECKED_IN":
                attendance_checked_in_count += 1
            else:
                attendance_pending_count += 1
                
        latest_conclusion_message = _get_latest_meeting_conclusion_message(db, selected_group.id)
        conclusion_text = (getattr(latest_conclusion_message, "content", "") or "").strip()
        conclusion_updated_text = ""
        if getattr(latest_conclusion_message, "created_at", None):
            conclusion_updated_text = _format_vn_dt_text(latest_conclusion_message.created_at)                

        detail = {
            "group": selected_group,
            "meeting": selected_meeting,
            "messages": messages,
            "meeting_documents": _build_meeting_documents(all_messages),
            "members": members,
            "attendance_rows": attendance_rows,
            "speaker_requests": speaker_requests,
            "host": host,
            "secretary": secretary,
            "designed_by": designed_by,
            "available_secretaries": available_secretaries,
            "available_hosts": available_hosts,
            "available_users": get_available_users_for_group(db, selected_group.id),
            "status_label": _meeting_status_label(selected_meeting.meeting_status),
            "scope_label": _meeting_scope_label(selected_meeting.meeting_scope),
            "current_attendance": current_attendance,
            "current_attendance_status": current_attendance_status,
            "attendance_pending_count": attendance_pending_count,
            "attendance_absent_count": attendance_absent_count,
            "attendance_checked_in_count": attendance_checked_in_count,
            "is_host": selected_meeting.host_user_id == current_user.id,
            "can_assign_host": _can_assign_meeting_host(selected_meeting, current_user.id),
            "host_attendance_status": host_attendance_status,
            "can_upload_documents": can_upload_documents,
            "can_manage_schedule": can_manage_schedule,
            "can_send_meeting_message": can_send_meeting_message,
            "can_register_speaker": can_register_speaker,
            "scheduled_start_value": _to_datetime_local_value(selected_meeting.scheduled_start_at),
            "scheduled_end_value": _to_datetime_local_value(selected_meeting.scheduled_end_at),
            "member_ids": member_ids,
            "conclusion_text": conclusion_text,
            "conclusion_updated_text": conclusion_updated_text,
            "can_edit_conclusion": selected_meeting.host_user_id == current_user.id,
            "can_export_minutes": selected_meeting.secretary_user_id == current_user.id,
            "can_delete_meeting": _can_delete_meeting(selected_meeting, current_user.id),
        }

    return request.app.state.templates.TemplateResponse(
        "meetings/index.html",
        {
            "request": request,
            "company_name": _company_name(),
            "app_name": _app_name(),
            "current_user": current_user,
            "current_user_display_name": get_display_name(current_user),
            "current_user_can_create_meeting": current_user_can_create_meeting,
            "groups": groups,
            "meeting_groups_by_month": meeting_groups_by_month,
            "selected_id": selected_group.id if selected_group else "",
            "selected_detail": detail,
            "all_active_users": active_users,
            "meeting_scope_options": [
                ("TEAM", "Họp nhóm"),
                ("FUNCTIONAL", "Họp QL công việc / QL chức năng"),
                ("DEPARTMENT", "Họp khoa"),
                ("GENERAL", "Họp chung"),
            ],
        },
    )


@router.post("/meetings/create")
def create_meeting(
    request: Request,
    name: str = Form(...),
    meeting_scope: str = Form("TEAM"),
    scheduled_start_at: str = Form(...),
    scheduled_end_at: str = Form(""),
    agenda: str = Form(""),
    participant_ids: List[str] = Form([]),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    current_user_role_codes = _load_role_codes_for_user(db, current_user.id)
    if not _can_create_meeting(current_user_role_codes):
        raise HTTPException(status_code=403, detail="Nhân viên không được tạo cuộc họp.")

    clean_name = (name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Tên cuộc họp không được để trống.")

    try:
        start_local = datetime.strptime((scheduled_start_at or "").strip(), "%Y-%m-%dT%H:%M")
        start_dt = start_local - timedelta(hours=7)
    except Exception:
        raise HTTPException(status_code=400, detail="Thời gian bắt đầu không hợp lệ.")

    end_dt = None
    if (scheduled_end_at or "").strip():
        try:
            end_local = datetime.strptime((scheduled_end_at or "").strip(), "%Y-%m-%dT%H:%M")
            end_dt = end_local - timedelta(hours=7)
        except Exception:
            raise HTTPException(status_code=400, detail="Thời gian kết thúc không hợp lệ.")

    clean_participants: List[str] = []
    for user_id in participant_ids or []:
        uid = str(user_id or "").strip()
        if uid and uid not in clean_participants:
            clean_participants.append(uid)
    if current_user.id not in clean_participants:
        clean_participants.insert(0, current_user.id)

    host_user_id, secretary_user_id = _pick_host_and_secretary(db, current_user, clean_participants, meeting_scope)

    group = create_group(
        db,
        name=clean_name,
        owner_user_id=current_user.id,
        group_type="MEETING",
    )

    for uid in clean_participants:
        if uid == current_user.id:
            continue
        add_member_to_group(db, group_id=group.id, user_id=uid, member_role="member", mark_as_new=True)

    meeting = create_meeting_session(
        db,
        group_id=group.id,
        designed_by_user_id=current_user.id,
        host_user_id=host_user_id,
        secretary_user_id=secretary_user_id,
        meeting_scope=meeting_scope,
        scheduled_start_at=start_dt,
        scheduled_end_at=end_dt,
        agenda=agenda,
    )
    ensure_meeting_attendance_rows(db, meeting.id, clean_participants)

    return RedirectResponse(url=f"/meetings?selected_id={group.id}", status_code=303)

@router.post("/meetings/{group_id}/delete")
def meeting_delete(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    group = get_group_by_id(db, group_id)
    if not group or (group.group_type or "").upper() != "MEETING":
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy dữ liệu cuộc họp.")

    if not _can_delete_meeting(meeting, current_user.id):
        raise HTTPException(
            status_code=403,
            detail="Chỉ Người thiết kế, Chủ trì hoặc Thư ký mới được xóa cuộc họp.",
        )

    _remove_meeting_group_attachment_files(db, group_id)

    db.query(ChatMeetingSpeakerRequests).filter(
        ChatMeetingSpeakerRequests.meeting_id == meeting.id
    ).delete(synchronize_session=False)

    db.query(ChatMeetingAttendances).filter(
        ChatMeetingAttendances.meeting_id == meeting.id
    ).delete(synchronize_session=False)

    db.delete(meeting)
    db.delete(group)
    db.commit()

    return RedirectResponse(url="/meetings", status_code=303)
    
@router.post("/meetings/{group_id}/schedule")
def meeting_update_schedule(
    group_id: str,
    request: Request,
    scheduled_start_at: str = Form(...),
    scheduled_end_at: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    if not _can_manage_meeting_schedule(meeting, current_user.id):
        raise HTTPException(
            status_code=403,
            detail="Chỉ Chủ trì, Thư ký hoặc người thiết kế cuộc họp mới được điều chỉnh thời gian."
        )

    try:
        start_local = datetime.strptime((scheduled_start_at or "").strip(), "%Y-%m-%dT%H:%M")
        start_dt = start_local - timedelta(hours=7)
    except Exception:
        raise HTTPException(status_code=400, detail="Thời gian bắt đầu không hợp lệ.")

    end_dt = None
    if (scheduled_end_at or "").strip():
        try:
            end_local = datetime.strptime((scheduled_end_at or "").strip(), "%Y-%m-%dT%H:%M")
            end_dt = end_local - timedelta(hours=7)
        except Exception:
            raise HTTPException(status_code=400, detail="Thời gian kết thúc không hợp lệ.")

    if end_dt and end_dt < start_dt:
        raise HTTPException(status_code=400, detail="Thời gian kết thúc không được trước thời gian bắt đầu.")

    meeting.scheduled_start_at = start_dt
    meeting.scheduled_end_at = end_dt
    db.add(meeting)
    db.commit()

    return RedirectResponse(url=f"/meetings?selected_id={group_id}", status_code=303)
    
    
@router.post("/meetings/{group_id}/documents/upload")
async def meeting_upload_document(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if current_user.id not in {meeting.host_user_id, meeting.secretary_user_id}:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì hoặc Thư ký mới được tải tài liệu phục vụ cuộc họp.")

    form = await request.form()
    upload = form.get("file")
    if not upload or not getattr(upload, "filename", ""):
        raise HTTPException(status_code=400, detail="Chưa chọn file.")

    ext = os.path.splitext(upload.filename)[1].lower()
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}{ext}"
    rel_dir = os.path.join("chat_uploads", group_id, "meeting_docs")
    abs_dir = os.path.join(os.path.dirname(__file__), "..", "static", rel_dir)
    abs_dir = os.path.abspath(abs_dir)
    os.makedirs(abs_dir, exist_ok=True)

    abs_path = os.path.join(abs_dir, stored_name)
    content = await upload.read()

    with open(abs_path, "wb") as f:
        f.write(content)

    message = create_message(
        db,
        group_id=group_id,
        sender_user_id=current_user.id,
        content=upload.filename,
        message_type="MEETING_DOC",
        reply_to_message_id=None,
    )

    rel_url = "/" + os.path.join("static", rel_dir, stored_name).replace("\\", "/")

    attachment = save_message_attachment(
        db,
        message_id=message.id,
        filename=upload.filename,
        stored_name=stored_name,
        path=rel_url,
        mime_type=getattr(upload, "content_type", None),
        size_bytes=len(content),
    )

    message_payload = _build_message_vm(db, message, current_user.id)

    payload = {
        "type": "meeting_document_uploaded",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "document": {
            "id": attachment.id,
            "filename": attachment.filename,
            "path": attachment.path,
            "is_previewable": _is_browser_previewable(attachment.filename),
            "sender_name": get_display_name(current_user),
            "created_at_text": message_payload.get("created_at_text") or "",
        },
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/messages/send")
async def meeting_send_message(
    group_id: str,
    request: Request,
    content: str = Form(...),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này hoặc đã bị loại khỏi cuộc họp do báo vắng.")
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    clean_content = (content or "").strip()
    if not clean_content:
        raise HTTPException(status_code=400, detail="Nội dung tin nhắn không được để trống.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    if not _can_user_send_meeting_message(db, meeting, current_user.id, attendance_row=attendance_row):
        raise HTTPException(
            status_code=403,
            detail="Bạn chỉ được gửi nội dung phát biểu sau khi được Chủ trì cho phép."
        )

    message = create_message(
        db,
        group_id=group_id,
        sender_user_id=current_user.id,
        content=clean_content,
        message_type="TEXT",
        reply_to_message_id=None,
    )

    if meeting.host_user_id != current_user.id:
        _consume_current_speaker_permission(db, meeting.id, current_user.id)

    message_payload = _build_message_vm(db, message, current_user.id)

    payload = {
        "type": "new_message",
        "message": message_payload,
        "can_send_meeting_message": _can_user_send_meeting_message(
            db,
            meeting,
            current_user.id,
            attendance_row=attendance_row,
        ),
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/conclusion")
async def meeting_save_conclusion(
    group_id: str,
    request: Request,
    content: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được cập nhật kết luận cuộc họp.")

    clean_content = (content or "").strip()
    if not clean_content:
        raise HTTPException(status_code=400, detail="Nội dung kết luận cuộc họp không được để trống.")

    latest_row = _get_latest_meeting_conclusion_message(db, group_id)
    if latest_row:
        latest_row.content = clean_content
        latest_row.updated_at = datetime.utcnow()
        db.add(latest_row)
        db.commit()
        db.refresh(latest_row)
        row = latest_row
    else:
        row = create_message(
            db,
            group_id=group_id,
            sender_user_id=current_user.id,
            content=clean_content,
            message_type="MEETING_CONCLUSION",
        )

    payload = {
        "type": "meeting_conclusion_updated",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "conclusion_text": row.content or "",
        "updated_at_text": _format_vn_dt_text(getattr(row, "updated_at", None) or getattr(row, "created_at", None)),
        "updated_by_user_id": current_user.id,
        "updated_by_name": get_display_name(current_user),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), payload)
    return JSONResponse({"ok": True, **payload})

@router.get("/meetings/{group_id}/minutes.txt")
def export_meeting_minutes_txt(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    selected_group = get_group_by_id(db, group_id)
    if not selected_group or selected_group.group_type != "MEETING":
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không có quyền xem biên bản cuộc họp này.")

    selected_meeting = get_meeting_by_group_id(db, selected_group.id)
    selected_meeting = _ensure_meeting_runtime_rules(db, selected_meeting)
    if not selected_meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy dữ liệu cuộc họp.")

    if selected_meeting.secretary_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Thư ký cuộc họp mới được xuất biên bản.")

    members = get_group_members(db, selected_group.id)
    attendance_rows = get_meeting_attendance_rows(db, selected_meeting.id)
    attendance_map = {row.user_id: row for row in attendance_rows}
    speaker_requests = list_speaker_requests(db, selected_meeting.id)

    all_messages = [
        _build_message_vm(db, msg, current_user.id)
        for msg in get_group_messages(db, selected_group.id, limit=1000)
    ]
    messages = [
        msg for msg in all_messages
        if (msg.get("message_type") or "").upper() not in {"MEETING_DOC", "MEETING_CONCLUSION"}
    ]

    current_attendance = attendance_map.get(current_user.id)
    host = db.get(Users, selected_meeting.host_user_id) if selected_meeting.host_user_id else None
    secretary = db.get(Users, selected_meeting.secretary_user_id) if selected_meeting.secretary_user_id else None
    designed_by = db.get(Users, selected_meeting.designed_by_user_id) if selected_meeting.designed_by_user_id else None

    for row in attendance_rows:
        row.attendance_status_label = _attendance_status_label(row.attendance_status)
        row.presence_status_label = "Đang ở phòng" if (row.presence_status or "").upper() == "ONLINE" else "Ngoài phòng"

    current_attendance_status = (getattr(current_attendance, "attendance_status", "") or "PENDING").upper()

    attendance_pending_count = 0
    attendance_absent_count = 0
    attendance_checked_in_count = 0
    for row in attendance_rows:
        status = (getattr(row, "attendance_status", "") or "PENDING").upper()
        if status == "ABSENT":
            attendance_absent_count += 1
        elif status == "CHECKED_IN":
            attendance_checked_in_count += 1
        else:
            attendance_pending_count += 1

    latest_conclusion_message = _get_latest_meeting_conclusion_message(db, selected_group.id)
    conclusion_text = (getattr(latest_conclusion_message, "content", "") or "").strip()
    conclusion_updated_text = ""
    if getattr(latest_conclusion_message, "created_at", None):
        conclusion_updated_text = _format_vn_dt_text(latest_conclusion_message.created_at)

    detail = {
        "group": selected_group,
        "meeting": selected_meeting,
        "messages": messages,
        "meeting_documents": _build_meeting_documents(all_messages),
        "members": members,
        "attendance_rows": attendance_rows,
        "speaker_requests": speaker_requests,
        "host": host,
        "secretary": secretary,
        "designed_by": designed_by,
        "status_label": _meeting_status_label(selected_meeting.meeting_status),
        "scope_label": _meeting_scope_label(selected_meeting.meeting_scope),
        "current_attendance_status": current_attendance_status,
        "attendance_pending_count": attendance_pending_count,
        "attendance_absent_count": attendance_absent_count,
        "attendance_checked_in_count": attendance_checked_in_count,
        "member_ids": [m.user_id for m in members],
        "conclusion_text": conclusion_text,
        "conclusion_updated_text": conclusion_updated_text,
    }

    text_content = _build_meeting_minutes_text(detail)
    file_name = f"bien_ban_hop_{group_id}.txt"
    headers = {
        "Content-Disposition": f'attachment; filename="{file_name}"'
    }
    return PlainTextResponse(text_content, headers=headers)

    
@router.post("/meetings/{group_id}/presence/join")
async def meeting_presence_join(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này hoặc đã bị loại khỏi cuộc họp do báo vắng.")
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    row = set_meeting_presence(db, meeting.id, current_user.id, True)
    payload = {
        "type": "meeting_presence_joined",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "meeting_status": meeting.meeting_status,
        "meeting_status_label": _meeting_status_label(meeting.meeting_status),
        "action_mode": "checkin" if meeting.meeting_status == "LIVE" else "absent",
        "attendance_status": getattr(row, "attendance_status", "PENDING"),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/presence/leave")
async def meeting_presence_leave(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    set_meeting_presence(db, meeting.id, current_user.id, False)
    payload = {
        "type": "meeting_presence_left",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/sync")
async def meeting_sync(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    current_attendance = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    current_attendance_status = (getattr(current_attendance, "attendance_status", "") or "PENDING").upper()

    if meeting.meeting_status == "LIVE":
        action_mode = "checkin"
    elif meeting.meeting_status == "UPCOMING":
        action_mode = "absent"
    else:
        action_mode = "closed"

    can_send_meeting_message = _can_user_send_meeting_message(
        db,
        meeting,
        current_user.id,
        attendance_row=current_attendance,
    )

    payload = {
        "type": "meeting_status_sync",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "meeting_status": meeting.meeting_status,
        "meeting_status_label": _meeting_status_label(meeting.meeting_status),
        "action_mode": action_mode,
        "current_attendance_status": current_attendance_status,
        "can_register_speaker": meeting.meeting_status == "LIVE" and current_attendance_status != "ABSENT" and not can_send_meeting_message,
        "can_send_meeting_message": can_send_meeting_message,
    }
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/absent")
async def meeting_absent(group_id: str, request: Request, reason: str = Form(""), db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    if meeting.meeting_status != "UPCOMING":
        raise HTTPException(status_code=400, detail="Chỉ được báo vắng trước giờ họp.")

    row = mark_meeting_absent(db, meeting.id, current_user.id, reason=reason)
    if current_user.id == meeting.host_user_id:
        meeting = auto_assign_meeting_host(db, meeting.id) or meeting   
    payload = {
        "type": "meeting_absent_reported",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "attendance_status": getattr(row, "attendance_status", "ABSENT"),
        "host_user_id": getattr(meeting, "host_user_id", None),        
        "attendance_status_label": _attendance_status_label(getattr(row, "attendance_status", "ABSENT")),
        "reason": (reason or "").strip(),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/absent/cancel")
async def meeting_cancel_absent(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    if meeting.meeting_status != "UPCOMING":
        raise HTTPException(status_code=400, detail="Chỉ được hủy báo vắng trước giờ họp.")

    row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy trạng thái tham dự của bạn.")

    row.attendance_status = "PENDING"
    row.absent_reason = None
    db.add(row)
    db.commit()
    db.refresh(row)

    payload = {
        "type": "meeting_absent_cancelled",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "attendance_status": getattr(row, "attendance_status", "PENDING"),
        "attendance_status_label": _attendance_status_label(getattr(row, "attendance_status", "PENDING")),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})
    
    
@router.post("/meetings/{group_id}/checkin")
async def meeting_checkin(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")
    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    row = mark_meeting_checkin(db, meeting.id, current_user.id)
    payload = {
        "type": "meeting_checkin_done",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "attendance_status": getattr(row, "attendance_status", "CHECKED_IN"),
        "attendance_status_label": _attendance_status_label(getattr(row, "attendance_status", "CHECKED_IN")),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/speaker/request")
async def meeting_speaker_request(group_id: str, request: Request, note: str = Form(""), db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này hoặc đã bị loại khỏi cuộc họp do báo vắng.")
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    if meeting.meeting_status != "LIVE":
        raise HTTPException(status_code=400, detail="Chỉ được đăng ký phát biểu khi cuộc họp đang diễn ra.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    if attendance_status == "ABSENT":
        raise HTTPException(status_code=400, detail="Bạn đã báo vắng nên không được đăng ký phát biểu.")

    row = create_speaker_request(db, meeting.id, current_user.id, note=note)
    payload = {
        "type": "meeting_speaker_requested",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "speaker_request_id": row.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    notify_ids = get_group_member_user_ids(db, group_id)
    await manager.notify_users_json(notify_ids, {"module": "meeting", **payload})
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/speaker/{speaker_request_id}/approve")
async def meeting_speaker_approve(group_id: str, speaker_request_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting or meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được cho phép phát biểu.")
    row = approve_speaker_request(db, speaker_request_id, current_user.id)
    if not row or row.meeting_id != meeting.id:
        raise HTTPException(status_code=404, detail="Không tìm thấy đăng ký phát biểu.")
    payload = {
        "type": "meeting_speaker_approved",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "speaker_request_id": row.id,
        "user_id": row.user_id,
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/speaker/{speaker_request_id}/move")
async def meeting_speaker_move(
    group_id: str,
    speaker_request_id: str,
    request: Request,
    direction: str = Form(...),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting or meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được sắp xếp thứ tự phát biểu.")
    row = move_speaker_request(db, speaker_request_id, (direction or "").strip().lower())
    if not row or row.meeting_id != meeting.id:
        raise HTTPException(status_code=404, detail="Không tìm thấy đăng ký phát biểu.")
    payload = {
        "type": "meeting_speaker_reordered",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "speaker_request_id": row.id,
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/host")
def meeting_assign_host(
    group_id: str,
    request: Request,
    host_user_id: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not _can_assign_meeting_host(meeting, current_user.id):
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì hoặc Người thiết kế mới được trao quyền Chủ trì.")

    clean_host_user_id = (host_user_id or "").strip()
    if not clean_host_user_id:
        raise HTTPException(status_code=400, detail="Chưa chọn người nhận quyền Chủ trì.")

    if not is_group_member(db, group_id, clean_host_user_id):
        raise HTTPException(status_code=400, detail="Người được chọn không còn thuộc cuộc họp.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, clean_host_user_id)
    attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    if attendance_status == "ABSENT":
        raise HTTPException(status_code=400, detail="Không thể trao quyền Chủ trì cho người đã báo vắng.")

    updated = assign_meeting_host(db, meeting.id, clean_host_user_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    return RedirectResponse(url=f"/meetings?selected_id={group_id}", status_code=303)
    
    
@router.post("/meetings/{group_id}/secretary")
def meeting_assign_secretary(group_id: str, request: Request, secretary_user_id: str = Form(""), db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting or meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được chỉ định thư ký.")
    updated = assign_meeting_secretary(db, meeting.id, secretary_user_id or None)
    if not updated:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    return RedirectResponse(url=f"/meetings?selected_id={group_id}", status_code=303)
