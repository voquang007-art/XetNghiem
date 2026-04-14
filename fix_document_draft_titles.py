from __future__ import annotations

import re
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


# =========================
# CẤU HÌNH
# =========================
DB_PATH = r"C:\HVGL_workXetnghiem\instance\workxetnghiem.sqlite3"
# Ví dụ:
# DB_PATH = r"C:\HVGL_WorkXetnghiem\instance\workxetnghiem.sqlite3"

TABLE_NAME = "document_drafts"
TITLE_COL = "title"
ID_COL = "id"


# =========================
# CÁC LỖI ĐÃ BIẾT -> SỬA TỰ ĐỘNG
# Anh có thể bổ sung thêm vào đây
# =========================
EXACT_REPLACEMENTS = {
    "Quy trunhf": "Quy trình",
    "Quy trinhf": "Quy trình",
    "Quy trihf": "Quy trình",
    "Quy trinh": "Quy trình",   # nếu anh muốn chuẩn hóa luôn dấu
}


# =========================
# MẪU NGHI NGỜ GÕ SAI KIỂU TELEX CHƯA THOÁT
# Chỉ để rà, KHÔNG tự sửa bừa
# =========================
SUSPECT_PATTERNS = [
    r"[a-zA-ZÀ-ỹ]+[fsrxj]\b",      # ví dụ: trunhf, nghieepj...
    r"\bquy\s+trunhf\b",
    r"\btrinhf\b",
    r"\bduj\b",
    r"\bthaor\b",
    r"\bvawn\b",
    r"\bvawn\b",
    r"\bphoois\b",
    r"\bhopwj\b",
]


def make_backup(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.stem}_backup_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def find_suspects(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT {ID_COL}, {TITLE_COL}
        FROM {TABLE_NAME}
        WHERE IFNULL(is_deleted, 0) = 0
        ORDER BY updated_at DESC, created_at DESC
        """
    )
    rows = cur.fetchall()

    suspects: list[tuple[str, str]] = []
    for row_id, title in rows:
        title = (title or "").strip()
        if not title:
            continue

        matched = False
        for pattern in SUSPECT_PATTERNS:
            if re.search(pattern, title, flags=re.IGNORECASE):
                matched = True
                break

        if matched:
            suspects.append((str(row_id), title))

    return suspects


def apply_exact_replacements(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT {ID_COL}, {TITLE_COL}
        FROM {TABLE_NAME}
        WHERE IFNULL(is_deleted, 0) = 0
        """
    )
    rows = cur.fetchall()

    changed: list[tuple[str, str, str]] = []

    for row_id, old_title in rows:
        old_title = (old_title or "").strip()
        if not old_title:
            continue

        new_title = EXACT_REPLACEMENTS.get(old_title, old_title)

        if new_title != old_title:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET {TITLE_COL} = ?, updated_at = CURRENT_TIMESTAMP
                WHERE {ID_COL} = ?
                """,
                (new_title, row_id),
            )
            changed.append((str(row_id), old_title, new_title))

    conn.commit()
    return changed


def main() -> None:
    db_path = Path(DB_PATH)

    if not db_path.exists():
        print(f"[LỖI] Không tìm thấy file DB: {db_path}")
        return

    backup_path = make_backup(db_path)
    print(f"[OK] Đã backup DB: {backup_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        changed = apply_exact_replacements(conn)

        print("\n=== KẾT QUẢ SỬA TỰ ĐỘNG ===")
        if not changed:
            print("Không có tiêu đề nào khớp bảng thay thế EXACT_REPLACEMENTS.")
        else:
            for row_id, old_title, new_title in changed:
                print(f"- ID: {row_id}")
                print(f"  CŨ : {old_title}")
                print(f"  MỚI: {new_title}")

        suspects = find_suspects(conn)

        print("\n=== DANH SÁCH TIÊU ĐỀ NGHI NGỜ CÒN SAI ===")
        if not suspects:
            print("Không phát hiện thêm tiêu đề nghi ngờ.")
        else:
            for row_id, title in suspects:
                print(f"- ID: {row_id} | TITLE: {title}")

        print("\n[HOÀN TẤT] Script đã chạy xong.")
        print("Anh mở lại ứng dụng/web rồi kiểm tra tab Phê duyệt dự thảo văn bản.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()