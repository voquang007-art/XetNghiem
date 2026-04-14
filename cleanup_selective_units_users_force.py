import sys
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime


def find_db_path() -> Path:
    if len(sys.argv) > 1:
        p = Path(sys.argv[1]).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Không tìm thấy DB: {p}")
        return p

    instance_dir = Path.cwd() / "instance"
    if not instance_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục instance: {instance_dir}")

    candidates = []
    for pattern in ("*.sqlite3", "*.db", "*.sqlite"):
        candidates.extend(instance_dir.glob(pattern))

    if not candidates:
        raise FileNotFoundError(f"Không tìm thấy file DB trong: {instance_dir}")

    preferred = []
    for p in candidates:
        name = p.name.lower()
        score = 0
        if "workxetnghiem" in name:
            score += 100
        if "sqlite3" in name:
            score += 10
        preferred.append((score, p))

    preferred.sort(key=lambda x: (-x[0], x[1].name))
    return preferred[0][1].resolve()


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.stem}_backup_FORCE_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def q_all(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def q_one(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    return row


def exec_sql(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cur.close()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = q_one(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return row is not None


def get_columns(conn: sqlite3.Connection, table_name: str):
    try:
        rows = q_all(conn, f"PRAGMA table_info({table_name})")
        return [r[1] for r in rows]
    except Exception:
        return []


def parse_indexes(raw: str, max_index: int):
    raw = (raw or "").strip()
    if not raw:
        return []

    selected = set()
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    for part in parts:
        if "-" in part:
            a, b = part.split("-", 1)
            if a.strip().isdigit() and b.strip().isdigit():
                start = int(a.strip())
                end = int(b.strip())
                if start > end:
                    start, end = end, start
                for i in range(start, end + 1):
                    if 1 <= i <= max_index:
                        selected.add(i)
        else:
            if part.isdigit():
                i = int(part)
                if 1 <= i <= max_index:
                    selected.add(i)
    return sorted(selected)


def print_title(title: str):
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)


def list_units(conn: sqlite3.Connection, level: int):
    return q_all(
        conn,
        """
        SELECT
            u.id,
            u.ten_don_vi,
            u.cap_do,
            COALESCE(p.ten_don_vi, '') AS parent_name,
            COALESCE(u.trang_thai, '') AS trang_thai,
            COALESCE(u.order_index, 0) AS order_index
        FROM units u
        LEFT JOIN units p ON p.id = u.parent_id
        WHERE u.cap_do = ?
        ORDER BY u.ten_don_vi, u.order_index, u.id
        """,
        (level,),
    )


def list_users(conn: sqlite3.Connection):
    return q_all(
        conn,
        """
        SELECT
            u.id,
            u.username,
            COALESCE(u.full_name, '') AS full_name,
            COALESCE(u.status, '') AS status,
            COALESCE(u.created_at, '') AS created_at
        FROM users u
        ORDER BY u.username
        """
    )


def count_unit_refs(conn: sqlite3.Connection, unit_id: str):
    child_count = q_one(conn, "SELECT COUNT(*) FROM units WHERE parent_id = ?", (unit_id,))[0]
    membership_count = q_one(conn, "SELECT COUNT(*) FROM user_unit_memberships WHERE unit_id = ?", (unit_id,))[0]

    plan_count = 0
    task_count = 0
    file_count = 0

    if table_exists(conn, "plans"):
        try:
            plan_count = q_one(conn, "SELECT COUNT(*) FROM plans WHERE unit_id = ?", (unit_id,))[0]
        except Exception:
            plan_count = 0

    if table_exists(conn, "tasks"):
        try:
            task_count = q_one(conn, "SELECT COUNT(*) FROM tasks WHERE unit_id = ?", (unit_id,))[0]
        except Exception:
            task_count = 0

    if table_exists(conn, "files"):
        try:
            file_count = q_one(conn, "SELECT COUNT(*) FROM files WHERE unit_id = ?", (unit_id,))[0]
        except Exception:
            file_count = 0

    return {
        "children": child_count,
        "memberships": membership_count,
        "plans": plan_count,
        "tasks": task_count,
        "files": file_count,
    }


def count_user_refs(conn: sqlite3.Connection, user_id: str):
    memberships = q_one(conn, "SELECT COUNT(*) FROM user_unit_memberships WHERE user_id = ?", (user_id,))[0]
    roles = q_one(conn, "SELECT COUNT(*) FROM user_roles WHERE user_id = ?", (user_id,))[0]

    plans = tasks = reports = 0

    if table_exists(conn, "plans"):
        try:
            plans = q_one(conn, "SELECT COUNT(*) FROM plans WHERE created_by = ?", (user_id,))[0]
        except Exception:
            plans = 0

    if table_exists(conn, "tasks"):
        task_cols = get_columns(conn, "tasks")
        for col in (
            "created_by",
            "creator_user_id",
            "owner_user_id",
            "assignee_id",
            "assigned_user_id",
            "assigned_to_user_id",
            "receiver_user_id",
        ):
            if col in task_cols:
                try:
                    tasks += q_one(conn, f"SELECT COUNT(*) FROM tasks WHERE {col} = ?", (user_id,))[0]
                except Exception:
                    pass

    if table_exists(conn, "task_reports"):
        report_cols = get_columns(conn, "task_reports")
        for col in ("user_id", "created_by"):
            if col in report_cols:
                try:
                    reports += q_one(conn, f"SELECT COUNT(*) FROM task_reports WHERE {col} = ?", (user_id,))[0]
                except Exception:
                    pass

    return {
        "memberships": memberships,
        "roles": roles,
        "plans": plans,
        "tasks": tasks,
        "reports": reports,
    }


def force_delete_plan_children(conn: sqlite3.Connection, plan_ids):
    if not plan_ids:
        return

    placeholders = ",".join("?" for _ in plan_ids)

    if table_exists(conn, "plan_items"):
        exec_sql(conn, f"DELETE FROM plan_items WHERE plan_id IN ({placeholders})", tuple(plan_ids))


def force_delete_task_children(conn: sqlite3.Connection, task_ids):
    if not task_ids:
        return

    placeholders = ",".join("?" for _ in task_ids)

    if table_exists(conn, "task_reports"):
        cols = get_columns(conn, "task_reports")
        if "task_id" in cols:
            exec_sql(conn, f"DELETE FROM task_reports WHERE task_id IN ({placeholders})", tuple(task_ids))

    if table_exists(conn, "files"):
        cols = get_columns(conn, "files")
        for fk_col in ("task_id", "related_task_id", "file_task_id"):
            if fk_col in cols:
                exec_sql(conn, f"DELETE FROM files WHERE {fk_col} IN ({placeholders})", tuple(task_ids))


def collect_descendant_unit_ids(conn: sqlite3.Connection, unit_id: str):
    seen = set()
    pending = [unit_id]

    while pending:
        current = pending.pop(0)
        if current in seen:
            continue
        seen.add(current)
        children = q_all(conn, "SELECT id FROM units WHERE parent_id = ?", (current,))
        for row in children:
            child_id = row[0]
            if child_id and child_id not in seen:
                pending.append(child_id)

    return list(seen)


def force_delete_unit(conn: sqlite3.Connection, unit_id: str):
    target_unit_ids = collect_descendant_unit_ids(conn, unit_id)

    placeholders = ",".join("?" for _ in target_unit_ids)

    # plans
    if table_exists(conn, "plans"):
        plan_rows = q_all(
            conn,
            f"SELECT id FROM plans WHERE unit_id IN ({placeholders})",
            tuple(target_unit_ids),
        )
        plan_ids = [r[0] for r in plan_rows if r and r[0]]
        force_delete_plan_children(conn, plan_ids)
        if plan_ids:
            plan_ph = ",".join("?" for _ in plan_ids)
            exec_sql(conn, f"DELETE FROM plans WHERE id IN ({plan_ph})", tuple(plan_ids))

    # tasks
    if table_exists(conn, "tasks"):
        task_rows = q_all(
            conn,
            f"SELECT id FROM tasks WHERE unit_id IN ({placeholders})",
            tuple(target_unit_ids),
        )
        task_ids = [r[0] for r in task_rows if r and r[0]]
        force_delete_task_children(conn, task_ids)
        if task_ids:
            task_ph = ",".join("?" for _ in task_ids)
            exec_sql(conn, f"DELETE FROM tasks WHERE id IN ({task_ph})", tuple(task_ids))

    # files gắn trực tiếp unit
    if table_exists(conn, "files"):
        cols = get_columns(conn, "files")
        if "unit_id" in cols:
            exec_sql(conn, f"DELETE FROM files WHERE unit_id IN ({placeholders})", tuple(target_unit_ids))

    # memberships
    exec_sql(conn, f"DELETE FROM user_unit_memberships WHERE unit_id IN ({placeholders})", tuple(target_unit_ids))

    # xóa units con trước, unit cha sau
    ordered = sorted(target_unit_ids, key=lambda x: 0 if x != unit_id else 1)
    for uid in ordered:
        if uid != unit_id:
            exec_sql(conn, "DELETE FROM units WHERE id = ?", (uid,))
    exec_sql(conn, "DELETE FROM units WHERE id = ?", (unit_id,))


def force_delete_user(conn: sqlite3.Connection, user_id: str):
    row = q_one(conn, "SELECT username FROM users WHERE id = ?", (user_id,))
    if not row:
        raise RuntimeError("Không tìm thấy user.")

    username = str(row[0] or "").strip().lower()
    if username == "admin":
        raise RuntimeError("Không được xóa user admin.")

    # plans do user tạo
    if table_exists(conn, "plans"):
        plan_rows = q_all(conn, "SELECT id FROM plans WHERE created_by = ?", (user_id,))
        plan_ids = [r[0] for r in plan_rows if r and r[0]]
        force_delete_plan_children(conn, plan_ids)
        if plan_ids:
            ph = ",".join("?" for _ in plan_ids)
            exec_sql(conn, f"DELETE FROM plans WHERE id IN ({ph})", tuple(plan_ids))

    # task_reports do user tạo
    if table_exists(conn, "task_reports"):
        cols = get_columns(conn, "task_reports")
        for col in ("user_id", "created_by"):
            if col in cols:
                exec_sql(conn, f"DELETE FROM task_reports WHERE {col} = ?", (user_id,))

    # tasks có user tham gia
    if table_exists(conn, "tasks"):
        task_cols = get_columns(conn, "tasks")
        related_task_ids = set()

        for col in (
            "created_by",
            "creator_user_id",
            "owner_user_id",
            "assignee_id",
            "assigned_user_id",
            "assigned_to_user_id",
            "receiver_user_id",
        ):
            if col in task_cols:
                rows = q_all(conn, f"SELECT id FROM tasks WHERE {col} = ?", (user_id,))
                for r in rows:
                    if r and r[0]:
                        related_task_ids.add(r[0])

        related_task_ids = list(related_task_ids)
        force_delete_task_children(conn, related_task_ids)
        if related_task_ids:
            ph = ",".join("?" for _ in related_task_ids)
            exec_sql(conn, f"DELETE FROM tasks WHERE id IN ({ph})", tuple(related_task_ids))

    # membership / roles
    exec_sql(conn, "DELETE FROM user_unit_memberships WHERE user_id = ?", (user_id,))
    exec_sql(conn, "DELETE FROM user_roles WHERE user_id = ?", (user_id,))
    exec_sql(conn, "DELETE FROM users WHERE id = ?", (user_id,))


def menu():
    print("\nChọn chức năng:")
    print("1. Xem Unit cấp 2")
    print("2. Xem Unit cấp 3")
    print("3. Xem User")
    print("4. FORCE xóa Unit cấp 2 theo chọn")
    print("5. FORCE xóa Unit cấp 3 theo chọn")
    print("6. FORCE xóa User theo chọn")
    print("0. Thoát")


def show_units(conn: sqlite3.Connection, level: int):
    rows = list_units(conn, level)
    print_title(f"DANH SÁCH UNIT CẤP {level}")
    for i, row in enumerate(rows, start=1):
        unit_id, ten_don_vi, cap_do, parent_name, trang_thai, order_index = row
        refs = count_unit_refs(conn, unit_id)
        print(
            f"{i:>3}. {ten_don_vi} | cap_do={cap_do} | parent={parent_name} | "
            f"status={trang_thai} | order={order_index} | "
            f"child={refs['children']} | mem={refs['memberships']} | "
            f"plans={refs['plans']} | tasks={refs['tasks']} | files={refs['files']} | id={unit_id}"
        )
    print(f"\nTổng: {len(rows)}")
    return rows


def show_users(conn: sqlite3.Connection):
    rows = list_users(conn)
    print_title("DANH SÁCH USER")
    for i, row in enumerate(rows, start=1):
        user_id, username, full_name, status, created_at = row
        refs = count_user_refs(conn, user_id)
        print(
            f"{i:>3}. {username} | {full_name} | status={status} | "
            f"memberships={refs['memberships']} | roles={refs['roles']} | "
            f"plans={refs['plans']} | tasks={refs['tasks']} | reports={refs['reports']} | "
            f"id={user_id}"
        )
    print(f"\nTổng: {len(rows)}")
    return rows


def force_delete_selected_units(conn: sqlite3.Connection, level: int):
    rows = show_units(conn, level)
    if not rows:
        return

    raw = input("\nNhập số thứ tự cần FORCE xóa (ví dụ 1,3,5 hoặc 2-4): ").strip()
    idxs = parse_indexes(raw, len(rows))
    if not idxs:
        print("Không có lựa chọn hợp lệ.")
        return

    print_title("CÁC UNIT SẼ FORCE XÓA")
    selected = []
    for idx in idxs:
        row = rows[idx - 1]
        selected.append(row)
        print(f"{idx}. {row[1]} | id={row[0]}")

    confirm = input("\nGõ FORCE YES để xác nhận xóa mạnh tay: ").strip()
    if confirm != "FORCE YES":
        print("Đã hủy.")
        return

    deleted = 0
    failed = 0
    for row in selected:
        unit_id = row[0]
        unit_name = row[1]
        try:
            force_delete_unit(conn, unit_id)
            print(f"Đã FORCE xóa unit: {unit_name} | {unit_id}")
            deleted += 1
        except Exception as ex:
            print(f"Không FORCE xóa được unit: {unit_name} | {unit_id} | Lý do: {ex}")
            failed += 1

    conn.commit()
    print(f"\nKết quả: xóa thành công {deleted}, lỗi {failed}.")


def force_delete_selected_users(conn: sqlite3.Connection):
    rows = show_users(conn)
    if not rows:
        return

    raw = input("\nNhập số thứ tự user cần FORCE xóa (ví dụ 1,3,5 hoặc 2-4): ").strip()
    idxs = parse_indexes(raw, len(rows))
    if not idxs:
        print("Không có lựa chọn hợp lệ.")
        return

    print_title("CÁC USER SẼ FORCE XÓA")
    selected = []
    for idx in idxs:
        row = rows[idx - 1]
        selected.append(row)
        print(f"{idx}. {row[1]} | {row[2]} | id={row[0]}")

    confirm = input("\nGõ FORCE YES để xác nhận xóa mạnh tay: ").strip()
    if confirm != "FORCE YES":
        print("Đã hủy.")
        return

    deleted = 0
    failed = 0
    for row in selected:
        user_id = row[0]
        username = row[1]
        try:
            force_delete_user(conn, user_id)
            print(f"Đã FORCE xóa user: {username} | {user_id}")
            deleted += 1
        except Exception as ex:
            print(f"Không FORCE xóa được user: {username} | {user_id} | Lý do: {ex}")
            failed += 1

    conn.commit()
    print(f"\nKết quả: xóa thành công {deleted}, lỗi {failed}.")


def main():
    db_path = find_db_path()
    print(f"DB đang dùng: {db_path}")

    backup_path = backup_db(db_path)
    print(f"Đã backup DB tại: {backup_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        while True:
            menu()
            choice = input("Nhập lựa chọn: ").strip()

            if choice == "1":
                show_units(conn, 2)
            elif choice == "2":
                show_units(conn, 3)
            elif choice == "3":
                show_users(conn)
            elif choice == "4":
                force_delete_selected_units(conn, 2)
            elif choice == "5":
                force_delete_selected_units(conn, 3)
            elif choice == "6":
                force_delete_selected_users(conn)
            elif choice == "0":
                print("Thoát.")
                break
            else:
                print("Lựa chọn không hợp lệ.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()