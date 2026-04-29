#!/usr/bin/env python3
from __future__ import annotations

import ctypes
import json
import os
import queue
import re
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
from ctypes import wintypes
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator, Optional

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

APP_TITLE = "IS Search"
APP_DIR = Path.home() / "AppData" / "Local" / "ISSearch" if os.name == "nt" else Path.home() / ".is_search"
DB_PATH = APP_DIR / "is_search_index.sqlite3"
LOG_PATH = APP_DIR / "is_search.log"
SETTINGS_PATH = APP_DIR / "settings.json"


def resource_path(*parts: str) -> Path:
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return base.joinpath(*parts)


IS_LOGO_PATH = resource_path("Logo", "IS.png")
Q_LOGO_PATH = resource_path("Logo", "Q Grey Logo.png")
Q_ICON_PATH = resource_path("Logo", "Q Grey Logo.ico")

FILE_ATTRIBUTE_HIDDEN = 0x2
FILE_ATTRIBUTE_SYSTEM = 0x4
INVALID_FILE_ATTRIBUTES = 0xFFFFFFFF
PROCESS_TERMINATE = 0x0001
TH32CS_SNAPPROCESS = 0x00000002
WM_CLOSE = 0x0010
PROCESS_SNAPSHOT_INVALID_HANDLE = ctypes.c_void_p(-1).value


class PROCESSENTRY32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wintypes.DWORD),
        ("cntUsage", wintypes.DWORD),
        ("th32ProcessID", wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t),
        ("th32ModuleID", wintypes.DWORD),
        ("cntThreads", wintypes.DWORD),
        ("th32ParentProcessID", wintypes.DWORD),
        ("pcPriClassBase", wintypes.LONG),
        ("dwFlags", wintypes.DWORD),
        ("szExeFile", wintypes.WCHAR * 260),
    ]

WINDOWS_EXCLUDED_PREFIXES = [
    r"C:\Windows\WinSxS",
    r"C:\Windows\Installer",
    r"C:\System Volume Information",
    r"C:\$Recycle.Bin",
]


def is_windows() -> bool:
    return os.name == "nt"


def set_windows_app_id() -> None:
    if not is_windows():
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("Quanta.ISSearch")
    except Exception:
        pass


def find_other_is_search_pids() -> set[int]:
    if not is_windows():
        return set()

    kernel32 = ctypes.windll.kernel32
    kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32FirstW.restype = wintypes.BOOL
    kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(PROCESSENTRY32W)]
    kernel32.Process32NextW.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    snapshot = kernel32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
    if snapshot in (None, PROCESS_SNAPSHOT_INVALID_HANDLE):
        return set()

    current_pid = os.getpid()
    pids: set[int] = set()
    entry = PROCESSENTRY32W()
    entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)

    try:
        has_entry = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
        while has_entry:
            pid = int(entry.th32ProcessID)
            exe_name = str(entry.szExeFile).lower()
            if pid != current_pid and exe_name == "is_search.exe":
                pids.add(pid)
            has_entry = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
    finally:
        kernel32.CloseHandle(snapshot)

    return pids


def request_process_windows_close(pids: set[int]) -> None:
    if not is_windows() or not pids:
        return

    user32 = ctypes.windll.user32
    user32.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    user32.GetWindowThreadProcessId.restype = wintypes.DWORD
    user32.PostMessageW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
    user32.PostMessageW.restype = wintypes.BOOL

    @ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    def enum_window(hwnd, _lparam):
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        if int(pid.value) in pids:
            user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)
        return True

    user32.EnumWindows(enum_window, 0)


def terminate_processes(pids: set[int]) -> int:
    if not is_windows() or not pids:
        return 0

    kernel32 = ctypes.windll.kernel32
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    closed = 0
    for pid in pids:
        handle = kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
        if not handle:
            continue
        try:
            if kernel32.TerminateProcess(handle, 0):
                closed += 1
        finally:
            kernel32.CloseHandle(handle)
    return closed


def close_other_is_search_instances() -> None:
    other_pids = find_other_is_search_pids()
    if not other_pids:
        return

    request_process_windows_close(other_pids)
    time.sleep(1.5)

    remaining_pids = find_other_is_search_pids()
    terminated = terminate_processes(remaining_pids)
    write_log(
        f"Closed {len(other_pids) - len(remaining_pids)} existing IS_Search.exe instance(s); "
        f"terminated {terminated} remaining instance(s)."
    )


def ensure_app_dir() -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)


def write_log(message: str) -> None:
    ensure_app_dir()
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with LOG_PATH.open("a", encoding="utf-8", errors="ignore") as f:
        f.write(f"[{stamp}] {message}\n")


def get_detected_roots() -> list[str]:
    if is_windows():
        try:
            roots = []
            bitmask = ctypes.windll.kernel32.GetLogicalDrives()
            for i in range(26):
                if bitmask & (1 << i):
                    roots.append(f"{chr(65+i)}:\\")
            return roots or ["C:\\"]
        except Exception:
            return ["C:\\"]
    return ["/"]


def load_saved_default_roots() -> list[str]:
    try:
        if not SETTINGS_PATH.exists():
            return []
        with SETTINGS_PATH.open("r", encoding="utf-8") as f:
            settings = json.load(f)
        roots = settings.get("default_roots", [])
        if not isinstance(roots, list):
            return []
        return split_roots(";".join(str(root) for root in roots))
    except Exception as exc:
        write_log(f"Could not load saved default roots: {exc}")
        return []


def save_default_roots(roots: list[str]) -> None:
    ensure_app_dir()
    with SETTINGS_PATH.open("w", encoding="utf-8") as f:
        json.dump({"default_roots": roots}, f, indent=2)


def get_default_roots() -> list[str]:
    return load_saved_default_roots() or get_detected_roots()


def normalize_root(root: str) -> Optional[str]:
    root = (root or "").strip().strip('"').strip("'")
    if not root:
        return None
    if is_windows():
        if re.fullmatch(r"[A-Za-z]", root):
            root += ":\\"
        elif re.fullmatch(r"[A-Za-z]:", root):
            root += "\\"
        root = root.replace("/", "\\")
    else:
        root = os.path.expanduser(root)
    return root


def normalized_root_key(root: str) -> str:
    value = normalize_root(root) or root
    return value.lower() if is_windows() else value


def split_roots(text: str) -> list[str]:
    raw = [normalize_root(p) for p in (text or "").split(";")]
    out = []
    seen = set()
    for item in raw:
        if not item:
            continue
        key = normalized_root_key(item)
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def is_hidden_or_system(path: str) -> bool:
    if not is_windows():
        return os.path.basename(path.rstrip("\\/")).startswith(".")
    try:
        attrs = ctypes.windll.kernel32.GetFileAttributesW(ctypes.c_wchar_p(path))
        if attrs == INVALID_FILE_ATTRIBUTES:
            return False
        return bool(attrs & (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM))
    except Exception:
        return False


def open_path(path: str) -> None:
    if is_windows():
        os.startfile(path)  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])


def open_folder_for_path(path: str) -> None:
    folder = path if os.path.isdir(path) else os.path.dirname(path) or "."
    if is_windows():
        os.startfile(folder)  # type: ignore[attr-defined]
    else:
        open_path(folder)


def format_size(value: Optional[int]) -> str:
    if value is None:
        return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(value)
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{value} B"


def format_timestamp(ts: float) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def wildcard_to_regex(pattern: str) -> str:
    out = []
    for ch in pattern:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return "^" + "".join(out) + "$"


def wildcard_to_like(pattern: str) -> str:
    out = []
    for ch in pattern:
        if ch == "*":
            out.append("%")
        elif ch == "?":
            out.append("_")
        elif ch in ("%", "_", "\\"):
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def has_wildcards(text: str) -> bool:
    return "*" in text or "?" in text


def choose_query_mode(mode: str, pattern: str) -> str:
    if mode == "Auto":
        return "Wildcard" if has_wildcards(pattern) else "Regex"
    return mode


def extract_literal_hint(pattern: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9._ -]{2,}", pattern)
    if not tokens:
        return ""
    return max(tokens, key=len).strip()


def path_prefix_excluded(path: str) -> bool:
    if not is_windows():
        return False
    p = path.lower()
    for prefix in WINDOWS_EXCLUDED_PREFIXES:
        x = prefix.lower()
        if p == x or p.startswith(x + os.sep.lower()):
            return True
    return False


@dataclass
class SearchOptions:
    pattern: str
    roots: list[str]
    match_name_only: bool
    search_type: str
    include_hidden: bool
    search_content: bool
    case_sensitive: bool
    max_results: int
    max_file_size_mb: float
    query_mode: str
    source_mode: str


@dataclass
class SearchResult:
    match_type: str
    item_type: str
    name: str
    full_path: str
    size_bytes: Optional[int]
    modified: float


def compile_query_pattern(options: SearchOptions) -> re.Pattern:
    mode = choose_query_mode(options.query_mode, options.pattern)
    flags = 0 if options.case_sensitive else re.IGNORECASE
    source = wildcard_to_regex(options.pattern) if mode == "Wildcard" else options.pattern
    return re.compile(source, flags)


def live_search_iter(
    options: SearchOptions,
    regex: re.Pattern,
    cancel_event: threading.Event,
    status_cb: Optional[Callable[[str], None]] = None,
) -> Iterator[SearchResult]:
    count = 0
    for root in options.roots or get_default_roots():
        if cancel_event.is_set():
            break
        if not os.path.exists(root):
            if status_cb:
                status_cb(f"Skipping missing root: {root}")
            continue
        if status_cb:
            status_cb(f"Scanning {root}")
        stack = [root]
        while stack and not cancel_event.is_set():
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    entries = list(it)
            except Exception:
                continue

            for entry in entries:
                if cancel_event.is_set():
                    return
                path = entry.path
                if path_prefix_excluded(path):
                    continue
                if not options.include_hidden and is_hidden_or_system(path):
                    continue
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                except Exception:
                    is_dir = False
                if is_dir:
                    stack.append(path)

                if options.search_type == "File" and is_dir:
                    continue
                if options.search_type == "Directory" and not is_dir:
                    continue

                subject = entry.name if options.match_name_only else path
                path_match = bool(regex.search(subject))
                content_match = False
                size_bytes = None
                modified = 0.0

                try:
                    st = entry.stat(follow_symlinks=False)
                    modified = st.st_mtime
                    size_bytes = None if is_dir else st.st_size
                except Exception:
                    pass

                if options.search_content and not is_dir:
                    limit = int(options.max_file_size_mb * 1024 * 1024)
                    if size_bytes is not None and size_bytes <= limit:
                        try:
                            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                                for line in f:
                                    if regex.search(line):
                                        content_match = True
                                        break
                        except Exception:
                            pass

                if not (path_match or content_match):
                    continue

                yield SearchResult(
                    match_type="Path+Content" if path_match and content_match else ("Content" if content_match else "Live"),
                    item_type="Directory" if is_dir else "File",
                    name=entry.name,
                    full_path=path,
                    size_bytes=size_bytes,
                    modified=modified,
                )

                count += 1
                if options.max_results > 0 and count >= options.max_results:
                    cancel_event.set()
                    return


class IndexDB:
    def __init__(self, db_path: Path):
        ensure_app_dir()
        self.db_path = db_path
        self._init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=60, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.DatabaseError:
            pass
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        conn = self.connect()
        try:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS roots (
                root TEXT PRIMARY KEY,
                last_indexed REAL NOT NULL DEFAULT 0,
                item_count INTEGER NOT NULL DEFAULT 0,
                active_generation INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                root TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 1,
                full_path TEXT NOT NULL,
                name TEXT NOT NULL,
                extension TEXT,
                is_dir INTEGER NOT NULL,
                size_bytes INTEGER,
                modified REAL,
                hidden INTEGER NOT NULL DEFAULT 0
            );
            """)
            conn.commit()

            root_cols = {row[1] for row in conn.execute("PRAGMA table_info(roots)").fetchall()}
            item_cols = {row[1] for row in conn.execute("PRAGMA table_info(items)").fetchall()}

            if "active_generation" not in root_cols:
                conn.execute("ALTER TABLE roots ADD COLUMN active_generation INTEGER NOT NULL DEFAULT 0")
                conn.commit()

            if "generation" not in item_cols:
                conn.execute("ALTER TABLE items ADD COLUMN generation INTEGER NOT NULL DEFAULT 1")
                conn.commit()

            # Upgrade older databases so previously indexed rows still work.
            conn.execute("UPDATE items SET generation = 1 WHERE generation IS NULL OR generation = 0")
            conn.execute("""
                UPDATE roots
                   SET active_generation = 1
                 WHERE (active_generation IS NULL OR active_generation = 0)
                   AND EXISTS (SELECT 1 FROM items WHERE items.root = roots.root)
            """)
            conn.commit()

            conn.execute("DROP INDEX IF EXISTS idx_items_root_path")
            conn.execute("DROP INDEX IF EXISTS idx_items_root_gen_path")
            conn.execute("DROP INDEX IF EXISTS idx_items_name")
            conn.execute("DROP INDEX IF EXISTS idx_items_path")
            conn.execute("DROP INDEX IF EXISTS idx_items_root_type")
            conn.commit()

            conn.executescript("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_items_root_gen_path ON items(root, generation, full_path);
            CREATE INDEX IF NOT EXISTS idx_items_name ON items(name);
            CREATE INDEX IF NOT EXISTS idx_items_path ON items(full_path);
            CREATE INDEX IF NOT EXISTS idx_items_root_type ON items(root, generation, is_dir);
            """)
            conn.commit()
        finally:
            conn.close()

    def get_root_stats(self) -> list[sqlite3.Row]:
        conn = self.connect()
        try:
            return conn.execute(
                "SELECT root, last_indexed, item_count, active_generation FROM roots ORDER BY root"
            ).fetchall()
        finally:
            conn.close()

    def get_indexed_roots(self) -> set[str]:
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT root FROM roots WHERE active_generation > 0"
            ).fetchall()
            return {normalized_root_key(str(r["root"])) for r in rows}
        finally:
            conn.close()

    def partition_roots(self, roots: list[str]) -> tuple[list[str], list[str]]:
        indexed_keys = self.get_indexed_roots()
        indexed_roots: list[str] = []
        live_roots: list[str] = []
        for root in roots:
            norm = normalize_root(root) or root
            if normalized_root_key(norm) in indexed_keys:
                indexed_roots.append(norm)
            else:
                live_roots.append(norm)
        return indexed_roots, live_roots

    def rebuild_roots(
        self,
        roots: list[str],
        cancel_event: threading.Event,
        status_cb: Optional[Callable[[str], None]] = None,
        progress_cb: Optional[Callable[[str, int], None]] = None,
    ) -> None:
        conn = self.connect()
        try:
            for root in roots:
                if cancel_event.is_set():
                    break
                self._rebuild_one_root(conn, root, cancel_event, status_cb, progress_cb)
        finally:
            conn.close()

    def _rebuild_one_root(
        self,
        conn: sqlite3.Connection,
        root: str,
        cancel_event: threading.Event,
        status_cb: Optional[Callable[[str], None]],
        progress_cb: Optional[Callable[[str, int], None]],
    ) -> None:
        norm_root = normalize_root(root) or root
        if not os.path.exists(norm_root):
            if status_cb:
                status_cb(f"Skipping missing root: {norm_root}")
            return

        if status_cb:
            status_cb(f"Indexing {norm_root}")

        row = conn.execute(
            "SELECT active_generation FROM roots WHERE root = ?",
            (norm_root,),
        ).fetchone()
        old_generation = int(row["active_generation"]) if row else 0
        new_generation = old_generation + 1

        conn.execute(
            "DELETE FROM items WHERE root = ? AND generation = ?",
            (norm_root, new_generation),
        )
        conn.commit()

        inserted = 0
        pending: list[tuple] = []
        stack = [norm_root]

        while stack and not cancel_event.is_set():
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        if cancel_event.is_set():
                            break
                        path = entry.path
                        if path_prefix_excluded(path):
                            continue
                        try:
                            is_dir = entry.is_dir(follow_symlinks=False)
                        except Exception:
                            is_dir = False
                        if is_dir:
                            stack.append(path)
                        try:
                            st = entry.stat(follow_symlinks=False)
                            size_bytes = None if is_dir else int(st.st_size)
                            modified = float(st.st_mtime)
                        except Exception:
                            size_bytes = None
                            modified = 0.0

                        hidden = 1 if is_hidden_or_system(path) else 0
                        pending.append((
                            norm_root,
                            new_generation,
                            path,
                            entry.name,
                            "" if is_dir else os.path.splitext(entry.name)[1],
                            1 if is_dir else 0,
                            size_bytes,
                            modified,
                            hidden,
                        ))
                        inserted += 1
                        if progress_cb and inserted % 500 == 0:
                            progress_cb(norm_root, inserted)

                        if len(pending) >= 1000:
                            conn.executemany(
                                """
                                INSERT OR REPLACE INTO items
                                (root, generation, full_path, name, extension, is_dir, size_bytes, modified, hidden)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                pending,
                            )
                            conn.commit()
                            pending.clear()
            except Exception:
                continue

        if cancel_event.is_set():
            conn.execute(
                "DELETE FROM items WHERE root = ? AND generation = ?",
                (norm_root, new_generation),
            )
            conn.commit()
            return

        if pending:
            conn.executemany(
                """
                INSERT OR REPLACE INTO items
                (root, generation, full_path, name, extension, is_dir, size_bytes, modified, hidden)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                pending,
            )
            conn.commit()

        conn.execute(
            """
            INSERT INTO roots(root, last_indexed, item_count, active_generation)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(root) DO UPDATE SET
                last_indexed = excluded.last_indexed,
                item_count = excluded.item_count,
                active_generation = excluded.active_generation
            """,
            (norm_root, time.time(), inserted, new_generation),
        )
        conn.execute(
            "DELETE FROM items WHERE root = ? AND generation <> ?",
            (norm_root, new_generation),
        )
        conn.commit()

    def search(
        self,
        options: SearchOptions,
        batch_cb: Callable[[list[SearchResult]], None],
        cancel_event: threading.Event,
        status_cb: Optional[Callable[[str], None]] = None,
    ) -> int:
        mode = choose_query_mode(options.query_mode, options.pattern)
        regex = compile_query_pattern(options)

        conn = self.connect()
        try:
            sql = """
            SELECT items.name, items.full_path, items.is_dir, items.size_bytes, items.modified
            FROM items
            INNER JOIN roots
                ON roots.root = items.root
               AND roots.active_generation = items.generation
            WHERE 1=1
            """
            params: list = []

            roots = [normalize_root(r) or r for r in options.roots]
            if roots:
                placeholders = ",".join("?" for _ in roots)
                sql += f" AND items.root IN ({placeholders})"
                params.extend(roots)

            if options.search_type == "File":
                sql += " AND items.is_dir = 0"
            elif options.search_type == "Directory":
                sql += " AND items.is_dir = 1"

            if not options.include_hidden:
                sql += " AND items.hidden = 0"

            subject_col = "items.name" if options.match_name_only else "items.full_path"

            if mode == "Wildcard":
                like = wildcard_to_like(options.pattern)
                if options.case_sensitive:
                    sql += f" AND {subject_col} LIKE ? ESCAPE '\\'"
                    params.append(like)
                else:
                    sql += f" AND lower({subject_col}) LIKE lower(?) ESCAPE '\\'"
                    params.append(like)
            else:
                hint = extract_literal_hint(options.pattern)
                if hint:
                    if options.case_sensitive:
                        sql += f" AND {subject_col} LIKE ?"
                        params.append(f"%{hint}%")
                    else:
                        sql += f" AND lower({subject_col}) LIKE lower(?)"
                        params.append(f"%{hint}%")

            sql += " ORDER BY items.name COLLATE NOCASE"

            if status_cb:
                status_cb("Searching indexed roots…")

            cur = conn.execute(sql, params)
            batch: list[SearchResult] = []
            count = 0

            while not cancel_event.is_set():
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    if cancel_event.is_set():
                        break
                    subject = row["name"] if options.match_name_only else row["full_path"]
                    if not regex.search(subject):
                        continue
                    batch.append(SearchResult(
                        match_type="Indexed",
                        item_type="Directory" if row["is_dir"] else "File",
                        name=row["name"],
                        full_path=row["full_path"],
                        size_bytes=row["size_bytes"],
                        modified=row["modified"] or 0.0,
                    ))
                    count += 1
                    if options.max_results > 0 and count >= options.max_results:
                        cancel_event.set()
                        break
                    if len(batch) >= 250:
                        batch_cb(batch)
                        batch = []
                if batch:
                    batch_cb(batch)
                    batch = []

            return count
        finally:
            conn.close()


class IndexedSearchWorker(threading.Thread):
    def __init__(self, db: IndexDB, options: SearchOptions, out_queue: queue.Queue, cancel_event: threading.Event):
        super().__init__(daemon=True)
        self.db = db
        self.options = options
        self.out_queue = out_queue
        self.cancel_event = cancel_event

    def run(self) -> None:
        started = time.time()
        try:
            count = self.db.search(
                self.options,
                batch_cb=lambda batch: self.out_queue.put(("search_batch", batch)),
                cancel_event=self.cancel_event,
                status_cb=lambda msg: self.out_queue.put(("search_status", msg)),
            )
            self.out_queue.put(("search_done", {
                "count": count,
                "duration": time.time() - started,
                "cancelled": self.cancel_event.is_set(),
                "mode": "Indexed",
            }))
        except Exception as exc:
            write_log(traceback.format_exc())
            self.out_queue.put(("search_error", f"Indexed search failed: {exc}"))


class LiveSearchWorker(threading.Thread):
    def __init__(self, options: SearchOptions, out_queue: queue.Queue, cancel_event: threading.Event):
        super().__init__(daemon=True)
        self.options = options
        self.out_queue = out_queue
        self.cancel_event = cancel_event

    def run(self) -> None:
        started = time.time()
        try:
            regex = compile_query_pattern(self.options)
            count = 0
            for result in live_search_iter(
                self.options,
                regex,
                self.cancel_event,
                status_cb=lambda msg: self.out_queue.put(("search_status", msg)),
            ):
                self.out_queue.put(("search_result", result))
                count += 1
            self.out_queue.put(("search_done", {
                "count": count,
                "duration": time.time() - started,
                "cancelled": self.cancel_event.is_set(),
                "mode": "Live",
            }))
        except re.error as exc:
            self.out_queue.put(("search_error", f"Invalid pattern: {exc}"))
        except Exception as exc:
            write_log(traceback.format_exc())
            self.out_queue.put(("search_error", f"Live search failed: {exc}"))


class MixedSearchWorker(threading.Thread):
    def __init__(self, db: IndexDB, options: SearchOptions, out_queue: queue.Queue, cancel_event: threading.Event):
        super().__init__(daemon=True)
        self.db = db
        self.options = options
        self.out_queue = out_queue
        self.cancel_event = cancel_event

    def run(self) -> None:
        started = time.time()
        total_count = 0
        try:
            indexed_roots, live_roots = self.db.partition_roots(self.options.roots)
            self.out_queue.put(("search_status", f"Mixed search: {len(indexed_roots)} indexed root(s), {len(live_roots)} live root(s)"))

            if indexed_roots and not self.cancel_event.is_set():
                indexed_options = replace(
                    self.options,
                    roots=indexed_roots,
                    search_content=False,
                    max_results=max(0, self.options.max_results - total_count) if self.options.max_results > 0 else 0,
                )
                total_count += self.db.search(
                    indexed_options,
                    batch_cb=lambda batch: self.out_queue.put(("search_batch", batch)),
                    cancel_event=self.cancel_event,
                    status_cb=lambda msg: self.out_queue.put(("search_status", msg)),
                )

            if live_roots and not self.cancel_event.is_set():
                live_limit = max(0, self.options.max_results - total_count) if self.options.max_results > 0 else 0
                live_options = replace(self.options, roots=live_roots, max_results=live_limit)
                regex = compile_query_pattern(live_options)
                for result in live_search_iter(
                    live_options,
                    regex,
                    self.cancel_event,
                    status_cb=lambda msg: self.out_queue.put(("search_status", msg)),
                ):
                    self.out_queue.put(("search_result", result))
                    total_count += 1

            self.out_queue.put(("search_done", {
                "count": total_count,
                "duration": time.time() - started,
                "cancelled": self.cancel_event.is_set(),
                "mode": "Mixed",
            }))
        except re.error as exc:
            self.out_queue.put(("search_error", f"Invalid pattern: {exc}"))
        except Exception as exc:
            write_log(traceback.format_exc())
            self.out_queue.put(("search_error", f"Mixed search failed: {exc}"))


class ManualIndexWorker(threading.Thread):
    def __init__(self, db: IndexDB, roots: list[str], out_queue: queue.Queue, cancel_event: threading.Event):
        super().__init__(daemon=True)
        self.db = db
        self.roots = roots
        self.out_queue = out_queue
        self.cancel_event = cancel_event

    def run(self) -> None:
        started = time.time()
        try:
            self.db.rebuild_roots(
                self.roots,
                self.cancel_event,
                status_cb=lambda msg: self.out_queue.put(("index_status", msg)),
                progress_cb=lambda root, n: self.out_queue.put(("index_progress", (root, n))),
            )
            self.out_queue.put(("index_done", {
                "duration": time.time() - started,
                "cancelled": self.cancel_event.is_set(),
            }))
        except Exception as exc:
            write_log(traceback.format_exc())
            self.out_queue.put(("index_error", f"Indexing failed: {exc}"))


class BackgroundIndexer(threading.Thread):
    def __init__(
        self,
        db: IndexDB,
        roots: list[str],
        interval_seconds: int,
        out_queue: queue.Queue,
        cancel_event: threading.Event,
        initial_roots: Optional[list[str]] = None,
    ):
        super().__init__(daemon=True)
        self.db = db
        self.roots = roots
        self.interval_seconds = max(30, interval_seconds)
        self.out_queue = out_queue
        self.cancel_event = cancel_event
        self.initial_roots = initial_roots or []

    def run(self) -> None:
        try:
            self.out_queue.put(("bg_state", f"Background indexer running every {self.interval_seconds // 60} min"))
            first_cycle = True
            while not self.cancel_event.is_set():
                cycle_started = time.time()
                roots = self.initial_roots if first_cycle and self.initial_roots else self.roots
                first_cycle = False
                self.db.rebuild_roots(
                    roots,
                    self.cancel_event,
                    status_cb=lambda msg: self.out_queue.put(("bg_status", msg)),
                    progress_cb=lambda root, n: self.out_queue.put(("bg_progress", (root, n))),
                )
                if self.cancel_event.is_set():
                    break
                self.out_queue.put(("bg_cycle_done", {"duration": time.time() - cycle_started}))
                remaining = self.interval_seconds
                while remaining > 0 and not self.cancel_event.is_set():
                    sleep_chunk = min(1, remaining)
                    time.sleep(sleep_chunk)
                    remaining -= sleep_chunk
            self.out_queue.put(("bg_state", "Background indexer stopped"))
        except Exception as exc:
            write_log(traceback.format_exc())
            self.out_queue.put(("bg_error", f"Background indexing failed: {exc}"))


class ISSearchApp:
    columns = [
        ("match_type", "Match", 110),
        ("item_type", "Type", 90),
        ("name", "Name", 260),
        ("full_path", "Full Path", 620),
        ("size", "Size", 100),
        ("modified", "Modified", 160),
    ]

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1540x860")
        self.root.minsize(1100, 680)

        self.db = IndexDB(DB_PATH)
        self.queue: queue.Queue = queue.Queue()

        self.search_cancel_event = threading.Event()
        self.index_cancel_event = threading.Event()
        self.bg_cancel_event = threading.Event()

        self.search_worker: Optional[threading.Thread] = None
        self.index_worker: Optional[threading.Thread] = None
        self.bg_indexer: Optional[threading.Thread] = None

        self.result_count = 0
        self.auto_after_id: Optional[str] = None
        self.sort_column = "modified"
        self.sort_reverse = False
        self.initial_indexing = False
        self.initial_index_roots: list[str] = []
        self.initial_index_placeholder = "still indexing..."
        self.initial_index_current_root = ""
        self.initial_index_current_count = 0
        self.logo_images: list[tk.PhotoImage] = []
        self.window_icon_image: Optional[tk.PhotoImage] = None

        self._apply_window_icon()
        self._build_ui()
        self.roots_var.set(";".join(get_default_roots()))
        self.pattern_entry.focus_set()
        self.refresh_index_summary()
        startup_roots = split_roots(self.roots_var.get()) or get_default_roots()
        missing_startup_roots = self.get_unindexed_roots(startup_roots)
        if missing_startup_roots:
            self.initial_index_roots = startup_roots
            self.start_background_indexing(initial=True, initial_roots=missing_startup_roots)
        else:
            self.start_background_indexing()
        self._poll_queue()

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)

        top = ttk.Frame(self.root, padding=12)
        top.grid(row=0, column=0, sticky="nsew")
        top.columnconfigure(0, weight=1)

        search_box = ttk.LabelFrame(top, text="Search")
        search_box.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        search_box.columnconfigure(0, weight=1)

        search_row = ttk.Frame(search_box, padding=10)
        search_row.grid(row=0, column=0, sticky="ew")
        search_row.columnconfigure(1, weight=1)

        self.pattern_var = tk.StringVar()
        self.pattern_var.trace_add("write", self._auto_search_changed)

        q_logo_frame = tk.Frame(search_row, bg="white", bd=0, highlightthickness=0, padx=8, pady=4)
        q_logo_frame.grid(row=0, column=0, rowspan=3, sticky="w", padx=(0, 12))
        self._add_header_logo(q_logo_frame, Q_LOGO_PATH, 0)

        ttk.Label(search_row, text="Pattern").grid(row=0, column=1, sticky="w")
        self.pattern_entry = ttk.Entry(search_row, textvariable=self.pattern_var, font=("Segoe UI", 11))
        self.pattern_entry.grid(row=1, column=1, sticky="ew", pady=(4, 0))
        self.pattern_entry.bind("<Return>", lambda _e: self.start_search())

        ttk.Label(search_row, text="Pattern mode").grid(row=0, column=2, sticky="w")
        self.query_mode_var = tk.StringVar(value="Auto")
        ttk.Combobox(
            search_row,
            textvariable=self.query_mode_var,
            values=["Auto", "Wildcard", "Regex"],
            state="readonly",
            width=12,
        ).grid(row=1, column=2, padx=(12, 10))

        self.search_button = ttk.Button(search_row, text="Search", command=self.start_search)
        self.search_button.grid(row=1, column=3)
        self.stop_search_button = ttk.Button(search_row, text="Stop", command=self.stop_search, state="disabled")
        self.stop_search_button.grid(row=1, column=4, padx=(8, 0))

        is_logo_frame = tk.Frame(search_row, bg="white", bd=0, highlightthickness=0, padx=8, pady=4)
        is_logo_frame.grid(row=0, column=5, rowspan=3, sticky="e", padx=(16, 0))
        self._add_header_logo(is_logo_frame, IS_LOGO_PATH, 0)

        self.help_var = tk.StringVar(value="Auto pattern: *SPEC*.xlsx uses wildcard to find any file with the word 'SPEC' ending in .xlsx.")
        ttk.Label(search_row, textvariable=self.help_var).grid(row=2, column=1, columnspan=4, sticky="w", pady=(8, 0))

        options = ttk.LabelFrame(top, text="Options")
        options.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        options.columnconfigure(0, weight=1)

        self.roots_var = tk.StringVar()
        self.type_var = tk.StringVar(value="Any")
        self.max_results_var = tk.StringVar(value="0")
        self.max_file_mb_var = tk.StringVar(value="20")
        self.match_name_only_var = tk.BooleanVar(value=True)
        self.include_hidden_var = tk.BooleanVar(value=False)
        self.case_sensitive_var = tk.BooleanVar(value=False)
        self.auto_search_var = tk.BooleanVar(value=False)

        grid = ttk.Frame(options, padding=10)
        grid.grid(row=0, column=0, sticky="ew")
        grid.columnconfigure(0, weight=1)

        ttk.Label(grid, text="Roots (semicolon-separated)").grid(row=0, column=0, sticky="w")
        ttk.Label(grid, text="Type").grid(row=0, column=1, sticky="w", padx=(12, 0))
        ttk.Label(grid, text="Max results").grid(row=0, column=2, sticky="w", padx=(12, 0))
        ttk.Label(grid, text="Max file MB").grid(row=0, column=3, sticky="w", padx=(12, 0))

        roots_row = ttk.Frame(grid)
        roots_row.grid(row=1, column=0, sticky="ew")
        roots_row.columnconfigure(0, weight=1)
        self.roots_entry = ttk.Entry(roots_row, textvariable=self.roots_var)
        self.roots_entry.grid(row=0, column=0, sticky="ew")
        ttk.Button(roots_row, text="Browse…", command=self.browse_root).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(roots_row, text="Save default", command=self.save_roots_as_default).grid(row=0, column=2, padx=(8, 0))

        ttk.Combobox(grid, textvariable=self.type_var, values=["Any", "File", "Directory"], state="readonly", width=12).grid(row=1, column=1, sticky="w", padx=(12, 0))
        ttk.Entry(grid, textvariable=self.max_results_var, width=12).grid(row=1, column=2, sticky="w", padx=(12, 0))
        ttk.Entry(grid, textvariable=self.max_file_mb_var, width=12).grid(row=1, column=3, sticky="w", padx=(12, 0))

        actions = ttk.Frame(grid)
        actions.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(12, 0))
        ttk.Checkbutton(actions, text="Match name only", variable=self.match_name_only_var).grid(row=0, column=0, padx=(0, 12))
        ttk.Checkbutton(actions, text="Include hidden/system", variable=self.include_hidden_var).grid(row=0, column=1, padx=(0, 12))
        ttk.Checkbutton(actions, text="Case sensitive", variable=self.case_sensitive_var).grid(row=0, column=2, padx=(0, 12))
        ttk.Checkbutton(actions, text="Auto search while typing", variable=self.auto_search_var).grid(row=0, column=3, padx=(0, 20))

        index_box = ttk.LabelFrame(top, text="Index")
        index_box.grid(row=2, column=0, sticky="ew")
        for i in range(6):
            index_box.columnconfigure(i, weight=1 if i in (2, 5) else 0)

        self.refresh_index_button = ttk.Button(index_box, text="Refresh index now", command=self.start_manual_index)
        self.refresh_index_button.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        self.start_bg_button = ttk.Button(index_box, text="Start background indexing", command=self.start_background_indexing)
        self.start_bg_button.grid(row=0, column=1, padx=10, pady=10, sticky="w")

        self.stop_bg_button = ttk.Button(index_box, text="Stop background indexing", command=self.stop_background_indexing, state="disabled")
        self.stop_bg_button.grid(row=0, column=2, padx=10, pady=10, sticky="w")

        ttk.Label(index_box, text="Interval (min)").grid(row=0, column=3, padx=(10, 4), pady=10, sticky="e")
        self.index_interval_var = tk.StringVar(value="15")
        ttk.Entry(index_box, textvariable=self.index_interval_var, width=8).grid(row=0, column=4, padx=(0, 10), pady=10, sticky="w")

        ttk.Button(index_box, text="Show indexed roots", command=self.show_indexed_roots).grid(row=0, column=5, padx=10, pady=10, sticky="w")

        self.index_summary_var = tk.StringVar(value="")
        self.bg_status_var = tk.StringVar(value="Background indexer: stopped")
        ttk.Label(index_box, textvariable=self.index_summary_var).grid(row=1, column=0, columnspan=3, padx=10, pady=(0, 8), sticky="w")
        ttk.Label(index_box, textvariable=self.bg_status_var).grid(row=1, column=3, columnspan=3, padx=10, pady=(0, 8), sticky="w")

        mid = ttk.Frame(self.root, padding=(12, 0, 12, 0))
        mid.grid(row=2, column=0, sticky="nsew")
        mid.columnconfigure(0, weight=1)
        mid.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(mid, columns=[c[0] for c in self.columns], show="headings", selectmode="browse")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.bind("<Double-1>", lambda _e: self.open_selected())
        self.tree.bind("<Button-3>", self.show_result_menu)
        self.tree.bind("<Button-2>", self.show_result_menu)

        self.result_menu = tk.Menu(self.root, tearoff=False)
        self.result_menu.add_command(label="Open", command=self.open_selected)
        self.result_menu.add_command(label="Open folder", command=self.open_selected_folder)
        self.result_menu.add_separator()
        self.result_menu.add_command(label="Copy path", command=self.copy_selected_path)

        vsb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(mid, orient="horizontal", command=self.tree.xview)
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        for key, title, width in self.columns:
            self.tree.heading(key, text=title, command=lambda k=key: self.sort_by(k))
            self.tree.column(key, width=width, anchor="w", stretch=True)

        bottom = ttk.Frame(self.root, padding=12)
        bottom.grid(row=3, column=0, sticky="ew")
        bottom.columnconfigure(0, weight=1)
        self.status_var = tk.StringVar(value=f"Ready. Index DB: {DB_PATH}")
        self.summary_var = tk.StringVar(value="0 results")
        ttk.Label(bottom, textvariable=self.status_var).grid(row=0, column=0, sticky="w")
        ttk.Label(bottom, text=f"Log: {LOG_PATH}").grid(row=1, column=0, sticky="w")
        ttk.Label(bottom, textvariable=self.summary_var).grid(row=0, column=1, sticky="e")

    def _apply_window_icon(self) -> None:
        if Q_ICON_PATH.exists():
            try:
                self.root.iconbitmap(default=str(Q_ICON_PATH))
            except Exception as exc:
                write_log(f"Could not load window icon {Q_ICON_PATH}: {exc}")
        if Q_LOGO_PATH.exists():
            try:
                self.window_icon_image = tk.PhotoImage(file=str(Q_LOGO_PATH))
                self.root.iconphoto(True, self.window_icon_image)
            except Exception as exc:
                write_log(f"Could not load toolbar icon {Q_LOGO_PATH}: {exc}")

    def _add_header_logo(self, parent: tk.Frame, image_path: Path, column: int) -> None:
        if not image_path.exists():
            write_log(f"Header logo missing: {image_path}")
            return
        try:
            image = tk.PhotoImage(file=str(image_path))
        except Exception as exc:
            write_log(f"Could not load header logo {image_path}: {exc}")
            return
        self.logo_images.append(image)
        tk.Label(parent, image=image, bg="white", bd=0, highlightthickness=0).grid(
            row=0,
            column=column,
            padx=(0 if column == 0 else 10, 0),
            sticky="ne",
        )

    def refresh_index_summary(self) -> None:
        rows = self.db.get_root_stats()
        total_items = sum(int(r["item_count"]) for r in rows)
        roots = len(rows)
        newest = max((float(r["last_indexed"]) for r in rows), default=0.0)
        newest_text = format_timestamp(newest) if newest else "never"
        self.index_summary_var.set(f"Indexed roots: {roots} | Items: {total_items:,} | Last update: {newest_text}")

    def get_unindexed_roots(self, roots: list[str]) -> list[str]:
        indexed_keys = self.db.get_indexed_roots()
        missing: list[str] = []
        for root in roots:
            norm = normalize_root(root) or root
            if not os.path.exists(norm):
                continue
            if normalized_root_key(norm) not in indexed_keys:
                missing.append(norm)
        return missing

    def initial_index_finished(self) -> bool:
        return bool(self.initial_index_roots) and not self.get_unindexed_roots(self.initial_index_roots)

    def is_initial_index_placeholder(self) -> bool:
        return self.pattern_var.get().startswith("still indexing")

    def update_initial_index_placeholder(
        self,
        root_name: Optional[str] = None,
        item_count: Optional[int] = None,
    ) -> None:
        if root_name:
            self.initial_index_current_root = root_name
        if item_count is not None:
            self.initial_index_current_count = item_count
        root_text = self.initial_index_current_root or "default drives"
        self.pattern_var.set(f"still indexing {root_text} - {self.initial_index_current_count:,} files")

    def set_initial_indexing_state(self, active: bool) -> None:
        self.initial_indexing = active
        if active:
            self.search_button.configure(state="disabled")
            self.pattern_entry.configure(state="disabled")
            if not self.pattern_var.get().strip():
                self.update_initial_index_placeholder()
            self.status_var.set("Still indexing. Search will be available when the default drive indexes finish.")
            self.help_var.set("Still indexing. Default drives will use indexed search once the first pass completes.")
        else:
            if self.is_initial_index_placeholder():
                self.pattern_var.set("")
            self.pattern_entry.configure(state="normal")
            self.search_button.configure(state="normal")
            self.help_var.set("Auto pattern: *SPEC*.xlsx uses wildcard to find any file with the word 'SPEC' ending in .xlsx.")

    def show_indexed_roots(self) -> None:
        rows = self.db.get_root_stats()
        if not rows:
            messagebox.showinfo(APP_TITLE, "No roots have been indexed yet.")
            return
        lines = []
        for row in rows:
            lines.append(f"{row['root']}  |  {int(row['item_count']):,} items  |  {format_timestamp(float(row['last_indexed']))}")
        messagebox.showinfo(APP_TITLE, "\n".join(lines))

    def browse_root(self) -> None:
        selected = filedialog.askdirectory(title="Choose root")
        if selected:
            roots = split_roots(self.roots_var.get())
            roots.append(selected)
            self.roots_var.set(";".join(split_roots(";".join(roots))))

    def save_roots_as_default(self) -> None:
        roots = split_roots(self.roots_var.get())
        if not roots:
            messagebox.showerror(APP_TITLE, "Add at least one root before saving defaults.")
            return
        try:
            save_default_roots(roots)
        except Exception as exc:
            write_log(traceback.format_exc())
            messagebox.showerror(APP_TITLE, f"Could not save default roots:\n{exc}")
            return
        self.status_var.set(f"Saved {len(roots)} default root(s). They will be used when IS Search starts.")

    def _auto_search_changed(self, *_args) -> None:
        if self.initial_indexing:
            return
        if not self.auto_search_var.get():
            return
        if self.auto_after_id:
            self.root.after_cancel(self.auto_after_id)
        self.auto_after_id = self.root.after(350, self._auto_start)

    def _auto_start(self) -> None:
        self.auto_after_id = None
        if (not self.search_worker or not self.search_worker.is_alive()) and self.pattern_var.get().strip():
            self.start_search()

    def collect_options(self) -> SearchOptions:
        pattern = self.pattern_var.get().strip()
        if not pattern:
            raise ValueError("Enter a search pattern.")
        try:
            max_results = int(self.max_results_var.get().strip() or "0")
            if max_results < 0:
                raise ValueError
        except ValueError:
            raise ValueError("Max results must be 0 or a positive integer.")
        try:
            max_file_mb = float(self.max_file_mb_var.get().strip() or "20")
            if max_file_mb <= 0:
                raise ValueError
        except ValueError:
            raise ValueError("Max file MB must be a positive number.")
        return SearchOptions(
            pattern=pattern,
            roots=split_roots(self.roots_var.get()) or get_default_roots(),
            match_name_only=self.match_name_only_var.get(),
            search_type=self.type_var.get(),
            include_hidden=self.include_hidden_var.get(),
            search_content=False,
            case_sensitive=self.case_sensitive_var.get(),
            max_results=max_results,
            max_file_size_mb=max_file_mb,
            query_mode=self.query_mode_var.get(),
            source_mode="Indexed only",
        )

    def clear_results(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.result_count = 0
        self.summary_var.set("0 results")

    def start_search(self) -> None:
        if self.initial_indexing:
            self.status_var.set("Still indexing. Search will be available when the first index finishes.")
            return

        try:
            options = self.collect_options()
        except ValueError as exc:
            messagebox.showerror(APP_TITLE, str(exc))
            return

        if self.search_worker and self.search_worker.is_alive():
            self.stop_search()
            return

        self.clear_results()
        self.search_cancel_event = threading.Event()

        indexed_roots, live_roots = self.db.partition_roots(options.roots)
        source_mode = options.source_mode

        if options.search_content:
            if source_mode == "Indexed only":
                messagebox.showerror(APP_TITLE, "Search file contents requires a live scan.")
                return
            self.status_var.set("Content search forces a live scan…")
            self.search_worker = LiveSearchWorker(options, self.queue, self.search_cancel_event)
        elif source_mode == "Live only":
            self.status_var.set("Starting live search…")
            self.search_worker = LiveSearchWorker(options, self.queue, self.search_cancel_event)
        elif source_mode == "Indexed only":
            if live_roots and indexed_roots:
                options = replace(options, roots=indexed_roots)
                live_roots = []
            if live_roots:
                messagebox.showerror(APP_TITLE, "Search is index-only, but one or more selected roots are not indexed yet.")
                return
            self.status_var.set("Starting indexed search…")
            self.search_worker = IndexedSearchWorker(self.db, options, self.queue, self.search_cancel_event)
        elif source_mode == "Mixed":
            self.status_var.set("Starting mixed search…")
            self.search_worker = MixedSearchWorker(self.db, options, self.queue, self.search_cancel_event)
        else:
            if indexed_roots and live_roots:
                self.status_var.set("Auto mode chose mixed search…")
                self.search_worker = MixedSearchWorker(self.db, options, self.queue, self.search_cancel_event)
            elif indexed_roots:
                self.status_var.set("Auto mode chose indexed search…")
                self.search_worker = IndexedSearchWorker(self.db, options, self.queue, self.search_cancel_event)
            else:
                self.status_var.set("Auto mode chose live search…")
                self.search_worker = LiveSearchWorker(options, self.queue, self.search_cancel_event)

        self.search_worker.start()
        self.search_button.configure(state="disabled")
        self.stop_search_button.configure(state="normal")

    def stop_search(self) -> None:
        if self.search_worker and self.search_worker.is_alive():
            self.search_cancel_event.set()
            self.status_var.set("Stopping search…")

    def start_manual_index(self) -> None:
        if self.index_worker and self.index_worker.is_alive():
            messagebox.showinfo(APP_TITLE, "Manual indexing is already running.")
            return
        roots = split_roots(self.roots_var.get()) or get_default_roots()
        self.index_cancel_event = threading.Event()
        self.index_worker = ManualIndexWorker(self.db, roots, self.queue, self.index_cancel_event)
        self.index_worker.start()
        self.refresh_index_button.configure(state="disabled")
        self.status_var.set("Indexing selected roots in the background…")

    def start_background_indexing(self, initial: bool = False, initial_roots: Optional[list[str]] = None) -> None:
        if self.bg_indexer and self.bg_indexer.is_alive():
            if not initial:
                messagebox.showinfo(APP_TITLE, "Background indexing is already running.")
            return
        roots = split_roots(self.roots_var.get()) or get_default_roots()
        try:
            interval_minutes = int(self.index_interval_var.get().strip() or "15")
            if interval_minutes <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror(APP_TITLE, "Interval must be a positive integer number of minutes.")
            return
        self.bg_cancel_event = threading.Event()
        self.bg_indexer = BackgroundIndexer(
            self.db,
            roots,
            interval_minutes * 60,
            self.queue,
            self.bg_cancel_event,
            initial_roots=initial_roots,
        )
        self.bg_indexer.start()
        self.start_bg_button.configure(state="disabled")
        self.stop_bg_button.configure(state="normal")
        if initial:
            self.set_initial_indexing_state(True)

    def stop_background_indexing(self) -> None:
        if self.bg_indexer and self.bg_indexer.is_alive():
            self.bg_cancel_event.set()
            self.bg_status_var.set("Stopping background indexer…")
            self.stop_bg_button.configure(state="disabled")

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.queue.get_nowait()

                if kind == "search_result":
                    self.add_result(payload)
                elif kind == "search_batch":
                    for item in payload:
                        self.add_result(item)
                elif kind == "search_status":
                    self.status_var.set(str(payload))
                elif kind == "search_done":
                    self.search_button.configure(state="disabled" if self.initial_indexing else "normal")
                    self.stop_search_button.configure(state="disabled")
                    self.summary_var.set(f"{payload.get('count', self.result_count):,} results")
                    suffix = " (stopped)" if payload.get("cancelled") else ""
                    self.status_var.set(f"{payload.get('mode', 'Search')} finished in {payload.get('duration', 0.0):.1f}s{suffix}")
                elif kind == "search_error":
                    self.search_button.configure(state="disabled" if self.initial_indexing else "normal")
                    self.stop_search_button.configure(state="disabled")
                    self.status_var.set(str(payload))
                    messagebox.showerror(APP_TITLE, f"{payload}\n\nLog: {LOG_PATH}")

                elif kind == "index_status":
                    self.status_var.set(str(payload))
                elif kind == "index_progress":
                    root_name, item_count = payload
                    if self.initial_indexing:
                        self.update_initial_index_placeholder(str(root_name), int(item_count))
                    self.status_var.set(f"Indexing {root_name}… {item_count:,} items staged")
                elif kind == "index_done":
                    self.refresh_index_button.configure(state="normal")
                    self.refresh_index_summary()
                    if self.initial_indexing and self.initial_index_finished():
                        self.set_initial_indexing_state(False)
                    suffix = " (stopped)" if payload.get("cancelled") else ""
                    self.status_var.set(f"Manual index finished in {payload.get('duration', 0.0):.1f}s{suffix}")
                elif kind == "index_error":
                    self.refresh_index_button.configure(state="normal")
                    self.status_var.set(str(payload))
                    messagebox.showerror(APP_TITLE, f"{payload}\n\nLog: {LOG_PATH}")

                elif kind == "bg_state":
                    self.bg_status_var.set(str(payload))
                    if "stopped" in str(payload).lower():
                        self.start_bg_button.configure(state="normal")
                        self.stop_bg_button.configure(state="disabled")
                        self.refresh_index_summary()
                elif kind == "bg_status":
                    if self.initial_indexing and str(payload).startswith("Indexing "):
                        self.update_initial_index_placeholder(str(payload).removeprefix("Indexing "), 0)
                    self.bg_status_var.set(f"Background indexer: {payload}")
                elif kind == "bg_progress":
                    root_name, item_count = payload
                    if self.initial_indexing:
                        self.update_initial_index_placeholder(str(root_name), int(item_count))
                    self.bg_status_var.set(f"Background indexer: {root_name}… {item_count:,} items staged")
                elif kind == "bg_cycle_done":
                    self.refresh_index_summary()
                    if self.initial_indexing and self.initial_index_finished():
                        self.set_initial_indexing_state(False)
                    self.bg_status_var.set(
                        f"Background indexer: cycle finished in {payload.get('duration', 0.0):.1f}s; waiting for next interval"
                    )
                elif kind == "bg_error":
                    self.bg_status_var.set(str(payload))
                    self.start_bg_button.configure(state="normal")
                    self.stop_bg_button.configure(state="disabled")
                    messagebox.showerror(APP_TITLE, f"{payload}\n\nLog: {LOG_PATH}")

        except queue.Empty:
            pass

        self.root.after(100, self._poll_queue)

    def add_result(self, result: SearchResult) -> None:
        self.tree.insert("", "end", values=(
            result.match_type,
            result.item_type,
            result.name,
            result.full_path,
            format_size(result.size_bytes),
            format_timestamp(result.modified),
        ))
        self.result_count += 1
        self.summary_var.set(f"{self.result_count:,} results")

    def show_result_menu(self, event: tk.Event) -> str:
        row_id = self.tree.identify_row(event.y)
        column_id = self.tree.identify_column(event.x)
        if not row_id or column_id != "#4":
            return "break"
        self.tree.selection_set(row_id)
        self.tree.focus(row_id)
        self.result_menu.tk_popup(event.x_root, event.y_root)
        self.result_menu.grab_release()
        return "break"

    def selected_path(self) -> Optional[str]:
        selection = self.tree.selection()
        if not selection:
            return None
        values = self.tree.item(selection[0]).get("values", [])
        return str(values[3]) if len(values) > 3 else None

    def open_selected(self) -> None:
        path = self.selected_path()
        if not path:
            return
        try:
            open_path(path)
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not open item:\n{exc}")

    def open_selected_folder(self) -> None:
        path = self.selected_path()
        if not path:
            return
        try:
            open_folder_for_path(path)
        except Exception as exc:
            messagebox.showerror(APP_TITLE, f"Could not open folder:\n{exc}")

    def copy_selected_path(self) -> None:
        path = self.selected_path()
        if not path:
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(path)
        self.status_var.set("Path copied to clipboard.")

    def sort_by(self, column_key: str) -> None:
        data = [(self.tree.set(item, column_key), item) for item in self.tree.get_children("")]
        reverse = self.sort_reverse if self.sort_column == column_key else False

        def sort_key(pair):
            value = pair[0]
            if column_key == "size":
                try:
                    num, unit = value.split()
                    scale = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}.get(unit, 1)
                    return float(num) * scale
                except Exception:
                    return -1
            return value.lower() if isinstance(value, str) else value

        data.sort(key=sort_key, reverse=reverse)
        for idx, (_, item) in enumerate(data):
            self.tree.move(item, "", idx)
        self.sort_column = column_key
        self.sort_reverse = not reverse


def main() -> int:
    ensure_app_dir()
    close_other_is_search_instances()
    write_log("IS Search starting")
    set_windows_app_id()
    root = tk.Tk()
    try:
        style = ttk.Style(root)
        if "vista" in style.theme_names():
            style.theme_use("vista")
        elif "clam" in style.theme_names():
            style.theme_use("clam")
    except Exception:
        pass
    ISSearchApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        write_log(traceback.format_exc())
        raise
