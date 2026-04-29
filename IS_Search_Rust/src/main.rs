#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    collections::HashSet,
    env,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local, Utc};
use eframe::egui::{self, Color32, TextureHandle};
use egui_extras::{Column, TableBuilder};
use regex::{Regex, RegexBuilder};
use rusqlite::{params, params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use sysinfo::{PidExt, ProcessExt, Signal, System, SystemExt};

const APP_TITLE: &str = "IS Search";
const APP_USER_MODEL_ID: &str = "Quanta.ISSearch";
const Q_LOGO_BYTES: &[u8] = include_bytes!("../assets/Q Grey Logo.png");
const IS_LOGO_BYTES: &[u8] = include_bytes!("../assets/IS.png");

const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
const FILE_ATTRIBUTE_SYSTEM: u32 = 0x4;

#[cfg(windows)]
const WINDOWS_EXCLUDED_PREFIXES: &[&str] = &[
    r"C:\Windows\WinSxS",
    r"C:\Windows\Installer",
    r"C:\System Volume Information",
    r"C:\$Recycle.Bin",
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum QueryMode {
    Auto,
    Wildcard,
    Regex,
}

impl QueryMode {
    fn label(self) -> &'static str {
        match self {
            QueryMode::Auto => "Auto",
            QueryMode::Wildcard => "Wildcard",
            QueryMode::Regex => "Regex",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SearchType {
    Any,
    File,
    Directory,
}

impl SearchType {
    fn label(self) -> &'static str {
        match self {
            SearchType::Any => "Any",
            SearchType::File => "File",
            SearchType::Directory => "Directory",
        }
    }
}

#[derive(Clone)]
struct SearchOptions {
    pattern: String,
    roots: Vec<String>,
    match_name_only: bool,
    search_type: SearchType,
    include_hidden: bool,
    case_sensitive: bool,
    max_results: usize,
    max_file_size_mb: f64,
    query_mode: QueryMode,
}

#[derive(Clone)]
struct SearchResult {
    match_type: String,
    item_type: String,
    name: String,
    full_path: String,
    size_bytes: Option<i64>,
    modified: f64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortColumn {
    Match,
    Type,
    Name,
    FullPath,
    Size,
    Modified,
}

#[derive(Serialize, Deserialize, Default)]
struct Settings {
    default_roots: Vec<String>,
}

#[derive(Clone)]
struct IndexDb {
    path: PathBuf,
}

enum AppEvent {
    SearchBatch(Vec<SearchResult>),
    SearchDone {
        count: usize,
        duration: f64,
        cancelled: bool,
    },
    SearchError(String),
    SearchStatus(String),
    IndexStatus(String),
    IndexProgress {
        root: String,
        count: usize,
        background: bool,
    },
    IndexDone {
        duration: f64,
        cancelled: bool,
        background: bool,
    },
    IndexError(String),
    BackgroundState(String),
}

enum ResultAction {
    Open(String),
    OpenFolder(String),
    Copy(String),
}

fn main() -> eframe::Result<()> {
    if let Err(error) = ensure_app_dir() {
        eprintln!("Could not create app data folder: {error}");
    }
    close_other_instances();
    write_log("IS Search Rust starting");
    set_windows_app_id();

    let icon = load_icon_data(Q_LOGO_BYTES).ok();
    let mut viewport = egui::ViewportBuilder::default()
        .with_inner_size([1540.0, 860.0])
        .with_min_inner_size([1100.0, 680.0])
        .with_title(APP_TITLE);
    if let Some(icon) = icon {
        viewport = viewport.with_icon(icon);
    }

    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };

    eframe::run_native(
        APP_TITLE,
        options,
        Box::new(|cc| Ok(Box::new(ISSearchApp::new(cc)))),
    )
}

fn app_dir() -> PathBuf {
    #[cfg(windows)]
    {
        if let Some(local_app_data) = env::var_os("LOCALAPPDATA") {
            return PathBuf::from(local_app_data).join("ISSearch");
        }
    }

    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".is_search")
}

fn db_path() -> PathBuf {
    app_dir().join("is_search_index.sqlite3")
}

fn log_path() -> PathBuf {
    app_dir().join("is_search.log")
}

fn settings_path() -> PathBuf {
    app_dir().join("settings.json")
}

fn ensure_app_dir() -> Result<()> {
    fs::create_dir_all(app_dir()).context("creating app data folder")
}

fn write_log(message: &str) {
    let _ = ensure_app_dir();
    let stamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let line = format!("[{stamp}] {message}\n");
    let _ = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path())
        .and_then(|mut f| {
            use std::io::Write;
            f.write_all(line.as_bytes())
        });
}

fn close_other_instances() {
    let current_pid = std::process::id();
    let mut system = System::new_all();
    system.refresh_all();

    let mut closed = 0usize;
    for (pid, process) in system.processes() {
        if pid.as_u32() == current_pid {
            continue;
        }
        if process.name().eq_ignore_ascii_case("IS_Search.exe") {
            let _ = process.kill_with(Signal::Term);
            closed += 1;
        }
    }

    if closed == 0 {
        return;
    }

    thread::sleep(Duration::from_millis(1500));
    system.refresh_all();
    let mut terminated = 0usize;
    for (pid, process) in system.processes() {
        if pid.as_u32() == current_pid {
            continue;
        }
        if process.name().eq_ignore_ascii_case("IS_Search.exe") && process.kill() {
            terminated += 1;
        }
    }

    write_log(&format!(
        "Requested close for {closed} existing IS_Search.exe instance(s); terminated {terminated} remaining instance(s)."
    ));
}

#[cfg(windows)]
fn set_windows_app_id() {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::UI::Shell::SetCurrentProcessExplicitAppUserModelID;

    let wide: Vec<u16> = OsStr::new(APP_USER_MODEL_ID)
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    unsafe {
        let _ = SetCurrentProcessExplicitAppUserModelID(wide.as_ptr());
    }
}

#[cfg(not(windows))]
fn set_windows_app_id() {}

fn load_icon_data(bytes: &[u8]) -> Result<egui::IconData> {
    let image = image::load_from_memory(bytes)?.to_rgba8();
    let (width, height) = image.dimensions();
    Ok(egui::IconData {
        rgba: image.into_raw(),
        width,
        height,
    })
}

fn load_color_image(bytes: &[u8]) -> Result<egui::ColorImage> {
    let image = image::load_from_memory(bytes)?.to_rgba8();
    let size = [image.width() as usize, image.height() as usize];
    Ok(egui::ColorImage::from_rgba_unmultiplied(
        size,
        image.as_raw(),
    ))
}

fn get_detected_roots() -> Vec<String> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::GetLogicalDrives;

        let bitmask = unsafe { GetLogicalDrives() };
        let mut roots = Vec::new();
        for i in 0..26 {
            if bitmask & (1 << i) != 0 {
                roots.push(format!("{}:\\", (b'A' + i as u8) as char));
            }
        }
        if roots.is_empty() {
            vec!["C:\\".to_string()]
        } else {
            roots
        }
    }

    #[cfg(not(windows))]
    {
        vec!["/".to_string()]
    }
}

fn load_saved_default_roots() -> Vec<String> {
    let Ok(text) = fs::read_to_string(settings_path()) else {
        return Vec::new();
    };
    let Ok(settings) = serde_json::from_str::<Settings>(&text) else {
        return Vec::new();
    };
    split_roots(&settings.default_roots.join(";"))
}

fn save_default_roots(roots: &[String]) -> Result<()> {
    ensure_app_dir()?;
    let settings = Settings {
        default_roots: roots.to_vec(),
    };
    fs::write(settings_path(), serde_json::to_string_pretty(&settings)?)?;
    Ok(())
}

fn get_default_roots() -> Vec<String> {
    let saved = load_saved_default_roots();
    if saved.is_empty() {
        get_detected_roots()
    } else {
        saved
    }
}

fn normalize_root(root: &str) -> Option<String> {
    let mut value = root.trim().trim_matches('"').trim_matches('\'').to_string();
    if value.is_empty() {
        return None;
    }

    #[cfg(windows)]
    {
        value = value.replace('/', "\\");
        let chars: Vec<char> = value.chars().collect();
        if chars.len() == 1 && chars[0].is_ascii_alphabetic() {
            value = format!("{}:\\", chars[0].to_ascii_uppercase());
        } else if chars.len() == 2 && chars[0].is_ascii_alphabetic() && chars[1] == ':' {
            value = format!("{}:\\", chars[0].to_ascii_uppercase());
        }
    }

    Some(value)
}

fn normalized_root_key(root: &str) -> String {
    let value = normalize_root(root).unwrap_or_else(|| root.to_string());
    if cfg!(windows) {
        value.to_lowercase()
    } else {
        value
    }
}

fn split_roots(text: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for raw in text.split(';') {
        let Some(root) = normalize_root(raw) else {
            continue;
        };
        let key = normalized_root_key(&root);
        if seen.insert(key) {
            out.push(root);
        }
    }
    out
}

fn is_hidden_or_system(path: &Path) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        let Ok(metadata) = fs::symlink_metadata(path) else {
            return false;
        };
        let attrs = metadata.file_attributes();
        attrs & (FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_SYSTEM) != 0
    }

    #[cfg(not(windows))]
    {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with('.'))
    }
}

fn path_prefix_excluded(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let p = path.to_string_lossy().to_lowercase();
        WINDOWS_EXCLUDED_PREFIXES.iter().any(|prefix| {
            let x = prefix.to_lowercase();
            p == x || p.starts_with(&(x + "\\"))
        })
    }

    #[cfg(not(windows))]
    {
        let _ = path;
        false
    }
}

fn open_path(path: &str) -> Result<()> {
    open::that(path).context("opening item")
}

fn open_folder_for_path(path: &str) -> Result<()> {
    let p = Path::new(path);
    let folder = if p.is_file() {
        p.parent().unwrap_or_else(|| Path::new("."))
    } else {
        p
    };
    open::that(folder).context("opening folder")
}

fn format_size(value: Option<i64>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut n = value as f64;
    for (i, unit) in units.iter().enumerate() {
        if n < 1024.0 || i == units.len() - 1 {
            return if *unit == "B" {
                format!("{n:.0} {unit}")
            } else {
                format!("{n:.1} {unit}")
            };
        }
        n /= 1024.0;
    }
    format!("{value} B")
}

fn format_timestamp(ts: f64) -> String {
    if ts <= 0.0 {
        return String::new();
    }
    let Some(dt_utc) = DateTime::<Utc>::from_timestamp(ts as i64, 0) else {
        return String::new();
    };
    let dt = dt_utc.with_timezone(&Local);
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn format_count(value: u64) -> String {
    let raw = value.to_string();
    let mut out = String::new();
    for (idx, ch) in raw.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn wildcard_to_regex(pattern: &str) -> String {
    let mut out = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            _ => out.push_str(&regex::escape(&ch.to_string())),
        }
    }
    out.push('$');
    out
}

fn wildcard_to_like(pattern: &str) -> String {
    let mut out = String::new();
    for ch in pattern.chars() {
        match ch {
            '*' => out.push('%'),
            '?' => out.push('_'),
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn has_wildcards(text: &str) -> bool {
    text.contains('*') || text.contains('?')
}

fn choose_query_mode(mode: QueryMode, pattern: &str) -> QueryMode {
    if mode == QueryMode::Auto {
        if has_wildcards(pattern) {
            QueryMode::Wildcard
        } else {
            QueryMode::Regex
        }
    } else {
        mode
    }
}

fn extract_literal_hint(pattern: &str) -> String {
    let Ok(re) = Regex::new(r"[A-Za-z0-9._ -]{2,}") else {
        return String::new();
    };
    re.find_iter(pattern)
        .map(|m| m.as_str().trim().to_string())
        .max_by_key(|s| s.len())
        .unwrap_or_default()
}

fn compile_query_pattern(options: &SearchOptions) -> Result<Regex> {
    let mode = choose_query_mode(options.query_mode, &options.pattern);
    let source = if mode == QueryMode::Wildcard {
        wildcard_to_regex(&options.pattern)
    } else {
        options.pattern.clone()
    };
    RegexBuilder::new(&source)
        .case_insensitive(!options.case_sensitive)
        .build()
        .context("invalid pattern")
}

impl IndexDb {
    fn new(path: PathBuf) -> Result<Self> {
        ensure_app_dir()?;
        let db = Self { path };
        db.init()?;
        Ok(db)
    }

    fn connect(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(conn)
    }

    fn init(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
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
            "#,
        )?;

        let root_cols = table_columns(&conn, "roots")?;
        let item_cols = table_columns(&conn, "items")?;
        if !root_cols.contains("active_generation") {
            conn.execute(
                "ALTER TABLE roots ADD COLUMN active_generation INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !item_cols.contains("generation") {
            conn.execute(
                "ALTER TABLE items ADD COLUMN generation INTEGER NOT NULL DEFAULT 1",
                [],
            )?;
        }

        conn.execute(
            "UPDATE items SET generation = 1 WHERE generation IS NULL OR generation = 0",
            [],
        )?;
        conn.execute(
            r#"
            UPDATE roots
               SET active_generation = 1
             WHERE (active_generation IS NULL OR active_generation = 0)
               AND EXISTS (SELECT 1 FROM items WHERE items.root = roots.root)
            "#,
            [],
        )?;

        conn.execute_batch(
            r#"
            DROP INDEX IF EXISTS idx_items_root_path;
            DROP INDEX IF EXISTS idx_items_root_gen_path;
            DROP INDEX IF EXISTS idx_items_name;
            DROP INDEX IF EXISTS idx_items_path;
            DROP INDEX IF EXISTS idx_items_root_type;

            CREATE UNIQUE INDEX IF NOT EXISTS idx_items_root_gen_path ON items(root, generation, full_path);
            CREATE INDEX IF NOT EXISTS idx_items_name ON items(name);
            CREATE INDEX IF NOT EXISTS idx_items_path ON items(full_path);
            CREATE INDEX IF NOT EXISTS idx_items_root_type ON items(root, generation, is_dir);
            "#,
        )?;
        Ok(())
    }

    fn root_stats(&self) -> Result<Vec<(String, f64, i64, i64)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT root, last_indexed, item_count, active_generation FROM roots ORDER BY root",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        Ok(rows.filter_map(|row| row.ok()).collect())
    }

    fn indexed_roots(&self) -> Result<HashSet<String>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT root FROM roots WHERE active_generation > 0")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        Ok(rows
            .filter_map(|row| row.ok())
            .map(|root| normalized_root_key(&root))
            .collect())
    }

    fn partition_roots(&self, roots: &[String]) -> Result<(Vec<String>, Vec<String>)> {
        let indexed = self.indexed_roots()?;
        let mut indexed_roots = Vec::new();
        let mut live_roots = Vec::new();
        for root in roots {
            let norm = normalize_root(root).unwrap_or_else(|| root.clone());
            if indexed.contains(&normalized_root_key(&norm)) {
                indexed_roots.push(norm);
            } else {
                live_roots.push(norm);
            }
        }
        Ok((indexed_roots, live_roots))
    }

    fn rebuild_roots(
        &self,
        roots: &[String],
        cancel: &AtomicBool,
        tx: &Sender<AppEvent>,
        background: bool,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        for root in roots {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            self.rebuild_one_root(&mut conn, root, cancel, tx, background)?;
        }
        Ok(())
    }

    fn rebuild_one_root(
        &self,
        conn: &mut Connection,
        root: &str,
        cancel: &AtomicBool,
        tx: &Sender<AppEvent>,
        background: bool,
    ) -> Result<()> {
        let norm_root = normalize_root(root).unwrap_or_else(|| root.to_string());
        if !Path::new(&norm_root).exists() {
            let _ = tx.send(AppEvent::IndexStatus(format!("Skipping missing root: {norm_root}")));
            return Ok(());
        }

        let _ = tx.send(if background {
            AppEvent::BackgroundState(format!("Indexing {norm_root}"))
        } else {
            AppEvent::IndexStatus(format!("Indexing {norm_root}"))
        });

        let old_generation = conn
            .query_row(
                "SELECT active_generation FROM roots WHERE root = ?1",
                params![norm_root],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0);
        let new_generation = old_generation + 1;

        conn.execute(
            "DELETE FROM items WHERE root = ?1 AND generation = ?2",
            params![norm_root, new_generation],
        )?;

        let mut inserted = 0usize;
        let mut pending = Vec::with_capacity(1000);
        let mut stack = vec![PathBuf::from(&norm_root)];

        while let Some(current) = stack.pop() {
            if cancel.load(Ordering::Relaxed) {
                break;
            }

            let Ok(entries) = fs::read_dir(&current) else {
                continue;
            };

            for entry in entries.flatten() {
                if cancel.load(Ordering::Relaxed) {
                    break;
                }

                let path = entry.path();
                if path_prefix_excluded(&path) {
                    continue;
                }

                let file_type = entry.file_type();
                let is_dir = file_type.as_ref().is_ok_and(|ft| ft.is_dir());
                if is_dir {
                    stack.push(path.clone());
                }

                let metadata = fs::symlink_metadata(&path).ok();
                let size_bytes = if is_dir {
                    None
                } else {
                    metadata.as_ref().map(|m| m.len() as i64)
                };
                let modified = metadata
                    .and_then(|m| m.modified().ok())
                    .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);

                let name = entry.file_name().to_string_lossy().to_string();
                let full_path = path.to_string_lossy().to_string();
                let extension = if is_dir {
                    String::new()
                } else {
                    path.extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| format!(".{ext}"))
                        .unwrap_or_default()
                };
                let hidden = if is_hidden_or_system(&path) { 1 } else { 0 };

                pending.push(IndexRow {
                    root: norm_root.clone(),
                    generation: new_generation,
                    full_path,
                    name,
                    extension,
                    is_dir,
                    size_bytes,
                    modified,
                    hidden,
                });
                inserted += 1;

                if inserted % 500 == 0 {
                    let _ = tx.send(AppEvent::IndexProgress {
                        root: norm_root.clone(),
                        count: inserted,
                        background,
                    });
                }

                if pending.len() >= 1000 {
                    insert_pending(conn, &pending)?;
                    pending.clear();
                }
            }
        }

        if cancel.load(Ordering::Relaxed) {
            conn.execute(
                "DELETE FROM items WHERE root = ?1 AND generation = ?2",
                params![norm_root, new_generation],
            )?;
            return Ok(());
        }

        if !pending.is_empty() {
            insert_pending(conn, &pending)?;
        }

        conn.execute(
            r#"
            INSERT INTO roots(root, last_indexed, item_count, active_generation)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(root) DO UPDATE SET
                last_indexed = excluded.last_indexed,
                item_count = excluded.item_count,
                active_generation = excluded.active_generation
            "#,
            params![
                norm_root,
                Local::now().timestamp() as f64,
                inserted as i64,
                new_generation
            ],
        )?;
        conn.execute(
            "DELETE FROM items WHERE root = ?1 AND generation <> ?2",
            params![norm_root, new_generation],
        )?;
        Ok(())
    }

    fn search(
        &self,
        options: &SearchOptions,
        cancel: &AtomicBool,
        tx: &Sender<AppEvent>,
    ) -> Result<usize> {
        let mode = choose_query_mode(options.query_mode, &options.pattern);
        let regex = compile_query_pattern(options)?;
        let conn = self.connect()?;

        let mut sql = String::from(
            r#"
            SELECT items.name, items.full_path, items.is_dir, items.size_bytes, items.modified
            FROM items
            INNER JOIN roots
                ON roots.root = items.root
               AND roots.active_generation = items.generation
            WHERE 1=1
            "#,
        );
        let mut params_vec: Vec<String> = Vec::new();

        let roots: Vec<String> = options
            .roots
            .iter()
            .map(|r| normalize_root(r).unwrap_or_else(|| r.clone()))
            .collect();
        if !roots.is_empty() {
            sql.push_str(" AND items.root IN (");
            sql.push_str(&vec!["?"; roots.len()].join(","));
            sql.push(')');
            params_vec.extend(roots);
        }

        match options.search_type {
            SearchType::File => sql.push_str(" AND items.is_dir = 0"),
            SearchType::Directory => sql.push_str(" AND items.is_dir = 1"),
            SearchType::Any => {}
        }

        if !options.include_hidden {
            sql.push_str(" AND items.hidden = 0");
        }

        let subject_col = if options.match_name_only {
            "items.name"
        } else {
            "items.full_path"
        };

        if mode == QueryMode::Wildcard {
            let like = wildcard_to_like(&options.pattern);
            if options.case_sensitive {
                sql.push_str(&format!(" AND {subject_col} LIKE ? ESCAPE '\\'"));
            } else {
                sql.push_str(&format!(" AND lower({subject_col}) LIKE lower(?) ESCAPE '\\'"));
            }
            params_vec.push(like);
        } else {
            let hint = extract_literal_hint(&options.pattern);
            if !hint.is_empty() {
                if options.case_sensitive {
                    sql.push_str(&format!(" AND {subject_col} LIKE ?"));
                } else {
                    sql.push_str(&format!(" AND lower({subject_col}) LIKE lower(?)"));
                }
                params_vec.push(format!("%{hint}%"));
            }
        }

        sql.push_str(" ORDER BY items.name COLLATE NOCASE");
        let _ = tx.send(AppEvent::SearchStatus("Searching indexed roots...".to_string()));

        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(params_from_iter(params_vec.iter()))?;
        let mut batch = Vec::with_capacity(250);
        let mut count = 0usize;

        while !cancel.load(Ordering::Relaxed) {
            let Some(row) = rows.next()? else {
                break;
            };
            let name: String = row.get(0)?;
            let full_path: String = row.get(1)?;
            let is_dir: i64 = row.get(2)?;
            let size_bytes: Option<i64> = row.get(3)?;
            let modified: Option<f64> = row.get(4)?;

            let subject = if options.match_name_only {
                &name
            } else {
                &full_path
            };
            if !regex.is_match(subject) {
                continue;
            }

            batch.push(SearchResult {
                match_type: "Indexed".to_string(),
                item_type: if is_dir != 0 {
                    "Directory".to_string()
                } else {
                    "File".to_string()
                },
                name,
                full_path,
                size_bytes,
                modified: modified.unwrap_or(0.0),
            });
            count += 1;

            if options.max_results > 0 && count >= options.max_results {
                cancel.store(true, Ordering::Relaxed);
                break;
            }
            if batch.len() >= 250 {
                let _ = tx.send(AppEvent::SearchBatch(std::mem::take(&mut batch)));
            }
        }

        if !batch.is_empty() {
            let _ = tx.send(AppEvent::SearchBatch(batch));
        }

        Ok(count)
    }
}

struct IndexRow {
    root: String,
    generation: i64,
    full_path: String,
    name: String,
    extension: String,
    is_dir: bool,
    size_bytes: Option<i64>,
    modified: f64,
    hidden: i64,
}

fn table_columns(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    Ok(rows.filter_map(|row| row.ok()).collect())
}

fn insert_pending(conn: &mut Connection, pending: &[IndexRow]) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(
            r#"
            INSERT OR REPLACE INTO items
            (root, generation, full_path, name, extension, is_dir, size_bytes, modified, hidden)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
        )?;
        for row in pending {
            stmt.execute(params![
                row.root,
                row.generation,
                row.full_path,
                row.name,
                row.extension,
                if row.is_dir { 1 } else { 0 },
                row.size_bytes,
                row.modified,
                row.hidden
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

struct ISSearchApp {
    db: IndexDb,
    tx: Sender<AppEvent>,
    rx: Receiver<AppEvent>,
    q_logo: Option<TextureHandle>,
    is_logo: Option<TextureHandle>,

    pattern: String,
    roots_text: String,
    query_mode: QueryMode,
    search_type: SearchType,
    max_results: String,
    max_file_mb: String,
    index_interval: String,
    match_name_only: bool,
    include_hidden: bool,
    case_sensitive: bool,
    auto_search: bool,
    last_pattern: String,
    auto_search_at: Option<Instant>,

    results: Vec<SearchResult>,
    result_count: usize,
    sort_column: SortColumn,
    sort_reverse: bool,

    status: String,
    index_summary: String,
    bg_status: String,
    dialog: Option<String>,

    search_cancel: Option<Arc<AtomicBool>>,
    index_cancel: Option<Arc<AtomicBool>>,
    bg_cancel: Option<Arc<AtomicBool>>,
    search_running: bool,
    manual_index_running: bool,
    background_running: bool,

    initial_indexing: bool,
    initial_index_roots: Vec<String>,
    initial_index_current_root: String,
    initial_index_current_count: usize,
}

impl ISSearchApp {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let (tx, rx) = mpsc::channel();
        let db = match IndexDb::new(db_path()) {
            Ok(db) => db,
            Err(error) => {
                write_log(&format!("Could not initialize index database: {error:?}"));
                IndexDb { path: db_path() }
            }
        };

        let q_logo = load_color_image(Q_LOGO_BYTES)
            .ok()
            .map(|image| cc.egui_ctx.load_texture("q_logo", image, Default::default()));
        let is_logo = load_color_image(IS_LOGO_BYTES)
            .ok()
            .map(|image| cc.egui_ctx.load_texture("is_logo", image, Default::default()));

        let roots = get_default_roots();
        let roots_text = roots.join(";");
        let mut app = Self {
            db,
            tx,
            rx,
            q_logo,
            is_logo,
            pattern: String::new(),
            roots_text,
            query_mode: QueryMode::Auto,
            search_type: SearchType::Any,
            max_results: "0".to_string(),
            max_file_mb: "20".to_string(),
            index_interval: "15".to_string(),
            match_name_only: true,
            include_hidden: false,
            case_sensitive: false,
            auto_search: false,
            last_pattern: String::new(),
            auto_search_at: None,
            results: Vec::new(),
            result_count: 0,
            sort_column: SortColumn::Modified,
            sort_reverse: false,
            status: format!("Ready. Index DB: {}", db_path().display()),
            index_summary: String::new(),
            bg_status: "Background indexer: stopped".to_string(),
            dialog: None,
            search_cancel: None,
            index_cancel: None,
            bg_cancel: None,
            search_running: false,
            manual_index_running: false,
            background_running: false,
            initial_indexing: false,
            initial_index_roots: Vec::new(),
            initial_index_current_root: String::new(),
            initial_index_current_count: 0,
        };

        app.refresh_index_summary();
        let startup_roots = split_roots(&app.roots_text);
        let missing = app.get_unindexed_roots(&startup_roots);
        if !missing.is_empty() {
            app.initial_indexing = true;
            app.initial_index_roots = startup_roots;
            app.update_initial_placeholder(None, None);
            app.start_background_indexing(true, Some(missing));
        } else {
            app.start_background_indexing(false, None);
        }
        app
    }

    fn refresh_index_summary(&mut self) {
        match self.db.root_stats() {
            Ok(rows) => {
                let roots = rows.len();
                let total: i64 = rows.iter().map(|row| row.2).sum();
                let newest = rows.iter().map(|row| row.1).fold(0.0, f64::max);
                let newest_text = if newest > 0.0 {
                    format_timestamp(newest)
                } else {
                    "never".to_string()
                };
                self.index_summary = format!(
                    "Indexed roots: {roots} | Items: {} | Last update: {newest_text}",
                    format_count(total.max(0) as u64)
                );
            }
            Err(error) => self.index_summary = format!("Index summary unavailable: {error}"),
        }
    }

    fn get_unindexed_roots(&self, roots: &[String]) -> Vec<String> {
        let indexed = self.db.indexed_roots().unwrap_or_default();
        roots
            .iter()
            .filter_map(|root| normalize_root(root))
            .filter(|root| Path::new(root).exists())
            .filter(|root| !indexed.contains(&normalized_root_key(root)))
            .collect()
    }

    fn initial_index_finished(&self) -> bool {
        !self.initial_index_roots.is_empty()
            && self.get_unindexed_roots(&self.initial_index_roots).is_empty()
    }

    fn update_initial_placeholder(&mut self, root: Option<String>, count: Option<usize>) {
        if let Some(root) = root {
            self.initial_index_current_root = root;
        }
        if let Some(count) = count {
            self.initial_index_current_count = count;
        }
        let root = if self.initial_index_current_root.is_empty() {
            "default drives"
        } else {
            &self.initial_index_current_root
        };
        self.pattern = format!(
            "still indexing {root} - {} files",
            format_count(self.initial_index_current_count as u64)
        );
    }

    fn unlock_initial_indexing_if_done(&mut self) {
        if self.initial_indexing && self.initial_index_finished() {
            self.initial_indexing = false;
            if self.pattern.starts_with("still indexing") {
                self.pattern.clear();
            }
            self.status = "Ready.".to_string();
        }
    }

    fn collect_options(&self) -> Result<SearchOptions> {
        let pattern = self.pattern.trim().to_string();
        if pattern.is_empty() {
            anyhow::bail!("Enter a search pattern.");
        }
        let max_results = self
            .max_results
            .trim()
            .parse::<usize>()
            .context("Max results must be 0 or a positive integer.")?;
        let max_file_size_mb = self
            .max_file_mb
            .trim()
            .parse::<f64>()
            .context("Max file MB must be a positive number.")?;
        if max_file_size_mb <= 0.0 {
            anyhow::bail!("Max file MB must be a positive number.");
        }

        Ok(SearchOptions {
            pattern,
            roots: {
                let roots = split_roots(&self.roots_text);
                if roots.is_empty() {
                    get_default_roots()
                } else {
                    roots
                }
            },
            match_name_only: self.match_name_only,
            search_type: self.search_type,
            include_hidden: self.include_hidden,
            case_sensitive: self.case_sensitive,
            max_results,
            max_file_size_mb,
            query_mode: self.query_mode,
        })
    }

    fn start_search(&mut self) {
        if self.initial_indexing {
            self.status = "Still indexing. Search will be available when the first index finishes.".to_string();
            return;
        }
        if self.search_running {
            self.stop_search();
            return;
        }

        let mut options = match self.collect_options() {
            Ok(options) => options,
            Err(error) => {
                self.dialog = Some(error.to_string());
                return;
            }
        };

        let (indexed_roots, live_roots) = match self.db.partition_roots(&options.roots) {
            Ok(roots) => roots,
            Err(error) => {
                self.dialog = Some(format!("Could not inspect indexed roots:\n{error}"));
                return;
            }
        };

        if !live_roots.is_empty() && !indexed_roots.is_empty() {
            options.roots = indexed_roots;
        } else if !live_roots.is_empty() {
            self.dialog =
                Some("Search is index-only, but one or more selected roots are not indexed yet.".to_string());
            return;
        }

        self.results.clear();
        self.result_count = 0;
        self.status = "Starting indexed search...".to_string();
        self.search_running = true;
        let cancel = Arc::new(AtomicBool::new(false));
        self.search_cancel = Some(cancel.clone());
        let db = self.db.clone();
        let tx = self.tx.clone();

        thread::spawn(move || {
            let started = Instant::now();
            match db.search(&options, cancel.as_ref(), &tx) {
                Ok(count) => {
                    let _ = tx.send(AppEvent::SearchDone {
                        count,
                        duration: started.elapsed().as_secs_f64(),
                        cancelled: cancel.load(Ordering::Relaxed),
                    });
                }
                Err(error) => {
                    write_log(&format!("Indexed search failed: {error:?}"));
                    let _ = tx.send(AppEvent::SearchError(format!(
                        "Indexed search failed: {error}"
                    )));
                }
            }
        });
    }

    fn stop_search(&mut self) {
        if let Some(cancel) = &self.search_cancel {
            cancel.store(true, Ordering::Relaxed);
            self.status = "Stopping search...".to_string();
        }
    }

    fn start_manual_index(&mut self) {
        if self.manual_index_running {
            self.dialog = Some("Manual indexing is already running.".to_string());
            return;
        }
        let roots = {
            let roots = split_roots(&self.roots_text);
            if roots.is_empty() {
                get_default_roots()
            } else {
                roots
            }
        };
        self.manual_index_running = true;
        self.status = "Indexing selected roots in the background...".to_string();
        let cancel = Arc::new(AtomicBool::new(false));
        self.index_cancel = Some(cancel.clone());
        let db = self.db.clone();
        let tx = self.tx.clone();

        thread::spawn(move || {
            let started = Instant::now();
            let result = db.rebuild_roots(&roots, cancel.as_ref(), &tx, false);
            match result {
                Ok(()) => {
                    let _ = tx.send(AppEvent::IndexDone {
                        duration: started.elapsed().as_secs_f64(),
                        cancelled: cancel.load(Ordering::Relaxed),
                        background: false,
                    });
                }
                Err(error) => {
                    write_log(&format!("Indexing failed: {error:?}"));
                    let _ = tx.send(AppEvent::IndexError(format!("Indexing failed: {error}")));
                }
            }
        });
    }

    fn start_background_indexing(&mut self, initial: bool, initial_roots: Option<Vec<String>>) {
        if self.background_running {
            if !initial {
                self.dialog = Some("Background indexing is already running.".to_string());
            }
            return;
        }

        let roots = {
            let roots = split_roots(&self.roots_text);
            if roots.is_empty() {
                get_default_roots()
            } else {
                roots
            }
        };
        let interval_minutes = self.max_background_interval_minutes();
        let cancel = Arc::new(AtomicBool::new(false));
        self.bg_cancel = Some(cancel.clone());
        self.background_running = true;

        let db = self.db.clone();
        let tx = self.tx.clone();
        let initial_roots = initial_roots.unwrap_or_default();
        thread::spawn(move || {
            let _ = tx.send(AppEvent::BackgroundState(format!(
                "running every {interval_minutes} min"
            )));
            let mut first_cycle = true;
            while !cancel.load(Ordering::Relaxed) {
                let started = Instant::now();
                let cycle_roots = if first_cycle && !initial_roots.is_empty() {
                    initial_roots.clone()
                } else {
                    roots.clone()
                };
                first_cycle = false;

                if let Err(error) = db.rebuild_roots(&cycle_roots, cancel.as_ref(), &tx, true) {
                    write_log(&format!("Background indexing failed: {error:?}"));
                    let _ = tx.send(AppEvent::IndexError(format!(
                        "Background indexing failed: {error}"
                    )));
                    break;
                }

                if cancel.load(Ordering::Relaxed) {
                    break;
                }
                let _ = tx.send(AppEvent::IndexDone {
                    duration: started.elapsed().as_secs_f64(),
                    cancelled: false,
                    background: true,
                });

                for _ in 0..(interval_minutes * 60) {
                    if cancel.load(Ordering::Relaxed) {
                        break;
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
            let _ = tx.send(AppEvent::BackgroundState("stopped".to_string()));
        });
    }

    fn max_background_interval_minutes(&self) -> u64 {
        self.index_interval
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|minutes| *minutes > 0)
            .unwrap_or(15)
    }

    fn stop_background_indexing(&mut self) {
        if let Some(cancel) = &self.bg_cancel {
            cancel.store(true, Ordering::Relaxed);
            self.bg_status = "Stopping background indexer...".to_string();
        }
    }

    fn save_roots_as_default(&mut self) {
        let roots = split_roots(&self.roots_text);
        if roots.is_empty() {
            self.dialog = Some("Add at least one root before saving defaults.".to_string());
            return;
        }
        match save_default_roots(&roots) {
            Ok(()) => {
                self.status = format!(
                    "Saved {} default root(s). They will be used when IS Search starts.",
                    roots.len()
                );
            }
            Err(error) => {
                self.dialog = Some(format!("Could not save default roots:\n{error}"));
            }
        }
    }

    fn browse_root(&mut self) {
        if let Some(folder) = rfd::FileDialog::new().pick_folder() {
            let mut roots = split_roots(&self.roots_text);
            roots.push(folder.to_string_lossy().to_string());
            self.roots_text = split_roots(&roots.join(";")).join(";");
        }
    }

    fn poll_events(&mut self) {
        while let Ok(event) = self.rx.try_recv() {
            match event {
                AppEvent::SearchBatch(batch) => {
                    self.results.extend(batch);
                    self.result_count = self.results.len();
                }
                AppEvent::SearchDone {
                    count,
                    duration,
                    cancelled,
                } => {
                    self.search_running = false;
                    self.search_cancel = None;
                    self.result_count = count;
                    let suffix = if cancelled { " (stopped)" } else { "" };
                    self.status = format!("Indexed finished in {duration:.1}s{suffix}");
                }
                AppEvent::SearchError(message) => {
                    self.search_running = false;
                    self.search_cancel = None;
                    self.status = message.clone();
                    self.dialog = Some(format!("{message}\n\nLog: {}", log_path().display()));
                }
                AppEvent::SearchStatus(message) => self.status = message,
                AppEvent::IndexStatus(message) => self.status = message,
                AppEvent::IndexProgress {
                    root,
                    count,
                    background,
                } => {
                    if self.initial_indexing {
                        self.update_initial_placeholder(Some(root.clone()), Some(count));
                    }
                    if background {
                        self.bg_status = format!(
                            "Background indexer: {root}... {} items staged",
                            format_count(count as u64)
                        );
                    } else {
                        self.status = format!(
                            "Indexing {root}... {} items staged",
                            format_count(count as u64)
                        );
                    }
                }
                AppEvent::IndexDone {
                    duration,
                    cancelled,
                    background,
                } => {
                    self.refresh_index_summary();
                    self.unlock_initial_indexing_if_done();
                    let suffix = if cancelled { " (stopped)" } else { "" };
                    if background {
                        self.bg_status = format!(
                            "Background indexer: cycle finished in {duration:.1}s; waiting for next interval"
                        );
                    } else {
                        self.manual_index_running = false;
                        self.index_cancel = None;
                        self.status = format!("Manual index finished in {duration:.1}s{suffix}");
                    }
                }
                AppEvent::IndexError(message) => {
                    self.manual_index_running = false;
                    self.index_cancel = None;
                    self.bg_status = message.clone();
                    self.dialog = Some(format!("{message}\n\nLog: {}", log_path().display()));
                }
                AppEvent::BackgroundState(message) => {
                    self.bg_status = format!("Background indexer: {message}");
                    if message.to_lowercase().contains("stopped") {
                        self.background_running = false;
                        self.bg_cancel = None;
                        self.refresh_index_summary();
                    }
                    if self.initial_indexing && message.starts_with("Indexing ") {
                        let root = message.trim_start_matches("Indexing ").to_string();
                        self.update_initial_placeholder(Some(root), Some(0));
                    }
                }
            }
        }
    }

    fn show_header(&mut self, ui: &mut egui::Ui) {
        egui::Frame::none()
            .stroke(egui::Stroke::new(1.0, Color32::from_gray(190)))
            .inner_margin(egui::Margin::same(10.0))
            .show(ui, |ui| {
                ui.vertical(|ui| {
                    ui.label("Search");
                    ui.horizontal(|ui| {
                        logo(ui, &self.q_logo);
                        ui.vertical(|ui| {
                            ui.label("Pattern");
                            let response = ui.add_enabled(
                                !self.initial_indexing,
                                egui::TextEdit::singleline(&mut self.pattern)
                                    .desired_width(f32::INFINITY),
                            );
                            if response.changed() && self.auto_search {
                                self.auto_search_at = Some(Instant::now() + Duration::from_millis(350));
                            }
                            if response.lost_focus()
                                && ui.input(|input| input.key_pressed(egui::Key::Enter))
                            {
                                self.start_search();
                            }
                        });
                        ui.vertical(|ui| {
                            ui.label("Pattern mode");
                            egui::ComboBox::from_id_source("query_mode")
                                .selected_text(self.query_mode.label())
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.query_mode, QueryMode::Auto, "Auto");
                                    ui.selectable_value(
                                        &mut self.query_mode,
                                        QueryMode::Wildcard,
                                        "Wildcard",
                                    );
                                    ui.selectable_value(&mut self.query_mode, QueryMode::Regex, "Regex");
                                });
                        });
                        if ui
                            .add_enabled(!self.initial_indexing, egui::Button::new("Search"))
                            .clicked()
                        {
                            self.start_search();
                        }
                        if ui
                            .add_enabled(self.search_running, egui::Button::new("Stop"))
                            .clicked()
                        {
                            self.stop_search();
                        }
                        logo(ui, &self.is_logo);
                    });
                    ui.add_space(6.0);
                    ui.label("Auto pattern: TAP*.xlsx uses wildcard. Default search uses the current index.");
                });
            });
    }

    fn show_options(&mut self, ui: &mut egui::Ui) {
        egui::Frame::none()
            .stroke(egui::Stroke::new(1.0, Color32::from_gray(190)))
            .inner_margin(egui::Margin::same(10.0))
            .show(ui, |ui| {
                ui.label("Options");
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label("Roots (semicolon-separated)");
                        ui.horizontal(|ui| {
                            ui.add(
                                egui::TextEdit::singleline(&mut self.roots_text)
                                    .desired_width(f32::INFINITY),
                            );
                            if ui.button("Browse...").clicked() {
                                self.browse_root();
                            }
                            if ui.button("Save default").clicked() {
                                self.save_roots_as_default();
                            }
                        });
                    });
                    ui.vertical(|ui| {
                        ui.label("Type");
                        egui::ComboBox::from_id_source("type")
                            .selected_text(self.search_type.label())
                            .show_ui(ui, |ui| {
                                ui.selectable_value(&mut self.search_type, SearchType::Any, "Any");
                                ui.selectable_value(&mut self.search_type, SearchType::File, "File");
                                ui.selectable_value(
                                    &mut self.search_type,
                                    SearchType::Directory,
                                    "Directory",
                                );
                            });
                    });
                    ui.vertical(|ui| {
                        ui.label("Max results");
                        ui.add(egui::TextEdit::singleline(&mut self.max_results).desired_width(80.0));
                    });
                    ui.vertical(|ui| {
                        ui.label("Max file MB");
                        ui.add(egui::TextEdit::singleline(&mut self.max_file_mb).desired_width(80.0));
                    });
                });
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.match_name_only, "Match name only");
                    ui.checkbox(&mut self.include_hidden, "Include hidden/system");
                    ui.checkbox(&mut self.case_sensitive, "Case sensitive");
                    ui.checkbox(&mut self.auto_search, "Auto search while typing");
                });
            });
    }

    fn show_index_controls(&mut self, ui: &mut egui::Ui) {
        egui::Frame::none()
            .stroke(egui::Stroke::new(1.0, Color32::from_gray(190)))
            .inner_margin(egui::Margin::same(10.0))
            .show(ui, |ui| {
                ui.label("Index");
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!self.manual_index_running, egui::Button::new("Refresh index now"))
                        .clicked()
                    {
                        self.start_manual_index();
                    }
                    if ui
                        .add_enabled(!self.background_running, egui::Button::new("Start background indexing"))
                        .clicked()
                    {
                        self.start_background_indexing(false, None);
                    }
                    if ui
                        .add_enabled(self.background_running, egui::Button::new("Stop background indexing"))
                        .clicked()
                    {
                        self.stop_background_indexing();
                    }
                    ui.label("Interval (min)");
                    ui.add(egui::TextEdit::singleline(&mut self.index_interval).desired_width(50.0));
                    if ui.button("Show indexed roots").clicked() {
                        self.show_indexed_roots();
                    }
                });
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.label(&self.index_summary);
                    ui.separator();
                    ui.label(&self.bg_status);
                });
            });
    }

    fn show_indexed_roots(&mut self) {
        match self.db.root_stats() {
            Ok(rows) if rows.is_empty() => self.dialog = Some("No roots have been indexed yet.".to_string()),
            Ok(rows) => {
                let lines: Vec<String> = rows
                    .into_iter()
                    .map(|row| {
                        format!(
                            "{}  |  {} items  |  {}",
                            row.0,
                            format_count(row.2.max(0) as u64),
                            format_timestamp(row.1)
                        )
                    })
                    .collect();
                self.dialog = Some(lines.join("\n"));
            }
            Err(error) => self.dialog = Some(format!("Could not load indexed roots:\n{error}")),
        }
    }

    fn show_results(&mut self, ui: &mut egui::Ui) {
        let mut sort_request = None;
        let mut action = None;

        TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(Column::initial(110.0))
            .column(Column::initial(90.0))
            .column(Column::initial(260.0))
            .column(Column::remainder().at_least(360.0))
            .column(Column::initial(100.0))
            .column(Column::initial(160.0))
            .header(22.0, |mut header| {
                header.col(|ui| header_button(ui, "Match", SortColumn::Match, &mut sort_request));
                header.col(|ui| header_button(ui, "Type", SortColumn::Type, &mut sort_request));
                header.col(|ui| header_button(ui, "Name", SortColumn::Name, &mut sort_request));
                header.col(|ui| header_button(ui, "Full Path", SortColumn::FullPath, &mut sort_request));
                header.col(|ui| header_button(ui, "Size", SortColumn::Size, &mut sort_request));
                header.col(|ui| header_button(ui, "Modified", SortColumn::Modified, &mut sort_request));
            })
            .body(|body| {
                body.rows(22.0, self.results.len(), |mut row| {
                    let idx = row.index();
                    let result = self.results[idx].clone();
                    row.col(|ui| {
                        ui.label(&result.match_type);
                    });
                    row.col(|ui| {
                        ui.label(&result.item_type);
                    });
                    row.col(|ui| {
                        ui.label(&result.name);
                    });
                    row.col(|ui| {
                        let response = ui.selectable_label(false, &result.full_path);
                        if response.double_clicked() {
                            action = Some(ResultAction::Open(result.full_path.clone()));
                        }
                        response.context_menu(|ui| {
                            if ui.button("Open").clicked() {
                                action = Some(ResultAction::Open(result.full_path.clone()));
                                ui.close_menu();
                            }
                            if ui.button("Open folder").clicked() {
                                action = Some(ResultAction::OpenFolder(result.full_path.clone()));
                                ui.close_menu();
                            }
                            ui.separator();
                            if ui.button("Copy path").clicked() {
                                action = Some(ResultAction::Copy(result.full_path.clone()));
                                ui.close_menu();
                            }
                        });
                    });
                    row.col(|ui| {
                        ui.label(format_size(result.size_bytes));
                    });
                    row.col(|ui| {
                        ui.label(format_timestamp(result.modified));
                    });
                });
            });

        if let Some(column) = sort_request {
            self.sort_by(column);
        }
        if let Some(action) = action {
            self.handle_result_action(action, ui.ctx());
        }
    }

    fn handle_result_action(&mut self, action: ResultAction, ctx: &egui::Context) {
        match action {
            ResultAction::Open(path) => {
                if let Err(error) = open_path(&path) {
                    self.dialog = Some(format!("Could not open item:\n{error}"));
                }
            }
            ResultAction::OpenFolder(path) => {
                if let Err(error) = open_folder_for_path(&path) {
                    self.dialog = Some(format!("Could not open folder:\n{error}"));
                }
            }
            ResultAction::Copy(path) => {
                ctx.copy_text(path);
                self.status = "Path copied to clipboard.".to_string();
            }
        }
    }

    fn sort_by(&mut self, column: SortColumn) {
        let reverse = if self.sort_column == column {
            self.sort_reverse
        } else {
            false
        };
        self.results.sort_by(|a, b| {
            let ord = match column {
                SortColumn::Match => a.match_type.to_lowercase().cmp(&b.match_type.to_lowercase()),
                SortColumn::Type => a.item_type.to_lowercase().cmp(&b.item_type.to_lowercase()),
                SortColumn::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
                SortColumn::FullPath => a.full_path.to_lowercase().cmp(&b.full_path.to_lowercase()),
                SortColumn::Size => a.size_bytes.unwrap_or(-1).cmp(&b.size_bytes.unwrap_or(-1)),
                SortColumn::Modified => a
                    .modified
                    .partial_cmp(&b.modified)
                    .unwrap_or(std::cmp::Ordering::Equal),
            };
            if reverse {
                ord.reverse()
            } else {
                ord
            }
        });
        self.sort_column = column;
        self.sort_reverse = !reverse;
    }
}

impl eframe::App for ISSearchApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.poll_events();

        if self.auto_search
            && !self.initial_indexing
            && !self.search_running
            && !self.pattern.trim().is_empty()
            && self.pattern != self.last_pattern
        {
            if self.auto_search_at.is_none() {
                self.auto_search_at = Some(Instant::now() + Duration::from_millis(350));
            }
            if self.auto_search_at.is_some_and(|when| Instant::now() >= when) {
                self.last_pattern = self.pattern.clone();
                self.auto_search_at = None;
                self.start_search();
            }
        }

        egui::TopBottomPanel::bottom("status_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(&self.status);
                ui.separator();
                ui.label(format!("{} results", format_count(self.result_count as u64)));
            });
            ui.label(format!("Log: {}", log_path().display()));
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            self.show_header(ui);
            ui.add_space(8.0);
            self.show_options(ui);
            ui.add_space(8.0);
            self.show_index_controls(ui);
            ui.add_space(8.0);
            self.show_results(ui);
        });

        if let Some(message) = self.dialog.clone() {
            egui::Window::new(APP_TITLE)
                .collapsible(false)
                .resizable(true)
                .show(ctx, |ui| {
                    ui.label(message);
                    if ui.button("OK").clicked() {
                        self.dialog = None;
                    }
                });
        }

        ctx.request_repaint_after(Duration::from_millis(100));
    }
}

fn logo(ui: &mut egui::Ui, texture: &Option<TextureHandle>) {
    egui::Frame::none()
        .fill(Color32::WHITE)
        .inner_margin(egui::Margin::same(4.0))
        .show(ui, |ui| {
            if let Some(texture) = texture {
                ui.image((texture.id(), texture.size_vec2()));
            }
        });
}

fn header_button(
    ui: &mut egui::Ui,
    text: &str,
    column: SortColumn,
    sort_request: &mut Option<SortColumn>,
) {
    if ui.button(text).clicked() {
        *sort_request = Some(column);
    }
}
