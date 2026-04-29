# IS Search Rust

Rust port of the IS Search desktop app.

## Build

Install Rust from https://rustup.rs, then run:

```powershell
cd IS_Search_Rust
cargo build --release
```

The compiled app will be:

```text
target\release\IS_Search.exe
```

## Notes

- Uses the same app data folder as the Python app: `%LOCALAPPDATA%\ISSearch`.
- Uses the same SQLite database file: `is_search_index.sqlite3`.
- Uses the same saved roots file: `settings.json`.
- Uses the same Q logo as the executable/window icon.
- Search is index-only.
- Background indexing starts automatically at a 15 minute interval by default.
- First startup blocks search until all configured default roots are indexed.
