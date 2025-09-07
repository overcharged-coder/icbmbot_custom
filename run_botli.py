#!/usr/bin/env python3
# run_botli.py — BotLi-style launcher for trash_bot.py
import os, sys, platform, argparse, yaml

def setenv(k, v):
    if v is None: return
    if isinstance(v, bool): v = "true" if v else "false"
    os.environ[k] = str(v)

def pick_engine(cfg):
    # Pick engine by name from engines:standard. Adjust for OS automatically.
    eng = (cfg.get("engines") or {}).get("standard") or {}
    name = eng.get("name") or ""
    # If user left a Linux build name on Windows, try to replace with a .exe
    if platform.system() == "Windows" and not name.lower().endswith(".exe"):
        # common Windows filenames users download; edit to match your file
        for guess in [
            "stockfish-windows-x86-64-avxvnni.exe",
            "stockfish-windows-x86-64-avx2.exe",
            "stockfish.exe",
        ]:
            if os.path.exists(os.path.join(eng.get("dir","."), guess)):
                name = guess
                break
    return os.path.join(eng.get("dir","."), name)

def apply_config_to_env(cfg):
    # --- token ---
    tok = (cfg.get("token") or "").strip()
    if tok and not os.getenv("LICHESS_API_TOKEN"):
        setenv("LICHESS_API_TOKEN", tok)

    # --- standard engine ---
    engine_path = pick_engine(cfg)
    if engine_path:
        setenv("STOCKFISH_PATH", engine_path)

    # --- UCI options (Threads/Hash/Move Overhead) ---
    std = (cfg.get("engines") or {}).get("standard") or {}
    uci = (std.get("uci_options") or {})
    setenv("ENGINE_THREADS", uci.get("Threads"))
    setenv("ENGINE_HASH_MB", uci.get("Hash"))
    setenv("MOVE_OVERHEAD_MS", uci.get("Move Overhead"))

    # --- Syzygy ---
    syz = (cfg.get("syzygy") or {}).get("standard") or {}
    if syz.get("enabled"):
        paths = ";".join(syz.get("paths") or [])
        setenv("SYZYGY_PATH", paths)
        setenv("SYZYGY_PROBE_DEPTH", 4)
        setenv("SYZYGY_PROBE_LIMIT", syz.get("max_pieces", 6))
        setenv("SYZYGY_50_MOVE_RULE", True)
    else:
        setenv("SYZYGY_PATH", "")

    # --- Opening books (map to envs your bot already reads) ---
    books = cfg.get("books") or {}
    # pick some sensible defaults from your YAML names
    setenv("POLYGLOT_BOOK_PATH_WHITE", books.get("Perfect2023"))
    setenv("POLYGLOT_BOOK_PATH_BLACK", books.get("niki"))
    setenv("POLYGLOT_BOOK_PATH",      books.get("draw"))
    # optional mode: decisive/drawish/mixed
    setenv("POLYGLOT_MODE", (cfg.get("opening_books") or {}).get("mode", "decisive"))

    # --- Challenge policy (only standard if variants engines are disabled) ---
    # If user kept variant sections commented/disabled, force standard only
    allowed = (cfg.get("challenge") or {}).get("variants") or ["standard"]
    standard_only = (allowed == ["standard"]) or all(v.lower()=="standard" for v in allowed)
    setenv("ACCEPT_VARIANT_STANDARD", True)
    if standard_only:
        # keep it safe if variant engines are commented out
        pass

    # time controls → pick widest range you already support via envs
    # You already control min base via ACCEPT_MIN_BASE_SECONDS; infer from config if present
    setenv("ACCEPT_MIN_BASE_SECONDS", (cfg.get("challenge") or {}).get("min_initial", 60))

    # --- Proactive challenges / matchmaking knobs map to your envs ---
    mm = cfg.get("matchmaking") or {}
    setenv("PROACTIVE_CHALLENGES", True)  # your bot already supports this
    # Use your clock defaults; you can override with tags if you like
    setenv("CHALLENGE_CLOCK_SEC", 600)    # 10+0 default
    setenv("CHALLENGE_INC", 0)
    setenv("CHALLENGE_RATED", True)

    # --- Concurrency / engine perf ---
    setenv("MAX_ACTIVE_GAMES", (cfg.get("challenge") or {}).get("concurrency", 1))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yml")
    ap.add_argument("--", dest="dashdash", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    args = ap.parse_args()

    try:
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        print(f"[run_botli] Could not open {args.config}.")
        sys.exit(2)

    apply_config_to_env(cfg)

    # finally, run your existing bot
    import subprocess
    cmd = [sys.executable, "trash_bot.py"]
    # pass through remaining args if any
    if args.dashdash: cmd += args.dashdash
    os.execv(sys.executable, [sys.executable, "trash_bot.py"])

if __name__ == "__main__":
    main()
