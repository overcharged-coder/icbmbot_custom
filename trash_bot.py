#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trash_bot.py — Lichess bot (single-game optimized, resilient, deduped)

Key fixes & improvements:
- ✅ Deduplicates game handlers with a thread-safe claim_game() so a game only ever has one handler thread (prevents repeated "Opponent ... You are Black" + spam).
- ✅ Tames "not your turn / game already over" errors: logs once, returns False, and lets the loop wait for the next update (no scary 'Stopping.' spam).
- ✅ Keeps capacity and active-games accounting consistent: claim before spawn; remove on finalize.
- ✅ Safer engine option config & Syzygy settings (only set supported options).
- ✅ More precise logging (includes game id tag).
- ✅ Robust reconnection for both incoming events and per-game streams.

Requirements:
  pip install berserk python-chess requests beautifulsoup4
  # Optional: python-chess[polyglot] for book, syzygy tablebases present on disk if used.

Environment variables (common):
  LICHESS_API_TOKEN : your bot token
  STOCKFISH_PATH    : path to your Stockfish binary
  MAX_ACTIVE_GAMES  : default 1 (best Elo/latency)
  POLYGLOT_BOOK_PATH: optional opening book
  SYZYGY_PATH       : optional TB path(s), e.g. "D:\\tb;E:\\tb:/mnt/tb"
"""

import os, sys, time, random, threading, traceback
from chatter import Chatter
from datetime import datetime
from typing import Optional, Tuple

# Third-party
import requests
from bs4 import BeautifulSoup
import berserk
import re
import chess
import chess.engine
from asyncio import TimeoutError as AsyncTimeoutError  # noqa: F401 (kept for compatibility)
import asyncio  # noqa: F401 (kept for compatibility)
from compute_time_management import compute_movetime
import chess.polyglot
# put this near the top (after imports), e.g. under INIT
from datetime import datetime

def print_banner(commit_hash: str = None):
    # Try a pretty banner
    try:
        import pyfiglet
        banner = pyfiglet.figlet_format("ICBM BOT", font="big")
    except Exception:
        # Minimal fallback if pyfiglet isn't installed
        banner = r"""
 ___ ___  ___  __  __     ____   ___ _____ 
|_ _/ _ \| _ )|  \/  |   | __ ) / _ \_   _|
 | | (_) | _ \| |\/| |   |  _ \| | | || |  
|___\___/|___/|_|  |_|   | |_) | |_| || |  
                         |____/ \___/ |_|  
""".lstrip("\n")

    # date+commit stamp (YYYYMMDD-<shortcommit>), commit optional
    stamp = datetime.now().strftime("%Y%m%d")
    if commit_hash:
        stamp = f"{stamp}-{commit_hash[:8]}"

    print(banner.rstrip())
    print(stamp)

# =====================
# SETTINGS
# =====================
# ---------- FEN cache toggle/path ----------
USE_FEN_CACHE = os.getenv("USE_FEN_CACHE", "false").lower() in ("1","true","yes","y","on")
FEN_CACHE_PATH = os.getenv("FEN_CACHE_PATH", r"G:\My Drive\stockfish_fen\stockfish_fen_cache.json").strip()
# ---------- Logging ----------
# If true, write structured JSONL per game into LOG_DIR/<gid>.jsonl
LOG_TO_FILE   = os.getenv("LOG_TO_FILE", "false").lower() in ("1","true","yes","y","on")
LOG_DIR       = os.getenv("LOG_DIR", "logs").strip()
# If true, print a compact single-line summary for each move to stdout
LOG_PRETTY    = os.getenv("LOG_PRETTY", "true").lower() in ("1","true","yes","y","on")
MAX_PV_UCIS   = int(os.getenv("MAX_PV_UCIS", "6"))

# --- Globals ---
rating_cache = {}  # key: (username_lower, perf_key) -> (timestamp, rating|None)
# ----------------------
# FEN evaluation cache
# ----------------------
import json

fen_cache = {}

def _migrate_cache_keys(cache: dict) -> dict:
    new = {}
    for old_key, entry in cache.items():
        try:
            nk = normalize_fen(old_key)
        except Exception:
            nk = old_key  # if something weird, keep it
        # prefer the entry with higher depth if colliding
        if nk in new:
            a = new[nk].get("depth_reached") or 0
            b = entry.get("depth_reached") or 0
            if b > a:
                new[nk] = entry
        else:
            new[nk] = entry
    return new

def load_fen_cache():
    if not USE_FEN_CACHE:
        return
    global fen_cache
    if not os.path.exists(FEN_CACHE_PATH):
        with open(FEN_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f)
    try:
        with open(FEN_CACHE_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        fen_cache = _migrate_cache_keys(raw)
        if fen_cache != raw:
            with open(FEN_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(fen_cache, f, indent=2)
    except Exception:
        fen_cache = {}


def save_fen_cache():
    if not USE_FEN_CACHE:
        return
    try:
        with open(FEN_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(fen_cache, f, indent=2)
    except Exception as e:
        log_exc("save_fen_cache", e)


MIN_CHALLENGE_RATING = int(os.getenv("MIN_CHALLENGE_RATING", "3000"))
RATING_CACHE_TTL     = int(os.getenv("RATING_CACHE_TTL", "600"))  # seconds
# ---------- Offload/Queue settings ----------
FEN_QUEUE_DIR         = os.getenv("FEN_QUEUE_DIR", r"G:\My Drive\chess_fen_queue").strip()
OFFLOAD_ON_MISS       = os.getenv("OFFLOAD_ON_MISS", "true").lower() in ("1","true","yes","y")
OFFLOAD_MIN_DEPTH     = int(os.getenv("OFFLOAD_MIN_DEPTH", "22"))   # how deep Colab should analyse
OFFLOAD_PRIORITY      = int(os.getenv("OFFLOAD_PRIORITY", "5"))     # 1..9 (1 = highest)
ENQUEUE_PV_PLIES      = int(os.getenv("ENQUEUE_PV_PLIES", "10"))     # how many PV steps to pre-enqueue
CACHE_HOT_RELOAD_MS   = int(os.getenv("CACHE_HOT_RELOAD_MS", "10000"))# check cache file mtime every N ms

# ensure queue dir exists
try:
    os.makedirs(FEN_QUEUE_DIR, exist_ok=True)
except Exception:
    pass


API_TOKEN      = os.getenv("LICHESS_API_TOKEN", "").strip()
STOCKFISH_PATH = os.getenv("STOCKFISH_PATH", "").strip()
SSL_CERT_FILE  = os.getenv("SSL_CERT_FILE", "").strip()

# Network / reconnect policy
RECONNECT_DELAY_SEC = float(os.getenv("RECONNECT_DELAY_SEC", "5.0"))
MAX_NET_RETRIES     = int(os.getenv("MAX_NET_RETRIES", "6"))
ONLY_TOURNAMENT_ID  = os.getenv("ONLY_TOURNAMENT_ID", "").strip()  # optional filter

# ---- Local timing floors / legacy fallbacks ----
MIN_MOVE_TIME = float(os.getenv("MIN_MOVE_TIME", "0.40"))  # seconds
MAX_MOVE_TIME = float(os.getenv("MAX_MOVE_TIME", "6.00"))  # clamp for pick_play_limit fallback

# If pick_play_limit() gets used, give it sane defaults so it won't crash
PANIC_TIME_SECS   = float(os.getenv("TM_PANIC_HARD_S", "5.0"))        # match your TM_PANIC_HARD_S
EARLY_PHASE_MOVE  = int(os.getenv("TM_PHASE1_PLY", "24"))             # opening→middlegame cutoff
EARLY_FRACTION    = float(os.getenv("EARLY_FRACTION", "0.040"))       # 4% of (time + inc credit)
LATE_FRACTION     = float(os.getenv("LATE_FRACTION", "0.030"))        # 3% later in the game
INC_WEIGHT        = float(os.getenv("INC_WEIGHT", "0.60"))            # credit 60% of increment

# Concurrency: best Elo with 1. You can set >1, but threads per game will shrink.
MAX_ACTIVE_GAMES = int(os.getenv("MAX_ACTIVE_GAMES", "1"))

# Engine resources
ENGINE_THREADS     = os.getenv("ENGINE_THREADS", "auto").strip()  # "auto" = cores // MAX_ACTIVE_GAMES
ENGINE_HASH_MB     = int(os.getenv("ENGINE_HASH_MB", "1024"))
MOVE_OVERHEAD_MS   = int(os.getenv("MOVE_OVERHEAD_MS", "60"))
SLOW_MOVER         = int(os.getenv("SLOW_MOVER", "80"))           # 70–100 typical
PANIC_DEPTH        = int(os.getenv("PANIC_DEPTH", "8"))
RETRY_DELAY_S      = float(os.getenv("ENGINE_RETRY_DELAY_S", "5.0"))
ENGINE_MAX_RETRIES = int(os.getenv("ENGINE_MAX_RETRIES", "2"))
RESPAWN_ON_RETRY   = os.getenv("ENGINE_RESPAWN_ON_RETRY", "false").lower() == "true"

# Tournament
TOURNAMENT_MODE   = os.getenv("TOURNAMENT_MODE", "false").lower() == "true"
TOURNAMENT_SOURCES = {"tournament", "swiss", "arena"}

# Resign policy
RESIGN_ENABLED      = os.getenv("RESIGN_ENABLED", "false").lower() == "true"
RESIGN_AFTER_MOVE   = int(os.getenv("RESIGN_AFTER_MOVE", "35"))
RESIGN_THRESHOLD    = int(os.getenv("RESIGN_THRESHOLD", "600"))  # cp, side-to-move POV

# Opening book (Polyglot)
BOOK_PLIES    = int(os.getenv("BOOK_PLIES", "82323232323232323"))
BOOK_TOP_N    = int(os.getenv("BOOK_TOP_N", "1"))  # sample from top-N by weight
BOOK_WEIGHTED = os.getenv("BOOK_WEIGHTED", "true").lower() == "true"

POLYGLOT_BOOK_PATH_WHITE = os.getenv("POLYGLOT_BOOK_PATH_WHITE", r"C:\Users\manoj\chess_bot\opening\bin\BOOK_W.bin").strip()
POLYGLOT_BOOK_PATH_BLACK = os.getenv("POLYGLOT_BOOK_PATH_BLACK", r"C:\Users\manoj\chess_bot\opening\bin\BOOK_B.bin").strip()
POLYGLOT_BOOK_PATH_DRAW  = os.getenv("POLYGLOT_BOOK_PATH",       r"C:\Users\manoj\chess_bot\opening\bin\BOOK.bin").strip()  # your topical/drawish book
BOOK_MODE_DEFAULT        = os.getenv("POLYGLOT_MODE", "decisive").strip().lower()  # decisive|drawish|mixed
MODE_TOGGLE_FILE         = os.getenv("POLYGLOT_MODE_FILE", r"C:\Users\manoj\chess_bot\mode.txt").strip()  # optional: file with 'decisive'|'drawish'|'mixed'

# Syzygy (endgame TB)
SYZ_PATH      = os.getenv("SYZYGY_PATH", r"C:\Users\manoj\3-4-5;C:\Users\manoj\6-7").strip()  # e.g. "D:\\tb;E:\\tb:/mnt/tb"
SYZ_PROBE_DEPTH = int(os.getenv("SYZYGY_PROBE_DEPTH", "4"))
SYZ_PROBE_LIMIT = int(os.getenv("SYZYGY_PROBE_LIMIT", "6"))
SYZ_50MRULE     = os.getenv("SYZYGY_50_MOVE_RULE", "true").lower() == "true"

# Challenge policy
ALLOW_HUMANS            = os.getenv("ALLOW_HUMANS", "true").lower() == "true"
ACCEPT_NONRATED         = os.getenv("ACCEPT_NONRATED", "true").lower() == "true"
ACCEPT_MIN_BASE_SECONDS = int(os.getenv("ACCEPT_MIN_BASE_SECONDS", "300"))  # default ≥5+0
ACCEPT_VARIANT_STANDARD = os.getenv("ACCEPT_VARIANT_STANDARD", "true").lower() == "true"

# Proactive challenging (defaults off; safest for rating)
PROACTIVE_CHALLENGES     = os.getenv("PROACTIVE_CHALLENGES", "true").lower() == "true"
OUTGOING_ONLY_WHEN_IDLE  = os.getenv("OUTGOING_ONLY_WHEN_IDLE", "true").lower() == "true"
MAX_OUTGOING_CHALLENGES  = int(os.getenv("MAX_OUTGOING_CHALLENGES", "1"))
RECHALLENGE_COOLDOWN     = int(os.getenv("RECHALLENGE_COOLDOWN", "900"))  # seconds
PENDING_TTL              = int(os.getenv("PENDING_TTL", "30"))            # seconds
CHALLENGE_RATED          = os.getenv("CHALLENGE_RATED", "true").lower() == "true"
CHALLENGE_CLOCK_SEC      = int(os.getenv("CHALLENGE_CLOCK_SEC", "600"))   # default 10+0
CHALLENGE_INC            = int(os.getenv("CHALLENGE_INC", "0"))
USE_LARGE_PAGES = os.getenv("USE_LARGE_PAGES", "true").lower() in ("1","true","yes","y")
# Rate limiting / backoff for outgoing POSTs
RATE_MIN_INTERVAL = float(os.getenv("RATE_MIN_INTERVAL", "60.0"))   # min seconds between challenge POSTs
RATE_BACKOFF_429  = float(os.getenv("RATE_BACKOFF_429", "300.0"))   # cooldown after a 429

# =====================
# INIT
# =====================

print("DEBUG ENV:", {
    "ENGINE_THREADS": ENGINE_THREADS,
    "ENGINE_HASH_MB": ENGINE_HASH_MB,
    "MOVE_OVERHEAD_MS": MOVE_OVERHEAD_MS,
    "MAX_ACTIVE_GAMES": MAX_ACTIVE_GAMES,
})
if SSL_CERT_FILE:
    os.environ["SSL_CERT_FILE"] = SSL_CERT_FILE

def log(msg: str, emoji: str = "", gid: Optional[str] = None):
    now = datetime.now().strftime("[%H:%M:%S]")
    tag = f" [{gid}]" if gid else ""
    try:
        print(f"{now}{tag} {emoji} {msg}")
    except UnicodeEncodeError:
        print(f"{now}{tag} {msg}")
    sys.stdout.flush()

def log_exc(where: str, e: Exception, gid: Optional[str] = None):
    tb = traceback.format_exc(limit=8)
    log(f"[!] {where}: {e}\n{tb}", "⚠️", gid=gid)

def fatal_blocking(msg: str):
    log(msg, "🛑")
    log("Fix the issue and press Ctrl+C to exit this hold loop.", "⏸️")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sys.exit(1)

def detect_stockfish() -> str:
    candidates = [
        STOCKFISH_PATH,
        r"C:\Program Files\stockfish\stockfish-windows-x86-64-avx2.exe",
        r"C:\Tools\stockfish\stockfish-windows-x86-64-avx2.exe",
        r".\stockfish-windows-x86-64-avx2.exe",
        r".\stockfish.exe",
        "/usr/bin/stockfish",
        "/usr/local/bin/stockfish",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return ""

if not API_TOKEN:
    fatal_blocking("Missing LICHESS_API_TOKEN env var.")

STOCKFISH_PATH = detect_stockfish()
if not STOCKFISH_PATH:
    fatal_blocking("Missing/invalid STOCKFISH_PATH. Put Stockfish somewhere simple and set the env var, or place it next to this script.")

# Lichess client (works with older/newer berserk)
try:
    session = berserk.TokenSession(API_TOKEN)
    try:
        client = berserk.Client(session=session, timeout=10)
    except TypeError:
        client = berserk.Client(session=session)
    MY_BOT_ID = (client.account.get() or {}).get("id", "").lower()
    if not MY_BOT_ID:
        log("Warning: could not read account id; is this a BOT token?", "❓")
except Exception as e:
    fatal_blocking(f"Failed to init Lichess client: {e}")
chatter = Chatter(client, min_interval=2.0, logger=log)
# =====================
# GLOBALS
# =====================
# ------------- JSONL per-game logging -------------
_game_logs = {}  # gid -> file handle
_logs_guard = threading.Lock()

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def game_log_open(gid: str):
    if not LOG_TO_FILE:
        return
    with _logs_guard:
        if gid in _game_logs:
            return
        _ensure_dir(LOG_DIR)
        fp = os.path.join(LOG_DIR, f"{gid}.jsonl")
        try:
            f = open(fp, "a", encoding="utf-8")
            _game_logs[gid] = f
        except Exception as e:
            log_exc("game_log_open", e, gid=gid)

def game_log_write(gid: str, record: dict):
    if not LOG_TO_FILE:
        return
    try:
        record = dict(record or {})
        record.setdefault("ts", int(time.time()))
        with _logs_guard:
            f = _game_logs.get(gid)
            if not f:
                return
            f.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            f.flush()
    except Exception as e:
        log_exc("game_log_write", e, gid=gid)

def game_log_close(gid: str):
    if not LOG_TO_FILE:
        return
    with _logs_guard:
        f = _game_logs.pop(gid, None)
        try:
            if f:
                f.flush()
                f.close()
        except Exception:
            pass

game_limit = threading.BoundedSemaphore(MAX_ACTIVE_GAMES)
lock = threading.Lock()
_http_lock = threading.Lock()

active_games = set()     # game ids with a running handler
pending_challenges = {}  # username(lower) -> first_sent_ts
last_challenged = {}     # username(lower) -> last_attempt_ts

wins = losses = draws = 0

TERMINAL_STATUSES = {
    "aborted","mate","resign","stalemate","timeout","outoftime",
    "draw","nostart","cheat","variantend","abandoned"
}

def _event_status(ev) -> str:
    st = ev.get("state") or {}
    return (ev.get("status") or st.get("status") or "").lower()

def _is_terminal(ev) -> bool:
    return _event_status(ev) in TERMINAL_STATUSES

# =====================
# OPENING BOOK
# =====================
# --- Config-driven multi-book support (deterministic) ---
try:
    import yaml
except Exception:
    yaml = None

BOOK_LIST_WHITE: list = []   # [(reader,label), ...]
BOOK_LIST_BLACK: list = []
BOOK_LIST_DRAW : list = []
BOOK_POLICY = {"white": "best_move", "black": "best_move", "draw": "best_move"}  # best_move|first_match

def _open_reader_labeled(path: str, label: str):
    try:
        if path and os.path.exists(path):
            rdr = chess.polyglot.open_reader(path)
            return (rdr, label)
    except Exception as e:
        log_exc(f"Polyglot open ({label})", e)
    return None

def _load_opening_books_from_config(cfg_path: str = "config.yml"):
    """
    Reads config.yml -> opening_books + books and fills BOOK_LIST_WHITE/BLACK/DRAW and BOOK_POLICY.
    Falls back to env-based single books if config or yaml missing.
    """
    global BOOK_LIST_WHITE, BOOK_LIST_BLACK, BOOK_LIST_DRAW, BOOK_POLICY
    if not yaml:
        log("PyYAML not installed; skipping config.yml opening books.", "ℹ️")
        return False
    if not os.path.exists(cfg_path):
        return False

    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    except Exception as e:
        log_exc("load config.yml", e)
        return False

    books_map = (cfg.get("books") or {})  # name -> path
    ob = (cfg.get("opening_books") or {})
    if not (ob.get("enabled", False) and books_map):
        return False

    def _build(group_key: str, default_policy="best_move"):
        g = (ob.get("books") or {}).get(group_key, {}) or {}
        policy = (g.get("selection") or default_policy).strip().lower()
        names = g.get("names") or []
        out = []
        for name in names:
            p = books_map.get(name)
            if p:
                r = _open_reader_labeled(os.path.expandvars(p), name)
                if r:
                    out.append(r)
        return out, policy

    wl, wp = _build("standard_white")
    bl, bp = _build("standard_black")
    dl, dp = _build("drawish")

    if wl: BOOK_LIST_WHITE = wl; BOOK_POLICY["white"] = wp
    if bl: BOOK_LIST_BLACK = bl; BOOK_POLICY["black"] = bp
    if dl: BOOK_LIST_DRAW  = dl; BOOK_POLICY["draw"]  = dp

    # Log summary
    def names(ll): return ", ".join(lbl for _, lbl in ll) if ll else "none"
    log(f"Opening books (cfg): WHITE=[{names(BOOK_LIST_WHITE)}] ({BOOK_POLICY['white']})", "📚")
    log(f"Opening books (cfg): BLACK=[{names(BOOK_LIST_BLACK)}] ({BOOK_POLICY['black']})", "📚")
    if BOOK_LIST_DRAW:
        log(f"Opening books (cfg): DRAW=[{names(BOOK_LIST_DRAW)}] ({BOOK_POLICY['draw']})", "📚")
    return True

_next_http_ok_at = 0.0
_http_lock = threading.Lock()

def http_throttle():
    global _next_http_ok_at
    with _http_lock:
        now = time.time()
        if now < _next_http_ok_at:
            time.sleep(_next_http_ok_at - now)
        _next_http_ok_at = time.time() + RATE_MIN_INTERVAL

def http_backoff(seconds: float):
    global _next_http_ok_at
    with _http_lock:
        _next_http_ok_at = max(_next_http_ok_at, time.time() + seconds)

# --- Server-side guard: ask Lichess if we're currently playing
_last_now_playing_check = 0.0
_now_playing_cache = 0
def has_active_play_on_server(ttl=5.0) -> bool:
    """Returns True if Lichess says we have any ongoing games. Cached briefly to avoid spamming the API."""
    global _last_now_playing_check, _now_playing_cache
    now = time.time()
    if now - _last_now_playing_check < ttl:
        return _now_playing_cache > 0
    try:
        now_playing = client.account.get_now_playing() or []
        _now_playing_cache = len(now_playing)
    except Exception as e:
        log_exc("has_active_play_on_server", e)
        _now_playing_cache = len(active_games)
    _last_now_playing_check = now
    return _now_playing_cache > 0

# =====================
# UTILS / HELPERS
# =====================

def _to_ms(value):
    """Convert Lichess clock field to milliseconds int, or None if invalid."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return int(value)
        return int(float(str(value)))
    except Exception:
        return None
import json

CACHE_FILE = FEN_CACHE_PATH


import re

def normalize_fen(fen: str) -> str:
    """
    Normalize FEN so positions map to the same key.
    We only keep: board layout, side to move, castling rights, and valid en passant square.
    """
    parts = fen.strip().split()
    if len(parts) < 4:
        raise ValueError(f"Invalid FEN: {fen}")

    board, turn, castling, ep = parts[:4]

    # sanitize en-passant
    if ep != "-" and not re.fullmatch(r"[a-h][36]", ep):
        ep = "-"

    return " ".join([board, turn, castling, ep])


def probe_engine_with_cache(engine, board, limit, gid=None):
    # If cache is disabled, just analyse once and return, no file IO.
    if not USE_FEN_CACHE:
        info = engine.analyse(board, limit, info=chess.engine.INFO_ALL)
        pv = info.get("pv") or []
        bestmove = pv[0] if pv else None
        entry = {
            "fen": normalize_fen(board.fen()),
            "bestmove": bestmove.uci() if bestmove else None,
            "pv": " ".join(m.uci() for m in pv),
            "depth_reached": info.get("depth"),
            "score": (info.get("score") or chess.engine.PovScore(None, chess.WHITE)).white().score(mate_score=10000) if info.get("score") else None,
            "seldepth": info.get("seldepth"),
            "nodes": info.get("nodes"),
            "nps": info.get("nps"),
            "hashfull": info.get("hashfull"),
            "source": "bot-local",
            "ts": int(time.time()),
        }
        return bestmove, entry

    # --- cache enabled path below ---
    refresh_fen_cache_if_changed()

    fen_full = board.fen()
    key = normalize_fen(fen_full)

    # ---- CACHE HIT ----
    if key in fen_cache:
        entry = fen_cache[key]
        print(f"[CACHE HIT] {key} → bestmove={entry.get('bestmove')} depth={entry.get('depth_reached')}")
        bm = entry.get("bestmove")
        return (chess.Move.from_uci(bm) if bm else None), entry

    # ---- MISS: enqueue job for Colab (only if cache is enabled) ----
    if OFFLOAD_ON_MISS:
        try:
            enqueue_fen_job(fen_full, min_depth=OFFLOAD_MIN_DEPTH, priority=OFFLOAD_PRIORITY, tag=(gid or ""))
        except Exception as e:
            log_exc("enqueue_on_miss", e, gid=gid)

    print(f"[CACHE MISS] {key} → analysing with Stockfish…")
    info = engine.analyse(board, limit, info=chess.engine.INFO_ALL)
    pv = info.get("pv") or []
    bestmove = pv[0] if pv else None

    entry = {
        "fen": key,
        "fen_full": fen_full,
        "bestmove": bestmove.uci() if bestmove else None,
        "pv": " ".join(m.uci() for m in pv),
        "depth_reached": info.get("depth"),
        "score": (info.get("score") or chess.engine.PovScore(None, chess.WHITE)).white().score(mate_score=10000) if info.get("score") else None,
        "seldepth": info.get("seldepth"),
        "nodes": info.get("nodes"),
        "nps": info.get("nps"),
        "hashfull": info.get("hashfull"),
        "source": "bot-local",
        "ts": int(time.time()),
    }
    fen_cache[key] = entry
    save_fen_cache()
    return bestmove, entry

import hashlib

_cache_last_mtime = 0.0
_cache_last_check = 0.0
def _compact_pv(pv_uci: str, max_plies: int) -> str:
    if not pv_uci:
        return ""
    ucis = pv_uci.split()
    if len(ucis) <= max_plies:
        return " ".join(ucis)
    return " ".join(ucis[:max_plies]) + " …"

def _now_ms():
    return int(time.time() * 1000)

def refresh_fen_cache_if_changed():
    if not USE_FEN_CACHE:
        return
    """Hot-reload shared cache file if Colab wrote to it."""
    global fen_cache, _cache_last_mtime, _cache_last_check
    now = _now_ms()
    # throttle mtime checks
    if now - _cache_last_check < CACHE_HOT_RELOAD_MS:
        return
    _cache_last_check = now

    try:
        mtime = os.path.getmtime(FEN_CACHE_PATH)
    except Exception:
        return
    if mtime <= _cache_last_mtime:
        return
    _cache_last_mtime = mtime
    try:
        with open(FEN_CACHE_PATH, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            data = json.loads(txt) if txt else {}
        fen_cache = _migrate_cache_keys(data)
        # (optional) print a small note so you see reloads
        print(f"[CACHE RELOAD] entries={len(fen_cache)} from {FEN_CACHE_PATH}")
    except Exception as e:
        # don’t blow up on a transient partial write
        log_exc("refresh_fen_cache_if_changed", e)

def _normalize_fen_4(fen_full: str) -> str:
    return normalize_fen(fen_full)

def _fen_filename_key(fen4: str) -> str:
    return hashlib.sha1(fen4.encode("utf-8")).hexdigest()[:16]

def enqueue_fen_job(fen_full: str, min_depth: int = None, priority: int = None, tag: str = "") -> str:
    """Drop a small JSON job in Drive so Colab worker picks it up."""
    min_depth = int(min_depth if min_depth is not None else OFFLOAD_MIN_DEPTH)
    priority  = int(priority  if priority  is not None else OFFLOAD_PRIORITY)

    fen4 = _normalize_fen_4(fen_full)
    key  = _fen_filename_key(fen4)
    job = {
        "fen": fen4,
        "fen_full": fen_full,
        "min_depth": min_depth,
        "priority": priority,
        "ts": int(time.time()),
        "tag": tag,
        "version": 1,
    }
    tmp = os.path.join(FEN_QUEUE_DIR, f"{key}.json.tmp")
    fin = os.path.join(FEN_QUEUE_DIR, f"{key}.json")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False, indent=2)
        os.replace(tmp, fin)
        print(f"[ENQUEUE] {fen4} → {fin}")
    except Exception as e:
        log_exc("enqueue_fen_job", e)
    return fin

def enqueue_pv_futures(board: chess.Board, pv_uci: str, steps: int, tag: str = ""):
    """Given a PV string, push the first N plies and enqueue each resulting position."""
    if not pv_uci or steps <= 0:
        return
    b = board.copy()
    for i, u in enumerate(pv_uci.split()[:steps]):
        try:
            mv = chess.Move.from_uci(u)
            if mv not in b.legal_moves:
                break
            b.push(mv)
            enqueue_fen_job(b.fen(), tag=f"{tag}#pv{i+1}")
        except Exception:
            break

def _to_sec(x):
    """
    Convert a Lichess clock field to integer seconds.
    Accepts seconds or milliseconds (auto-detects if value looks like ms).
    """
    if x is None:
        return None
    try:
        v = float(x)
        if v > 1000.0:  # likely milliseconds
            v = v / 1000.0
        return int(max(0, round(v)))
    except Exception:
        return None

def _phase_cap_seconds_local(ply: int) -> float:
    try:
        cap_open = float(os.getenv("TM_PHASE_CAP_OPEN_S", "30.0"))
        cap_mid  = float(os.getenv("TM_PHASE_CAP_MID_S", "45.0"))
        cap_end  = float(os.getenv("TM_PHASE_CAP_END_S", "60.0"))
    except Exception:
        cap_open, cap_mid, cap_end = 30.0, 45.0, 60.0
    if ply < 30: return cap_open
    if ply < 70: return cap_mid
    return cap_end


def play_or_rescue(engine, board, limit, sec_left, gid=None):
    """
    Run engine.play with one quick rescue attempt:
    - Always respawn before retry (keeps CommandState sane).
    - If low on time, skip retry and return None (caller does micro-fallback).
    - On retry, halve movetime to avoid a second timeout.

    Returns either:
    * chess.engine.PlayResult
    * ("_ENGINE_SWAP_", <new_engine>, chess.engine.PlayResult)
    * None (caller should do an instant micro-fallback)
    """
    overhead = max(0.02, (MOVE_OVERHEAD_MS or 0) / 1000.0)
    guard_extra = 2.5 * overhead + 0.25
    can_wait = (sec_left is not None) and (sec_left > (RETRY_DELAY_S + guard_extra))

    # First attempt
    try:
        return engine.play(board, limit)
    except Exception as e:
        # If we can't afford to wait, bail out
        if not can_wait or ENGINE_MAX_RETRIES <= 0:
            log(f"engine.play failed: {e}; instant fallback.", "🧯", gid=gid)
            return None

        # Respawn and retry once, with a shorter limit
        try:
            engine.quit()
        except Exception:
            pass
        try:
            new_engine = spawn_engine()
        except Exception as e2:
            log(f"Engine respawn failed: {e2}; instant fallback.", "🧯", gid=gid)
            return None

        # Cut time for retry to 50% (or keep nodes/depth as-is)
        new_limit = limit
        try:
            if getattr(limit, "time", None) is not None:
                new_limit = chess.engine.Limit(time=max(0.02, float(limit.time) * 0.5))
        except Exception:
            pass

        log(f"engine.play timeout; retrying in {RETRY_DELAY_S:.1f}s (1/{ENGINE_MAX_RETRIES})", "🔁", gid=gid)
        time.sleep(RETRY_DELAY_S)
        try:
            res = new_engine.play(board, new_limit)
            return ("_ENGINE_SWAP_", new_engine, res)
        except Exception as e3:
            log(f"engine.play failed after retry: {e3}; instant fallback.", "🧯", gid=gid)
            return None

def auto_threads() -> int:
    if ENGINE_THREADS != "auto":
        try:
            t = int(ENGINE_THREADS)
            return max(1, t)
        except Exception:
            pass
    cores = os.cpu_count() or 2
    return max(1, cores // max(1, MAX_ACTIVE_GAMES))

def extract_times(ev: dict, my_color: chess.Color) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """Extract remaining time and increment for our side. Returns (my_ms, inc_ms, opp_ms, opp_inc)."""
    def to_ms(val):
        if val is None:
            return None
        try:
            if isinstance(val, (int, float)): return int(val)
            if isinstance(val, str):          return int(float(val))
            if isinstance(val, datetime):     return int(val.timestamp() * 1000)
        except Exception:
            pass
        return None

    wtime = to_ms(ev.get("wtime")); btime = to_ms(ev.get("btime"))
    winc  = to_ms(ev.get("winc"));  binc  = to_ms(ev.get("binc"))
    if my_color == chess.WHITE:
        return (wtime, winc, btime, binc)
    else:
        return (btime, binc, wtime, winc)

def _current_mode() -> str:
    m = BOOK_MODE_DEFAULT
    if MODE_TOGGLE_FILE:
        try:
            s = (open(MODE_TOGGLE_FILE, "r", encoding="utf-8").read().strip().lower())
            if s in ("decisive", "drawish", "mixed"):
                m = s
        except Exception:
            pass
    return m

_last_mode = None  # global tracker
def get_mode_with_log() -> str:
    global _last_mode
    mode = _current_mode()
    if mode != _last_mode:
        log(f"Polyglot mode switched: {_last_mode or 'n/a'} → {mode}", "🔀")
        _last_mode = mode
    return mode

# Try loading from config.yml; if that succeeds, we’ll prefer BOOK_LIST_* over single readers
_loaded_cfg_books = _load_opening_books_from_config("config.yml")

def _readers_for_turn(my_color_is_white: bool):
    """
    Returns a list of (reader,label) to consult in order for this move,
    honoring polyglot mode (decisive/drawish/mixed) and config.yml lists.
    """
    mode = get_mode_with_log()

    # Prefer config-driven lists if present
    def cfg_lists():
        if my_color_is_white and BOOK_LIST_WHITE:
            primary = BOOK_LIST_WHITE
        elif (not my_color_is_white) and BOOK_LIST_BLACK:
            primary = BOOK_LIST_BLACK
        else:
            primary = []

        fallback = BOOK_LIST_DRAW or []
        return primary, fallback

    def env_singletons():
        primary = []
        if my_color_is_white and BOOK_READER_WHITE:
            primary = [(BOOK_READER_WHITE, BOOK_LABEL_WHITE)]
        elif (not my_color_is_white) and BOOK_READER_BLACK:
            primary = [(BOOK_READER_BLACK, BOOK_LABEL_BLACK)]
        fallback = [(BOOK_READER_DRAW, BOOK_LABEL_DRAW)] if BOOK_READER_DRAW else []
        return primary, fallback

    primary, fallback = cfg_lists() if _loaded_cfg_books else env_singletons()

    if mode == "drawish":
        return BOOK_LIST_DRAW if _loaded_cfg_books else ([(BOOK_READER_DRAW, BOOK_LABEL_DRAW)] if BOOK_READER_DRAW else [])
    if mode == "decisive":
        return primary
    # mixed
    return primary + fallback


def _find_book_move(board: chess.Board, my_color_is_white: bool):
    """
    Deterministic multi-book chooser.
    - 'best_move': union moves from all readers; choose highest weight
      (ties: earlier book in list, then UCI).
    - 'first_match': pick top-weight move from the first reader that has any entry.
    """
    readers = _readers_for_turn(my_color_is_white)
    if not readers:
        return None, None

    policy = BOOK_POLICY["white" if my_color_is_white else "black"]
    if get_mode_with_log() == "drawish":
        policy = BOOK_POLICY.get("draw", policy)

    def list_entries():
        for idx, (rdr, label) in enumerate(readers):
            if not rdr:
                continue
            try:
                entries = list(rdr.find_all(board))
            except Exception:
                entries = []
            if entries:
                yield idx, label, entries

    if policy == "first_match":
        for idx, label, entries in list_entries():
            entries.sort(key=lambda e: int(getattr(e, "weight", 0)), reverse=True)
            e = entries[0]
            return e.move, label
        return None, None

    # best_move policy: union all candidate moves
    candidates = {}
    for idx, label, entries in list_entries():
        for e in entries:
            w = int(getattr(e, "weight", 0))
            u = e.move.uci()
            cur = candidates.get(u)
            # keep the heaviest; tie-break by earlier book
            if (cur is None) or (w > cur[0]) or (w == cur[0] and idx < cur[1]):
                candidates[u] = (w, idx, label)

    if not candidates:
        return None, None

    # pick by: weight desc → book order asc → UCI asc
    best_uci, (_w, _idx, best_label) = sorted(
        candidates.items(),
        key=lambda kv: (-kv[1][0], kv[1][1], kv[0])
    )[0]

    return chess.Move.from_uci(best_uci), best_label



import urllib3  # noqa: F401 (compat)
from requests.exceptions import ConnectionError, ReadTimeout, ChunkedEncodingError
from berserk.exceptions import ApiError, ResponseError

def _is_transient_net_err(e: Exception) -> bool:
    s = str(e).lower()
    return any([
        isinstance(e, (ConnectionError, ReadTimeout, ChunkedEncodingError)),
        "remote end closed connection" in s,
        "connection aborted" in s,
        "protocolerror" in s,
        "temporarily unavailable" in s,
        "gateway timeout" in s,
        "bad gateway" in s,
        "connection reset" in s,
        "api timeout" in s,
        "cancellederror" in s,
        "timeouterror" in s,
    ])

def _retry_call(desc: str, fn, *args, gid=None, **kwargs):
    """Retry wrapper for berserk/requests calls with serialization and exponential backoff on 429."""
    attempt = 0
    backoff_429 = 60  # start at 60s, double each time up to 30min
    while True:
        try:
            with _http_lock:  # ensure no parallel requests
                return fn(*args, **kwargs)
        except (ApiError, ResponseError) as e:
            if "429" in str(e):
                retry_after = backoff_429
                try:
                    if hasattr(e, "response") and e.response is not None:
                        retry_after = int(e.response.headers.get("Retry-After", retry_after))
                except Exception:
                    pass
                log(f"{desc}: 429 Too Many Requests. Sleeping {retry_after}s before retry…", "⚠️", gid=gid)
                time.sleep(retry_after)
                backoff_429 = min(backoff_429 * 2, 1800)  # exponential, cap 30min
                continue
            if _is_transient_net_err(e):
                attempt += 1
                if attempt > MAX_NET_RETRIES:
                    raise RuntimeError(f"{desc}: exceeded retries ({MAX_NET_RETRIES})")
                log(f"{desc}: transient net error; retry {attempt}/{MAX_NET_RETRIES} after {RECONNECT_DELAY_SEC:.0f}s", "🔁", gid=gid)
                time.sleep(RECONNECT_DELAY_SEC)
                continue
            raise
        except Exception as e:
            if _is_transient_net_err(e):
                attempt += 1
                if attempt > MAX_NET_RETRIES:
                    raise RuntimeError(f"{desc}: exceeded retries ({MAX_NET_RETRIES})")
                log(f"{desc}: transient net error; retry {attempt}/{MAX_NET_RETRIES} after {RECONNECT_DELAY_SEC:.0f}s", "🔁", gid=gid)
                time.sleep(RECONNECT_DELAY_SEC)
                continue
            raise

def pick_play_limit(my_ms_left: Optional[int], inc_ms: Optional[int], movenum: int) -> chess.engine.Limit:
    if my_ms_left is None or my_ms_left <= 0:
        return chess.engine.Limit(time=max(0.4, MIN_MOVE_TIME))
    sec = my_ms_left / 1000.0
    inc = (inc_ms or 0) / 1000.0
    if sec < PANIC_TIME_SECS:
        return chess.engine.Limit(depth=PANIC_DEPTH)
    frac = EARLY_FRACTION if movenum < EARLY_PHASE_MOVE else LATE_FRACTION
    budget = sec + INC_WEIGHT * inc
    movetime = max(MIN_MOVE_TIME, min(MAX_MOVE_TIME, budget * frac))
    return chess.engine.Limit(time=movetime)

def should_resign(board: chess.Board, engine: chess.engine.SimpleEngine) -> bool:
    if not RESIGN_ENABLED or board.fullmove_number < RESIGN_AFTER_MOVE:
        return False
    try:
        info = engine.analyse(board, chess.engine.Limit(time=0.05))
        score = info["score"].white().score(mate_score=10000)
        score = score if board.turn == chess.WHITE else -score
        return score < -RESIGN_THRESHOLD
    except Exception:
        return False

def acceptable_challenge(ch: dict) -> bool:
    try:
        if ACCEPT_VARIANT_STANDARD and ((ch.get("variant") or {}).get("key") != "standard"):
            return False
        time_ctrl = ch.get("timeControl") or {}
        limit = int((time_ctrl.get("limit") or 0))  # seconds
        rated = ch.get("rated", False)
        if not ACCEPT_NONRATED and not rated:
            return False
        if limit < ACCEPT_MIN_BASE_SECONDS:
            log(f"Decline: base {limit}s < ACCEPT_MIN_BASE_SECONDS={ACCEPT_MIN_BASE_SECONDS}", "⏱️")
            return False
        challenger = ch.get("challenger", {}) or {}
        is_bot = challenger.get("title") == "BOT"
        if not ALLOW_HUMANS and not is_bot:
            return False
        dest = ch.get("destUser", {}) or {}
        challenger_id = (challenger.get("id") or "").lower()
        dest_id = (dest.get("id") or "").lower()
        if challenger_id == MY_BOT_ID or dest_id != MY_BOT_ID:
            return False
        status = (ch.get("status") or "").lower()
        if status and status not in {"created", "pending"}:
            return False
        return True
    except Exception as e:
        log_exc("acceptable_challenge", e)
        return False

def get_online_bots(max_names=200):
    fallback_bots = [
        "LeelaMultiPoss", "Magic11bot_Pro", "Grzechu86", "sus2098",
        "MastacticaTeoriabot", "Yuki_1324", "Alexandrya", "NecroMindX",
        "InvinxibleFlxsh", "pangubot", "TheFreshman1902", "YoBot_v2",
        "newest_stockfish", "TheSophomore1902", "Lili-ai",
        "Moment-That-Inspires", "MaggiChess16", "OpeningerEX",
        "ChampionKitten", "ToromBot", "cinder-bot", "NimsiluBot",
        "caissa-x", "strain-on-veins", "ArasanX", "NNUE_Drift",
        "Cheszter", "RaspFish", "European_Chess_Bot", "HIWIN1234",
        "DarkOnBot", "CheckmateX1", "PlayingLikeGiftian",
        "eNErGyOFbEiNGbOT", "BOT_Stockfish13", "PetersBot",
        "pawnocchio_bot", "LaQuaisha", "edixonbot", "SupraNova_V11",
        "icbmsaregoated2", "GCNbyFayE", "preunpatching-bot",
        "MAGIC11BOT", "YnodeGrayWolf", "KarimBOT", "Gold_Engine",
        "Classic_BOT-v2", "Cheng-4-NN", "PZChessBot", "pawn_git",
        "Weiawaga", "Legend_Engine", "TheUnforgivingBot", "odonata-bot",
        "duchessAI", "PatriciaBot", "simbelmyne-bot",
    ]

    try:
        import requests
        from bs4 import BeautifulSoup
        r = requests.get(
            "https://lichess.org/player/bots",
            headers={"User-Agent": "strong-bot/1.0 (+https://lichess.org)"},
            timeout=5,
        )
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        bots = []
        seen = set()
        for a in soup.select("a.user-link"):
            name = (a.text or "").strip().replace("\xa0", " ")
            if name.upper().startswith("BOT "):
                name = name[4:].strip()
            if not name or name.lower() == MY_BOT_ID:
                continue
            if name.lower() not in seen:
                seen.add(name.lower())
                bots.append(name)
        return bots[:max_names] if bots else fallback_bots[:max_names]
    except Exception:
        # Just quietly fallback, no traceback, no spam
        return fallback_bots[:max_names]


def sweep_stale_pendings():
    now = time.time()
    stale = []
    with lock:
        for u, ts in list(pending_challenges.items()):
            if now - ts >= PENDING_TTL:
                stale.append(u)
        for u in stale:
            pending_challenges.pop(u, None)
    if stale:
        log(f"Cleared stale pendings: {', '.join(stale)}", "🧹")

def with_lock(fn):
    def wrap(*a, **k):
        with lock:
            return fn(*a, **k)
    return wrap

@with_lock
def claim_game(gid: str) -> bool:
    """Mark gid as active if not already; return True if we claimed it."""
    if gid in active_games:
        return False
    active_games.add(gid)
    return True

@with_lock
def remove_active(gid: str):
    active_games.discard(gid)

@with_lock
def add_pending(u: str):
    pending_challenges[(u or "").lower()] = time.time()

@with_lock
def remove_pending(u: str):
    pending_challenges.pop((u or "").lower(), None)

@with_lock
def mark_challenged(u: str):
    last_challenged[(u or "").lower()] = time.time()

@with_lock
def inc_result(kind: str):
    global wins, losses, draws
    if kind == "win":
        wins += 1
    elif kind == "loss":
        losses += 1
    elif kind == "draw":
        draws += 1
    return wins, losses, draws

# =====================
# ERROR SWALLOWERS
# =====================

def _is_404(e: Exception) -> bool:
    try:
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        if code == 404:
            return True
    except Exception:
        pass
    msg = str(e).lower()
    return "404" in msg or "not found" in msg

def swallow_or_log(where: str, e: Exception, context: str = "", gid: Optional[str] = None) -> bool:
    """Returns True if the error is an 'already gone' 404 race we can ignore."""
    if _is_404(e):
        log(f"{where}: 404 not found {context} — ignoring.", "ℹ️", gid=gid)
        return True
    log_exc(where, e, gid=gid)
    return False

# =====================
# GAME LOOP
# =====================

def spawn_engine() -> chess.engine.SimpleEngine:
    """
    Start Stockfish with sane options, try to enable Windows Large Pages,
    and print a clear one-line summary of what was applied.
    """
    eng = chess.engine.SimpleEngine.popen_uci(os.getenv("STOCKFISH_PATH"))
    eng.ping()  # handshake

    # --------- Helpers ---------
    def _opt_exists(name: str) -> bool:
        try:
            return name in eng.options
        except Exception:
            return False

    def _set_opt(name: str, val):
        """Set a UCI option if the engine advertises it; swallow if unsupported."""
        try:
            if _opt_exists(name):
                eng.configure({name: val})
                return True
        except Exception:
            pass
        return False

    # --------- Threads ---------
    threads_env = os.getenv("ENGINE_THREADS", "auto").strip().lower()
    if threads_env == "auto":
        req_threads = max(1, (os.cpu_count() or 1) // max(1, int(os.getenv("MAX_ACTIVE_GAMES", "1"))))
    else:
        try:
            req_threads = max(1, int(threads_env))
        except Exception:
            req_threads = max(1, os.cpu_count() or 1)

    # --------- Hash ---------
    try:
        req_hash = int(os.getenv("ENGINE_HASH_MB", "256"))
    except Exception:
        req_hash = 256
    try:
        if "Hash" in eng.options and hasattr(eng.options["Hash"], "max"):
            req_hash = min(req_hash, int(eng.options["Hash"].max))
    except Exception:
        pass

    # --------- Move Overhead ---------
    try:
        req_overhead = int(os.getenv("MOVE_OVERHEAD_MS", "200"))
    except Exception:
        req_overhead = 200

    # --------- Syzygy ---------
    syz_path   = os.getenv("SYZYGY_PATH", "") or ""
    syz_depth  = int(os.getenv("SYZYGY_PROBE_DEPTH", "4"))
    syz_limit  = int(os.getenv("SYZYGY_PROBE_LIMIT", "6"))
    syz_50rule = os.getenv("SYZYGY_50_MOVE_RULE", "true").lower() in ("1","true","y","yes")

    # --------- Large Pages ---------
    use_lp_env = os.getenv("USE_LARGE_PAGES", "true").lower() in ("1", "true", "yes", "y")
    lp_option_used = None
    lp_option_names = (
        "Use Large Pages",            # modern SF
        "UseLargePages",              # rare builds
        "Use Windows Large Pages",    # some forks
    )

    # --------- Apply options ---------
    _set_opt("Threads", req_threads)
    _set_opt("Hash", req_hash)
    _set_opt("Move Overhead", req_overhead)

    if syz_path:
        _set_opt("SyzygyPath", syz_path)
        _set_opt("SyzygyProbeDepth", syz_depth)
        _set_opt("SyzygyProbeLimit", syz_limit)
        _set_opt("Syzygy50MoveRule", syz_50rule)

    if use_lp_env:
        for name in lp_option_names:
            if _set_opt(name, True):
                lp_option_used = name
                break

    # NNUE should already be on by default; no need to toggle.
    eng.ping()

    # --------- Summary ---------
    print(
        "[UCI] Stockfish="
        f"{os.path.basename(os.getenv('STOCKFISH_PATH','?'))} | "
        f"Threads={req_threads} | Hash={req_hash} MB | Overhead={req_overhead} ms | "
        f"SyzygyPath={'set' if syz_path else 'none'} | "
        f"LargePages={'ON via ' + lp_option_used if lp_option_used else ('requested' if use_lp_env else 'off')}"
    )

    # Optional: tiny prime to encourage SF to print its init info lines (including LP) to stdout
    try:
        _ = eng.analyse(chess.Board(), chess.engine.Limit(nodes=1))
    except Exception:
        pass

    return eng

def finalize_game(game_id, my_color):
    try:
        result = client.games.export(game_id) or {}
        status = (result.get("status") or "").lower()
        winner = result.get("winner")
        if status in {"mate", "resign", "timeout", "outoftime", "abandoned", "nostart"}:
            if winner is None:
                w,l,d = inc_result("draw")
                log(f"Game ended ({status}). {w}W/{l}L/{d}D", "➖", gid=game_id)
            else:
                mine = (winner == "white" and my_color == chess.WHITE) or (winner == "black" and my_color == chess.BLACK)
                kind = "win" if mine else "loss"
                w,l,d = inc_result(kind)
                log(("Victory!" if mine else "Defeat.") + f" {w}W/{l}L/{d}D", "🏆" if mine else "💀", gid=game_id)
        elif status == "draw" or winner is None:
            w,l,d = inc_result("draw")
            log(f"Draw. {w}W/{l}L/{d}D", "🤝", gid=game_id)
        else:
            log(f"Game ended: {status}", "⚡", gid=game_id)
    except Exception as e:
        swallow_or_log("finalize_game/export", e, f"(game={game_id})", gid=game_id)
    finally:
        try:
            remove_active(game_id)
        except Exception:
            pass
        try:
            game_log_write(game_id, {"type": "game_end", "gid": game_id})
            game_log_close(game_id)
        except Exception:
            pass
        log(f"Finished game {game_id}", "🏁", gid=game_id)

def accept_challenge_safe(cid: str):
    try:
        _retry_call("accept_challenge", client.bots.accept_challenge, cid)
        log(f"Accepted challenge {cid}.", "💪")
    except berserk.exceptions.ResponseError as e:
        if swallow_or_log("accept_challenge", e, f"(cid={cid})"):
            return

def decline_challenge_safe(cid: str):
    try:
        _retry_call("decline_challenge", client.bots.decline_challenge, cid)
        log(f"Declined challenge {cid}.", "⛔")
    except berserk.exceptions.ResponseError as e:
        if swallow_or_log("decline_challenge", e, f"(cid={cid})"):
            return

def safe_make_move(
    game_id: str,
    board: chess.Board,
    engine: chess.engine.SimpleEngine,
    my_ms: Optional[int],
    inc_ms: Optional[int],
    my_color: chess.Color,
    base_minutes_override: Optional[int] = None,
) -> Tuple[bool, chess.engine.SimpleEngine]:
    """
    Plays a move with robust time management (panic paths, book, Syzygy, search).
    Returns (success, engine) — engine may be replaced if we respawned it.
    """
    try:
        cur_ply = (board.fullmove_number - 1) * 2 + (1 if board.turn == chess.WHITE else 2)
        overhead = max(0.02, (MOVE_OVERHEAD_MS or 0) / 1000.0)
        sec_left = max(0.0, (my_ms or 0) / 1000.0) if my_ms is not None else None
        inc_s    = max(0.0, (inc_ms or 0) / 1000.0) if inc_ms is not None else 0.0

        # base minutes (for time mgmt function)
        base_minutes = None
        try:
            if base_minutes_override is not None:
                base_minutes = max(1, int(base_minutes_override))
            else:
                base_minutes = max(1, int(round(CHALLENGE_CLOCK_SEC / 60)))
        except Exception:
            base_minutes = None

        # panic thresholds
        ultra_panic = (sec_left is not None and sec_left <= 2.0)
        hard_panic  = (sec_left is not None and sec_left <= 12.0)

        # phase caps
        def _phase_cap_seconds_local(ply: int) -> float:
            try:
                cap_open = float(os.getenv("TM_PHASE_CAP_OPEN_S", "30"))
                cap_mid  = float(os.getenv("TM_PHASE_CAP_MID_S", "45"))
                cap_end  = float(os.getenv("TM_PHASE_CAP_END_S", "60"))
            except Exception:
                cap_open, cap_mid, cap_end = 30, 45, 60
            if ply < 30: return cap_open
            if ply < 70: return cap_mid
            return cap_end

        # fallback micro-move helper
        def _micro_fallback_move(tag: str) -> bool:
            try:
                res = engine.play(board, chess.engine.Limit(nodes=2000))
                mv = res.move
            except Exception:
                mv = next(iter(board.legal_moves), None)
            if not mv:
                try:
                    _retry_call("resign_game(no-move)", client.bots.resign_game, game_id, gid=game_id)
                    log("No legal move; resigned.", "🏳️", gid=game_id)
                except Exception as e2:
                    swallow_or_log("resign_game(no-move)", e2, f"(game={game_id})", gid=game_id)
                return False
            try:
                _retry_call(f"make_move({tag})", client.bots.make_move, game_id, mv.uci(), gid=game_id)
            except berserk.exceptions.ResponseError as e:
                if "not your turn" in str(e).lower() or "game already over" in str(e).lower():
                    log(f"Not our turn / game over ({tag})", "ℹ️", gid=game_id)
                    return False
                swallow_or_log(f"make_move({tag})", e, f"(game={game_id})", gid=game_id)
                return False
            board.push(mv)
            log(f"{tag.upper()} move {board.fullmove_number}: {mv.uci()}", "⚡", gid=game_id)
            return True

        # 0) ultra panic
        if ultra_panic:
            return (_micro_fallback_move("ultra"), engine)

        # 1) hard panic
        if hard_panic:
            try:
                res = engine.play(board, chess.engine.Limit(time=0.06))
                mv = res.move
            except Exception as e:
                log(f"engine.play hard-panic error: {e}", "🧯", gid=game_id)
                return (_micro_fallback_move("panic"), engine)
            if mv:
                try:
                    _retry_call("make_move(panic)", client.bots.make_move, game_id, mv.uci(), gid=game_id)
                except berserk.exceptions.ResponseError as e:
                    if "not your turn" in str(e).lower() or "game already over" in str(e).lower():
                        log("Not our turn / game over (panic)", "ℹ️", gid=game_id)
                        return (False, engine)
                    swallow_or_log("make_move(panic)", e, f"(game={game_id})", gid=game_id)
                    return (False, engine)
                board.push(mv)
                log(f"PANIC move {board.fullmove_number}: {mv.uci()}", "⏱️", gid=game_id)
                return (True, engine)
            return (_micro_fallback_move("panic-empty"), engine)

        # 2) book move
        if cur_ply <= BOOK_PLIES:
            mv, src_label = _find_book_move(board, my_color_is_white=(my_color == chess.WHITE))
            if mv:
                try:
                    _retry_call("make_move(book)", client.bots.make_move, game_id, mv.uci(), gid=game_id)
                except berserk.exceptions.ResponseError as e:
                    if "not your turn" in str(e).lower() or "game already over" in str(e).lower():
                        log("Not our turn / game over (book)", "ℹ️", gid=game_id)
                        return (False, engine)
                    swallow_or_log("make_move(book)", e, f"(game={game_id})", gid=game_id)
                    return (False, engine)
                board.push(mv)
                log(f"Book move {board.fullmove_number}: {mv.uci()} from {src_label or 'book'}", "📖", gid=game_id)
                return (True, engine)

        # 3) syzygy probe
        try:
            tb_info = engine.analyse(board, chess.engine.Limit(depth=1))
            if tb_info.get("tbhits", 0) and tb_info.get("pv"):
                mv = tb_info["pv"][0]
                try:
                    _retry_call("make_move(TB)", client.bots.make_move, game_id, mv.uci(), gid=game_id)
                except berserk.exceptions.ResponseError as e:
                    if "not your turn" in str(e).lower() or "game already over" in str(e).lower():
                        log("Not our turn / game over (TB)", "ℹ️", gid=game_id)
                        return (False, engine)
                    swallow_or_log("make_move(TB)", e, f"(game={game_id})", gid=game_id)
                    return (False, engine)
                board.push(mv)
                log(f"TB-HIT! Move {board.fullmove_number}: {mv.uci()} score={tb_info.get('score')}", "📚", gid=game_id)
                return (True, engine)
        except Exception as e:
            log_exc("TB probe", e, gid=game_id)

        # 4) normal search
        try:
            if my_ms is not None:
                movetime_s = compute_movetime(
                    remaining_time_s=sec_left,
                    increment_s=inc_s,
                    ply=cur_ply,
                    in_check=board.is_check(),
                    forced=(board.legal_moves.count() <= 2),
                    base_minutes=base_minutes,
                )
                movetime_s = min(movetime_s, _phase_cap_seconds_local(cur_ply))
                guard = 2.5 * overhead
                think = max(0.02, min(movetime_s, sec_left - guard if sec_left else movetime_s))
                limit = chess.engine.Limit(time=think)
            else:
                limit = chess.engine.Limit(time=max(0.4, MIN_MOVE_TIME))
        except Exception as e:
            log_exc("compute_movetime", e, gid=game_id)
            limit = chess.engine.Limit(time=max(0.4, MIN_MOVE_TIME))

        t0 = time.perf_counter()
        mv, entry = None, {}
        try:
            mv, entry = probe_engine_with_cache(engine, board, limit, gid=game_id)
        except Exception as e:
            log(f"analyse() failed: {e}, fallback to play()", "🧯", gid=game_id)
            try:
                res = engine.play(board, limit)
                mv = res.move
            except Exception as e2:
                log(f"play() also failed: {e2}", "🧯", gid=game_id)
                return (False, engine)

        if not mv:
            try:
                _retry_call("resign_game(no-move)", client.bots.resign_game, game_id, gid=game_id)
                log("No legal move; resigned.", "🏳️", gid=game_id)
            except Exception as e:
                swallow_or_log("resign_game(no-move)", e, f"(game={game_id})", gid=game_id)
            return (False, engine)

        try:
            _retry_call("make_move(normal)", client.bots.make_move, game_id, mv.uci(), gid=game_id)
        except berserk.exceptions.ResponseError as e:
            if "not your turn" in str(e).lower() or "game already over" in str(e).lower():
                log("Not our turn / game over (normal)", "ℹ️", gid=game_id)
                return (False, engine)
            swallow_or_log("make_move(normal)", e, f"(game={game_id})", gid=game_id)
            return (False, engine)
        board.push(mv)
        t1 = time.perf_counter()
        elapsed = max(0.0, t1 - t0)

        # Compose details
        depth   = entry.get("depth_reached")
        seldep  = entry.get("seldepth")
        nodes   = entry.get("nodes")
        nps     = entry.get("nps")
        hashf   = entry.get("hashfull")
        score   = entry.get("score")
        pv_uci  = entry.get("pv") or ""
        pv_show = _compact_pv(pv_uci, MAX_PV_UCIS)

        # Pretty single-line
        if LOG_PRETTY:
            ms_disp = (my_ms or 0)
            log(
                f"Move {board.fullmove_number}: {mv.uci()}  "
                f"[d={depth}/{seldep or '-'} | nps={nps or '-'} | nodes={nodes or '-'} | hash={hashf or '-'} | "
                f"score={score if score is not None else '-'} | {elapsed:.2f}s | clock={int(ms_disp/1000) if my_ms is not None else 'n/a'}s]  "
                f"pv: {pv_show}",
                "♟️", gid=game_id
            )

        # Structured JSONL
        game_log_write(game_id, {
            "type": "move_played",
            "gid": game_id,
            "fullmove": board.fullmove_number,
            "uci": mv.uci(),
            "depth": depth,
            "seldepth": seldep,
            "nodes": nodes,
            "nps": nps,
            "hashfull": hashf,
            "score_cp_white_pov": score,
            "pv_uci": pv_uci,
            "elapsed_s": round(elapsed, 3),
            "our_clock_ms_before": my_ms,
            "our_inc_ms": inc_ms
        })
        return (True, engine)

    except Exception as e:
        log_exc("safe_make_move", e, gid=game_id)
        return (False, engine)

def stream_game_state_with_reconnect(game_id):
    """Generator that yields game state events, reconnecting on transient errors with fixed delay."""
    while True:
        try:
            for event in client.bots.stream_game_state(game_id):
                yield event
            time.sleep(RECONNECT_DELAY_SEC)  # clean end (rare)
        except Exception as e:
            if _is_transient_net_err(e):
                log("Game stream dropped; reconnecting…", "🔌", gid=game_id)
            else:
                log_exc("stream_game_state", e, gid=game_id)
            time.sleep(RECONNECT_DELAY_SEC)

def _event_winner(ev) -> str:
    st = ev.get("state") or {}
    return (ev.get("winner") or st.get("winner") or "")

def handle_game(game_id: str, my_color_init: Optional[chess.Color] = None):
    """Per-game handler: reconnecting stream, robust color detection, safe move loop, clean finalize."""
    with game_limit:  # throttle concurrent thinking to MAX_ACTIVE_GAMES
        board = chess.Board()
        my_color = my_color_init
        warned_time_once = False
        engine = None
        announced = False

        # --- Per-game time control we detected/inferred (seconds) ---
        game_base_sec: Optional[int] = None  # initial base in seconds (e.g., 480 for 8+1)
        game_inc_sec: Optional[int] = None   # increment in seconds     (e.g., 1   for 8+1)

        try:
            engine = spawn_engine()

            def ensure_my_color(ev: Optional[dict] = None):
                """Set my_color if unknown. Prefer gameFull payload; fallback to a one-time export."""
                nonlocal my_color
                if my_color is not None:
                    return
                # Try from a fresh gameFull event
                if ev and (ev.get("type") or "") == "gameFull":
                    w = (ev.get("white") or {}).get("id", "")
                    my_color = chess.WHITE if (w or "").lower() == MY_BOT_ID else chess.BLACK
                    return
                # Fallback: query export once
                try:
                    data = client.games.export(game_id) or {}
                    players = data.get("players") or {}
                    winfo = (players.get("white") or {}).get("user") or {}
                    wid = (winfo.get("id") or "").lower()
                    my_color = chess.WHITE if wid == MY_BOT_ID else chess.BLACK
                except Exception as e:
                    swallow_or_log("ensure_my_color/export", e, f"(game={game_id})", gid=game_id)

            for event in stream_game_state_with_reconnect(game_id):
                try:
                    et = (event.get("type") or "").strip()

                    # ----------------
                    # Full snapshot
                    # ----------------
                    if et == "gameFull":
                        ensure_my_color(event)

                        # Capture official time control if present
                        clk = event.get("clock") or {}
                        ini_s = _to_sec(clk.get("initial"))
                        inc_s = _to_sec(clk.get("increment"))
                        if ini_s is not None:
                            game_base_sec = ini_s
                        if inc_s is not None:
                            game_inc_sec = inc_s
                        if (game_base_sec is not None) and (game_inc_sec is not None):
                            try:
                                log(f"Detected TC: {game_base_sec//60}+{game_inc_sec}", "⏱️", gid=game_id)
                            except Exception:
                                pass

                        # Early terminal?
                        if _is_terminal(event):
                            log(
                                f"Game already terminal on join: {_event_status(event)} "
                                f"(winner={_event_winner(event) or 'n/a'})",
                                "🔚", gid=game_id
                            )
                            break

                        # Sync board
                        moves = (event.get("state") or {}).get("moves", "").split()
                        board.reset()
                        for mv in moves:
                            try:
                                board.push_uci(mv)
                            except Exception:
                                pass

          
                        # Announce once
                        if my_color is not None and not announced:
                            opp = (event.get("black") if my_color == chess.WHITE else event.get("white")) or {}
                            name = opp.get("name") or opp.get("id") or "?"
                            rating = opp.get("rating", "?")
                            url = f"https://lichess.org/{game_id}"
                            coltxt = 'White' if my_color == chess.WHITE else 'Black'
                            tc = f"{(game_base_sec or 0)//60}+{game_inc_sec or 0}" if (game_base_sec is not None and game_inc_sec is not None) else "n/a"
                            log(f"Game {game_id} vs {name} ({rating}) | {coltxt} | TC {tc} | {url}", "♟️", gid=game_id)

                            game_log_open(game_id)
                            game_log_write(game_id, {
                                "type": "game_start",
                                "gid": game_id,
                                "color": coltxt.lower(),
                                "opponent": name,
                                "opponent_rating": rating,
                                "time_control": tc,
                                "source": (event.get('source') or (event.get('tournamentId') and 'tournament') or 'play'),
                                "url": url
                            })
                            announced = True
                        try:
                            me_txt = "White" if my_color == chess.WHITE else "Black"
                            eng_name = os.path.basename(STOCKFISH_PATH) or "engine"
                            chatter.greet_players(game_id, me_txt, name, eng_name, gid=game_id)
                            chatter.greet_spectators(game_id, me_txt, name, eng_name, gid=game_id)
                        except Exception as e:
                            log_exc("chat greet", e, gid=game_id)

                        # Try to extract clocks from the snapshot state to move immediately if it's our turn
                        st = event.get("state") or {}
                        wtime = _to_ms(st.get("wtime")); btime = _to_ms(st.get("btime"))
                        winc  = _to_ms(st.get("winc"));  binc  = _to_ms(st.get("binc"))
                        my_ms_join  = wtime if my_color == chess.WHITE else btime
                        inc_ms_join = winc  if my_color == chess.WHITE else binc

                        time.sleep(0.2)
                        if my_color is not None and board.turn == my_color and not board.is_game_over():
                            ok, engine = safe_make_move(
                                game_id, board, engine,
                                my_ms_join, inc_ms_join, my_color,
                                base_minutes_override=(game_base_sec // 60 if game_base_sec else None),
                            )

                    # ----------------
                    # Incremental update
                    # ----------------
                    elif et == "gameState":
                        ensure_my_color()

                        if _is_terminal(event):
                            log(
                                f"Terminal update: {_event_status(event)} "
                                f"(winner={_event_winner(event) or 'n/a'})",
                                "🔚", gid=game_id
                            )
                            break

                        # Sync board from incremental moves
                        moves = (event.get("moves") or "").split()
                        board.reset()
                        for mv in moves:
                            try:
                                board.push_uci(mv)
                            except Exception:
                                pass

                        if board.is_game_over():
                            log("Game over by board state (mate/stalemate/etc.).", "🔚", gid=game_id)
                        #    break  # optional: you can `break` here — stream will finish soon anyway

                        # Clock extraction after board sync
                        my_ms, inc_ms, *_ = extract_times(event, my_color)
                        if my_color is not None and my_ms is None and not warned_time_once:
                            log("Clock fields non-numeric; will use fixed movetime.", "🧐", gid=game_id)
                            warned_time_once = True

                        # If the official TC wasn’t provided, infer base from first seen clocks (once)
                        if game_base_sec is None:
                            wms = _to_ms(event.get("wtime")); bms = _to_ms(event.get("btime"))
                            start_ms = max(wms or 0, bms or 0)
                            if start_ms > 0:
                                game_base_sec = int(round(start_ms / 1000.0))
                        if game_inc_sec is None:
                            # Prefer "our" side's inc; either side is fine on Lichess
                            game_inc_sec = _to_sec((event.get("winc") if my_color == chess.WHITE else event.get("binc")))

                        # Only think/play on our turn
                        if my_color is not None and board.turn == my_color:
                            if should_resign(board, engine):
                                try:
                                    _retry_call("resign_game", client.bots.resign_game, game_id, gid=game_id)
                                    log("Position hopeless; resigned.", "🏳️", gid=game_id)
                                    break
                                except Exception as e:
                                    swallow_or_log("resign_game", e, f"(game={game_id})", gid=game_id)

                            ok, engine = safe_make_move(
                                game_id, board, engine, my_ms, inc_ms, my_color,
                                base_minutes_override=(game_base_sec // 60 if game_base_sec else None),
                            )

                    # Ignore chatLine and other event types silently

                except Exception as loop_err:
                    log_exc("handle_game/loop", loop_err, gid=game_id)
                    continue

        except Exception as setup_err:
            log_exc("handle_game/setup", setup_err, gid=game_id)
        finally:
            try:
                if engine:
                    engine.quit()
            except Exception:
                pass
            finalize_game(game_id, my_color)


# =====================
# CHALLENGING
# =====================

def perf_for_limit(limit_sec: int, inc_sec: int = 0) -> str:
    """Map your challenge clock to a Lichess perf key."""
    # Roughly matches Lichess buckets: <3m bullet, <8m blitz, <=25m rapid, else classical
    if limit_sec < 180: return "bullet"
    if limit_sec < 480: return "blitz"
    if limit_sec <= 1500: return "rapid"
    return "classical"

def get_rating_cached(username: str, perf_key: str) -> Optional[int]:
    """Fetch a bot's rating for the given perf, with caching and sensible fallbacks."""
    u = (username or "").lower()
    now = time.time()
    key = (u, perf_key)

    # cache hit
    with lock:
        hit = rating_cache.get(key)
        if hit and (now - hit[0]) < RATING_CACHE_TTL:
            return hit[1]

    # fetch
    rating = None
    try:
        # berserk: prefer get_public_data; some versions use get_by_id
        try:
            data = client.users.get_public_data(username) or {}
        except AttributeError:
            data = client.users.get_by_id(username) or {}
        perfs = data.get("perfs") or {}
        # primary perf
        rating = (perfs.get(perf_key) or {}).get("rating")
        # fallbacks if that perf isn’t present
        if rating is None:
            for alt in ("rapid", "blitz", "classical", "bullet"):
                r = (perfs.get(alt) or {}).get("rating")
                if isinstance(r, int):
                    rating = r
                    break
    except Exception as e:
        log_exc("get_rating_cached", e)

    with lock:
        rating_cache[key] = (now, rating)
    return rating

def should_challenge(username: str) -> bool:
    try:
        u = (username or "").lower()
        now = time.time()
        with lock:
            if len(pending_challenges) >= MAX_OUTGOING_CHALLENGES:
                return False
            if u in pending_challenges:
                return False
            last = last_challenged.get(u)
            if last is not None and (now - last) < RECHALLENGE_COOLDOWN:
                return False
        # rating gate based on your configured challenge time control perf_key
        perf_key = perf_for_limit(CHALLENGE_CLOCK_SEC, CHALLENGE_INC)
        rating = get_rating_cached(username, perf_key)
        if rating is None or rating < MIN_CHALLENGE_RATING:
            log(f"Skip {username}: {perf_key} rating {rating or 'n/a'} < {MIN_CHALLENGE_RATING}.", "⬇️")
            return False
        return True
    except Exception as e:
        log_exc("should_challenge", e)
        return False

def challenge_bot(username: str) -> None:
    """Send a challenge to another bot/user with retry handling."""
    try:
        http_throttle()
        _retry_call(
            "challenge_create",
            client.challenges.create,
            username,
            rated=CHALLENGE_RATED,
            clock_limit=CHALLENGE_CLOCK_SEC,
            clock_increment=CHALLENGE_INC,
            color="random",
        )
        log(f"Challenge sent to {username}", "🎯")
    except berserk.exceptions.ResponseError as e:
        if _is_404(e):
            log("Challenge target vanished (404); skipping.", "ℹ️")
        else:
            log_exc("challenge_bot", e)
    except Exception as e:
        log_exc("challenge_bot", e)

def challenger_loop():
    if not PROACTIVE_CHALLENGES:
        return
    if TOURNAMENT_MODE:
        log("Tournament-only mode: proactive challenging disabled.", "⛔")
        return

    log("Challenger loop started…", "💬")
    bots_cache, last_cache = [], 0.0
    last_now_playing_check = 0.0
    cached_now_playing = 0

    def server_has_active(ttl=5.0) -> bool:
        nonlocal last_now_playing_check, cached_now_playing
        now = time.time()
        if now - last_now_playing_check < ttl:
            return cached_now_playing > 0
        try:
            np = list(client.games.get_ongoing())
            cached_now_playing = len(np)
        except Exception as e:
            log_exc("challenger_loop/get_now_playing", e)
            with lock:
                cached_now_playing = len(active_games)
        last_now_playing_check = now
        return cached_now_playing > 0

    while True:
        try:
            sweep_stale_pendings()
            # Snapshot current capacity
            with lock:
                active_n = len(active_games)
                pend_n   = len(pending_challenges)

            # Hard stops: full or pending outbound
            if active_n >= MAX_ACTIVE_GAMES or pend_n > 0:
                time.sleep(10); continue

            # Only challenge when idle if configured
            if OUTGOING_ONLY_WHEN_IDLE:
                if active_n > 0 or server_has_active():
                    time.sleep(10); continue

            # Refresh the online bot list periodically
            now = time.time()
            if not bots_cache or (now - last_cache) > 120:
                bots_cache = get_online_bots(max_names=200)
                random.shuffle(bots_cache)
                last_cache = now
            if not bots_cache:
                time.sleep(20); continue

            # Walk candidates and issue at most what capacity allows
            for bot_name in list(bots_cache):
                with lock:
                    active_n = len(active_games)
                    pend_n   = len(pending_challenges)
                    if active_n >= MAX_ACTIVE_GAMES or pend_n >= MAX_OUTGOING_CHALLENGES:
                        break
                    if OUTGOING_ONLY_WHEN_IDLE and (active_n > 0 or server_has_active()):
                        break
                    if not PROACTIVE_CHALLENGES:
                        break
                if not should_challenge(bot_name):
                    continue
                challenge_bot(bot_name)
                time.sleep(20)

        except Exception as e:
            log_exc("challenger_loop", e)
            time.sleep(10)

# =====================
# EVENT LISTENER
# =====================

def stream_incoming_events_forever():
    while True:
        try:
            for ev in client.bots.stream_incoming_events():
                yield ev
        except berserk.exceptions.ResponseError as e:
            log_exc("stream_incoming_events", e)
            time.sleep(RECONNECT_DELAY_SEC)
        except Exception as e:
            if _is_transient_net_err(e):
                log("Incoming stream dropped; reconnecting…", "🔌")
            else:
                log_exc("stream_incoming_events/outer", e)
            time.sleep(RECONNECT_DELAY_SEC)

def event_listener():
    """Top-level incoming-event loop: accepts/declines challenges and starts game handlers.
    Robust to duplicate gameStart events and keeps outgoing-challenge bookkeeping tidy.
    """
    log("Listening for events…", "🛰️")
    while True:
        try:
            for event in stream_incoming_events_forever():
                try:
                    et = (event.get("type") or "").strip()

                    # -----------------
                    # CHALLENGE events
                    # -----------------
                    if et == "challenge":
                        ch = event.get("challenge") or {}
                        cid = ch.get("id")
                        if not cid:
                            continue
                        challenger = (ch.get("challenger") or {}) or {}
                        dest       = (ch.get("destUser") or {}) or {}
                        challenger_id = (challenger.get("id") or "").lower()
                        dest_id       = (dest.get("id") or "").lower()
                        status        = (ch.get("status") or "created").lower()
                        is_inbound  = (dest_id == MY_BOT_ID)       # someone challenged us
                        is_outgoing = (challenger_id == MY_BOT_ID) # we challenged someone

                        # Tournament mode: decline all inbound challenges
                        if TOURNAMENT_MODE and is_inbound:
                            decline_challenge_safe(cid)
                            try:
                                name = (challenger.get("name") or challenger.get("id") or "?")
                                log(f"Declined (tournament-only mode) challenge from {name}", "🚫")
                            except Exception:
                                pass
                            continue

                        if is_outgoing:
                            # Clean pending when Lichess closes the loop on our outgoing challenge.
                            opp_id = (dest.get("id") or "").lower()
                            if status not in {"created", "pending"}:
                                if opp_id:
                                    remove_pending(opp_id)
                                log(f"Outgoing challenge {cid} to {opp_id} ended (status={status}); cleared pending.", "🧹")
                            continue

                        if not is_inbound:
                            continue

                        # Capacity & policy gate
                        with lock:
                            capacity_ok = len(active_games) < MAX_ACTIVE_GAMES
                        if capacity_ok and acceptable_challenge(ch):
                            accept_challenge_safe(cid)
                            try:
                                name = (challenger.get("name") or challenger.get("id") or "?")
                                log(f"Accepted challenge from {name}", "🤝")
                            except Exception:
                                pass
                        else:
                            decline_challenge_safe(cid)

                    # ---------------
                    # gameStart event
                    # ---------------
                    elif et == "gameStart":
                        g = event.get("game") or {}
                        gid = g.get("id")
                        if not gid:
                            continue

                        # Source gating in tournament mode
                        source = (g.get("source") or "").lower()
                        if TOURNAMENT_MODE:
                            if source not in TOURNAMENT_SOURCES:
                                log(f"Ignoring non-tournament gameStart ({gid}), source={source or 'n/a'}", "🚫")
                                continue
                            tid = (g.get("tournamentId") or g.get("swissId") or "").lower()
                            if ONLY_TOURNAMENT_ID and tid != ONLY_TOURNAMENT_ID:
                                log(f"Ignoring other tournament gameStart ({gid}), id={tid or 'n/a'}", "🚫")
                                continue

                        my_col_str = (g.get("color") or "").lower()
                        my_color_init = chess.WHITE if my_col_str == "white" else (chess.BLACK if my_col_str == "black" else None)

                        # Clear pending if we’d challenged this opponent
                        opp = (g.get("opponent") or {}).get("id")
                        if opp:
                            remove_pending(opp)

                        # Deduplicate handler threads for the same game id
                        if not claim_game(gid):
                            log(f"Ignoring duplicate gameStart for {gid}", "🔁")
                            continue

                        threading.Thread(target=handle_game, args=(gid, my_color_init), daemon=True).start()

                    # ---------------
                    # gameFinish event
                    # ---------------
                    elif et == "gameFinish":
                        opp = (event.get("opponent") or {}).get("id")
                        if opp:
                            remove_pending(opp)

                    # Ignore other types

                except Exception as inner:
                    log_exc("event_listener/inner", inner)
                    continue

        except Exception as outer:
            log_exc("event_listener/outer", outer)
            time.sleep(5)

# =====================
# MAIN
# =====================

def start():
    print_banner(commit_hash=os.getenv("GIT_COMMIT", "d0191cc"))
    chatter.load_messages("config.yml")
    if USE_FEN_CACHE:
        load_fen_cache()   # <---- add this line
    else:
                 log("FEN cache disabled (USE_FEN_CACHE=false).", "🚫")
    if TOURNAMENT_MODE:
        log("Tournament mode enabled: no sending challenges & accepting only tournament games.", "🏁")
    threading.Thread(target=event_listener, daemon=True).start()
    threading.Thread(target=challenger_loop, daemon=True).start()
    while True:
        time.sleep(60)

if __name__ == "__main__":
    try:
        start()
    except KeyboardInterrupt:
        log("Shutting down by user request.", "👋")
    except Exception as e:
        log_exc("MAIN FATAL", e)
    finally:
        if USE_FEN_CACHE:
            save_fen_cache()   # <---- always persist cache
