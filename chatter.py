# chatter.py — tiny, safe Lichess chat helper for trash_bot.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any
import time, os

try:
    import yaml  # optional
except Exception:
    yaml = None


@dataclass
class Messages:
    greeting: str = "Good luck & have fun!"
    goodbye: str = "GG! Thanks for the game."
    greeting_spectators: str = "Welcome, spectators! Enjoy the game 👋"
    goodbye_spectators: str = "Thanks for watching!"

    @staticmethod
    def _clean(val: Optional[str]) -> Optional[str]:
        if val is None:
            return None
        if not isinstance(val, str):
            return None
        v = val.strip()
        return v if v else None

    @classmethod
    def from_yaml(cls, path: str) -> "Messages":
        """
        Load from YAML if present. For each field:
        - use YAML value if it's a non-empty string
        - else fall back to the default in this class
        """
        base = cls()  # defaults
        if not yaml or not os.path.exists(path):
            return base
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            msg = cfg.get("messages") or {}

            greeting            = cls._clean(msg.get("greeting"))               or base.greeting
            goodbye             = cls._clean(msg.get("goodbye"))                or base.goodbye
            greeting_spectators = cls._clean(msg.get("greeting_spectators"))    or base.greeting_spectators
            goodbye_spectators  = cls._clean(msg.get("goodbye_spectators"))     or base.goodbye_spectators

            return cls(
                greeting=greeting,
                goodbye=goodbye,
                greeting_spectators=greeting_spectators,
                goodbye_spectators=goodbye_spectators,
            )
        except Exception:
            return base


def _render(tmpl: str, context: Dict[str, Any]) -> str:
    if not tmpl:
        return ""
    try:
        result = tmpl.format(**context)
        print("DEBUG RENDER:", result)   # <--- add this
        return result
    except Exception as e:
        print("DEBUG RENDER ERROR:", e, "TEMPLATE:", tmpl, "CTX:", context)
        return tmpl



class Chatter:
    """
    - Pulls messages from config.yml (fallback to defaults only if absent/blank)
    - Lightweight rate limiting (min_interval seconds)
    - Safe: swallows errors so it never crashes the bot
    - Templates can use {me}, {opponent}, {engine}
    """
    def __init__(self, client, min_interval: float = 2.0, max_len: int = 500, logger=None):
        self.client = client
        self.min_interval = float(min_interval)
        self.max_len = int(max_len)
        self._next_ok = 0.0
        self.log = logger or (lambda *_a, **_k: None)
        self.messages = Messages()

    def load_messages(self, path: str = "config.yml"):
        self.messages = Messages.from_yaml(path)

    def _wait_rate(self):
        now = time.time()
        if now < self._next_ok:
            time.sleep(self._next_ok - now)
        self._next_ok = time.time() + self.min_interval

    def _post(self, game_id: str, room: str, text: str, gid: Optional[str] = None):
        text = (text or "").strip()
        if not text:
            return
        if len(text) > self.max_len:
            text = text[: self.max_len - 1] + "…"
        try:
            self._wait_rate()
            # Correct usage: use the berserk client method, and pass room properly
            self.client.bots.post_message(game_id, text, room)
        except Exception as e:
            self.log(f"chat post failed ({room}): {e}", "💬", gid=gid)


    # Convenience methods ------------------------------

    def greet_players(self, game_id: str, me: str, opponent: str, engine: str, gid: Optional[str] = None):
        self._post(game_id, "player",  _render(self.messages.greeting, {"me": me, "opponent": opponent, "engine": engine}), gid)

    def greet_spectators(self, game_id: str, me: str, opponent: str, engine: str, gid: Optional[str] = None):
        self._post(game_id, "spectator", _render(self.messages.greeting_spectators, {"me": me, "opponent": opponent, "engine": engine}), gid)

    def goodbye_players(self, game_id: str, me: str, opponent: str, engine: str, gid: Optional[str] = None):
        self._post(game_id, "player", _render(self.messages.goodbye, {"me": me, "opponent": opponent, "engine": engine}), gid)

    def goodbye_spectators(self, game_id: str, me: str, opponent: str, engine: str, gid: Optional[str] = None):
        self._post(game_id, "spectator", _render(self.messages.goodbye_spectators, {"me": me, "opponent": opponent, "engine": engine}), gid)
