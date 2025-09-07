# compute_time_management.py
import random

def compute_movetime(
    remaining_time_s: float,
    increment_s: float,
    ply: int,
    in_check: bool = False,
    forced: bool = False,
    base_minutes: int = 15,
) -> float:
    """
    Human-like time manager for chess engines.

    Features:
    - Scales usage by phase (opening / middlegame / endgame).
    - Keeps a safety reserve but not too aggressive.
    - Uses increment intelligently.
    - Adds jitter for human-like variance.
    """

    # ---- Normalize inputs ----
    T = max(0.0, remaining_time_s)   # remaining clock time
    I = max(0.0, increment_s)        # increment per move
    P = max(0, ply)                  # ply count

    # ---- Safety reserve (always preserved) ----
    # (lighter than before: 3s + 1×increment)
    safe_bank = 3.0 + 1.0 * I
    if T <= safe_bank:
        # Sudden death: play almost instantly
        return max(0.2, T * 0.25)

    # ---- Estimate remaining moves ----
    # scale by base time control
    if base_minutes >= 15:
        RM = 35   # longer games → expect more moves
    elif base_minutes >= 5:
        RM = 25
    else:
        RM = 15   # blitz/bullet → assume fewer moves

    # ---- Effective pool ----
    # We count half the increment into usable pool
    pool = (T - safe_bank) + 0.5 * I * RM
    budget = pool / RM

    # ---- Phase scaling ----
    if P < 20:
        budget *= 1.5   # opening → think more
    elif P < 60:
        budget *= 1.2   # middlegame
    else:
        budget *= 1.0   # endgame → a bit faster

    # ---- Tactical nudges ----
    if in_check:
        budget *= 1.3
    if forced:
        budget *= 0.7

    # ---- Practical clamps ----
    budget = min(budget, T - safe_bank)   # never spend the bank
    budget = min(budget, 60.0)            # absolute cap per move
    budget = max(budget, 2.0)             # never less than 2s in normal play

    # ---- Add jitter (±20%) ----
    jitter = random.uniform(0.85, 1.15)
    budget *= jitter

    return float(budget)


# For quick testing
if __name__ == "__main__":
    scenarios = [
        ("15+10 opening", 900, 10, 5),
        ("15+10 middlegame", 600, 10, 40),
        ("3+2 blitz", 180, 2, 15),
        ("1+0 bullet", 60, 0, 10),
        ("low time scramble", 15, 2, 50, True, True),
    ]

    for name, T, I, ply, *flags in scenarios:
        in_check = flags[0] if len(flags) > 0 else False
        forced = flags[1] if len(flags) > 1 else False
        mvtime = compute_movetime(T, I, ply, in_check, forced)
        print(f"{name:20s} → {mvtime:.2f}s")
