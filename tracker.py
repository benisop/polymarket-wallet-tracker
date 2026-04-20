"""
Polymarket Wallet Tracker - MICRO COPY EDITION

Objetivo: encontrar wallets copiables con capital de $50-100 USD
- PnL REAL desde lb-api.polymarket.com/profit (validado <$10 vs leaderboard oficial)
- Cursor pagination con 'end' (sin límite de 3000 records)
- Filtra scalpers de timeframe corto (BTC 5min etc)
- Edad mínima de wallet: 14 días
- Solo acepta si pnl_week > 0 (verificado desde lb-api)

Output (mismos nombres que antes, con columna 'tier' nueva):
- data/wallets_full_{fecha}.csv   (todas procesadas, con tier asignado)
- data/top_wallets_{fecha}.csv    (solo no-rejected, ordenadas MICRO > SIGNAL > WATCH)
- data/trades_{fecha}.csv         (trades individuales)
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone
from collections import Counter

print("=== POLYMARKET WALLET TRACKER (MICRO COPY) ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

# ---------------- CONFIG ----------------
CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "FINANCE", "CULTURE", "ECONOMICS", "TECH"]

# Ventanas
RECENT_DAYS   = 10
SHORT_DAYS    = 7
HISTORY_DAYS  = 20

# Filtros copiabilidad
MAX_AVG_TRADE = 300     # USD - techo para que sea copiable con $50-100
MIN_PNL_RECENT = 2000   # USD - piso de PnL en últimos 7 días (desde lb-api)
MIN_TRADES_RECENT = 5
MIN_DAYS_ACTIVE = 3
MIN_MARKETS = 2
MAX_TOP_MARKET_PCT = 60

# Nuevos filtros
MIN_WALLET_AGE_DAYS = 14
MAX_SCALPER_PCT = 30
SCALPER_THRESHOLD_SECONDS = 2 * 3600  # 2 horas

LEADERBOARD_LIMIT = 50

cutoff_ts_recent = int((datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)).timestamp())
cutoff_ts_short  = int((datetime.now(timezone.utc) - timedelta(days=SHORT_DAYS)).timestamp())
cutoff_ts_hist   = int((datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)).timestamp())

# ---------------- HELPERS ----------------
def get_real_pnl(address, window="week"):
    """
    Fuente de verdad para PnL. Endpoint undocumented pero estable:
    https://lb-api.polymarket.com/profit?window={day,week,month,all}&address=X
    Validado <$10 error vs leaderboard oficial.
    """
    try:
        r = requests.get(
            "https://lb-api.polymarket.com/profit",
            params={"window": window, "address": address},
            timeout=10,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if isinstance(data, dict):
            for key in ["amount", "profit", "pnl", "value"]:
                if key in data:
                    return float(data[key])
            return None
        elif isinstance(data, (int, float)):
            return float(data)
        return None
    except Exception:
        return None


def fetch_all_activity_cursor(addr, start_ts, max_records=2500, page_size=500):
    """Cursor pagination con 'end' - sin límite de 3000 del offset."""
    trades = []
    end_param = None
    while len(trades) < max_records:
        params = {
            "user": addr,
            "limit": page_size,
            "start": start_ts,
            "type": "TRADE",
        }
        if end_param is not None:
            params["end"] = end_param
        try:
            r = requests.get(
                "https://data-api.polymarket.com/activity",
                params=params,
                timeout=15,
            )
            if r.status_code != 200:
                break
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            trades.extend(batch)
            if len(batch) < page_size:
                break
            last_ts = min(t.get("timestamp", 0) for t in batch)
            if last_ts <= start_ts:
                break
            end_param = last_ts - 1
        except Exception:
            break
        time.sleep(0.12)
    return trades


def get_market_end_date(slug_or_condition, cache):
    """Fecha de resolución del market desde Gamma API. Cacheada."""
    if not slug_or_condition:
        return None
    if slug_or_condition in cache:
        return cache[slug_or_condition]
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"slug": slug_or_condition, "limit": 1},
            timeout=8,
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                end_str = data[0].get("endDate") or data[0].get("end_date_iso")
                if end_str:
                    ts = int(datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp())
                    cache[slug_or_condition] = ts
                    return ts
        cache[slug_or_condition] = None
        return None
    except Exception:
        cache[slug_or_condition] = None
        return None


# ---------------- [1/5] LEADERBOARD ----------------
print(f"\n[1/5] Leaderboard WEEK + MONTH, {LEADERBOARD_LIMIT}/cat...")
all_wallets = {}

for period in ["WEEK", "MONTH"]:
    for cat in CATEGORIES:
        try:
            r = requests.get(
                "https://data-api.polymarket.com/v1/leaderboard",
                params={
                    "category": cat,
                    "timePeriod": period,
                    "orderBy": "PNL",
                    "limit": LEADERBOARD_LIMIT,
                },
                timeout=15,
            )
            data = r.json() if r.status_code == 200 else []
            if not isinstance(data, list):
                continue
            for t in data:
                a = t.get("proxyWallet", "").lower()
                if not a:
                    continue
                if a not in all_wallets:
                    all_wallets[a] = {
                        "proxyWallet": a,
                        "userName": t.get("userName", a[:10]),
                        "lb_pnl_week_cat": 0.0,
                        "lb_pnl_month_cat": 0.0,
                        "vol_lb": 0.0,
                        "categories": [],
                    }
                w = all_wallets[a]
                pnl = float(t.get("pnl", 0))
                vol = float(t.get("vol", 0))
                if period == "WEEK":
                    w["lb_pnl_week_cat"] = max(w["lb_pnl_week_cat"], pnl)
                else:
                    w["lb_pnl_month_cat"] = max(w["lb_pnl_month_cat"], pnl)
                w["vol_lb"] = max(w["vol_lb"], vol)
                if cat not in w["categories"]:
                    w["categories"].append(cat)
            print(f"  {period}/{cat}: {len(data)} wallets")
        except Exception as e:
            print(f"  {period}/{cat}: {e}")
        time.sleep(0.25)

print(f"Total wallets únicas: {len(all_wallets)}")

prefilter = {
    a: d for a, d in all_wallets.items()
    if d["lb_pnl_week_cat"] >= 500 or d["lb_pnl_month_cat"] >= 1500
}
print(f"Prefilter: {len(prefilter)}")


# ---------------- [2/5] VALIDACIÓN PnL REAL ----------------
print(f"\n[2/5] Validando PnL REAL (lb-api)...")
validated = {}
skipped_neg_pnl = 0
skipped_lb_fail = 0

for i, (addr, info) in enumerate(prefilter.items()):
    pnl_week_real = get_real_pnl(addr, window="week")

    if pnl_week_real is None:
        info["pnl_week_real"] = info["lb_pnl_week_cat"]
        info["pnl_source"] = "leaderboard_fallback"
        skipped_lb_fail += 1
    else:
        info["pnl_week_real"] = pnl_week_real
        info["pnl_source"] = "lb_api"

    if info["pnl_week_real"] < MIN_PNL_RECENT:
        skipped_neg_pnl += 1
        continue

    validated[addr] = info

    if (i + 1) % 20 == 0:
        print(f"  [{i+1}/{len(prefilter)}] procesadas, {len(validated)} validadas")

    time.sleep(0.15)

print(f"  Descartadas por PnL < {MIN_PNL_RECENT}: {skipped_neg_pnl}")
print(f"  Fallback a leaderboard: {skipped_lb_fail}")
print(f"  Validadas: {len(validated)}")


# ---------------- [3/5] HISTORIAL Y MÉTRICAS ----------------
print(f"\n[3/5] Historial de trades...")
wallet_stats = []
all_trades = []
market_end_cache = {}

for i, (addr, info) in enumerate(validated.items()):
    name = info.get("userName", addr[:10])
    try:
        trades = fetch_all_activity_cursor(addr, cutoff_ts_hist)
        if not trades:
            continue

        # Edad de la wallet
        try:
            r_first = requests.get(
                "https://data-api.polymarket.com/activity",
                params={
                    "user": addr,
                    "limit": 1,
                    "type": "TRADE",
                    "sortBy": "TIMESTAMP",
                    "sortDirection": "ASC",
                },
                timeout=8,
            )
            first_trade_ts = None
            if r_first.status_code == 200:
                fd = r_first.json()
                if isinstance(fd, list) and fd:
                    first_trade_ts = fd[0].get("timestamp", 0)
        except Exception:
            first_trade_ts = None

        if first_trade_ts is None:
            first_trade_ts = min(t.get("timestamp", 0) for t in trades if t.get("timestamp", 0) > 0) if trades else 0

        wallet_age_days = (datetime.now(timezone.utc).timestamp() - first_trade_ts) / 86400 if first_trade_ts else 0

        if wallet_age_days < MIN_WALLET_AGE_DAYS:
            print(f"  [{i+1}/{len(validated)}] {name[:20]:20s} SKIP - age {wallet_age_days:.1f}d")
            continue

        recent_hist = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts_hist and t.get("side") in ["BUY", "SELL"]]
        recent_10d  = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts_recent and t.get("side") in ["BUY", "SELL"]]
        recent_7d   = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts_short  and t.get("side") in ["BUY", "SELL"]]

        if not recent_10d:
            continue

        buys_10d = [t for t in recent_10d if t.get("side") == "BUY"]
        avg_buy = (sum(float(t.get("usdcSize", 0)) for t in buys_10d) / len(buys_10d)) if buys_10d else 0.0
        vol_10d = sum(float(t.get("usdcSize", 0)) for t in recent_10d)
        vol_7d  = sum(float(t.get("usdcSize", 0)) for t in recent_7d)

        days_10d = len(set(datetime.fromtimestamp(t.get("timestamp", 0), tz=timezone.utc).strftime("%Y-%m-%d") for t in recent_10d))
        markets_10d = list(set(t.get("title", "")[:80] for t in recent_10d))
        mc = Counter(t.get("title", "") for t in recent_10d)
        top_pct = (mc.most_common(1)[0][1] / len(recent_10d) * 100) if recent_10d else 0

        # Detección scalper
        scalper_trades = 0
        checked = 0
        sample = recent_10d[:15] if len(recent_10d) > 15 else recent_10d
        for t in sample:
            slug = t.get("slug") or t.get("eventSlug")
            if not slug:
                continue
            end_ts = get_market_end_date(slug, market_end_cache)
            if end_ts is None:
                continue
            checked += 1
            time_to_resolution = end_ts - t.get("timestamp", 0)
            if 0 < time_to_resolution < SCALPER_THRESHOLD_SECONDS:
                scalper_trades += 1
            time.sleep(0.05)

        scalper_pct = (scalper_trades / checked * 100) if checked > 0 else 0
        is_scalper = scalper_pct > MAX_SCALPER_PCT

        pnl_week_real = info["pnl_week_real"]
        roi_7d_real = (pnl_week_real / vol_7d * 100) if vol_7d > 0 else 0.0
        copiable_ratio = (100 / avg_buy) if avg_buy > 0 else 0

        stats = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "wallet": addr,
            "name": name,
            "wallet_age_days": round(wallet_age_days, 1),
            "pnl_week_real": round(pnl_week_real, 2),
            "pnl_source": info.get("pnl_source", "unknown"),
            "lb_pnl_week_cat": round(info["lb_pnl_week_cat"], 2),
            "lb_pnl_month_cat": round(info["lb_pnl_month_cat"], 2),
            "vol_7d": round(vol_7d, 2),
            "vol_10d": round(vol_10d, 2),
            "roi_7d_real_pct": round(roi_7d_real, 2),
            "trades_10d": len(recent_10d),
            "buys_10d": len(buys_10d),
            "days_active_10d": days_10d,
            "markets_count_10d": len(markets_10d),
            "avg_buy_size": round(avg_buy, 2),
            "copiable_ratio_100usd": round(copiable_ratio, 2),
            "top_market_pct": round(top_pct, 1),
            "is_bot_single_market": top_pct > MAX_TOP_MARKET_PCT,
            "scalper_pct": round(scalper_pct, 1),
            "is_scalper": is_scalper,
            "scalper_checked": checked,
            "categories": ", ".join(info.get("categories", [])),
            "markets_sample": " | ".join(markets_10d[:3]),
        }

        # Copy score
        size_bonus = max(0, (MAX_AVG_TRADE - avg_buy) / MAX_AVG_TRADE) * 30
        roi_bonus = min(max(roi_7d_real, 0), 100) * 0.4
        activity_bonus = min(days_10d, 10) * 2
        diversification_bonus = min(len(markets_10d), 10) * 1
        age_bonus = min(wallet_age_days / 30, 1) * 10
        stats["copy_score"] = round(size_bonus + roi_bonus + activity_bonus + diversification_bonus + age_bonus, 2)

        wallet_stats.append(stats)

        for t in recent_hist:
            trade = dict(t)
            trade["wallet_name"] = name
            trade["wallet_addr"] = addr
            trade["snapshot_date"] = stats["date"]
            all_trades.append(trade)

        flag = ""
        if is_scalper: flag += " [SCALPER]"
        if stats["is_bot_single_market"]: flag += " [BOT-1MKT]"
        print(f"  [{i+1}/{len(validated)}] {name[:20]:20s} age={wallet_age_days:>4.0f}d pnl_w=${pnl_week_real:>7.0f} avg=${avg_buy:>5.0f} roi={roi_7d_real:>6.1f}% scalp={scalper_pct:>4.0f}% score={stats['copy_score']:>5.1f}{flag}")
    except Exception as e:
        print(f"  [{i+1}/{len(validated)}] {name}: ERROR {e}")
    time.sleep(0.15)


# ---------------- [4/5] CLASIFICACIÓN EN TIERS ----------------
print("\n[4/5] Clasificando en tiers...")
os.makedirs("data", exist_ok=True)
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

df = pd.DataFrame(wallet_stats)

if df.empty:
    print("No hay datos.")
    pd.DataFrame().to_csv(f"data/wallets_full_{today}.csv", index=False)
    pd.DataFrame().to_csv(f"data/top_wallets_{today}.csv", index=False)
else:
    # Asignar tier a cada wallet
    def classify(row):
        base_ok = (
            not row["is_bot_single_market"]
            and not row["is_scalper"]
            and row["trades_10d"] >= MIN_TRADES_RECENT
            and row["days_active_10d"] >= MIN_DAYS_ACTIVE
            and row["markets_count_10d"] >= MIN_MARKETS
            and row["wallet_age_days"] >= MIN_WALLET_AGE_DAYS
        )
        if not base_ok:
            return "REJECTED"

        if (row["avg_buy_size"] <= MAX_AVG_TRADE
            and row["pnl_week_real"] >= MIN_PNL_RECENT
            and row["roi_7d_real_pct"] >= 5):
            return "MICRO"

        if (row["avg_buy_size"] > MAX_AVG_TRADE
            and row["pnl_week_real"] >= MIN_PNL_RECENT
            and row["roi_7d_real_pct"] >= 8):
            return "SIGNAL"

        return "WATCH"

    df["tier"] = df.apply(classify, axis=1)

    # top_wallets = todo menos REJECTED, ordenado MICRO > SIGNAL > WATCH y luego por score
    tier_order = {"MICRO": 0, "SIGNAL": 1, "WATCH": 2, "REJECTED": 3}
    df["_tier_order"] = df["tier"].map(tier_order)
    df_top = df[df["tier"] != "REJECTED"].sort_values(
        ["_tier_order", "copy_score"], ascending=[True, False]
    ).drop(columns=["_tier_order"])

    df_to_save = df.drop(columns=["_tier_order"])

    df_to_save.to_csv(f"data/wallets_full_{today}.csv", index=False)
    df_top.to_csv(f"data/top_wallets_{today}.csv", index=False)

    tier_counts = df["tier"].value_counts().to_dict()
    print(f"\n  Total procesadas: {len(df)}")
    for t in ["MICRO", "SIGNAL", "WATCH", "REJECTED"]:
        print(f"    {t:10s}: {tier_counts.get(t, 0)}")

    # ---------------- [5/5] PRINT TOP ----------------
    print("\n[5/5] TOP MICRO (copiables $50-100):")
    micro = df[df["tier"] == "MICRO"].sort_values("copy_score", ascending=False)
    if not micro.empty:
        cols = ["name", "wallet_age_days", "pnl_week_real", "roi_7d_real_pct", "avg_buy_size", "trades_10d", "days_active_10d", "scalper_pct", "copy_score", "categories"]
        print(micro[cols].head(15).to_string(index=False))
    else:
        print("  Ninguna wallet cumple criterios MICRO hoy.")

    print("\n  TOP SIGNAL (whales - señal direccional):")
    signal = df[df["tier"] == "SIGNAL"].sort_values("roi_7d_real_pct", ascending=False)
    if not signal.empty:
        cols = ["name", "pnl_week_real", "roi_7d_real_pct", "avg_buy_size", "trades_10d", "categories"]
        print(signal[cols].head(10).to_string(index=False))

    if all_trades:
        pd.DataFrame(all_trades).to_csv(f"data/trades_{today}.csv", index=False)
        print(f"\nTrades guardados: {len(all_trades)}")

print("\nDone.")
