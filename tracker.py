import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone

print(f"=== POLYMARKET WALLET TRACKER ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "FINANCE", "CULTURE", "ECONOMICS", "TECH"]
CUTOFF_DAYS = 20

# ── 1. Leaderboard completo ──────────────────────────────────
print("\n[1/3] Descargando leaderboard...")
all_wallets = {}

for cat in CATEGORIES:
    try:
        r = requests.get(
            "https://data-api.polymarket.com/v1/leaderboard",
            params={"category": cat, "limit": 100},
            timeout=15
        )
        data = r.json()
        for trader in data:
            addr = trader.get("proxyWallet", "").lower()
            if addr:
                if addr not in all_wallets:
                    all_wallets[addr] = trader
                    all_wallets[addr]["categories"] = []
                all_wallets[addr]["categories"].append(cat)
        print(f"  {cat}: {len(data)} traders")
    except Exception as e:
        print(f"  Error {cat}: {e}")
    time.sleep(0.3)

print(f"Total wallets únicas: {len(all_wallets)}")

active = {
    a: d for a, d in all_wallets.items()
    if d.get("pnl", 0) > 500 and d.get("vol", 0) > 1000
}
print(f"Wallets activas (PnL>$500, vol>$1K): {len(active)}")

# ── 2. Historial de trades ───────────────────────────────────
print("\n[2/3] Descargando historial...")

cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).timestamp())
wallet_stats = []
all_trades = []

for i, (addr, info) in enumerate(active.items()):
    name = info.get("userName", addr[:10])
    try:
        r = requests.get(
            "https://data-api.polymarket.com/activity",
            params={"user": addr, "limit": 100, "start": cutoff_ts},
            timeout=10
        )
        trades = r.json()
        if not isinstance(trades, list):
            continue

        recent = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts and t.get("side") in ["BUY", "SELL"]]
        if not recent:
            continue

        buys = [t for t in recent if t.get("side") == "BUY"]
        days_active = len(set(
            datetime.fromtimestamp(t.get("timestamp", 0), tz=timezone.utc).strftime("%Y-%m-%d")
            for t in recent
        ))
        avg_size = sum(float(t.get("usdcSize", 0)) for t in buys) / len(buys) if buys else 0
        markets = list(set(t.get("title", "")[:50] for t in recent))
        top_market_pct = 0
        if recent:
            from collections import Counter
            mc = Counter(t.get("title","") for t in recent)
            top_market_pct = mc.most_common(1)[0][1] / len(recent) * 100

        stats = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "wallet": addr,
            "name": name,
            "pnl_alltime": round(info.get("pnl", 0), 2),
            "vol_alltime": round(info.get("vol", 0), 2),
            "trades_20d": len(recent),
            "days_active_20d": days_active,
            "markets_count_20d": len(markets),
            "avg_trade_size": round(avg_size, 2),
            "top_market_pct": round(top_market_pct, 1),
            "is_bot": top_market_pct > 50,
            "categories": ", ".join(info.get("categories", [])),
            "markets_sample": " | ".join(markets[:3]),
            "consistency_score": round(
                days_active * 3 +
                len(markets) * 2 +
                min(info.get("pnl", 0) / 1000, 20), 2
            )
        }

        wallet_stats.append(stats)
        for t in recent:
            t["wallet_name"] = name
            t["wallet_addr"] = addr
            t["snapshot_date"] = stats["date"]
            all_trades.append(t)

        print(f"  [{i+1}/{len(active)}] {name}: {len(recent)} trades, {days_active} días, bot={stats['is_bot']}")

    except Exception as e:
        print(f"  Error {name}: {e}")
    time.sleep(0.2)

# ── 3. Guardar CSVs ──────────────────────────────────────────
print("\n[3/3] Guardando...")

os.makedirs("data", exist_ok=True)

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

df = pd.DataFrame(wallet_stats)
if not df.empty:
    df_human = df[
        (df["is_bot"] == False) &
        (df["trades_20d"] >= 5) &
        (df["days_active_20d"] >= 3) &
        (df["avg_trade_size"] >= 50) &
        (df["markets_count_20d"] >= 2)
    ].sort_values("consistency_score", ascending=False)

    df.to_csv(f"data/wallets_full_{today}.csv", index=False)
    df_human.to_csv(f"data/top_wallets_{today}.csv", index=False)
    print(f"✅ data/wallets_full_{today}.csv ({len(df)} wallets)")
    print(f"✅ data/top_wallets_{today}.csv ({len(df_human)} wallets filtradas)")

    print(f"\n=== TOP 10 ===")
    cols = ["name","trades_20d","days_active_20d","markets_count_20d","avg_trade_size","pnl_alltime","consistency_score"]
    print(df_human[cols].head(10).to_string(index=False))

if all_trades:
    df_trades = pd.DataFrame(all_trades)
    df_trades.to_csv(f"data/trades_{today}.csv", index=False)
    print(f"✅ data/trades_{today}.csv ({len(df_trades)} trades)")

print("\nDone.")
PYEOF
Salida

import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone

print(f"=== POLYMARKET WALLET TRACKER ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "FINANCE", "CULTURE", "ECONOMICS", "TECH"]
CUTOFF_DAYS = 20

# ── 1. Leaderboard completo ──────────────────────────────────
print("\n[1/3] Descargando leaderboard...")
all_wallets = {}

for cat in CATEGORIES:
    try:
        r = requests.get(
            "https://data-api.polymarket.com/v1/leaderboard",
            params={"category": cat, "limit": 100},
            timeout=15
        )
        data = r.json()
        for trader in data:
            addr = trader.get("proxyWallet", "").lower()
            if addr:
                if addr not in all_wallets:
                    all_wallets[addr] = trader
                    all_wallets[addr]["categories"] = []
                all_wallets[addr]["categories"].append(cat)
        print(f"  {cat}: {len(data)} traders")
    except Exception as e:
        print(f"  Error {cat}: {e}")
    time.sleep(0.3)

print(f"Total wallets únicas: {len(all_wallets)}")

active = {
    a: d for a, d in all_wallets.items()
    if d.get("pnl", 0) > 500 and d.get("vol", 0) > 1000
}
print(f"Wallets activas (PnL>$500, vol>$1K): {len(active)}")

# ── 2. Historial de trades ───────────────────────────────────
print("\n[2/3] Descargando historial...")

cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).timestamp())
wallet_stats = []
all_trades = []

for i, (addr, info) in enumerate(active.items()):
    name = info.get("userName", addr[:10])
    try:
        r = requests.get(
            "https://data-api.polymarket.com/activity",
            params={"user": addr, "limit": 100, "start": cutoff_ts},
            timeout=10
        )
        trades = r.json()
        if not isinstance(trades, list):
            continue

        recent = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts and t.get("side") in ["BUY", "SELL"]]
        if not recent:
            continue

        buys = [t for t in recent if t.get("side") == "BUY"]
        days_active = len(set(
            datetime.fromtimestamp(t.get("timestamp", 0), tz=timezone.utc).strftime("%Y-%m-%d")
            for t in recent
        ))
        avg_size = sum(float(t.get("usdcSize", 0)) for t in buys) / len(buys) if buys else 0
        markets = list(set(t.get("title", "")[:50] for t in recent))
        top_market_pct = 0
        if recent:
            from collections import Counter
            mc = Counter(t.get("title","") for t in recent)
            top_market_pct = mc.most_common(1)[0][1] / len(recent) * 100

        stats = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "wallet": addr,
            "name": name,
            "pnl_alltime": round(info.get("pnl", 0), 2),
            "vol_alltime": round(info.get("vol", 0), 2),
            "trades_20d": len(recent),
            "days_active_20d": days_active,
            "markets_count_20d": len(markets),
            "avg_trade_size": round(avg_size, 2),
            "top_market_pct": round(top_market_pct, 1),
            "is_bot": top_market_pct > 50,
            "categories": ", ".join(info.get("categories", [])),
            "markets_sample": " | ".join(markets[:3]),
            "consistency_score": round(
                days_active * 3 +
                len(markets) * 2 +
                min(info.get("pnl", 0) / 1000, 20), 2
            )
        }

        wallet_stats.append(stats)
        for t in recent:
            t["wallet_name"] = name
            t["wallet_addr"] = addr
            t["snapshot_date"] = stats["date"]
            all_trades.append(t)

        print(f"  [{i+1}/{len(active)}] {name}: {len(recent)} trades, {days_active} días, bot={stats['is_bot']}")

    except Exception as e:
        print(f"  Error {name}: {e}")
    time.sleep(0.2)

# ── 3. Guardar CSVs ──────────────────────────────────────────
print("\n[3/3] Guardando...")

os.makedirs("data", exist_ok=True)

today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

df = pd.DataFrame(wallet_stats)
if not df.empty:
    df_human = df[
        (df["is_bot"] == False) &
        (df["trades_20d"] >= 5) &
        (df["days_active_20d"] >= 3) &
        (df["avg_trade_size"] >= 50) &
        (df["markets_count_20d"] >= 2)
    ].sort_values("consistency_score", ascending=False)

    df.to_csv(f"data/wallets_full_{today}.csv", index=False)
    df_human.to_csv(f"data/top_wallets_{today}.csv", index=False)
    print(f"✅ data/wallets_full_{today}.csv ({len(df)} wallets)")
    print(f"✅ data/top_wallets_{today}.csv ({len(df_human)} wallets filtradas)")

    print(f"\n=== TOP 10 ===")
    cols = ["name","trades_20d","days_active_20d","markets_count_20d","avg_trade_size","pnl_alltime","consistency_score"]
    print(df_human[cols].head(10).to_string(index=False))

if all_trades:
    df_trades = pd.DataFrame(all_trades)
    df_trades.to_csv(f"data/trades_{today}.csv", index=False)
    print(f"✅ data/trades_{today}.csv ({len(df_trades)} trades)")

print("\nDone.")
