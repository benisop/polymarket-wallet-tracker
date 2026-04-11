import requests
import pandas as pd
import time
import os
import math
from datetime import datetime, timedelta, timezone
from collections import Counter

print("=== POLYMARKET WALLET TRACKER ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "FINANCE", "CULTURE", "ECONOMICS", "TECH"]
CUTOFF_DAYS = 30
cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).timestamp())

# ── [1/3] LEADERBOARD — 200 por categoría ─────────────────────────────────────
print("\n[1/3] Leaderboard...")
all_wallets = {}

for cat in CATEGORIES:
    for offset in [0, 100]:
        try:
            r = requests.get(
                "https://data-api.polymarket.com/v1/leaderboard",
                params={"category": cat, "limit": 100, "offset": offset},
                timeout=15,
            )
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                break
            for t in data:
                a = t.get("proxyWallet", "").lower()
                if a:
                    if a not in all_wallets:
                        all_wallets[a] = t
                        all_wallets[a]["categories"] = []
                    all_wallets[a]["categories"].append(cat)
            print(f"  {cat} offset={offset}: {len(data)} wallets")
        except Exception as e:
            print(f"  {cat} offset={offset}: {e}")
        time.sleep(0.3)

active = {
    a: d for a, d in all_wallets.items()
    if d.get("pnl", 0) > 50 and d.get("vol", 0) > 200
}
print(f"\nWallets candidatas totales: {len(active)}")

# ── [2/3] HISTORIAL ────────────────────────────────────────────────────────────
print("\n[2/3] Historial de trades...")
wallet_stats = []
all_trades = []

for i, (addr, info) in enumerate(active.items()):
    name = info.get("userName", addr[:10])
    try:
        r = requests.get(
            "https://data-api.polymarket.com/activity",
            params={"user": addr, "limit": 500, "start": cutoff_ts},
            timeout=15,
        )
        trades = r.json()
        if not isinstance(trades, list) or len(trades) == 0:
            continue

        recent = [
            t for t in trades
            if t.get("timestamp", 0) >= cutoff_ts and t.get("side") in ["BUY", "SELL"]
        ]
        if len(recent) < 3:
            continue

        buys  = [t for t in recent if t.get("side") == "BUY"]
        sells = [t for t in recent if t.get("side") == "SELL"]

        buy_sizes = [float(t.get("usdcSize", 0)) for t in buys]
        all_sizes = [float(t.get("usdcSize", 0)) for t in recent]
        avg_buy   = sum(buy_sizes) / len(buy_sizes) if buy_sizes else 0
        avg_all   = sum(all_sizes) / len(all_sizes) if all_sizes else 0

        days = len(set(
            datetime.fromtimestamp(t.get("timestamp", 0), tz=timezone.utc).strftime("%Y-%m-%d")
            for t in recent
        ))

        market_counter = Counter(t.get("title", "") for t in recent)
        markets = list(market_counter.keys())
        top_pct = market_counter.most_common(1)[0][1] / len(recent) * 100 if recent else 0
        hhi = sum((v / len(recent)) ** 2 for v in market_counter.values())

        winning_sells = sum(1 for t in sells if float(t.get("price", 0)) > 0.5)
        win_rate = winning_sells / len(sells) if sells else 0.5

        if avg_buy <= 500:
            size_tier = "MICRO"
        elif avg_buy <= 2000:
            size_tier = "SMALL"
        elif avg_buy <= 10000:
            size_tier = "MEDIUM"
        else:
            size_tier = "WHALE"

        # Quality score (5 dimensiones, max 100)
        d_score = min(days / CUTOFF_DAYS * 20, 20)
        m_score = min(len(markets) / 20 * 20, 20) * (1 - hhi)
        w_score = (win_rate - 0.52) / 0.38 * 20 if 0.52 <= win_rate <= 0.90 else 0
        t_score = min(math.log10(len(recent) + 1) / math.log10(501) * 20, 20)
        pnl = info.get("pnl", 0)
        vol = info.get("vol", 1)
        efficiency = pnl / vol if vol > 0 else 0
        e_score = min(max(efficiency * 100, 0), 20)
        quality_score = round(d_score + m_score + w_score + t_score + e_score, 2)

        is_bot = (
            (top_pct > 60 and len(markets) <= 3) or
            (hhi > 0.70) or
            (win_rate > 0.95 and len(sells) > 10) or
            (avg_buy < 1.0 and len(recent) > 50)
        )

        stats = {
            "date":               datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "wallet":             addr,
            "name":               name,
            "pnl_alltime":        round(pnl, 2),
            "vol_alltime":        round(vol, 2),
            "pnl_efficiency":     round(efficiency, 4),
            "trades_30d":         len(recent),
            "days_active_30d":    days,
            "markets_count_30d":  len(markets),
            "avg_buy_size":       round(avg_buy, 2),
            "avg_size_all":       round(avg_all, 2),
            "top_market_pct":     round(top_pct, 1),
            "hhi_concentration":  round(hhi, 3),
            "win_rate_approx":    round(win_rate, 3),
            "is_bot":             is_bot,
            "size_tier":          size_tier,
            "categories":         ", ".join(info.get("categories", [])),
            "markets_sample":     " | ".join(markets[:3]),
            "quality_score":      quality_score,
        }
        wallet_stats.append(stats)

        for t in recent:
            trade = dict(t)
            trade["wallet_name"]   = name
            trade["wallet_addr"]   = addr
            trade["snapshot_date"] = stats["date"]
            trade["size_tier"]     = size_tier
            trade["quality_score"] = quality_score
            all_trades.append(trade)

        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(active)}] procesadas...")

    except Exception as e:
        print(f"  Error {name}: {e}")
    time.sleep(0.2)

print(f"\nWallets con datos: {len(wallet_stats)}")

# ── [3/3] GUARDAR ──────────────────────────────────────────────────────────────
print("\n[3/3] Guardando...")
os.makedirs("data", exist_ok=True)
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

if wallet_stats:
    df = pd.DataFrame(wallet_stats)

    # Lista A: monitoreo amplio (hasta 100 wallets, filtros mínimos)
    df_monitor = df[
        (df.is_bot == False) &
        (df.trades_30d >= 5) &
        (df.avg_buy_size >= 10) &
        (df.pnl_alltime > 100)
    ].sort_values("quality_score", ascending=False).head(100)

    # Lista B: copy trading activo (filtros estrictos, hasta 20 wallets)
    df_copy = df[
        (df.is_bot == False) &
        (df.trades_30d >= 10) &
        (df.days_active_30d >= 3) &
        (df.markets_count_30d >= 3) &
        (df.avg_buy_size >= 20) &
        (df.hhi_concentration <= 0.55) &
        (df.win_rate_approx >= 0.52) &
        (df.pnl_alltime > 200)
    ].sort_values("quality_score", ascending=False).head(20)

    # MICRO: copiables con $200
    df_micro = df_copy[df_copy.size_tier == "MICRO"].sort_values("quality_score", ascending=False)

    df.to_csv(f"data/wallets_full_{today}.csv", index=False)
    df_monitor.to_csv(f"data/monitor_{today}.csv", index=False)
    df_copy.to_csv(f"data/top_wallets_{today}.csv", index=False)
    df_micro.to_csv(f"data/micro_wallets_{today}.csv", index=False)

    # Master acumulado con append
    master_path = "data/wallets_master.csv"
    if os.path.exists(master_path):
        df_master = pd.read_csv(master_path)
        df_master = pd.concat([df_master, df], ignore_index=True)
        df_master = df_master.drop_duplicates(subset=["date", "wallet"], keep="last")
    else:
        df_master = df
    df_master.to_csv(master_path, index=False)

    print(f"\n{'='*55}")
    print(f"  wallets_full (raw):     {len(df)}")
    print(f"  monitor Lista A:        {len(df_monitor)}  → monitor_{today}.csv")
    print(f"  copy trading Lista B:   {len(df_copy)}  → top_wallets_{today}.csv")
    print(f"  MICRO copiables:        {len(df_micro)}  → micro_wallets_{today}.csv")
    print(f"  master acumulado:       {len(df_master)} registros")
    print(f"{'='*55}")

    print(f"\nTop wallets Lista B:")
    cols = ["name","size_tier","days_active_30d","avg_buy_size",
            "win_rate_approx","markets_count_30d","pnl_alltime","quality_score"]
    print(df_copy[cols].to_string(index=False))

    if not df_micro.empty:
        print(f"\nMICRO wallets (copiables con $200):")
        print(df_micro[cols].to_string(index=False))

# Trades master con append
if all_trades:
    df_trades_new = pd.DataFrame(all_trades)
    trades_master = "data/trades_master.csv"

    if os.path.exists(trades_master):
        df_trades_old = pd.read_csv(trades_master)
        df_trades_all = pd.concat([df_trades_old, df_trades_new], ignore_index=True)
        df_trades_all = df_trades_all.drop_duplicates(
            subset=["wallet_addr", "timestamp", "side", "title"],
            keep="last"
        )
    else:
        df_trades_all = df_trades_new

    df_trades_all.to_csv(trades_master, index=False)
    df_trades_new.to_csv(f"data/trades_{today}.csv", index=False)

    print(f"\nTrades hoy:   {len(df_trades_new)}")
    print(f"Trades total: {len(df_trades_all)} acumulados")

print("\nDone.")
