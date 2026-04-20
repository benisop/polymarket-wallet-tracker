import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone
from collections import Counter

print("=== POLYMARKET WALLET TRACKER (MICRO-COPY EDITION) ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

# ---------------- CONFIG ----------------
# Objetivo: wallets copiables con capital de $50-100 USD.
# Queremos ROI alto reciente, avg trade PEQUEÑO, ganancias consistentes.

CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "FINANCE", "CULTURE", "ECONOMICS", "TECH"]

# Ventanas de análisis
RECENT_DAYS   = 10   # ventana principal de performance
SHORT_DAYS    = 7    # ventana corta para cross-check
HISTORY_DAYS  = 20   # para traer trades y calcular métricas

# Filtros para quedarnos en la wallet
MAX_AVG_TRADE = 300     # USD - techo para que sea copiable con $50-100
MIN_PNL_RECENT = 2000   # USD - piso de PnL en últimos 10 días (solo ganadores fuertes)
MIN_TRADES_RECENT = 5   # actividad mínima para que tenga sentido estadístico
MIN_DAYS_ACTIVE = 3     # debe tradear en >=3 días distintos
MIN_MARKETS = 2         # diversificación mínima
MAX_TOP_MARKET_PCT = 60 # si >60% de trades son en 1 mercado, es bot de un nicho

# Pulls de leaderboard por tipo
LEADERBOARD_LIMIT = 50  # máx permitido por la API

cutoff_ts_recent = int((datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)).timestamp())
cutoff_ts_short  = int((datetime.now(timezone.utc) - timedelta(days=SHORT_DAYS)).timestamp())
cutoff_ts_hist   = int((datetime.now(timezone.utc) - timedelta(days=HISTORY_DAYS)).timestamp())

# ---------------- [1/4] LEADERBOARD RECIENTE ----------------
# Traemos WEEK (7d) + MONTH (30d) de cada categoría.
# WEEK filtra por performance reciente - ahí viven las wallets que nos interesan.

print(f"\n[1/4] Leaderboard reciente (WEEK + MONTH, {LEADERBOARD_LIMIT}/cat)...")
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
                        "pnl_week": 0.0,
                        "pnl_month": 0.0,
                        "vol_lb": float(t.get("vol", 0)),
                        "categories": [],
                        "periods": [],
                    }
                w = all_wallets[a]
                pnl = float(t.get("pnl", 0))
                vol = float(t.get("vol", 0))
                if period == "WEEK":
                    w["pnl_week"] = max(w["pnl_week"], pnl)
                else:
                    w["pnl_month"] = max(w["pnl_month"], pnl)
                # Guardamos el MAYOR volumen visto (overall suele ser el real)
                w["vol_lb"] = max(w["vol_lb"], vol)
                if cat not in w["categories"]:
                    w["categories"].append(cat)
                if period not in w["periods"]:
                    w["periods"].append(period)
            print(f"  {period}/{cat}: {len(data)} wallets")
        except Exception as e:
            print(f"  {period}/{cat}: {e}")
        time.sleep(0.25)

print(f"Total wallets únicas en leaderboards: {len(all_wallets)}")

# Pre-filtro: la wallet debe tener PnL positivo decente en al menos una ventana.
# Usamos un piso bajo aquí porque el filtro duro viene después con los trades reales.
prefilter = {
    a: d for a, d in all_wallets.items()
    if (d["pnl_week"] >= 500 or d["pnl_month"] >= 1500)
}
print(f"Wallets tras prefilter (pnl_week>=500 o pnl_month>=1500): {len(prefilter)}")

# ---------------- [2/4] HISTORIAL DE TRADES ----------------
print(f"\n[2/4] Historial de trades (últimos {HISTORY_DAYS}d)...")
wallet_stats = []
all_trades = []

def fetch_all_activity(addr, start_ts, max_pages=5, page_size=500):
    """Pagina /activity hasta cubrir la ventana o llegar al tope."""
    trades = []
    offset = 0
    for _ in range(max_pages):
        try:
            r = requests.get(
                "https://data-api.polymarket.com/activity",
                params={
                    "user": addr,
                    "limit": page_size,
                    "offset": offset,
                    "start": start_ts,
                    "type": "TRADE",
                },
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
            offset += page_size
        except Exception:
            break
        time.sleep(0.15)
    return trades

for i, (addr, info) in enumerate(prefilter.items()):
    name = info.get("userName", addr[:10])
    try:
        trades = fetch_all_activity(addr, cutoff_ts_hist)
        if not trades:
            continue

        # Filtramos solo BUY/SELL en las ventanas
        recent_hist  = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts_hist and t.get("side") in ["BUY", "SELL"]]
        recent_10d   = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts_recent and t.get("side") in ["BUY", "SELL"]]
        recent_7d    = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts_short  and t.get("side") in ["BUY", "SELL"]]

        if not recent_10d:
            continue

        buys_10d = [t for t in recent_10d if t.get("side") == "BUY"]
        sells_10d = [t for t in recent_10d if t.get("side") == "SELL"]

        # Avg trade size sobre BUYS (lo que vas a copiar)
        avg_buy = (sum(float(t.get("usdcSize", 0)) for t in buys_10d) / len(buys_10d)) if buys_10d else 0.0
        # Volumen total (buys + sells) en 10d como proxy de capital desplegado
        vol_10d = sum(float(t.get("usdcSize", 0)) for t in recent_10d)
        vol_7d  = sum(float(t.get("usdcSize", 0)) for t in recent_7d)

        # Días activos y diversificación
        days_10d = len(set(datetime.fromtimestamp(t.get("timestamp", 0), tz=timezone.utc).strftime("%Y-%m-%d") for t in recent_10d))
        markets_10d = list(set(t.get("title", "")[:80] for t in recent_10d))
        mc = Counter(t.get("title", "") for t in recent_10d)
        top_pct = (mc.most_common(1)[0][1] / len(recent_10d) * 100) if recent_10d else 0

        # PnL reciente - usamos el del leaderboard como fuente primaria
        pnl_week  = info["pnl_week"]
        pnl_month = info["pnl_month"]

        # ROI aproximado: pnl_week / vol_7d. Puede pasar de 100% (es por unidad de volumen).
        roi_7d  = (pnl_week / vol_7d * 100) if vol_7d > 0 else 0.0
        roi_10d = (pnl_month / vol_10d * 100) if vol_10d > 0 else 0.0  # pnl_month como mejor proxy dado que la API no da 10d exacto

        # Posición mínima copiable: si tu capital es $50 y el avg es $150, representa 33% del capital.
        # A menor avg_buy, más copiable es.
        copiable_ratio = (100 / avg_buy) if avg_buy > 0 else 0  # "cuánto % del avg puedes copiar con $100"

        stats = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "wallet": addr,
            "name": name,
            "pnl_week": round(pnl_week, 2),
            "pnl_month": round(pnl_month, 2),
            "vol_7d": round(vol_7d, 2),
            "vol_10d": round(vol_10d, 2),
            "roi_7d_pct": round(roi_7d, 2),
            "roi_10d_pct": round(roi_10d, 2),
            "trades_10d": len(recent_10d),
            "buys_10d": len(buys_10d),
            "sells_10d": len(sells_10d),
            "days_active_10d": days_10d,
            "markets_count_10d": len(markets_10d),
            "avg_buy_size": round(avg_buy, 2),
            "copiable_ratio_100usd": round(copiable_ratio, 2),
            "top_market_pct": round(top_pct, 1),
            "is_bot_single_market": top_pct > MAX_TOP_MARKET_PCT,
            "categories": ", ".join(info.get("categories", [])),
            "leaderboard_periods": ", ".join(info.get("periods", [])),
            "markets_sample": " | ".join(markets_10d[:3]),
        }

        # Score de copiabilidad - combina ROI, tamaño pequeño, consistencia
        # Penaliza trades gigantes (no copiables) y premia ROI alto + días activos
        size_bonus = max(0, (MAX_AVG_TRADE - avg_buy) / MAX_AVG_TRADE) * 30  # 0-30
        roi_bonus = min(roi_7d, 100) * 0.4  # 0-40 (cap al 100% ROI semanal)
        activity_bonus = min(days_10d, 10) * 2  # 0-20
        diversification_bonus = min(len(markets_10d), 10) * 1  # 0-10
        stats["copy_score"] = round(size_bonus + roi_bonus + activity_bonus + diversification_bonus, 2)

        wallet_stats.append(stats)

        for t in recent_hist:
            trade = dict(t)
            trade["wallet_name"] = name
            trade["wallet_addr"] = addr
            trade["snapshot_date"] = stats["date"]
            all_trades.append(trade)

        print(f"  [{i+1}/{len(prefilter)}] {name[:20]:20s} pnl_w=${pnl_week:>7.0f} avg=${avg_buy:>5.0f} roi_7d={roi_7d:>6.1f}% score={stats['copy_score']:>5.1f}")
    except Exception as e:
        print(f"  [{i+1}/{len(prefilter)}] {name}: ERROR {e}")
    time.sleep(0.15)

# ---------------- [3/4] FILTRADO EN TIERS ----------------
print("\n[3/4] Filtrando en tiers...")
os.makedirs("data", exist_ok=True)
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

df = pd.DataFrame(wallet_stats)

if df.empty:
    print("No hay datos. Revisa la API.")
else:
    # Base: no bots de un solo mercado, actividad mínima
    base = df[
        (df.is_bot_single_market == False)
        & (df.trades_10d >= MIN_TRADES_RECENT)
        & (df.days_active_10d >= MIN_DAYS_ACTIVE)
        & (df.markets_count_10d >= MIN_MARKETS)
    ].copy()

    # TIER MICRO: copiables con $50-100
    # Avg trade <= $300, PnL semana >= $2000, ROI 7d >= 5%
    micro = base[
        (base.avg_buy_size <= MAX_AVG_TRADE)
        & (base.pnl_week >= MIN_PNL_RECENT)
        & (base.roi_7d_pct >= 5)
    ].sort_values("copy_score", ascending=False)

    # TIER SIGNAL: wallets buenas pero con trades demasiado grandes - solo señal
    # Avg > $300 pero ROI alto y PnL reciente fuerte
    signal = base[
        (base.avg_buy_size > MAX_AVG_TRADE)
        & (base.pnl_week >= MIN_PNL_RECENT)
        & (base.roi_7d_pct >= 8)
    ].sort_values("roi_7d_pct", ascending=False)

    # TIER WATCHLIST: pasaron base pero no cumplen MICRO ni SIGNAL
    # Útil para monitorear en próximos días
    watch = base[
        ~base.index.isin(micro.index) & ~base.index.isin(signal.index)
    ].sort_values("copy_score", ascending=False).head(30)

    # Guardar todo
    df.to_csv(f"data/wallets_full_{today}.csv", index=False)
    micro.to_csv(f"data/micro_wallets_{today}.csv", index=False)
    signal.to_csv(f"data/signal_wallets_{today}.csv", index=False)
    watch.to_csv(f"data/watch_wallets_{today}.csv", index=False)

    print(f"\n  Total procesadas:  {len(df)}")
    print(f"  Base (pasan filtros duros de actividad): {len(base)}")
    print(f"  MICRO (copiables con $50-100):           {len(micro)}")
    print(f"  SIGNAL (solo señal, trades grandes):     {len(signal)}")
    print(f"  WATCH (monitorear):                      {len(watch)}")

    # ---------------- [4/4] PRINT TOP ----------------
    print("\n[4/4] TOP MICRO (estas son las que te sirven):")
    if not micro.empty:
        cols = ["name", "pnl_week", "roi_7d_pct", "avg_buy_size", "trades_10d", "days_active_10d", "markets_count_10d", "copy_score", "categories"]
        print(micro[cols].head(15).to_string(index=False))
    else:
        print("  Ninguna wallet cumple criterios MICRO hoy. Revisa SIGNAL o WATCH.")

    print("\n    TOP SIGNAL (no copiables directo pero útiles como indicador):")
    if not signal.empty:
        cols = ["name", "pnl_week", "roi_7d_pct", "avg_buy_size", "trades_10d", "categories"]
        print(signal[cols].head(10).to_string(index=False))

    if all_trades:
        pd.DataFrame(all_trades).to_csv(f"data/trades_{today}.csv", index=False)
        print(f"\nTrades guardados: {len(all_trades)}")

print("\nDone.")
