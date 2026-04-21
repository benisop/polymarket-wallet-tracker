import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone

print("=== POLYMARKET MICRO WALLET TRACKER v2 ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
CUTOFF_DAYS     = 20
RECENT_DAYS     = 7
MIN_WINS        = 5
MIN_TRADE_USD   = 10
MAX_TRADE_USD   = 300
MIN_WIN_RATE    = 0.60
MIN_ROI         = 0.15     # 15% ROI mínimo sobre volumen

cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=CUTOFF_DAYS)).timestamp())
recent_ts = int((datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)).timestamp())

CATEGORY_KEYWORDS = {
    "POLITICS": ["election","president","congress","senate","vote","trump","biden","harris","party","democrat","republican","minister","government","poll"],
    "SPORTS":   ["nba","nfl","mlb","nhl","soccer","football","basketball","baseball","tennis","f1","world cup","championship","league","match","game","team","player","win","score"],
    "CRYPTO":   ["bitcoin","btc","eth","ethereum","crypto","sol","price","usd","market cap","altcoin","defi","token","coin"],
}

def get_category(title: str) -> str:
    t = title.lower()
    for cat, kw in CATEGORY_KEYWORDS.items():
        if any(k in t for k in kw):
            return cat
    return "OTHERS"

# ─── FASE 1: SEED — wallets desde leaderboard reciente (30d) ─────────────────
print("\n[1/4] Obteniendo seed wallets desde leaderboard 30d...")

seed_wallets = {}
CATEGORIES = ["OVERALL","POLITICS","SPORTS","CRYPTO","FINANCE","CULTURE","ECONOMICS","TECH"]

for cat in CATEGORIES:
    try:
        r = requests.get(
            "https://data-api.polymarket.com/v1/leaderboard",
            params={"category": cat, "limit": 100, "window": "1m"},
            timeout=15
        )
        data = r.json()
        if not isinstance(data, list):
            print(f"  {cat}: respuesta inesperada")
            continue
        for t in data:
            a = t.get("proxyWallet", "").lower()
            if not a:
                continue
            pnl = float(t.get("pnl", 0))
            vol = float(t.get("volume", t.get("vol", 0)))
            # Solo agrega si tiene ROI razonable (evita puro volumen)
            if vol > 0 and (pnl / vol) >= MIN_ROI and pnl > 50:
                if a not in seed_wallets:
                    seed_wallets[a] = {"pnl_30d": pnl, "vol_30d": vol, "categories": []}
                if cat not in seed_wallets[a]["categories"]:
                    seed_wallets[a]["categories"].append(cat)
        print(f"  {cat}: ok ({len(data)} traders)")
    except Exception as e:
        print(f"  {cat}: {e}")
    time.sleep(0.3)

print(f"  Seed total: {len(seed_wallets)} wallets con ROI >= {MIN_ROI*100:.0f}%")

# ─── FASE 2: ANÁLISIS DE ACTIVIDAD RECIENTE ──────────────────────────────────
print(f"\n[2/4] Analizando actividad ({CUTOFF_DAYS}d)...")

results = []

for i, (addr, seed_info) in enumerate(seed_wallets.items()):
    try:
        r = requests.get(
            "https://data-api.polymarket.com/activity",
            params={"user": addr, "limit": 500},
            timeout=10
        )
        trades = r.json()
        if not isinstance(trades, list) or len(trades) == 0:
            continue

        # Filtrar por ventana temporal
        recent_trades = [t for t in trades if t.get("timestamp", 0) >= cutoff_ts]
        if not recent_trades:
            continue

        # Solo BUY (no contar sells dobles)
        buys = [t for t in recent_trades if t.get("side") == "BUY"]
        if not buys:
            continue

        # Filtro de tamaño de trade: solo micro-trades
        micro_buys = [t for t in buys if MIN_TRADE_USD <= float(t.get("usdcSize", 0)) <= MAX_TRADE_USD]
        if len(micro_buys) < MIN_WINS:
            continue

        # Mercados únicos ganados (necesitamos redemptions como proxy de wins)
        # Usamos SELL con price cercano a 1.0 como proxy de win
        sells = [t for t in recent_trades if t.get("side") == "SELL"]
        win_sells = [t for t in sells if float(t.get("price", 0)) >= 0.85]

        # Mercados únicos con win
        win_markets = list(set(t.get("conditionId", t.get("slug", "")) for t in win_sells))
        n_wins = len(win_markets)

        if n_wins < MIN_WINS:
            continue

        # Calcular win rate
        all_markets_traded = list(set(t.get("conditionId", t.get("slug", "")) for t in buys))
        win_rate = n_wins / len(all_markets_traded) if all_markets_traded else 0

        if win_rate < MIN_WIN_RATE:
            continue

        # Zombie positions: mercados comprados pero nunca vendidos ni redimidos
        bought_markets  = set(t.get("conditionId", "") for t in buys)
        sold_markets    = set(t.get("conditionId", "") for t in sells)
        zombie_markets  = bought_markets - sold_markets
        zombie_ratio    = len(zombie_markets) / len(bought_markets) if bought_markets else 0

        # Penaliza mucho zombie (inflan win rate sin cerrar)
        if zombie_ratio > 0.5:
            continue

        # Avg trade size
        avg_size = sum(float(t.get("usdcSize", 0)) for t in micro_buys) / len(micro_buys)

        # Entry timing bonus: compró cuando precio era bajo (< 0.5) en mercados que ganó
        early_buys = [t for t in micro_buys
                      if float(t.get("price", 1)) < 0.5
                      and t.get("conditionId", "") in set(t2.get("conditionId","") for t2 in win_sells)]
        timing_bonus = min(len(early_buys) / max(n_wins, 1), 1.0)

        # Actividad reciente (últimos 7d) — peso extra
        recent_micro = [t for t in micro_buys if t.get("timestamp", 0) >= recent_ts]
        recency_factor = 1.3 if len(recent_micro) >= 2 else 1.0

        # ROI sobre volumen micro
        micro_vol = sum(float(t.get("usdcSize", 0)) for t in micro_buys)
        roi_pct = seed_info["pnl_30d"] / seed_info["vol_30d"] if seed_info["vol_30d"] > 0 else 0

        # ── RECENT SMART SCORE ──
        score = round(
            (roi_pct * 100 * 0.35) +    # ROI% reciente
            (win_rate * 100 * 0.30) +   # Win rate
            (n_wins * 2 * 0.20) +       # Multiplicidad de wins
            (timing_bonus * 20 * 0.15)  # Entry timing bonus
        , 2) * recency_factor

        # Categoría dominante por mercados
        all_titles = " ".join(t.get("title", "") for t in micro_buys)
        category = get_category(all_titles)
        if seed_info["categories"] and seed_info["categories"][0] != "OVERALL":
            category = seed_info["categories"][0]

        # Posiciones abiertas (conviction plays)
        open_pos = []
        try:
            rp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": addr, "sizeThreshold": MIN_TRADE_USD},
                timeout=8
            )
            positions = rp.json()
            if isinstance(positions, list):
                open_pos = [
                    {
                        "title": p.get("title", "")[:60],
                        "outcome": p.get("outcome", ""),
                        "size_usd": round(float(p.get("currentValue", p.get("initialValue", 0))), 2),
                        "price": round(float(p.get("curPrice", 0)), 3),
                        "pnl_pct": round(float(p.get("percentPnl", 0)), 1)
                    }
                    for p in positions
                    if float(p.get("currentValue", p.get("initialValue", 0))) >= MIN_TRADE_USD
                    and float(p.get("currentValue", 0)) <= MAX_TRADE_USD * 2
                ]
        except:
            pass

        name = ""
        try:
            rn = requests.get(f"https://data-api.polymarket.com/profile/{addr}", timeout=5)
            pdata = rn.json()
            name = pdata.get("name", pdata.get("username", addr[:10]))
        except:
            name = addr[:10]

        results.append({
            "wallet":           addr,
            "name":             name,
            "category":         category,
            "smart_score":      round(score, 2),
            "roi_pct":          round(roi_pct * 100, 1),
            "win_rate":         round(win_rate * 100, 1),
            "n_wins_20d":       n_wins,
            "micro_trades_20d": len(micro_buys),
            "recent_trades_7d": len(recent_micro),
            "avg_trade_usd":    round(avg_size, 2),
            "timing_bonus":     round(timing_bonus, 2),
            "zombie_ratio":     round(zombie_ratio, 2),
            "pnl_30d":          round(seed_info["pnl_30d"], 2),
            "open_positions":   len(open_pos),
            "conviction_plays": str([f"{p['title']} → {p['outcome']} @ {p['price']}" for p in open_pos[:3]]),
            "scan_date":        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })

        print(f"  [{i+1}] {name}: score={score:.1f} wr={win_rate*100:.0f}% wins={n_wins} zombie={zombie_ratio:.1%}")

    except Exception as e:
        pass

    time.sleep(0.25)

# ─── FASE 3: OUTPUT ───────────────────────────────────────────────────────────
print(f"\n[3/4] Generando outputs... ({len(results)} wallets calificadas)")

os.makedirs("data", exist_ok=True)
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

if results:
    df = pd.DataFrame(results).sort_values("smart_score", ascending=False)

    # Archivo completo
    df.to_csv(f"data/micro_wallets_{today}.csv", index=False)

    # Por categoría
    for cat in ["POLITICS", "SPORTS", "CRYPTO", "OTHERS"]:
        df_cat = df[df.category == cat]
        if not df_cat.empty:
            df_cat.to_csv(f"data/micro_wallets_{cat.lower()}_{today}.csv", index=False)
            print(f"  {cat}: {len(df_cat)} wallets")

    # Top 10 resumen
    print(f"\n{'─'*80}")
    print(f"TOP MICRO WALLETS — {today}")
    print(f"{'─'*80}")
    cols = ["name","category","smart_score","roi_pct","win_rate","n_wins_20d","avg_trade_usd","open_positions"]
    print(df[cols].head(10).to_string(index=False))
    print(f"{'─'*80}")

    # Conviction plays (wallets con posiciones abiertas activas)
    df_conviction = df[(df.open_positions > 0) & (df.smart_score >= df.smart_score.quantile(0.7))]
    if not df_conviction.empty:
        print(f"\nCONVICTION PLAYS ({len(df_conviction)} wallets con posiciones abiertas):")
        for _, row in df_conviction.head(5).iterrows():
            print(f"  {row['name']} (score={row['smart_score']}) → {row['conviction_plays']}")

    print(f"\nGuardado: data/micro_wallets_{today}.csv + {len([c for c in ['POLITICS','SPORTS','CRYPTO','OTHERS']])} por categoría")

else:
    print("  Sin resultados. Ajusta MIN_WINS o MIN_WIN_RATE.")

print("\n[4/4] Done.")
