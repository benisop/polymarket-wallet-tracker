"""
Polymarket Wallet Tracker v3 - BOTTOM-UP EDITION

Estrategia completamente distinta a v1/v2:
- NO usa el leaderboard (esas wallets las trackea todo el mundo)
- Parte desde markets resueltos recientes
- Busca quién acertó el lado ganador con tickets pequeños ($20-$200)
- Identifica wallets que aciertan en MÚLTIPLES markets distintos = edge real

Lógica:
1. Traer N markets resueltos en últimos RESOLVED_DAYS días (Gamma API)
2. Por cada market, traer los trades del lado ganador (Data API /activity?market=X)
3. Filtrar: solo BUYs entre MIN_TRADE y MAX_TRADE USDC
4. Acumular por wallet: ¿en cuántos markets distintos acertó? ¿PnL estimado?
5. Output: wallets con MIN_WINS aciertos en markets distintos, ordenadas por win_markets

Outputs:
- data/v3_hidden_wallets_{fecha}.csv  (wallets encontradas)
- data/v3_top_wallets_{fecha}.csv     (filtradas, las que más sirven)
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

print("=== POLYMARKET WALLET TRACKER v3 (BOTTOM-UP) ===")
print(f"Fecha: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

# ---------------- CONFIG ----------------
RESOLVED_DAYS = 15        # buscar markets resueltos en últimos N días
MAX_MARKETS   = 40        # cuántos markets resueltos analizar (más = más lento)
MIN_TRADE     = 20        # USD mínimo por trade (filtrar dust)
MAX_TRADE     = 250       # USD máximo por trade (copiable con $50-100)
MIN_WINS      = 4         # mínimo de markets distintos donde acertó
MIN_WIN_RATE  = 0.55      # win rate mínimo sobre sus trades en ventana
MIN_AGE_DAYS  = 20        # edad mínima de la wallet
MAX_TRADES_PER_MARKET = 200  # límite de trades a traer por market (evitar timeout)

cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=RESOLVED_DAYS)).timestamp())
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
os.makedirs("data", exist_ok=True)

# ---------------- [1/4] MARKETS RESUELTOS ----------------
print(f"\n[1/4] Buscando markets resueltos (últimos {RESOLVED_DAYS}d)...")

markets_found = []
offset = 0

while len(markets_found) < MAX_MARKETS:
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={
                "closed": "true",
                "limit": 50,
                "offset": offset,
                "order": "endDate",
                "ascending": "false",  # más recientes primero
            },
            timeout=15,
        )
        if r.status_code != 200:
            print(f"  Gamma API error: {r.status_code}")
            break
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break

        for m in batch:
            # Solo markets resueltos dentro de la ventana
            end_str = m.get("endDate") or m.get("end_date_iso", "")
            if not end_str:
                continue
            try:
                end_ts = int(datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp())
            except Exception:
                continue
            if end_ts < cutoff_ts:
                break  # ya estamos fuera de la ventana, los demás son más viejos

            # Necesitamos saber cuál fue el outcome ganador
            # Campo: "winnerIndex" o "resolution" o tokens con "winner"
            winner_outcome = None
            tokens = m.get("tokens") or m.get("clobTokenIds") or []
            # Intentamos con el campo outcomePrices: el ganador tiene price=1.0
            outcome_prices = m.get("outcomePrices")
            outcomes = m.get("outcomes")
            if outcome_prices and outcomes:
                try:
                    prices = [float(p) for p in outcome_prices] if isinstance(outcome_prices, list) else []
                    if prices:
                        winner_idx = prices.index(max(prices))
                        if isinstance(outcomes, list) and len(outcomes) > winner_idx:
                            winner_outcome = outcomes[winner_idx]
                except Exception:
                    pass

            condition_id = m.get("conditionId") or m.get("condition_id")
            if not condition_id:
                continue

            markets_found.append({
                "condition_id": condition_id,
                "title": m.get("question") or m.get("title", "")[:80],
                "end_ts": end_ts,
                "winner_outcome": winner_outcome,
                "volume": float(m.get("volume") or m.get("volumeNum") or 0),
            })

            if len(markets_found) >= MAX_MARKETS:
                break

        # Si el último market del batch ya está fuera de la ventana, parar
        if batch:
            last_end = batch[-1].get("endDate") or ""
            try:
                last_ts = int(datetime.fromisoformat(last_end.replace("Z", "+00:00")).timestamp())
                if last_ts < cutoff_ts:
                    break
            except Exception:
                pass

        offset += 50
        time.sleep(0.3)

    except Exception as e:
        print(f"  Error Gamma API: {e}")
        break

print(f"  Markets resueltos encontrados: {len(markets_found)}")
# Mostrar muestra
for m in markets_found[:5]:
    print(f"  → {m['title'][:60]} | winner: {m['winner_outcome']} | vol: ${m['volume']:,.0f}")


# ---------------- [2/4] TRADES POR MARKET ----------------
print(f"\n[2/4] Analizando trades por market...")

# wallet -> dict de métricas acumuladas
wallet_data = defaultdict(lambda: {
    "win_markets": set(),    # markets donde compró el lado ganador
    "lose_markets": set(),   # markets donde compró el lado perdedor
    "total_invested": 0.0,
    "estimated_pnl": 0.0,
    "first_seen_ts": float("inf"),
    "trade_sizes": [],
    "market_titles": [],
})

for i, market in enumerate(markets_found):
    cid = market["condition_id"]
    winner = market["winner_outcome"]
    title = market["title"]

    try:
        r = requests.get(
            "https://data-api.polymarket.com/activity",
            params={
                "market": cid,
                "type": "TRADE",
                "side": "BUY",
                "limit": MAX_TRADES_PER_MARKET,
            },
            timeout=15,
        )
        if r.status_code != 200:
            print(f"  [{i+1}/{len(markets_found)}] {title[:40]:40s} ERROR {r.status_code}")
            time.sleep(0.5)
            continue

        trades = r.json()
        if not isinstance(trades, list):
            continue

        # Filtrar por tamaño de trade
        relevant = [
            t for t in trades
            if MIN_TRADE <= float(t.get("usdcSize", 0)) <= MAX_TRADE
            and t.get("proxyWallet")
        ]

        wins = losses = 0
        for t in relevant:
            addr = t.get("proxyWallet", "").lower()
            usdc = float(t.get("usdcSize", 0))
            outcome = t.get("outcome", "")
            price = float(t.get("price", 0.5))
            ts = t.get("timestamp", 0)
            size_tokens = float(t.get("size", 0))

            w = wallet_data[addr]
            w["first_seen_ts"] = min(w["first_seen_ts"], ts) if ts else w["first_seen_ts"]
            w["total_invested"] += usdc
            w["trade_sizes"].append(usdc)

            # Determinar si fue ganador
            # winner_outcome puede ser "Yes"/"No" o el nombre del equipo
            is_winner = False
            if winner and outcome:
                is_winner = (
                    outcome.lower() == winner.lower()
                    or winner.lower() in outcome.lower()
                    or outcome.lower() in winner.lower()
                )

            if is_winner:
                wins += 1
                w["win_markets"].add(cid)
                w["market_titles"].append(title[:50])
                # PnL estimado: compró a `price`, resolvió a 1.0
                pnl = usdc * (1.0 / price - 1) if price > 0 else 0
                w["estimated_pnl"] += pnl
            else:
                losses += 1
                if winner:  # solo cuenta como pérdida si sabemos el ganador
                    w["lose_markets"].add(cid)
                    w["estimated_pnl"] -= usdc

        vol_str = f"${market['volume']:>10,.0f}"
        print(f"  [{i+1}/{len(markets_found)}] {title[:45]:45s} {len(relevant):>4} trades copiables | w={wins} l={losses}")

    except Exception as e:
        print(f"  [{i+1}/{len(markets_found)}] {title[:40]:40s} ERROR {e}")

    time.sleep(0.25)

print(f"\n  Wallets únicas encontradas: {len(wallet_data)}")


# ---------------- [3/4] SCORING Y FILTROS ----------------
print("\n[3/4] Calculando métricas y filtrando...")
now_ts = datetime.now(timezone.utc).timestamp()

rows = []
for addr, w in wallet_data.items():
    win_count = len(w["win_markets"])
    lose_count = len(w["lose_markets"])
    total_markets = win_count + lose_count

    if total_markets == 0:
        continue

    win_rate = win_count / total_markets
    avg_trade = sum(w["trade_sizes"]) / len(w["trade_sizes"]) if w["trade_sizes"] else 0

    # Edad de la wallet desde el primer trade visto
    age_days = (now_ts - w["first_seen_ts"]) / 86400 if w["first_seen_ts"] != float("inf") else 0

    # Filtros duros
    if win_count < MIN_WINS:
        continue
    if win_rate < MIN_WIN_RATE:
        continue
    if age_days < MIN_AGE_DAYS:
        continue
    if avg_trade > MAX_TRADE:
        continue

    rows.append({
        "date": today,
        "wallet": addr,
        "win_markets": win_count,
        "lose_markets": lose_count,
        "total_markets": total_markets,
        "win_rate": round(win_rate, 3),
        "estimated_pnl": round(w["estimated_pnl"], 2),
        "total_invested": round(w["total_invested"], 2),
        "avg_trade_size": round(avg_trade, 2),
        "age_days": round(age_days, 1),
        "sample_markets": " | ".join(list(set(w["market_titles"]))[:3]),
    })

df = pd.DataFrame(rows)

if df.empty:
    print("  Sin resultados. Ajustar MIN_WINS o MIN_WIN_RATE.")
else:
    # Ordenar: primero por win_markets (más aciertos), luego win_rate
    df = df.sort_values(["win_markets", "win_rate", "estimated_pnl"], ascending=[False, False, False])

    df.to_csv(f"data/v3_hidden_wallets_{today}.csv", index=False)

    # Top tier: win_markets >= MIN_WINS+1, win_rate >= 0.65
    top = df[(df.win_markets >= MIN_WINS + 1) | (df.win_rate >= 0.65)].copy()
    top.to_csv(f"data/v3_top_wallets_{today}.csv", index=False)

    print(f"\n  Total wallets filtradas: {len(df)}")
    print(f"  Top tier (≥{MIN_WINS+1} wins o ≥65% WR): {len(top)}")


# ---------------- [4/4] PRINT ----------------
print("\n[4/4] Resultados:")
if not df.empty:
    cols = ["wallet", "win_markets", "lose_markets", "win_rate", "estimated_pnl", "avg_trade_size", "age_days", "sample_markets"]
    print(df[cols].head(20).to_string(index=False))
else:
    print("  Sin wallets que cumplan criterios.")

print("\nDone.")
