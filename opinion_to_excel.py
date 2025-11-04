
#!/usr/bin/env python3
# opinion_to_excel.py
import os
import time
import argparse
from datetime import datetime
import pandas as pd

try:
    from opinion_clob_sdk import Client
    from opinion_clob_sdk.model import TopicType, TopicStatusFilter
except Exception as e:
    raise SystemExit("Install SDK first: pip install opinion_clob_sdk") from e

def epoch_to_iso(ts):
    if not ts: return None
    try: return datetime.utcfromtimestamp(int(ts)).isoformat() + "Z"
    except: return str(ts)

def collect_markets(client, status=None):
    page, limit = 1, 20
    all_markets = []
    while True:
        resp = client.get_markets(topic_type=None, page=page, limit=limit, status=status)
        if getattr(resp, "errno", None) != 0:
            raise RuntimeError(f"API error: {getattr(resp, 'errmsg', 'unknown')}")
        items = resp.result.list if resp.result and getattr(resp.result, "list", None) is not None else []
        if not items: break
        for m in items:
            rec = {
                "marketId": getattr(m, "marketId", None),
                "marketTitle": getattr(m, "marketTitle", None),
                "status": getattr(m, "status", None),
                "marketType": getattr(m, "marketType", None),
                "conditionId": getattr(m, "conditionId", None),
                "quoteToken": getattr(m, "quoteToken", None),
                "chainId": getattr(m, "chainId", None),
                "volume": getattr(m, "volume", None),
                "yesTokenId": getattr(m, "yesTokenId", None),
                "noTokenId": getattr(m, "noTokenId", None),
                "yesLabel": getattr(m, "yesLabel", None),
                "noLabel": getattr(m, "noLabel", None),
                "rules": getattr(m, "rules", None),
                "cutoffAt": epoch_to_iso(getattr(m, "cutoffAt", None)),
                "resolvedAt": epoch_to_iso(getattr(m, "resolvedAt", None)),
            }
            all_markets.append(rec)
        if len(items) < limit: break
        page += 1
        time.sleep(0.15)
    return all_markets

def attach_prices(client, markets):
    for rec in markets:
        rec["latest_price_yes"] = None
        rec["latest_price_no"] = None
        rec["price_timestamp"] = None
        for side in ("yesTokenId", "noTokenId"):
            token_id = rec.get(side)
            if token_id:
                try:
                    pr = client.get_latest_price(token_id=token_id)
                    if getattr(pr, "errno", None) == 0 and getattr(pr, "result", None):
                        data = pr.result.data
                        price = getattr(data, "price", None) or (data.get("price") if isinstance(data, dict) else None)
                        ts = getattr(data, "timestamp", None) or (data.get("timestamp") if isinstance(data, dict) else None)
                        if side == "yesTokenId":
                            rec["latest_price_yes"] = price
                        else:
                            rec["latest_price_no"] = price
                        if ts: rec["price_timestamp"] = epoch_to_iso(ts)
                except: pass
    return markets

def main():
    parser = argparse.ArgumentParser(description="Opinion.Trade markets to Excel")
    parser.add_argument("--host", default=os.getenv("OPINION_HOST", "https://proxy.opinion.trade:8443"))
    parser.add_argument("--apikey", default=os.getenv("OPINION_API_KEY"))
    parser.add_argument("--only-active", action="store_true")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    client = Client(host=args.host, apikey=args.apikey)

    status = TopicStatusFilter.ACTIVATED if args.only_active else None

    print("Fetching markets...")
    markets = collect_markets(client, status=status)
    print(f"Found {len(markets)} markets")

    print("Fetching prices...")
    markets = attach_prices(client, markets)

    df = pd.DataFrame(markets)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out = args.output or f"opinion_markets_{ts}.xlsx"
    df.to_excel(out, index=False)
    print(f"Saved: {out}")

if __name__ == "__main__":
    main()
