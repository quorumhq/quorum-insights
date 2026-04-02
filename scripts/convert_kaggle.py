"""Convert REES46 eCommerce CSV to Insights JSONL format.

Maps:
  event_type (view/cart/purchase/remove_from_cart) → event_name
  category_code (electronics.smartphone) → event_name for feature analysis
  user_id → user_id
  event_time → event_date
"""

import csv
import json
import sys
from pathlib import Path

INPUT = Path(__file__).parent.parent / "data" / "sample_nov.csv"
OUTPUT = Path(__file__).parent.parent / "demo_events.jsonl"


def main():
    count = 0
    users = set()

    with open(INPUT, "r") as fin, open(OUTPUT, "w") as fout:
        reader = csv.DictReader(fin)
        for row in reader:
            user_id = row["user_id"]
            event_time = row["event_time"]  # "2019-11-01 00:00:00 UTC"
            event_type = row["event_type"]  # view, cart, purchase, remove_from_cart
            category = row.get("category_code", "") or ""
            brand = row.get("brand", "") or ""

            # Extract date
            event_date = event_time[:10]  # "2019-11-01"

            # Base event (the raw event type)
            fout.write(json.dumps({
                "user_id": user_id,
                "event_date": event_date,
                "event_name": event_type,
            }) + "\n")
            count += 1

            # Feature-level event from category (for feature correlation analysis)
            # e.g. "electronics.smartphone" → "browse_electronics.smartphone"
            if category and event_type == "view":
                # Use top-level category as feature
                top_category = category.split(".")[0]
                fout.write(json.dumps({
                    "user_id": user_id,
                    "event_date": event_date,
                    "event_name": f"browse_{top_category}",
                }) + "\n")
                count += 1

            users.add(user_id)

    print(f"Converted {count} events, {len(users)} unique users → {OUTPUT}")


if __name__ == "__main__":
    main()
