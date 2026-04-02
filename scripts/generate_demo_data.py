"""Generate realistic demo event data for testing the Insights UI."""

import json
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

NUM_USERS = 500
DAYS = 90
START = date(2025, 12, 1)
OUTPUT = Path(__file__).parent.parent / "demo_events.jsonl"

FEATURES = [
    "search", "dashboard", "export", "settings", "invite_teammate",
    "create_project", "upload_file", "api_call", "share_link",
    "onboarding_step_1", "onboarding_step_2", "onboarding_step_3",
    "notifications_enabled", "billing_page", "help_docs",
]

PLANS = ["free", "pro", "enterprise"]

def main():
    rows = []

    for uid in range(NUM_USERS):
        signup_day = random.randint(0, 30)
        signup_date = START + timedelta(days=signup_day)
        plan = random.choices(PLANS, weights=[60, 30, 10])[0]

        # Signup event
        rows.append({
            "user_id": f"user-{uid:04d}",
            "event_date": signup_date.isoformat(),
            "event_name": "signup",
        })

        # Onboarding (70% do step 1, 50% step 2, 30% step 3)
        for step, prob in [("onboarding_step_1", 0.70), ("onboarding_step_2", 0.50), ("onboarding_step_3", 0.30)]:
            if random.random() < prob:
                rows.append({
                    "user_id": f"user-{uid:04d}",
                    "event_date": (signup_date + timedelta(days=random.randint(0, 2))).isoformat(),
                    "event_name": step,
                })

        # Activation events — some users do "create_project" early
        did_create_project = random.random() < 0.35
        if did_create_project:
            rows.append({
                "user_id": f"user-{uid:04d}",
                "event_date": (signup_date + timedelta(days=random.randint(0, 5))).isoformat(),
                "event_name": "create_project",
            })

        did_invite = random.random() < 0.20
        if did_invite:
            rows.append({
                "user_id": f"user-{uid:04d}",
                "event_date": (signup_date + timedelta(days=random.randint(1, 7))).isoformat(),
                "event_name": "invite_teammate",
            })

        # Determine retention probability based on behavior
        base_retention = 0.15
        if did_create_project:
            base_retention += 0.25
        if did_invite:
            base_retention += 0.20
        if plan == "pro":
            base_retention += 0.10
        elif plan == "enterprise":
            base_retention += 0.15

        # Daily activity with decay
        for d in range(1, DAYS - signup_day):
            # Decay probability
            prob = base_retention * max(0.05, 1.0 - 0.015 * d)

            if random.random() < prob:
                event_date = signup_date + timedelta(days=d)

                # Pick 1-3 features per active day
                num_events = random.randint(1, 3)
                used_features = random.sample(
                    ["search", "dashboard", "export", "api_call", "share_link", "upload_file"],
                    min(num_events, 6),
                )
                for feat in used_features:
                    rows.append({
                        "user_id": f"user-{uid:04d}",
                        "event_date": event_date.isoformat(),
                        "event_name": feat,
                    })

                # Pageview always
                rows.append({
                    "user_id": f"user-{uid:04d}",
                    "event_date": event_date.isoformat(),
                    "event_name": "pageview",
                })

    # Add an anomaly — spike on day 45
    spike_date = START + timedelta(days=45)
    for i in range(80):
        rows.append({
            "user_id": f"spike-user-{i:03d}",
            "event_date": spike_date.isoformat(),
            "event_name": "signup",
        })
        rows.append({
            "user_id": f"spike-user-{i:03d}",
            "event_date": spike_date.isoformat(),
            "event_name": "pageview",
        })

    random.shuffle(rows)

    with open(OUTPUT, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    print(f"Generated {len(rows)} events for {NUM_USERS + 80} users → {OUTPUT}")
    print(f"Date range: {START} to {START + timedelta(days=DAYS)}")


if __name__ == "__main__":
    main()
