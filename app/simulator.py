import asyncio, random, uuid
from datetime import datetime, timezone, timedelta
from typing import AsyncIterator, Dict
import os  # ✅ NEW

# --- Use-cases (your list) ---
TX_TYPES = ["P2P_SEND", "AIRTIME_RECHARGE", "DSTV_PAYMENT", "CASHOUT", "CASHIN", "MERCHPAY"]
TX_STATUS = ["approved", "declined", "reversed", "chargeback"]

# --- Show 5 risk grades (instead of A/B/C/D) ---
GRADES = ["Low Risk", "Medium-Low Risk", "Medium Risk", "Medium-High Risk", "High Risk"]
GRADE_WEIGHTS = [0.40, 0.25, 0.20, 0.10, 0.05]

# --- Single currency ---
CURRENCIES = ["USD"]

COUNTRIES = ["ZW", "ZA", "NG", "KE", "AE", "GB", "US"]
CHANNELS = ["card_present", "card_not_present"]

# Your account prefixes and format xxxx-xxxx
PREFIXES = ["71", "78", "772", "771", "773", "775", "776", "777", "778"]

# Proper merchant + MCC per use-case
USECASE_META = {
    "P2P_SEND":        {"merchant": "P2P Wallet",        "mcc": "4829"},
    "AIRTIME_RECHARGE":{"merchant": "Airtime Vendor",    "mcc": "4814"},
    "DSTV_PAYMENT":    {"merchant": "DsTV",              "mcc": "4899"},
    "CASHOUT":         {"merchant": "Agent Cashout",     "mcc": "6011"},
    "CASHIN":          {"merchant": "Agent Cashin",      "mcc": "6012"},
    "MERCHPAY":        {"merchant": "Merchant Payments", "mcc": "5411"},
}

def rand_ip():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def rand_device():
    return f"D{random.randint(10000, 99999)}"

def make_account_id() -> str:
    """
    8 digits total, starting with one of:
    71, 78, 772, 771, 773, 775, 776, 777, 778
    Format: xxxx-xxxx
    """
    p = random.choice(PREFIXES)
    remaining = 8 - len(p)
    tail = "".join(str(random.randint(0, 9)) for _ in range(max(0, remaining)))
    digits = (p + tail)[:8]
    return f"{digits[:4]}-{digits[4:]}"

def weighted_status():
    r = random.random()
    if r < 0.88:
        return "approved"
    if r < 0.94:
        return "declined"
    if r < 0.985:
        return "reversed"
    return "chargeback"

def amount_by_type(tx_type: str):
    if tx_type == "AIRTIME_RECHARGE":
        return random.uniform(0.5, 20)
    if tx_type == "DSTV_PAYMENT":
        return random.uniform(10, 80)
    if tx_type == "CASHIN":
        return random.uniform(5, 200)
    if tx_type == "CASHOUT":
        return random.uniform(10, 500)
    if tx_type == "P2P_SEND":
        return random.uniform(1, 800)
    if tx_type == "MERCHPAY":
        return random.uniform(1, 300)
    return random.uniform(1, 200)

def new_txn_id() -> str:
    # avoids UNIQUE constraint collisions during seeding
    return "T" + uuid.uuid4().hex[:10]

async def stream_transactions(tps: float = 2.0) -> AsyncIterator[Dict]:
    min_sleep = max(0.01, 1.0 / max(0.1, tps))

    # pool of accounts to form "history"
    accounts = []
    for _ in range(80):
        accounts.append({
            "account_id": make_account_id(),
            "home_country": random.choice(["ZW", "ZA", "AE", "US"]),
            "grade": random.choices(GRADES, weights=GRADE_WEIGHTS, k=1)[0]
        })

    while True:
        a = random.choice(accounts)

        account_id = a["account_id"]
        home = a["home_country"]
        grade = a["grade"]

        tx_type = random.choice(TX_TYPES)
        tx_status = weighted_status()

        meta = USECASE_META[tx_type]
        merchant, mcc = meta["merchant"], meta["mcc"]
        channel = random.choice(CHANNELS)

        # 88% match home country, 12% mismatch
        country = home if random.random() < 0.88 else random.choice([c for c in COUNTRIES if c != home])

        # device/ip reuse patterns
        device_id = rand_device() if random.random() < 0.25 else f"D{account_id.replace('-', '')[:4]}000"
        ip = rand_ip() if random.random() < 0.30 else f"10.0.{random.randint(1, 200)}.{random.randint(2, 254)}"

        amount = round(amount_by_type(tx_type), 2)
        currency = "USD"

        txn = {
            "txn_id": new_txn_id(),
            "ts": datetime.now(timezone.utc),

            "account_id": account_id,
            "customer_grade": grade,
            "device_id": device_id,
            "ip_address": ip,

            "merchant": merchant,
            "mcc": mcc,
            "amount": float(amount),
            "currency": currency,
            "country": country,

            "channel": channel,
            "transaction_type": tx_type,
            "transaction_status": tx_status,

            "home_country": home,
        }

        yield txn
        await asyncio.sleep(min_sleep)

async def seed_historical_transactions(insert_txn_fn, days: int = 14, target_total: int = 12000, flag_path: str = "seeded.flag"):
    """
    Seeds ~14 days of retrospective data, distributed across days and hours.
    insert_txn_fn is usually app.repo.insert_txn

    ✅ NEW: uses a flag file so it only seeds once per volume.
    """
    # ✅ NEW: only seed once (per persistent volume)
    try:
        if os.path.exists(flag_path):
            print(f"[seed] skip: already seeded (found {flag_path})")
            return
    except Exception:
        # if filesystem is weird, just continue seeding (better than crashing)
        pass

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    # create stable pool
    accounts = []
    for _ in range(120):
        accounts.append({
            "account_id": make_account_id(),
            "home_country": random.choice(["ZW", "ZA", "AE", "US"]),
            "grade": random.choices(GRADES, weights=GRADE_WEIGHTS, k=1)[0]
        })

    for i in range(target_total):
        a = random.choice(accounts)
        tx_type = random.choice(TX_TYPES)
        meta = USECASE_META[tx_type]

        # spread timestamps across the window
        ts = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

        home = a["home_country"]
        country = home if random.random() < 0.88 else random.choice([c for c in COUNTRIES if c != home])

        txn = {
            "txn_id": new_txn_id(),
            "ts": ts,

            "account_id": a["account_id"],
            "customer_grade": a["grade"],
            "device_id": rand_device(),
            "ip_address": rand_ip(),

            "merchant": meta["merchant"],
            "mcc": meta["mcc"],
            "amount": float(round(amount_by_type(tx_type), 2)),
            "currency": "USD",
            "country": country,

            "channel": random.choice(CHANNELS),
            "transaction_type": tx_type,
            "transaction_status": weighted_status(),
            "home_country": home,
        }

        await insert_txn_fn(txn)

        # optional progress log every 1k
        if (i + 1) % 1000 == 0:
            print(f"[seed] inserted {i+1}/{target_total}")

    # ✅ NEW: write flag file after successful seed
    try:
        with open(flag_path, "w", encoding="utf-8") as f:
            f.write(datetime.now(timezone.utc).isoformat())
        print(f"[seed] done, wrote {flag_path}")
    except Exception as e:
        print("[seed] done but could not write flag:", repr(e))
