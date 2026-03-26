"""Config for cadence, confidence, review queue, and classification guardrails."""

from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class CadenceWindow:
    min_days: int
    max_days: int


CADENCE_WINDOWS = {
    "weekly": CadenceWindow(6, 8),
    "fortnightly": CadenceWindow(12, 16),
    "monthly": CadenceWindow(26, 35),
    "quarterly": CadenceWindow(80, 100),
    "yearly": CadenceWindow(330, 390),
}

ANNUAL_FACTORS = {
    "weekly": 52,
    "fortnightly": 26,
    "monthly": 12,
    "quarterly": 4,
    "yearly": 1,
}

PAYMENT_RAIL_PREFIXES = (
    "DD",
    "DC",
    "BP",
    "VT",
    "EP",
    "AP",
    "CQ",
    "EFTPOS",
    "PAYMENT",
    "ANZ ATM",
    "ATM",
    "VISA DEBIT PURCHASE",
    "VISA DEBIT",
    "BPAY",
    "INTERNET BANKING",
    "MOBILE BANKING",
    "NPP",
    "OSKO",
    "PAYID",
    "WISE",
)

TRANSFER_KEYWORDS = (
    "TRANSFER",
    "PAYMENT TO",
    "PAYMENT FROM",
    "OSKO",
    "NPP",
    "PAYID",
    "WISE",
    "BANKING PAYMENT",
)

REFUND_KEYWORDS = (
    "REFUND",
    "REVERSAL",
    "REVERSED",
    "CHARGEBACK",
    "RETURN",
)

PAYROLL_KEYWORDS = (
    "PAYROLL",
    "SALARY",
    "WAGES",
    "PAY RUN",
    "PAYMENT FROM EMPLOYER",
)

CASH_KEYWORDS = (
    "ATM",
    "CASH WITHDRAW",
    "CASH OUT",
)

DEBT_PAYMENT_KEYWORDS = (
    "CREDIT CARD PAYMENT",
    "LOAN PAYMENT",
    "MORTGAGE PAYMENT",
    "AFTERPAY",
    "ZIP PAY",
)

FEE_KEYWORDS = (
    "FEE",
    "CHARGE",
    "SURCHARGE",
)

PERSON_TRANSFER_HINTS = (
    "TO ",
    "FROM ",
)

# Shared terminal/noise indicators used by parser adapters.
TERMINAL_SECTION_PREFIXES = (
    "TOTAL FEES",
    "SUMMARY",
    "FEES SUMMARY",
    "END OF STATEMENT",
    "THIS INFORMATION HAS BEEN PREPARED",
    "TERMS AND CONDITIONS",
)

NOISE_SECTION_PREFIXES = (
    "IMPORTANT INFORMATION",
    "HOW TO CONTACT",
    "COMPLAINTS",
    "NEED TO GET IN TOUCH",
    "PLEASE RETAIN THIS STATEMENT",
    "WELCOME TO YOUR ANZ ACCOUNT AT A GLANCE",
    "ACCOUNT DETAILS",
    "PAGE ",
)

LEAKAGE_TOKENS = (
    "IMPORTANT INFORMATION",
    "HOW TO CONTACT",
    "COMPLAINTS",
    "TERMS AND CONDITIONS",
    "FEES SUMMARY",
    "PRIVACY",
    "PAGE ",
)

PROVISIONAL_DOCUMENT_MARKERS = (
    "PROVISIONAL LIST",
    "NOT A STATEMENT OF ACCOUNT",
    "MAY INCLUDE TRANSACTIONS",
    "MAY NOT INCLUDE ALL TRANSACTIONS",
)

LOW_CONFIDENCE_THRESHOLD = 0.55
AUTO_MEMORY_THRESHOLD = 0.86
BALANCED_REVIEW_CONFIDENCE_THRESHOLD = 0.6
BALANCED_REVIEW_CADENCE_THRESHOLD = 0.55

UNKNOWN_EXPENSE_CATEGORY = "Discretionary"
UNKNOWN_EXPENSE_SUBCATEGORY = "Presents"
CLASSIFICATION_VERSION = "budget_v2_accuracy_v1"

CANONICAL_EXPENSE_CATEGORIES = {
    "General / Home",
    "Motor Vehicle / Travel",
    "Insurance",
    "Entertainment",
    "Health & Fitness",
    "Financing Costs",
    "Discretionary",
}


def _contains_any(text: str, *tokens: str) -> bool:
    return any(token in text for token in tokens)


def canonicalize_expense_taxonomy(
    category: str | None,
    subcategory: str | None,
    descriptor: str | None = None,
) -> tuple[str, str]:
    resolved_category = (category or "").strip()
    resolved_subcategory = (subcategory or "").strip()
    descriptor_upper = re.sub(r"\s+", " ", (descriptor or "")).upper()

    if not resolved_category:
        return UNKNOWN_EXPENSE_CATEGORY, UNKNOWN_EXPENSE_SUBCATEGORY

    if resolved_category in CANONICAL_EXPENSE_CATEGORIES:
        return resolved_category, resolved_subcategory or UNKNOWN_EXPENSE_SUBCATEGORY

    if resolved_category == "Expenses":
        old_bucket = resolved_subcategory
    else:
        old_bucket = resolved_category if not resolved_subcategory else f"{resolved_category} / {resolved_subcategory}"

    old_bucket = old_bucket.strip()
    if old_bucket in {"Housing", "Expenses / Housing"}:
        return "General / Home", "Rent"
    if old_bucket in {"Groceries", "Expenses / Groceries"}:
        return "General / Home", "Grocery Shopping"
    if old_bucket in {"Utilities / Telecom", "Expenses / Utilities / Telecom"}:
        if _contains_any(descriptor_upper, "MOBILE", "PHONE", "SIM", "AMAYSIM", "VODAFONE", "OPTUS MOBILE", "TELSTRA MOBILE"):
            return "General / Home", "Mobile Phone"
        if _contains_any(descriptor_upper, "INTERNET", "BROADBAND", "NBN", "AUSSIE BROADBAND", "SUPERLOOP", "TPG"):
            return "General / Home", "Internet"
        if _contains_any(descriptor_upper, "WATER", "CITY WEST WATER", "YARRA VALLEY WATER", "SOUTH EAST WATER"):
            return "General / Home", "Water"
        return "General / Home", "Power"
    if old_bucket in {"Transport", "Expenses / Transport"}:
        if _contains_any(descriptor_upper, "PARK", "PARKING", "WILSON", "SECURE PARKING", "EASY PARK"):
            return "Motor Vehicle / Travel", "Parking"
        if _contains_any(descriptor_upper, "FUEL", "PETROL", "DIESEL", "AMPOL", "SHELL", "BP ", "CALTEX", "UNITED PETROLEUM"):
            return "Motor Vehicle / Travel", "Fuel"
        if _contains_any(descriptor_upper, "REGO", "REGISTRATION", "VICROADS", "VICTORIAROADS"):
            return "Motor Vehicle / Travel", "Registration"
        if _contains_any(descriptor_upper, "WOF", "ROADWORTHY", "INSPECTION"):
            return "Motor Vehicle / Travel", "Wof / Vehicle inspection costs"
        if _contains_any(descriptor_upper, "MECHANIC", "SERVICE", "TYRE", "REPAIR", "AUTO"):
            return "Motor Vehicle / Travel", "General Maintenance"
        return "Motor Vehicle / Travel", "Public Transport"
    if old_bucket in {"Health / Fitness", "Expenses / Health / Fitness"}:
        if _contains_any(descriptor_upper, "SUPPLEMENT", "VITAMIN", "PROTEIN", "CHEMIST", "PHARMACY"):
            return "Health & Fitness", "Supplements"
        return "Health & Fitness", "Gym membership"
    if old_bucket in {"Dining / Takeaways", "Expenses / Dining / Takeaways"}:
        return "Entertainment", "Eating out / takeaways"
    if old_bucket in {"Digital Services / Software", "Expenses / Digital Services / Software"}:
        if _contains_any(descriptor_upper, "OPENAI", "CHATGPT"):
            return "Discretionary", "Chatgpt"
        if _contains_any(descriptor_upper, "YOUTUBE", "GOOGLE YOUTUBE"):
            return "Discretionary", "youtube"
        return "Entertainment", "Movies / Activities"
    if old_bucket in {"Electronics / Shopping", "Expenses / Electronics / Shopping"}:
        if _contains_any(descriptor_upper, "UNIQLO", "H&M", "ZARA", "COTTON ON", "CLOTHING", "APPAREL"):
            return "Discretionary", "Clothing"
        return "Discretionary", UNKNOWN_EXPENSE_SUBCATEGORY
    if old_bucket in {"Fees / Charges", "Expenses / Fees / Charges"}:
        if _contains_any(descriptor_upper, "LOAN", "AFTERPAY", "ZIP", "INTEREST"):
            return "Financing Costs", "Student loan"
        return UNKNOWN_EXPENSE_CATEGORY, UNKNOWN_EXPENSE_SUBCATEGORY
    if old_bucket in {"Tax / Government", "Expenses / Tax / Government"}:
        if _contains_any(descriptor_upper, "LOAN", "HECS", "HELP", "STUDENT LOAN", "INLAND REVENUE"):
            return "Financing Costs", "Student loan"
        if _contains_any(descriptor_upper, "REGO", "REGISTRATION", "VICROADS", "VICTORIAROADS"):
            return "Motor Vehicle / Travel", "Registration"
        return UNKNOWN_EXPENSE_CATEGORY, UNKNOWN_EXPENSE_SUBCATEGORY
    if old_bucket in {"Unknown Merchant Spend", "Expenses / Unknown Merchant Spend"}:
        return UNKNOWN_EXPENSE_CATEGORY, UNKNOWN_EXPENSE_SUBCATEGORY

    return resolved_category, resolved_subcategory or UNKNOWN_EXPENSE_SUBCATEGORY

MERCHANT_FAMILY_RULES: tuple[dict[str, object], ...] = (
    {
        "category": "Financing Costs",
        "subcategory": "Student loan",
        "aliases": ("STUDENT LOAN", "HECS", "HELP DEBT", "LOAN PAYMENT", "LOAN REPAYMENT"),
        "confidence": 0.95,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
        "ancillary": False,
    },
    {
        "category": "Financing Costs",
        "subcategory": "Student loan",
        "aliases": ("AFTERPAY", "ZIP PAY", "ZIP MONEY", "KLARNA", "LATITUDE FINANCE"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
        "ancillary": False,
    },
    {
        "category": UNKNOWN_EXPENSE_CATEGORY,
        "subcategory": UNKNOWN_EXPENSE_SUBCATEGORY,
        "aliases": ("INTL TXN FEE", "INTERNATIONAL TRANSACTION FEE", "FOREIGN TRANSACTION FEE", "TXN FEE"),
        "confidence": 0.96,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
        "ancillary": True,
    },
    {
        "category": UNKNOWN_EXPENSE_CATEGORY,
        "subcategory": UNKNOWN_EXPENSE_SUBCATEGORY,
        "aliases": ("ACCOUNT FEE", "BANK FEE", "CARD FEE", "SERVICE FEE"),
        "confidence": 0.9,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
        "ancillary": True,
    },
    {
        "category": "General / Home",
        "subcategory": "Grocery Shopping",
        "aliases": ("WOOLWORTHS", "ALDI", "COLES", "WW METRO", "WOOLIES"),
        "confidence": 0.92,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Motor Vehicle / Travel",
        "subcategory": "Public Transport",
        "aliases": ("MYKI", "UBER", "DIDI", "DIDI MOBILITY", "PTV"),
        "confidence": 0.9,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Health & Fitness",
        "subcategory": "Gym membership",
        "aliases": ("GOODLIFE", "FITNESS FIRST", "FITNESS", "GYM", "PILATES"),
        "confidence": 0.88,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Health & Fitness",
        "subcategory": "Supplements",
        "aliases": ("SUPPLEMENT", "PROTEIN", "VITAMIN", "CHEMIST", "PHARMACY"),
        "confidence": 0.84,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "General / Home",
        "subcategory": "Power",
        "aliases": ("ORIGIN", "AGL", "ENERGYAU", "RED ENERGY", "POWERSHOP"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "General / Home",
        "subcategory": "Mobile Phone",
        "aliases": ("AMAYSIM", "VODAFONE", "OPTUS MOBILE", "TELSTRA MOBILE"),
        "confidence": 0.91,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "General / Home",
        "subcategory": "Internet",
        "aliases": ("AUSSIE BROADBAND", "SUPERLOOP", "TELSTRA", "OPTUS", "TPG", "NBN"),
        "confidence": 0.91,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "General / Home",
        "subcategory": "Water",
        "aliases": ("CITY WEST WATER", "SOUTH EAST WATER", "YARRA VALLEY WATER", "WATER"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Discretionary",
        "subcategory": "Chatgpt",
        "aliases": ("OPENAI", "CHATGPT"),
        "confidence": 0.94,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Discretionary",
        "subcategory": "youtube",
        "aliases": ("YOUTUBE PREMIUM", "GOOGLE YOUTUBE", "YOUTUBE"),
        "confidence": 0.93,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Entertainment",
        "subcategory": "Movies / Activities",
        "aliases": ("NETFLIX", "SPOTIFY", "DISNEY", "AMAZON PRIME", "PRIME VIDEO", "CLOUDFLARE"),
        "confidence": 0.88,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Entertainment",
        "subcategory": "Eating out / takeaways",
        "aliases": (
            "MCDONALDS",
            "CAFE",
            "RESTAURANT",
            "THAI",
            "GRILL",
            "TREAT OF FRANCE",
            "PIZZA",
            "BURGER",
            "KFC",
        ),
        "confidence": 0.82,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Discretionary",
        "subcategory": UNKNOWN_EXPENSE_SUBCATEGORY,
        "aliases": ("JB HI FI", "JB HIFI", "OFFICEWORKS", "APPLE", "HARVEY NORMAN"),
        "confidence": 0.88,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Motor Vehicle / Travel",
        "subcategory": "Fuel",
        "aliases": ("AMPOL", "SHELL", "BP", "CALTEX", "7-ELEVEN FUEL", "UNITED PETROLEUM"),
        "confidence": 0.9,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Motor Vehicle / Travel",
        "subcategory": "Parking",
        "aliases": ("PARKING", "WILSON PARKING", "SECURE PARKING", "EASY PARK"),
        "confidence": 0.88,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Motor Vehicle / Travel",
        "subcategory": "Registration",
        "aliases": ("VICTORIAROADS", "VICROADS", "REGISTRATION", "REGO"),
        "confidence": 0.91,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Motor Vehicle / Travel",
        "subcategory": "Wof / Vehicle inspection costs",
        "aliases": ("INSPECTION", "ROADWORTHY", "WOF"),
        "confidence": 0.87,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Motor Vehicle / Travel",
        "subcategory": "General Maintenance",
        "aliases": ("MECHANIC", "SERVICE CENTRE", "TYRE", "AUTO SERVICE", "REPAIRS"),
        "confidence": 0.86,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Insurance",
        "subcategory": "Health Insurance",
        "aliases": ("BUPA", "MEDIBANK", "HCF", "NIB"),
        "confidence": 0.92,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Insurance",
        "subcategory": "Home and Contents",
        "aliases": ("AAMI HOME", "NRMA HOME", "ALLIANZ HOME", "CONTENTS INSURANCE"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Insurance",
        "subcategory": "Life Insurance",
        "aliases": ("LIFE INSURANCE", "TAL LIFE", "AIA LIFE"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Insurance",
        "subcategory": "Motor Vehicle Insurance",
        "aliases": ("AAMI", "NRMA", "ALLIANZ", "BUDGET DIRECT", "CAR INSURANCE"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Insurance",
        "subcategory": "Income Protection Insurance",
        "aliases": ("INCOME PROTECTION",),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "General / Home",
        "subcategory": "Rent",
        "aliases": ("RENT", "PROPERTY", "TRUST", "UPM TRUST"),
        "confidence": 0.72,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
    {
        "category": "Discretionary",
        "subcategory": "Charity",
        "aliases": ("CHARITY", "DONATION", "GOFUNDME", "RED CROSS"),
        "confidence": 0.88,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Discretionary",
        "subcategory": "Animals",
        "aliases": ("PET", "VET", "ANIMAL"),
        "confidence": 0.87,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Discretionary",
        "subcategory": "Clothing",
        "aliases": ("UNIQLO", "H&M", "ZARA", "COTTON ON", "CLOTHING", "APPAREL"),
        "confidence": 0.87,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Discretionary",
        "subcategory": UNKNOWN_EXPENSE_SUBCATEGORY,
        "aliases": ("GIFT", "PRESENT", "FLOWERS"),
        "confidence": 0.84,
        "bucket_lean": "discretionary",
        "baseline_eligible": False,
    },
    {
        "category": "Entertainment",
        "subcategory": "Movies / Activities",
        "aliases": ("EVENT", "CINEMA", "MOVIE", "TICKET", "ACTIVITY"),
        "confidence": 0.9,
        "bucket_lean": "baseline",
        "baseline_eligible": True,
    },
)

SAFE_BROAD_MERCHANT_BUCKETS: tuple[dict[str, object], ...] = (
    {"category": "General / Home", "subcategory": "Grocery Shopping", "tokens": ("GROCERY", "SUPERMARKET")},
    {"category": "General / Home", "subcategory": "Mobile Phone", "tokens": ("MOBILE", "PHONE", "TELCO", "SIM")},
    {"category": "General / Home", "subcategory": "Internet", "tokens": ("INTERNET", "BROADBAND", "NBN")},
    {"category": "General / Home", "subcategory": "Power", "tokens": ("POWER", "ELECTRIC", "ENERGY", "GAS")},
    {"category": "General / Home", "subcategory": "Water", "tokens": ("WATER",)},
    {"category": "Motor Vehicle / Travel", "subcategory": "Fuel", "tokens": ("FUEL", "PETROL", "DIESEL")},
    {"category": "Motor Vehicle / Travel", "subcategory": "Public Transport", "tokens": ("TRANSPORT", "TRANSIT", "TRAM", "TRAIN", "BUS", "TAXI")},
    {"category": "Motor Vehicle / Travel", "subcategory": "Parking", "tokens": ("PARK", "PARKING")},
    {"category": "Entertainment", "subcategory": "Eating out / takeaways", "tokens": ("FOOD", "CAFE", "DINING", "RESTAURANT", "TAKEAWAY")},
    {"category": "Entertainment", "subcategory": "Movies / Activities", "tokens": ("MOVIE", "CINEMA", "TICKET", "ACTIVITY", "EVENT")},
    {"category": "Health & Fitness", "subcategory": "Gym membership", "tokens": ("GYM", "FITNESS", "PILATES")},
    {"category": "Health & Fitness", "subcategory": "Supplements", "tokens": ("SUPPLEMENT", "VITAMIN", "PROTEIN")},
    {"category": "Discretionary", "subcategory": "Clothing", "tokens": ("CLOTHING", "APPAREL", "FASHION")},
    {"category": "Discretionary", "subcategory": "Charity", "tokens": ("DONATION", "CHARITY")},
    {"category": "Discretionary", "subcategory": "Animals", "tokens": ("PET", "VET", "ANIMAL")},
    {"category": "Discretionary", "subcategory": "youtube", "tokens": ("YOUTUBE",)},
    {"category": "Discretionary", "subcategory": "Chatgpt", "tokens": ("CHATGPT", "OPENAI")},
    {"category": "Financing Costs", "subcategory": "Student loan", "tokens": ("LOAN", "HECS", "HELP", "AFTERPAY", "ZIP")},
    {"category": UNKNOWN_EXPENSE_CATEGORY, "subcategory": UNKNOWN_EXPENSE_SUBCATEGORY, "tokens": ("SHOP", "STORE", "MARKET", "RETAIL", "PURCHASE")},
)
