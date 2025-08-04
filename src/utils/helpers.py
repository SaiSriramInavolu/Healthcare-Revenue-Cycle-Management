import re
import hashlib
from datetime import datetime
import pandas as pd

def standardize_phone(phone):
    if not isinstance(phone, str):
        return None
    digits = re.sub(r'\D', '', phone)
    return f"+91-{digits[-10:]}" if len(digits) >= 10 else None

def validate_email(email):
    if not isinstance(email, str):
        return False
    return re.match(r"[^@]+@[^@]+\.[^@]+", email) is not None

def generate_unified_id(first_name, last_name, dob):
    try:
        dob_str = pd.to_datetime(dob).strftime('%Y%m%d') if dob else ''
        key = f"{first_name.strip().lower()}_{last_name.strip().lower()}_{dob_str}"
        return hashlib.md5(key.encode()).hexdigest()
    except Exception as e:
        return None

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None
