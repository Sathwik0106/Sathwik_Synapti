"""
Device Database Service (Supabase Integration)
==============================================
Provides persistent storage for registered mobile/BCI devices using Supabase PostgREST.
Guarantees device persistence across transmitter server restarts.
"""
import os
import logging
import requests
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://mjcxuapcoahckxyttebn.supabase.co").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY") or os.environ.get("SUPABASE_KEY") or ""

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=representation"
}

REQUEST_TIMEOUT = 5  # seconds


def get_all_devices() -> List[Dict]:
    """Fetch all registered devices from the Supabase devices table."""
    url = f"{SUPABASE_URL}/rest/v1/devices?select=device_id,username,device_type"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            devices = resp.json()
            logger.info(f"[SUPABASE] Successfully fetched {len(devices)} device(s) from database.")
            return devices
        else:
            logger.warning(f"[SUPABASE] Failed to fetch devices (HTTP {resp.status_code}): {resp.text}")
            return []
    except Exception as e:
        logger.error(f"[SUPABASE] Error connecting to database: {e}")
        return []


def upsert_device(device_id: str, username: str, device_type: str = "android") -> Optional[Dict]:
    """
    Insert or update a device record in Supabase using device_id as conflict target.
    If username changed for an existing device_id, it updates the record seamlessly.
    """
    if not device_id or not username:
        logger.warning("[SUPABASE] Invalid device_id or username for upsert.")
        return None

    url = f"{SUPABASE_URL}/rest/v1/devices?on_conflict=device_id"
    payload = [{
        "device_id": device_id.strip(),
        "username": username.strip().lower(),
        "device_type": (device_type or "android").strip().lower()
    }]

    try:
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (200, 201):
            records = resp.json()
            saved = records[0] if records else payload[0]
            logger.info(f"[SUPABASE] Upserted device '{username}' (ID={device_id[:8]}...) successfully.")
            return saved
        elif resp.status_code == 409:
            logger.error(f"[SUPABASE] Username conflict: '{username}' is already taken by another device: {resp.text}")
            return None
        else:
            logger.warning(f"[SUPABASE] Upsert failed (HTTP {resp.status_code}): {resp.text}")
            return None
    except Exception as e:
        logger.error(f"[SUPABASE] Upsert network error: {e}")
        return None


def delete_all_devices() -> bool:
    """Clear all registered devices from the Supabase devices table."""
    url = f"{SUPABASE_URL}/rest/v1/devices?device_id=neq."
    try:
        resp = requests.delete(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (200, 204):
            logger.info("[SUPABASE] Cleared all devices from database.")
            return True
        else:
            logger.warning(f"[SUPABASE] Delete failed (HTTP {resp.status_code}): {resp.text}")
            return False
    except Exception as e:
        logger.error(f"[SUPABASE] Delete network error: {e}")
        return False
