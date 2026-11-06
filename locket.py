#!/usr/bin/env python3
"""
Locket Web — All-in-one Flask app on Locket's official API (api.locketcamera.com,
Firebase Auth, Firestore) sent with iOS-style headers. The binhake action-API
(https://locket.binhake.dev/server/) is only a last-resort fallback.

Server-side console logs every Firebase/Locket call. The full friend feed is
walked page by page, cached, and refreshed through /api/moments/poll. Token
auto-refresh + Remember me (30-day permanent session).

Requires: pip install flask requests pillow websocket-client
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import threading
import os
import sys
import re
import base64
import tempfile
import time
import uuid
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from PIL import Image, ImageOps
from flask import Flask, request, session, jsonify, render_template_string

try:
    import websocket  # websocket-client
except ImportError:
    websocket = None

# ============================================================
# Console logger (server-side, colored) — separate from the
# per-request debug records that get shipped to the browser.
# ============================================================

class ColorFormatter(logging.Formatter):
    COLORS = {"DEBUG": "\033[36m", "INFO": "\033[32m", "WARNING": "\033[33m",
              "ERROR": "\033[31m", "CRITICAL": "\033[35m"}
    RESET = "\033[0m"; DIM = "\033[2m"; BOLD = "\033[1m"

    def format(self, record):
        c = self.COLORS.get(record.levelname, "")
        ts = self.formatTime(record, "%H:%M:%S")
        msg = record.getMessage()
        lines = msg.split("\n")
        out = f"{self.DIM}{ts}{self.RESET} {c}{record.levelname:8}{self.RESET} {self.BOLD}{lines[0]}{self.RESET}"
        for line in lines[1:]:
            out += f"\n{' '*20}{self.DIM}{line}{self.RESET}"
        return out


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColorFormatter())
logger = logging.getLogger("LocketWeb")
VERBOSE = os.environ.get("LOCKET_VERBOSE", "0") == "1"
logger.setLevel(logging.DEBUG if VERBOSE else logging.INFO)
logger.handlers = []
logger.addHandler(handler)


def _snip(text: Optional[str], n: int = 900) -> str:
    if text is None:
        return ""
    return text if len(text) <= n else text[:n] + f"…(+{len(text)-n}b)"


def console_log(label: str, method: str, url: str, payload=None, status=None, resp_text=None, duration=None):
    if not VERBOSE:
        # One compact line, no json.dumps / banner building on the hot path.
        logger.info("%s %s -> %s %s", label, method, status if status is not None else "-",
                    f"{duration:.2f}s" if duration is not None else "")
        return
    lines = [f"{'='*60}", f"  {label}", f"  {method} {url}"]
    if payload is not None:
        try:
            p = json.dumps(payload, ensure_ascii=False, indent=2)
        except Exception:
            p = str(payload)
        lines.append(f"  >>> PAYLOAD:\n    {_snip(p).replace(chr(10), chr(10)+'    ')}")
    if status is not None:
        color = "\033[32m" if status < 300 else "\033[33m" if status < 400 else "\033[31m"
        lines.append(f"  <<< STATUS: {color}{status}\033[0m")
    if resp_text is not None:
        lines.append(f"  <<< BODY: {_snip(resp_text, 600)}")
    if duration is not None:
        lines.append(f"  <<< TIME: {duration:.3f}s")
    lines.append("=" * 60)
    logger.info("\n".join(lines))


def make_debug(kind: str, label: str, method: str, url: str, payload=None,
                status=None, resp_text=None, duration=None, error=None) -> Dict[str, Any]:
    """One entry for the browser-side debug console."""
    return {
        "t": time.time(),
        "kind": kind,           # "send" | "recv" | "error"
        "label": label,
        "method": method,
        "url": url,
        "payload": payload if payload is None else json.loads(json.dumps(payload, default=str, ensure_ascii=False)),
        "status": status,
        "body": _snip(resp_text, 1500) if resp_text else None,
        "duration": round(duration, 3) if duration is not None else None,
        "error": str(error) if error else None,
    }


# ============================================================
# Config
# ============================================================

FIREBASE_API_KEY = "AIzaSyCQngaaXQIfJaH0aS2l7REgIjD7nL431So"
LOGIN_URL = f"https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key={FIREBASE_API_KEY}"
ACCOUNT_URL = f"https://www.googleapis.com/identitytoolkit/v3/relyingparty/getAccountInfo?key={FIREBASE_API_KEY}"
REFRESH_URL = f"https://securetoken.googleapis.com/v1/token?key={FIREBASE_API_KEY}"

BINHAKE_API = "https://locket.binhake.dev/server/"
WS_URL = "wss://locket.binhake.dev/server/"

# Official Locket API (fallback when binhake uploadMedia is down).
# Captured via HTTP Toolkit from Android app 1.237.0 (2026-08-26):
#   1) resumable upload → firebasestorage bucket locket-img
#   2) POST https://api.locketcamera.com/postMomentV2
LOCKET_API = "https://api.locketcamera.com"
STORAGE_BUCKET = "locket-img"
FIREBASE_STORAGE_BASE = f"https://firebasestorage.googleapis.com/v0/b/{STORAGE_BUCKET}/o"


# Moments live-cache (per local_id). Avoids re-fetching the full history every tab open.
_MOMENTS_LOCK = threading.Lock()
_MOMENTS_CACHE: Dict[str, Dict[str, Any]] = {}
# token refresh tracking
_TOKEN_LOCK = threading.Lock()

_UPLOAD_DEDUPE_LOCK = threading.Lock()
_UPLOAD_DEDUPE: Dict[str, Any] = {}
_UPLOAD_DEDUPE_TTL_SEC = 600

def _dedupe_prune(now: float) -> None:
    stale = [k for k, v in _UPLOAD_DEDUPE.items() if now - v["at"] > _UPLOAD_DEDUPE_TTL_SEC]
    for k in stale:
        _UPLOAD_DEDUPE.pop(k, None)

IOS_UA = "com.locket.Locket/1.100.0 iPhone/18.2 hw/iPhone14_3"
FIREBASE_GMPID = "1:641029076083:ios:cc8eb46290d69b234fa606"
IOS_BUNDLE = "com.locket.Locket"
LOCKET_IOS_APP_VERSION = "3.0"
LOCKET_IOS_BUILD = "2523"
LOCKET_IOS_UA = f"{IOS_BUNDLE}/{LOCKET_IOS_APP_VERSION} iPhone/18.1.0 hw/iPhone16,1"
FEED_MAX_ITEMS = 1500
FEED_PER_FRIEND = 20
FEED_WINDOW_START = 60
FEED_WINDOW_STEP = 60
FEED_DEEPEN_BUDGET_SEC = 9.0
FEED_MAX_ROUNDS = 25
FEED_CRAWL_WORKERS = 12
FEED_CRAWL_BUDGET_SEC = 12.0
FEED_RECRAWL_SEC = 600
FEED_PRECHECK_MIN_SEC = 20
FEED_UIDS_TTL_SEC = 600
FEED_HISTORY_PER_USER = 20
FEED_FIRESTORE_HISTORY = os.environ.get("LOCKET_FIRESTORE_HISTORY", "1") != "0"
FEED_HISTORY_DIR = os.environ.get("LOCKET_HISTORY_DIR", "").strip() or tempfile.gettempdir()
FIREBASE_CLIENT_HEADER = "H4sIAAAAAAAAAKtWykhNLCpJSk0sKVayio7VUSpLLSrOzM9TslIyUqoFAFyivEQfAAAA"
LOCKET_READ_RETRIES = 2
LOCKET_APPCHECK_TOKEN = os.environ.get("LOCKET_APPCHECK_TOKEN", "").strip()
LOCKET_INSTANCE_ID_TOKEN = os.environ.get("LOCKET_INSTANCE_ID_TOKEN", "").strip()
LOCKET_INSTALLATION_ID = os.environ.get("LOCKET_INSTALLATION_ID", "").strip()
FEED_USE_HISTORY = os.environ.get("LOCKET_FEED_HISTORY", "1") != "0"
FEED_HISTORY_DB = os.environ.get("LOCKET_FIRESTORE_DB", "locket").strip() or "locket"
FEED_HISTORY_PAGE = 60
FEED_HISTORY_PAGE_MORE = 100
FEED_HISTORY_RETRY_SEC = 600
FIREBASE_AUTH_UA = "FirebaseAuth.iOS/10.23.1 com.locket.Locket/1.82.0 iPhone/18.0 hw/iPhone12_1"
APPCHECK_PROVIDER_URL = os.environ.get("LOCKET_APPCHECK_PROVIDER_URL", "").strip()
APPCHECK_PROVIDER_KEY = os.environ.get("LOCKET_APPCHECK_PROVIDER_KEY", "").strip()
APPCHECK_PROVIDER_APP_VERSION = os.environ.get("LOCKET_APPCHECK_PROVIDER_APP_VERSION", "2.5.2").strip()
APPCHECK_MIN_LEFT_SEC = 180
APPCHECK_RETRY_SEC = 30
FEED_DELETED_LIMIT = 60

WEB_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
]

GOLD_BADGE = "https://locket.binhake.dev/assets/images/locket_gold_badge_small_Normal@2x.png"
CELEB_BADGE = "https://locket.binhake.dev/assets/images/celebrity_badge_small_Normal@2x.png"
FONT_URL = "https://raw.githubusercontent.com/itskevinz/assets/main/proxima_soft_bold.otf"
FAVICON_URL = "https://locket.binhake.dev/assets/images/app_icon/app_icon_preview_Normal@2x.png"

# Engine name shown in Settings — "Lumen" for the light/glow theme this app already
# leans on (gold badge, glow shadows, moments = little bursts of light).
APP_CODENAME = "Lumen"
APP_VERSION = "2.9"
APP_BUILD = "2026.11.04"
APP_VERSION_STRING = f"{APP_CODENAME} {APP_VERSION} · build {APP_BUILD}"


class LocketError(RuntimeError):
    pass


@dataclass
class LocketSession:
    id_token: str
    refresh_token: Optional[str]
    local_id: str
    email: str
    display_name: str
    photo_url: str
    ua: str
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)


# ============================================================
# binhake_core — auth (Firebase, unchanged) + every other call
# through https://locket.binhake.dev/server/ (action-based POST,
# cookie-authenticated with user_id, plus a WebSocket for moments).
# ============================================================

def _fb_headers() -> Dict[str, str]:
    return {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "User-Agent": IOS_UA,
        "X-Client-Version": "iOS/FirebaseSDK/10.23.1/FirebaseCore-iOS",
        "X-Firebase-GMPID": FIREBASE_GMPID,
        "X-Ios-Bundle-Identifier": IOS_BUNDLE,
    }


def _web_headers(id_token: str, ua: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {id_token}",
        "Content-Type": "application/json",
        "Origin": "https://locket.binhake.dev",
        "Referer": "https://locket.binhake.dev/posts.html",
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.7",
        "X-Client-OS": "Windows",
        "Sec-Ch-Ua": '"Not=A?Brand";v="99", "Chromium";v="151"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Gpc": "1",
        "Dnt": "1",
    }


def _make_http() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=20, pool_maxsize=40,
        # GET only — retrying POST automatically risks double-submitting a non-idempotent
        # action (e.g. posting the same moment twice) if the first attempt actually landed
        # server-side but the response got lost. Reads are safe to retry, writes aren't.
        max_retries=Retry(total=2, backoff_factor=0.6,
                           status_forcelist=[500, 502, 503, 504],
                           allowed_methods=["GET"]),
    )
    s.mount("https://", adapter)
    return s


# Shared retrying session for read-only upstream calls (currently: image proxy). A single
# transient 502/503/504 from Firebase/Google Storage used to fail the request outright and
# surface as our own 502 all the way to the browser — this was previously built but never
# actually wired up anywhere, so every hiccup was a hard failure with no retry.
_http = _make_http()


class LoginError(RuntimeError):
    """Carries whatever debug entries were captured before the failure, so
    the browser's debug console still shows the real request/response even
    when login fails (bad password, network down, Firebase change, etc)."""
    def __init__(self, message: str, debug: List[Dict[str, Any]], friendly: Optional[str] = None):
        super().__init__(message)
        self.debug = debug
        self.friendly = friendly or message


def login(email: str, password: str, timeout: int = 30) -> Tuple[LocketSession, List[Dict[str, Any]]]:
    """Firebase login — the one call that stays on Locket's own API, because
    binhake authenticates the exact same way under the hood."""
    debug: List[Dict[str, Any]] = []
    payload = {"email": email.strip(), "password": password,
               "clientType": "CLIENT_TYPE_IOS", "returnSecureToken": True}
    debug.append(make_debug("send", "FIREBASE LOGIN", "POST", LOGIN_URL, {"email": email, "password": "***"}))

    t0 = time.time()
    try:
        r = requests.post(LOGIN_URL, json=payload, headers=_fb_headers(), timeout=timeout)
    except requests.exceptions.RequestException as e:
        dt = time.time() - t0
        console_log("FIREBASE LOGIN — CONNECTION FAILED", "POST", LOGIN_URL, {"email": email}, None, str(e), dt)
        debug.append(make_debug("error", "FIREBASE LOGIN", "POST", LOGIN_URL, None, None, None, dt, e))
        raise LoginError(str(e), debug,
                          friendly=f"Không kết nối được tới máy chủ Firebase/Locket ({type(e).__name__}). "
                                    "Kiểm tra mạng, DNS, hoặc tường lửa trên máy chạy server.") from e

    dt = time.time() - t0
    console_log("FIREBASE LOGIN", "POST", LOGIN_URL, {"email": email}, r.status_code, r.text, dt)
    debug.append(make_debug("recv" if r.ok else "error", "FIREBASE LOGIN", "POST", LOGIN_URL,
                             None, r.status_code, r.text, dt))
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        friendly = "Sai email hoặc mật khẩu."
        try:
            msg = (r.json().get("error") or {}).get("message", "")
            if msg:
                friendly = f"Đăng nhập thất bại: {msg}"
        except Exception:
            pass
        raise LoginError(str(e), debug, friendly=friendly) from e
    data = r.json()

    t1 = time.time()
    try:
        r2 = requests.post(ACCOUNT_URL, json={"idToken": data["idToken"]}, headers=_fb_headers(), timeout=timeout)
        dt2 = time.time() - t1
        console_log("ACCOUNT INFO", "POST", ACCOUNT_URL, {"idToken": "***"}, r2.status_code, r2.text, dt2)
        debug.append(make_debug("recv", "ACCOUNT INFO", "POST", ACCOUNT_URL, None, r2.status_code, r2.text, dt2))
        display_name, photo_url = "", ""
        if r2.ok:
            users = (r2.json() or {}).get("users") or []
            if users:
                display_name = users[0].get("displayName") or ""
                photo_url = users[0].get("photoUrl") or ""
    except requests.exceptions.RequestException as e:
        # Non-fatal — we already have a valid id_token, account info is just extra polish.
        dt2 = time.time() - t1
        debug.append(make_debug("error", "ACCOUNT INFO", "POST", ACCOUNT_URL, None, None, None, dt2, e))
        display_name, photo_url = "", ""

    if not display_name:
        display_name = data.get("email", email).split("@")[0]

    ses = LocketSession(
        id_token=data["idToken"], refresh_token=data.get("refreshToken"),
        local_id=data["localId"], email=data.get("email", email),
        display_name=display_name, photo_url=photo_url,
        ua=WEB_UA_POOL[hash(data["localId"]) % len(WEB_UA_POOL)], raw=data,
    )
    return ses, debug


def refresh_id_token(s: LocketSession, timeout: int = 20) -> bool:
    """Exchange refreshToken for a fresh idToken. API key is iOS-restricted so
    we must send the same X-Ios-Bundle-Identifier / GMPID headers as login."""
    if not s.refresh_token:
        return False
    with _TOKEN_LOCK:
        t0 = time.time()
        try:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "*/*",
                "User-Agent": IOS_UA,
                "X-Client-Version": "iOS/FirebaseSDK/10.23.1/FirebaseCore-iOS",
                "X-Firebase-GMPID": FIREBASE_GMPID,
                "X-Ios-Bundle-Identifier": IOS_BUNDLE,
            }
            r = requests.post(
                REFRESH_URL,
                data={"grant_type": "refresh_token", "refresh_token": s.refresh_token},
                headers=headers,
                timeout=timeout,
            )
            dt = time.time() - t0
            console_log("FIREBASE REFRESH", "POST", REFRESH_URL, {"refresh_token": "***"}, r.status_code, r.text, dt)
            if not r.ok:
                return False
            data = r.json()
            new_id = data.get("id_token") or data.get("idToken")
            new_rt = data.get("refresh_token") or data.get("refreshToken") or s.refresh_token
            if not new_id:
                return False
            s.id_token = new_id
            s.refresh_token = new_rt
            try:
                session["token"] = new_id
                session["refresh_token"] = new_rt
                session["token_issued_at"] = time.time()
            except Exception:
                pass
            return True
        except Exception as e:
            logger.warning("Token refresh failed: %s", e)
            return False


def ensure_fresh_token(s: LocketSession) -> LocketSession:
    issued = session.get("token_issued_at") or 0
    if time.time() - issued > 50 * 60:
        if refresh_id_token(s):
            session["token_issued_at"] = time.time()
    return s


def binhake_call(action: str, s: LocketSession, extra: Optional[Dict[str, Any]] = None,
                  timeout: int = 25) -> Tuple[Any, Dict[str, Any]]:
    """Generic POST to the binhake action-API. Returns (parsed_json, debug_entry)."""
    payload = {"action": action, **(extra or {})}
    cookies = {"user_id": s.local_id}
    headers = _web_headers(s.id_token, s.ua)
    t0 = time.time()
    err = None
    r = None
    try:
        r = requests.post(BINHAKE_API, headers=headers, json=payload, cookies=cookies, timeout=timeout)
        dt = time.time() - t0
        console_log(f"LOCK {action}", "POST", "api://lock/" + action, payload, r.status_code, r.text, dt)
        r.raise_for_status()
        dbg = make_debug("recv", f"binhake:{action}", "POST", BINHAKE_API, payload, r.status_code, r.text, dt)
        try:
            return r.json(), dbg
        except ValueError:
            raise LocketError(f"binhake:{action} returned non-JSON body")
    except requests.HTTPError as e:
        dt = time.time() - t0
        dbg = make_debug("error", f"binhake:{action}", "POST", BINHAKE_API, payload,
                          getattr(r, "status_code", None), getattr(r, "text", None), dt, e)
        console_log(f"LOCK {action} FAIL", "POST", "api://lock/" + action, payload,
                     getattr(r, "status_code", None), getattr(r, "text", None), dt)
        raise
    except Exception as e:
        dt = time.time() - t0
        dbg = make_debug("error", f"binhake:{action}", "POST", BINHAKE_API, payload, None, None, dt, e)
        console_log(f"LOCK {action} ERROR", "POST", "api://lock/" + action, payload, None, str(e), dt)
        raise LocketError(str(e)) from e


def _first(d: Dict[str, Any], *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _split_display_name(name: str) -> Tuple[str, str]:
    parts = (name or "").strip().split(None, 1)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]



def _flatten_firestore_value(v):
    if not isinstance(v, dict):
        return v
    if "stringValue" in v:
        return v["stringValue"]
    if "integerValue" in v:
        return int(v["integerValue"])
    if "doubleValue" in v:
        return float(v["doubleValue"])
    if "booleanValue" in v:
        return v["booleanValue"]
    if "timestampValue" in v:
        from datetime import datetime, timezone
        try:
            raw_ts = v["timestampValue"]
            frac_match = re.search(r"\.(\d+)", raw_ts)
            nanos = int((frac_match.group(1) + "000000000")[:9]) if frac_match else 0
            base_ts = re.sub(r"\.\d+", "", raw_ts).replace("Z", "+00:00")
            dt = datetime.fromisoformat(base_ts)
            return {"_seconds": int(dt.timestamp()), "_nanoseconds": nanos}
        except Exception:
            return {"_seconds": 0, "_nanoseconds": 0}
    if "nullValue" in v:
        return None
    if "arrayValue" in v:
        values = v["arrayValue"].get("values", [])
        return [_flatten_firestore_value(x) for x in values]
    if "mapValue" in v:
        fields = v["mapValue"].get("fields", {})
        return {k: _flatten_firestore_value(val) for k, val in fields.items()}
    return v


def flatten_firestore(doc):
    if not isinstance(doc, dict):
        return doc
    fields = doc.get("fields")
    if not isinstance(fields, dict):
        return doc
    result = {}
    for k, v in fields.items():
        result[k] = _flatten_firestore_value(v)
    for key in ("createTime", "updateTime", "name"):
        if key in doc:
            result[key] = doc[key]
    return result

def _normalize_user(u: Dict[str, Any]) -> Dict[str, Any]:
    """binhake's exact field names aren't guaranteed everywhere, so every
    likely alias is checked. If a field is missing here, check the debug
    console — the raw payload is right there and this function just needs
    another key added to the matching _first(...) call."""
    if not isinstance(u, dict):
        return {}
    first_name = _first(u, "first_name", "firstName", default="")
    last_name = _first(u, "last_name", "lastName", default="")
    if not first_name and not last_name:
        # confirmed real shape from getInfo: only a combined "displayName"
        first_name, last_name = _split_display_name(_first(u, "displayName", "display_name", "name", default=""))
    pic = _first(u, "profile_picture_url", "profilePictureUrl",
                 "avatar", "avatar_url", "photo_url", "photoUrl", default="") or ""
    if isinstance(pic, str):
        pic = pic.replace("firebasestorage.googleapis.com:443", "firebasestorage.googleapis.com")
    return {
        "uid": _first(u, "uid", "user_id", "userId", "id", default=""),
        "username": _first(u, "username", "user_name", default=""),
        "first_name": first_name,
        "last_name": last_name,
        "profile_picture_url": pic,
        "celebrity": bool(_first(u, "celebrity", "is_celebrity", default=False)),
        "badge": _first(u, "badge", "badge_type", default="") or "",
        "streak": _find_streak(u) or 0,
        "celebrity_data": _norm_celeb_data(u.get("celebrity_data")),
    }


_SELF_INFO_LOCK = threading.Lock()
_SELF_INFO_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_SELF_INFO_TTL_SEC = 120


def get_self_info(s: LocketSession, force: bool = False) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not force:
        with _SELF_INFO_LOCK:
            hit = _SELF_INFO_CACHE.get(s.local_id)
        if hit and time.time() - hit[0] < _SELF_INFO_TTL_SEC:
            return dict(hit[1]), []

    debug: List[Dict[str, Any]] = []
    info: Dict[str, Any] = {}
    try:
        info, d = fetch_user_v2(s, s.local_id)
        debug.extend(d)
    except Exception as e:
        logger.warning("fetchUserV2 self failed: %s", e)

    streak = fetch_official_streak(s)
    if streak is None:
        streak = info.get("streak") or 0
        if not streak:
            try:
                streak = _find_streak(fetch_firestore_user_doc(s)) or 0
            except Exception as e:
                logger.debug("firestore user doc failed: %s", e)
        if not streak:
            try:
                data, dbg = binhake_call("getInfo", s)
                debug.append(dbg)
                if isinstance(data, dict):
                    streak = _find_streak({"streak": data.get("streak")}) or 0
                    if not info.get("first_name"):
                        body = data.get("data") if isinstance(data.get("data"), dict) else {}
                        info = {**_normalize_user(body), **{k: v for k, v in info.items() if v}}
            except Exception as e:
                logger.debug("binhake getInfo failed: %s", e)

    info["streak"] = streak
    info["uid"] = s.local_id
    if not info.get("first_name") and not info.get("username"):
        info["first_name"] = s.display_name
    if not info.get("profile_picture_url"):
        info["profile_picture_url"] = s.photo_url
    with _SELF_INFO_LOCK:
        _SELF_INFO_CACHE[s.local_id] = (time.time(), dict(info))
    return info, debug


def _friends_sparse(friends: List[Dict[str, Any]]) -> bool:
    """True when list is empty or almost all entries lack name/avatar (binhake
    returning bare UIDs)."""
    if not friends:
        return True
    rich = 0
    for p in friends:
        has_name = bool((p.get("first_name") or p.get("last_name") or p.get("username") or "").strip())
        has_pic = bool((p.get("profile_picture_url") or "").strip())
        if has_name or has_pic:
            rich += 1
    # If fewer than 20% have any profile field, treat as broken upstream
    return rich < max(1, len(friends) // 5)


_FRIEND_META: Dict[str, Dict[str, Any]] = {}


def _fs_doc_time(doc: Dict[str, Any]) -> int:
    # Ưu tiên field "date" (nếu có), sau đó createTime/updateTime của document Firestore.
    from datetime import datetime, timezone
    cands: List[str] = []
    f = (doc.get("fields") or {}).get("date")
    if isinstance(f, dict) and f.get("timestampValue"):
        cands.append(str(f["timestampValue"]))
    for k in ("createTime", "updateTime"):
        if doc.get(k):
            cands.append(str(doc[k]))
    for raw in cands:
        m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z?$", raw)
        if not m:
            continue
        try:
            return int(datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            continue
    return 0


def list_friend_uids_firestore(s: LocketSession, timeout: int = 25
                                 ) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Official friends list via Firestore REST (same source the Android app
    listens on). Returns friend UIDs only — profiles come from fetchUserV2."""
    debug: List[Dict[str, Any]] = []
    url = (
        f"https://firestore.googleapis.com/v1/projects/locket-4252a/"
        f"databases/(default)/documents/users/{s.local_id}/friends"
        f"?pageSize=300"
    )
    headers = {
        "Authorization": f"Bearer {s.id_token}",
        "Accept": "application/json",
        "User-Agent": "okhttp/4.12.0",
    }
    debug.append(make_debug("send", "firestore:friends", "GET", url, None))
    t0 = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        dt = time.time() - t0
        console_log("FIRESTORE friends", "GET", url, None, r.status_code, r.text[:400], dt)
        debug.append(make_debug("recv" if r.ok else "error", "firestore:friends", "GET", url,
                                 None, r.status_code, r.text, dt))
        r.raise_for_status()
        body = r.json() if r.text else {}
    except Exception as e:
        dt = time.time() - t0
        debug.append(make_debug("error", "firestore:friends", "GET", url, None, None, None, dt, e))
        raise LocketError(f"Firestore friends list failed: {e}") from e

    uids: List[str] = []
    seen = set()
    meta: Dict[str, int] = {}
    complete = True
    for doc in (body.get("documents") or []):
        if not isinstance(doc, dict):
            continue
        fields = doc.get("fields") or {}
        # doc name ends with /friends/{friendUid}; field "user" is the canonical uid
        uid = None
        user_f = fields.get("user")
        if isinstance(user_f, dict) and "stringValue" in user_f:
            uid = user_f["stringValue"]
        if not uid:
            name = doc.get("name") or ""
            if "/friends/" in name:
                uid = name.rsplit("/friends/", 1)[-1]
        if uid and uid not in seen:
            seen.add(uid)
            uids.append(uid)
            meta[uid] = _fs_doc_time(doc)
    # pagination
    next_token = body.get("nextPageToken")
    while next_token:
        page_url = url + f"&pageToken={next_token}"
        try:
            r2 = requests.get(page_url, headers=headers, timeout=timeout)
            r2.raise_for_status()
            body2 = r2.json() if r2.text else {}
            for doc in (body2.get("documents") or []):
                if not isinstance(doc, dict):
                    continue
                fields = doc.get("fields") or {}
                uid = None
                user_f = fields.get("user")
                if isinstance(user_f, dict) and "stringValue" in user_f:
                    uid = user_f["stringValue"]
                if not uid:
                    name = doc.get("name") or ""
                    if "/friends/" in name:
                        uid = name.rsplit("/friends/", 1)[-1]
                if uid and uid not in seen:
                    seen.add(uid)
                    uids.append(uid)
                    meta[uid] = _fs_doc_time(doc)
            next_token = body2.get("nextPageToken")
        except Exception as e:
            logger.warning("firestore friends page skip: %s", e)
            complete = False
            break
    logger.info("firestore friends → %d uids", len(uids))
    _FRIEND_META[s.local_id] = {"items": meta, "complete": complete, "at": time.time()}
    return uids, debug


def fetch_user_v2(s: LocketSession, user_uid: str, timeout: int = 15
                   ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    res = _locket_post_api(s, "fetchUserV2", {"user_uid": user_uid},
                            timeout=timeout, quiet=user_uid != s.local_id)
    data = res.get("data", res) if isinstance(res, dict) else {}
    profile = _normalize_user(data if isinstance(data, dict) else {})
    profile["uid"] = profile.get("uid") or user_uid
    return profile, []


def fetch_firestore_user_doc(s: LocketSession, timeout: int = 15) -> Dict[str, Any]:
    url = (f"https://firestore.googleapis.com/v1/projects/locket-4252a/"
           f"databases/(default)/documents/users/{s.local_id}")
    r = requests.get(url, headers={"Authorization": f"Bearer {s.id_token}",
                                    "Accept": "application/json"}, timeout=timeout)
    r.raise_for_status()
    return flatten_firestore(r.json())


def get_friends_official(s: LocketSession, timeout: int = 45
                          ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Firestore friend UIDs + parallel fetchUserV2 profiles (official path)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    debug: List[Dict[str, Any]] = []
    uids, d1 = list_friend_uids_firestore(s, timeout=min(25, timeout))
    debug.extend(d1)
    if not uids:
        return [], debug

    friends: List[Dict[str, Any]] = []
    # Cap concurrency so Vercel / Firebase don't rate-limit us
    workers = min(10, max(4, len(uids)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fetch_user_v2, s, uid, min(12, timeout)): uid for uid in uids}
        for fut in as_completed(futs):
            uid = futs[fut]
            try:
                profile, d = fut.result()
                debug.extend(d)
                if not profile.get("uid"):
                    profile["uid"] = uid
                friends.append(profile)
            except Exception as e:
                logger.warning("fetchUserV2 %s failed: %s", uid[:8], e)
                friends.append(_normalize_user({"uid": uid}))

    def sort_key(p):
        name = (f"{p.get('first_name','')} {p.get('last_name','')}").strip() or p.get("username") or p.get("uid") or ""
        return name.casefold()

    celebs = sorted([p for p in friends if p.get("celebrity")], key=sort_key)
    normals = sorted([p for p in friends if not p.get("celebrity")], key=sort_key)
    logger.info("official friends resolved %d/%d profiles",
                sum(1 for p in friends if p.get("first_name") or p.get("username") or p.get("profile_picture_url")),
                len(friends))
    return celebs + normals, debug


_PROFILE_LOCK = threading.Lock()
_PROFILE_CACHE: Dict[str, Dict[str, Any]] = {}
_FRIENDS_TTL_SEC = 600


def _profile_state(local_id: str) -> Dict[str, Any]:
    with _PROFILE_LOCK:
        st = _PROFILE_CACHE.get(local_id)
        if st is None:
            st = {"friends": [], "friends_at": 0.0, "profiles": {}}
            _PROFILE_CACHE[local_id] = st
        return st


def _sort_friends(friends: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(p):
        name = (f"{p.get('first_name','')} {p.get('last_name','')}").strip() or p.get("username") or p.get("uid") or ""
        return name.casefold()
    celebs = sorted([p for p in friends if p.get("celebrity")], key=sort_key)
    normals = sorted([p for p in friends if not p.get("celebrity")], key=sort_key)
    return celebs + normals


def _get_friends_binhake(s: LocketSession) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    debug = [make_debug("send", "binhake:getFriendsList_v3", "POST", BINHAKE_API,
                         {"action": "getFriendsList_v3"})]
    data, dbg = binhake_call("getFriendsList_v3", s)
    debug.append(dbg)
    raw_list: List[Any] = []
    if isinstance(data, dict):
        for path in (data.get("data"), data.get("friends")):
            if isinstance(path, list):
                raw_list = path
                break
    return [_normalize_user(u) for u in raw_list if isinstance(u, dict)], debug


def get_friends(s: LocketSession) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    debug: List[Dict[str, Any]] = []
    friends: List[Dict[str, Any]] = []
    try:
        friends, d = get_friends_official(s)
        debug.extend(d)
    except Exception as e:
        logger.warning("official friends failed: %s", e)
        debug.append(make_debug("error", "official:friends", "GET",
                                 "firestore+fetchUserV2", None, None, None, None, e))
    if _friends_sparse(friends):
        try:
            fallback, d = _get_friends_binhake(s)
            debug.extend(d)
            if fallback and (not friends or not _friends_sparse(fallback)):
                friends = fallback
        except Exception as e:
            logger.warning("binhake friends fallback failed: %s", e)
    return _sort_friends(friends), debug


def get_friends_cached(s: LocketSession, force: bool = False) -> List[Dict[str, Any]]:
    st = _profile_state(s.local_id)
    if not force and st["friends"] and time.time() - st["friends_at"] < _FRIENDS_TTL_SEC:
        return st["friends"]
    friends, _ = get_friends(s)
    if friends:
        with _PROFILE_LOCK:
            st["friends"] = friends
            st["friends_at"] = time.time()
            for f in friends:
                if f.get("uid"):
                    st["profiles"][f["uid"]] = f
    return st["friends"] or friends


def _enrich_moments(s: LocketSession, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return []
    st = _profile_state(s.local_id)
    if not st["friends"]:
        try:
            get_friends_cached(s)
        except Exception as e:
            logger.warning("friends for enrich failed: %s", e)
    profiles = st["profiles"]
    if s.local_id not in profiles:
        try:
            profiles[s.local_id], _ = fetch_user_v2(s, s.local_id)
        except Exception as e:
            logger.debug("self profile failed: %s", e)

    missing: List[str] = []
    _seen_missing = set()
    for m in items:
        uid = m.get("user")
        if isinstance(uid, str) and uid and uid not in profiles and uid not in _seen_missing:
            _seen_missing.add(uid)
            missing.append(uid)
            if len(missing) >= 12:
                break
    if missing:
        with ThreadPoolExecutor(max_workers=min(6, len(missing))) as pool:
            futs = {pool.submit(fetch_user_v2, s, u, 12): u for u in missing}
            for fut in as_completed(futs):
                uid = futs[fut]
                try:
                    profile, _ = fut.result()
                except Exception:
                    profile = {}
                profile = profile or {}
                profile["uid"] = profile.get("uid") or uid
                profiles[uid] = profile

    out: List[Dict[str, Any]] = []
    for m in items:
        p = profiles.get(m.get("user")) or {}
        c = dict(m)
        c["first_name"] = c.get("first_name") or p.get("first_name") or ""
        c["last_name"] = c.get("last_name") or p.get("last_name") or ""
        c["profile_picture_url"] = c.get("profile_picture_url") or p.get("profile_picture_url") or ""
        c["from_celebrity"] = bool(c.get("from_celebrity") or p.get("celebrity"))
        out.append(c)
    return out


def _extract_moments_list(data: Any) -> List[Dict[str, Any]]:
    """Normalize binhake HTTP/WS payloads into a flat list of moment dicts."""
    if data is None:
        return []
    if isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        raw = None
        for path in (
            data.get("data"),
            data.get("moments"),
            (data.get("result") or {}).get("data") if isinstance(data.get("result"), dict) else None,
            data.get("items"),
        ):
            if isinstance(path, list):
                raw = path
                break
            if isinstance(path, dict):
                raw = [path]
                break
        if raw is None:
            # Single firestore doc?
            if "fields" in data or "thumbnail_url" in data or "canonical_uid" in data:
                raw = [data]
            else:
                raw = []
    else:
        raw = []

    out: List[Dict[str, Any]] = []
    seen = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        flat = flatten_firestore(item) if "fields" in item else item
        if not isinstance(flat, dict):
            continue
        k = _moment_key(flat)
        if k in seen:
            continue
        seen.add(k)
        out.append(flat)
    return out


def _moment_timestamp(m: Dict[str, Any]) -> int:
    """Unix seconds. Real Locket payloads use date as milliseconds (e.g. 1792164530909)."""
    if not isinstance(m, dict):
        return 0

    def _as_sec(v) -> int:
        if v is None:
            return 0
        if isinstance(v, bool):
            return 0
        if isinstance(v, (int, float)):
            x = float(v)
            if x > 1e12:  # milliseconds
                return int(x / 1000.0)
            if x > 1e9:   # seconds
                return int(x)
            return int(x) if x > 0 else 0
        if isinstance(v, str) and v.strip():
            s = v.strip()
            try:
                return _as_sec(float(s))
            except Exception:
                pass
            try:
                from datetime import datetime
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                return int(datetime.fromisoformat(s).timestamp())
            except Exception:
                return 0
        if isinstance(v, dict):
            for k in ("_seconds", "seconds", "secondsValue"):
                if k in v and v[k] is not None:
                    return _as_sec(v[k])
            if "timestampValue" in v:
                return _as_sec(v["timestampValue"])
        return 0

    for key in ("date", "createTime", "updateTime", "timestamp", "created_at", "createTime"):
        sec = _as_sec(m.get(key))
        if sec:
            return sec
    return 0


def _overlay_to_client_shape(ov: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(ov.get("data"), dict):
        out = dict(ov)
        text = ov["data"].get("text") or ov.get("alt_text") or ""
        out["alt_text"] = out.get("alt_text") or text
        return out
    skip = ("overlay_id", "overlay_type", "alt_text")
    data = {k: v for k, v in ov.items() if k not in skip}
    text = ov.get("text") or ov.get("alt_text") or ""
    kind = ov.get("type") or "standard"
    return {
        "overlay_id": ov.get("overlay_id") or f"caption:{kind}",
        "overlay_type": ov.get("overlay_type") or "caption",
        "alt_text": text,
        "data": data,
    }


def _normalize_overlays(overlays: Any) -> List[Dict[str, Any]]:
    if overlays is None:
        return []
    if isinstance(overlays, list):
        return [_overlay_to_client_shape(x) for x in overlays if isinstance(x, dict)]
    if isinstance(overlays, dict):
        if overlays.get("overlay_type") or overlays.get("type") or overlays.get("text"):
            return [_overlay_to_client_shape(overlays)]
        out = []
        for v in overlays.values():
            if isinstance(v, dict) and (v.get("text") or v.get("overlay_type") or v.get("type")):
                out.append(_overlay_to_client_shape(v))
        return out
    return []


def _normalize_moment_official(m: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize both official camelCase and binhake/snake_case moment payloads.

    Real official / CDN shape (from live capture):
      thumbnailUrl, videoUrl, date=<ms int>, overlays=<object>, user=<uid str>
    Binhake / cached shape:
      thumbnail_url, video_url, date={_seconds,_nanoseconds}, overlays=[...]
    """
    if not isinstance(m, dict):
        return {}
    out = dict(m)

    # ids
    cid = out.get("canonical_uid") or out.get("id") or out.get("moment_uid") or out.get("md5") or ""
    out["canonical_uid"] = cid
    if not out.get("name") and cid:
        out["name"] = f"moments/{cid}"

    # user → plain uid
    u = out.get("user")
    if isinstance(u, dict):
        out["user"] = u.get("uid") or u.get("user_id") or u.get("id") or ""
        if not out.get("first_name"):
            out["first_name"] = u.get("first_name") or u.get("firstName") or ""
        if not out.get("last_name"):
            out["last_name"] = u.get("last_name") or u.get("lastName") or ""
        if not out.get("profile_picture_url"):
            out["profile_picture_url"] = (
                u.get("profile_picture_url") or u.get("profilePictureUrl")
                or u.get("photo_url") or u.get("photoUrl") or ""
            )

    # media — camelCase first (official), then snake_case
    thumb = (
        out.get("thumbnail_url") or out.get("thumbnailUrl")
        or out.get("image_url") or out.get("imageUrl") or out.get("url") or ""
    )
    # strip erroneous :443 that some CDN URLs embed
    if isinstance(thumb, str):
        thumb = thumb.replace("cdn.locketcamera.com:443", "cdn.locketcamera.com")
        thumb = thumb.replace("firebasestorage.googleapis.com:443", "firebasestorage.googleapis.com")
    out["thumbnail_url"] = thumb
    out["url"] = out.get("url") or thumb

    vurl = out.get("video_url") or out.get("videoUrl") or out.get("video") or ""
    if isinstance(vurl, str):
        vurl = vurl.replace("cdn.locketcamera.com:443", "cdn.locketcamera.com")
        vurl = vurl.replace("firebasestorage.googleapis.com:443", "firebasestorage.googleapis.com")
    out["video_url"] = vurl

    # date always {_seconds, _nanoseconds} for the web UI timeAgo()
    sec = _moment_timestamp(out)
    ns = 0
    raw_date = out.get("date")
    if isinstance(raw_date, (int, float)) and float(raw_date) > 1e12:
        ns = int((float(raw_date) % 1000) * 1_000_000)
    elif isinstance(raw_date, dict) and "_nanoseconds" in raw_date:
        try:
            ns = int(raw_date.get("_nanoseconds") or 0)
        except Exception:
            ns = 0
    out["date"] = {"_seconds": sec, "_nanoseconds": ns}
    out["timestamp"] = sec

    # overlays → list; caption text
    ovs = _normalize_overlays(out.get("overlays"))
    out["overlays"] = ovs
    if not out.get("caption"):
        for ov in ovs:
            text = ov.get("text") or ov.get("alt_text") or ""
            if not text and isinstance(ov.get("data"), dict):
                text = ov["data"].get("text") or ""
            if text:
                out["caption"] = text
                break

    if out.get("from_celebrity") is None and out.get("isCelebrity") is not None:
        out["from_celebrity"] = bool(out.get("isCelebrity"))

    return out


_STREAK_KEYS = {"streak", "streak_count", "streakcount", "current_streak", "currentstreak"}


def _find_streak(obj, depth=0):
    if depth > 4:
        return None
    if isinstance(obj, dict):
        for key, val in obj.items():
            if str(key).lower() not in _STREAK_KEYS or isinstance(val, bool):
                continue
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str) and val.isdigit():
                return int(val)
            if isinstance(val, dict):
                count = _first(val, "count", "streak", "value", "current")
                if isinstance(count, (int, float)) and not isinstance(count, bool):
                    return int(count)
                if isinstance(count, str) and count.isdigit():
                    return int(count)
        for val in obj.values():
            found = _find_streak(val, depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for val in obj[:20]:
            found = _find_streak(val, depth + 1)
            if found is not None:
                return found
    return None


_APPCHECK_TOKENS: Dict[str, str] = {}


def _appcheck_for(s: LocketSession) -> str:
    return _APPCHECK_TOKENS.get(s.local_id) or LOCKET_APPCHECK_TOKEN


def _jwt_exp(token: str) -> float:
    try:
        part = token.split(".")[1]
        part += "=" * (-len(part) % 4)
        return float(json.loads(base64.urlsafe_b64decode(part)).get("exp") or 0)
    except Exception:
        return 0.0


def set_appcheck_token(local_id: str, token: str) -> None:
    if token:
        _APPCHECK_TOKENS[local_id] = token
    else:
        _APPCHECK_TOKENS.pop(local_id, None)


_APPCHECK_LOCK = threading.Lock()
_APPCHECK_FAIL: Dict[str, float] = {}
_APPCHECK_CLIENT_ID = str(uuid.uuid4())


def appcheck_status(local_id: str) -> Dict[str, Any]:
    provider = bool(APPCHECK_PROVIDER_URL and APPCHECK_PROVIDER_KEY)
    token = _APPCHECK_TOKENS.get(local_id) or LOCKET_APPCHECK_TOKEN
    if not token:
        return {"has": False, "source": "none", "expired": False, "expires_in": None, "provider": provider}
    exp = _jwt_exp(token)
    left = int(exp - time.time()) if exp else None
    return {"has": True, "source": "session" if local_id in _APPCHECK_TOKENS else "env",
            "expired": bool(exp) and left is not None and left <= 0, "expires_in": left,
            "provider": provider}


def fetch_appcheck_from_provider(s: LocketSession, timeout: int = 20) -> str:
    url = APPCHECK_PROVIDER_URL.rstrip("/") + "/config/app-check"
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "authorization": f"Bearer {s.id_token}",
        "active-key": APPCHECK_PROVIDER_KEY,
        "app-version": APPCHECK_PROVIDER_APP_VERSION,
        "client-unique-id": _APPCHECK_CLIENT_ID,
        "User-Agent": "okhttp/4.12.0",
    }
    r = requests.post(url, json={"token": s.id_token}, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    token = str((data or {}).get("appCheckToken") or (data or {}).get("token") or "").strip()
    if token.count(".") != 2:
        raise LocketError("App Check provider returned no token")
    return token


def ensure_appcheck(s: LocketSession) -> Dict[str, Any]:
    state = appcheck_status(s.local_id)
    fresh = state["has"] and not state["expired"] and (
        state["expires_in"] is None or state["expires_in"] > APPCHECK_MIN_LEFT_SEC)
    if fresh or not (APPCHECK_PROVIDER_URL and APPCHECK_PROVIDER_KEY):
        return state
    with _APPCHECK_LOCK:
        state = appcheck_status(s.local_id)
        if state["has"] and not state["expired"] and (
                state["expires_in"] is None or state["expires_in"] > APPCHECK_MIN_LEFT_SEC):
            return state
        if time.time() - _APPCHECK_FAIL.get(s.local_id, 0.0) < APPCHECK_RETRY_SEC:
            return state
        try:
            set_appcheck_token(s.local_id, fetch_appcheck_from_provider(s))
        except Exception as e:
            _APPCHECK_FAIL[s.local_id] = time.time()
            logger.warning("app check provider failed: %s", e)
    return appcheck_status(s.local_id)


_LOCKET_HEADER_PROFILE: Dict[str, str] = {"name": "full"}
_LOCKET_HEADER_ORDER = ("full", "minimal")


def _locket_installation_id(s: LocketSession) -> str:
    if LOCKET_INSTALLATION_ID:
        return LOCKET_INSTALLATION_ID
    return hashlib.md5(s.local_id.encode()).hexdigest()[:22]


def _locket_read_headers(s: LocketSession, profile: str = "full") -> Dict[str, str]:
    headers = {
        "User-Agent": LOCKET_IOS_UA,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "*/*",
        "x-ios-bundle-identifier": IOS_BUNDLE,
        "Authorization": f"Bearer {s.id_token}",
    }
    appcheck = _appcheck_for(s)
    if appcheck:
        headers["X-Firebase-AppCheck"] = appcheck
    if LOCKET_INSTANCE_ID_TOKEN:
        headers["Firebase-Instance-Id-Token"] = LOCKET_INSTANCE_ID_TOKEN
    if profile == "full":
        headers["User-Agent"] = FIREBASE_AUTH_UA
        headers["Accept"] = "application/json"
        headers["Accept-Language"] = "en-US"
        headers["X-Firebase-GMPID"] = FIREBASE_GMPID
        headers["X-Client-Version"] = "iOS/FirebaseSDK/10.23.1/FirebaseCore-iOS"
        headers["X-Firebase-Client"] = FIREBASE_CLIENT_HEADER
    return headers


def _locket_read_analytics(s: LocketSession) -> Dict[str, Any]:
    int64 = "type.googleapis.com/google.protobuf.Int64Value"
    return {
        "ios_version": f"{LOCKET_IOS_APP_VERSION}.{LOCKET_IOS_BUILD}",
        "experiments": {
            "flag_18": {"@type": int64, "value": "1203"},
            "flag_7": {"@type": int64, "value": "800"},
            "flag_10": {"@type": int64, "value": "505"},
        },
        "installation_id": _locket_installation_id(s),
        "platform": "ios",
    }


def _locket_http_post(url: str, headers: Dict[str, str], raw: bytes, timeout: int
                       ) -> Tuple[Optional[int], str, Optional[Exception]]:
    req = urllib.request.Request(url, data=raw, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "ignore"), None
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore"), None
    except Exception as e:
        return None, "", e


def _locket_post_api(s: LocketSession, endpoint: str, payload: Optional[Dict[str, Any]] = None,
                      timeout: int = 25, quiet: bool = False, retries: Optional[int] = None,
                      analytics: bool = True) -> Dict[str, Any]:
    max_retries = LOCKET_READ_RETRIES if retries is None else retries
    url = f"{LOCKET_API}/{endpoint}"
    data_body = dict(payload or {})
    if analytics:
        data_body["analytics"] = _locket_read_analytics(s)
    body = {"data": data_body}
    raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
    preferred = _LOCKET_HEADER_PROFILE["name"]
    order = [preferred] + [p for p in _LOCKET_HEADER_ORDER if p != preferred]
    refreshed = False
    status: Optional[int] = None
    text = ""
    error: Optional[Exception] = None
    used = preferred
    t0 = time.time()
    for profile in order:
        used = profile
        for attempt in range(max_retries + 1):
            status, text, error = _locket_http_post(url, _locket_read_headers(s, profile), raw, timeout)
            if status == 401 and not refreshed:
                refreshed = True
                if refresh_id_token(s):
                    continue
                break
            transient = status is None or status in (429, 500, 502, 503, 504)
            if transient and attempt < max_retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            break
        if status is not None and status not in (401, 403):
            break
    dt = time.time() - t0
    if status is None:
        raise LocketError(f"{endpoint} request failed: {error}")
    if not quiet or status >= 400:
        shown = {k: v for k, v in (payload or {}).items() if k != "excluded_users"}
        excluded = (payload or {}).get("excluded_users")
        if excluded:
            shown["excluded_users"] = f"[{len(excluded)} uids]"
        console_log(f"LOCKET {endpoint} [{used}]", "POST", url, shown, status, text[:400], dt)
    if status < 400:
        _LOCKET_HEADER_PROFILE["name"] = used
    try:
        data = json.loads(text) if text else {}
    except ValueError:
        raise LocketError(f"{endpoint} returned non-JSON body (HTTP {status})")
    if status >= 400:
        err = LocketError(f"{endpoint} HTTP {status}: {text[:200]}")
        err.status = status
        raise err
    if isinstance(data, dict) and isinstance(data.get("result"), dict):
        return data["result"]
    return data if isinstance(data, dict) else {"data": data}


def _moment_author(m: Dict[str, Any]) -> str:
    u = m.get("user")
    if isinstance(u, dict):
        u = u.get("uid") or u.get("user_id") or u.get("id")
    return str(u) if u else ""


def _official_feed_page(s: LocketSession, sync_token: Optional[str] = None,
                         excluded_users: Optional[List[str]] = None,
                         quiet: bool = True, timeout: int = 20, fetch_streak: bool = False
                         ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    payload: Dict[str, Any] = {
        "excluded_users": list(excluded_users or []),
        "should_count_missed_moments": True,
    }
    if fetch_streak:
        payload["fetch_streak"] = True
    else:
        payload["include_group_moments"] = True
    if sync_token:
        payload["sync_token"] = sync_token
    res = _locket_post_api(s, "getLatestMomentV2", payload, timeout=timeout, quiet=quiet)
    data = res.get("data") if isinstance(res, dict) else None
    if isinstance(data, dict):
        data = data.get("moments") or data.get("data") or []
    return (data if isinstance(data, list) else []), (res if isinstance(res, dict) else {})


def fetch_official_streak(s: LocketSession) -> Optional[int]:
    try:
        _, res = _official_feed_page(s, None, [], quiet=True, timeout=15, fetch_streak=True)
    except Exception as e:
        logger.debug("official streak failed: %s", e)
        return None
    streak = res.get("streak")
    count = streak.get("count") if isinstance(streak, dict) else streak
    if isinstance(count, bool) or not isinstance(count, (int, float)):
        return None
    return int(count)


def _feed_entry_to_moment(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    norm = _normalize_moment_official(entry)
    if not (norm.get("thumbnail_url") or norm.get("video_url")):
        return None
    norm["source"] = "official"
    return norm


def crawl_latest_per_user(s: LocketSession, uids: List[str],
                           budget: float = FEED_CRAWL_BUDGET_SEC) -> Dict[str, Any]:
    targets: List[str] = []
    for u in list(uids) + [s.local_id]:
        if u and u not in targets:
            targets.append(u)
    t0 = time.time()
    deadline = t0 + budget

    def work(uid: str):
        if time.time() > deadline:
            return uid, None, None
        excluded = [u for u in targets if u != uid]
        try:
            raw, _ = _official_feed_page(s, None, excluded, quiet=True, timeout=15)
        except Exception as e:
            return uid, [], str(e)
        out: List[Dict[str, Any]] = []
        for entry in raw:
            if isinstance(entry, dict):
                moment = _feed_entry_to_moment(entry)
                if moment is not None:
                    out.append(moment)
        return uid, out, None

    items: List[Dict[str, Any]] = []
    errors: List[str] = []
    empty_uids: List[str] = []
    empty = skipped = 0
    with ThreadPoolExecutor(max_workers=FEED_CRAWL_WORKERS) as pool:
        for uid, out, err in pool.map(work, targets):
            if out is None:
                skipped += 1
            elif err:
                errors.append(err)
            elif not out:
                empty += 1
                empty_uids.append(uid)
            else:
                items.extend(out)
    return {
        "items": items,
        "targets": len(targets),
        "empty_uids": empty_uids,
        "empty": empty,
        "skipped": skipped,
        "errors": errors,
        "elapsed": time.time() - t0,
    }


_FS_RUNQUERY_URL = "https://firestore.googleapis.com/v1/projects/locket-4252a/databases/(default)/documents:runQuery"
_HIST: Dict[str, Any] = {"method": None, "tried": {}, "none_at": 0.0}


def fetch_user_history_firestore(s: LocketSession, uid: str, limit: int = FEED_HISTORY_PER_USER,
                                  timeout: int = 15) -> List[Dict[str, Any]]:
    body = {"structuredQuery": {
        "from": [{"collectionId": "moments"}],
        "where": {"fieldFilter": {"field": {"fieldPath": "user"}, "op": "EQUAL",
                                  "value": {"stringValue": uid}}},
        "limit": limit,
    }}
    headers = {"Authorization": f"Bearer {s.id_token}", "Accept": "application/json",
               "Content-Type": "application/json", "User-Agent": "okhttp/4.12.0"}
    r = requests.post(_FS_RUNQUERY_URL, json=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    rows = r.json()
    out: List[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        doc = row.get("document") if isinstance(row, dict) else None
        if not isinstance(doc, dict):
            continue
        flat = flatten_firestore(doc)
        if not isinstance(flat, dict):
            continue
        name = str(doc.get("name") or "")
        if name and not flat.get("canonical_uid"):
            flat["canonical_uid"] = name.rsplit("/", 1)[-1]
        moment = _feed_entry_to_moment(flat)
        if moment is not None:
            out.append(moment)
    return out


def _excluding(targets: List[str], uid: str) -> List[str]:
    return [u for u in targets if u and u != uid]


def fetch_older_via_sync(s: LocketSession, uid: str, targets: List[str], after_id: str,
                          timeout: int = 15) -> List[Dict[str, Any]]:
    raw, _ = _official_feed_page(s, after_id, _excluding(targets, uid), quiet=True, timeout=timeout)
    out: List[Dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict) or _moment_author(entry) != uid:
            continue
        moment = _feed_entry_to_moment(entry)
        if moment is not None:
            out.append(moment)
    return out


def detect_history_method(s: LocketSession, candidates: List[Tuple[str, str]],
                           targets: List[str]) -> str:
    method = _HIST["method"]
    if method in ("sync", "firestore"):
        return method
    if method == "none" and time.time() - _HIST["none_at"] < 1800:
        return "none"
    tried: Dict[str, Any] = {}
    found = "none"
    for uid, latest_id in candidates[:3]:
        try:
            older = [m for m in fetch_older_via_sync(s, uid, targets, latest_id) if _moment_id(m) != latest_id]
            tried["sync"] = len(older)
            if older:
                found = "sync"
                break
        except Exception as e:
            tried["sync"] = "error: " + str(e)[:120]
        if FEED_FIRESTORE_HISTORY:
            try:
                docs = fetch_user_history_firestore(s, uid)
                extra = {_moment_id(m) for m in docs} - {latest_id}
                tried["firestore"] = len(extra)
                if extra:
                    found = "firestore"
                    break
            except Exception as e:
                tried["firestore"] = "error: " + str(e)[:120]
    _HIST.update(method=found, tried=tried, none_at=time.time())
    logger.info("history method detected: %s %s", found, tried)
    return found


def fetch_history_page(s: LocketSession, page_token: Optional[str] = None,
                        page_size: int = FEED_HISTORY_PAGE, timeout: int = 20
                        ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    url = (f"https://firestore.googleapis.com/v1/projects/locket-4252a/databases/"
           f"{FEED_HISTORY_DB}/documents/history/{s.local_id}/entries")
    params: Dict[str, Any] = {"orderBy": "date desc", "pageSize": page_size}
    if page_token:
        params["pageToken"] = page_token
    refreshed = False
    r = None
    for attempt in range(LOCKET_READ_RETRIES + 1):
        headers = {
            "Authorization": f"Bearer {s.id_token}",
            "Accept": "application/json",
            "User-Agent": FIREBASE_AUTH_UA,
            "X-Ios-Bundle-Identifier": IOS_BUNDLE,
        }
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 401 and not refreshed:
            refreshed = True
            if refresh_id_token(s):
                continue
        if r.status_code in (429, 500, 502, 503, 504) and attempt < LOCKET_READ_RETRIES:
            time.sleep(0.5 * (attempt + 1))
            continue
        break
    if r is None or r.status_code >= 400:
        code = r.status_code if r is not None else "n/a"
        body = r.text[:160] if r is not None else ""
        raise LocketError(f"history list HTTP {code}: {body}")
    data = r.json()
    out: List[Dict[str, Any]] = []
    for doc in data.get("documents") or []:
        if not isinstance(doc, dict):
            continue
        flat = flatten_firestore(doc)
        if not isinstance(flat, dict):
            continue
        name = str(doc.get("name") or "")
        if name and not flat.get("canonical_uid"):
            flat["canonical_uid"] = name.rsplit("/", 1)[-1]
        moment = _feed_entry_to_moment(flat)
        if moment is not None:
            out.append(moment)
    return out, data.get("nextPageToken") or None


def fetch_deleted_moment_ids(s: LocketSession, limit: int = FEED_DELETED_LIMIT, timeout: int = 15) -> set:
    url = (f"https://firestore.googleapis.com/v1/projects/locket-4252a/databases/(default)/"
           f"documents/users/{s.local_id}:runQuery")
    body = {"structuredQuery": {
        "from": [{"collectionId": "deleted_moments"}],
        "orderBy": [{"field": {"fieldPath": "date"}, "direction": "DESCENDING"}],
        "limit": limit,
    }}
    headers = {"Authorization": f"Bearer {s.id_token}", "Accept": "application/json",
               "Content-Type": "application/json", "User-Agent": "okhttp/4.12.0"}
    r = requests.post(url, json=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    out: set = set()
    for row in r.json() if isinstance(r.json(), list) else []:
        doc = row.get("document") if isinstance(row, dict) else None
        if not isinstance(doc, dict):
            continue
        flat = flatten_firestore(doc)
        mid = flat.get("moment_uid") if isinstance(flat, dict) else None
        if not mid:
            mid = str(doc.get("name") or "").rsplit("/", 1)[-1]
        if mid:
            out.add(str(mid))
    return out


def fetch_user_doc_firestore(s: LocketSession, uid: str, timeout: int = 12) -> Dict[str, Any]:
    url = (f"https://firestore.googleapis.com/v1/projects/locket-4252a/databases/"
           f"(default)/documents/users/{uid}")
    r = requests.get(url, headers={"Authorization": f"Bearer {s.id_token}",
                                    "Accept": "application/json"}, timeout=timeout)
    r.raise_for_status()
    flat = flatten_firestore(r.json())
    return flat if isinstance(flat, dict) else {}


def get_moments_http(s: LocketSession, page_token=None, client_target: str = "all",
                      timeout: int = 35) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Legacy binhake HTTP getMoments — kept only as last-resort fallback."""
    debug: List[Dict[str, Any]] = []
    extra = {
        "id": None,
        "pageToken": page_token,
        "clientTarget": client_target,
        "_client": {"os": "Windows"},
    }
    debug.append(make_debug("send", "binhake:getMoments:http", "POST", BINHAKE_API,
                             {"action": "getMoments", **extra}))
    try:
        data, dbg = binhake_call("getMoments", s, extra=extra, timeout=timeout)
        debug.append(dbg)
        items = _extract_moments_list(data)
        if not items and isinstance(data, dict):
            keys = list(data.keys())
            sample = str(data)[:400]
            logger.warning("getMoments HTTP empty — keys=%s sample=%s", keys, sample)
        else:
            logger.info("getMoments HTTP → %d items", len(items))
        return items, debug
    except Exception as e:
        debug.append(make_debug("error", "binhake:getMoments:http", "POST", BINHAKE_API,
                                 None, None, None, None, e))
        logger.warning("getMoments HTTP failed: %s", e)
        raise


def get_moments_ws(s: LocketSession, page_token=None, client_target="all", timeout: int = 55
                    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """binhake serves moment history over WebSocket.

    Real browser flow (captured HAR 2026-08-14):
      auth → authenticated → getMoments
      server sends keep-alive {"oa":N} every ~10s
      client sends {"action":"ping"} every ~25s
      data arrives as action=getNewMoments ~25–35s after getMoments

    Old timeout of 25s closed the socket before data arrived → empty list.
    """
    if not websocket:
        raise LocketError("websocket-client is not installed (pip install websocket-client)")

    debug: List[Dict[str, Any]] = []
    result = {"frames": [], "done": threading.Event()}
    ws_holder = [None]
    last_app_ping = [0.0]
    opened_at = [0.0]

    def on_open(ws):
        ws_holder[0] = ws
        opened_at[0] = time.time()
        last_app_ping[0] = time.time()
        auth_frame = {"action": "auth", "token": s.id_token, "_client": {"os": "Windows"}}
        debug.append(make_debug("send", "ws:auth", "WS", WS_URL, {"action": "auth", "token": "***"}))
        ws.send(json.dumps(auth_frame))

    def on_message(ws, message):
        try:
            data = json.loads(message)
        except Exception as e:
            debug.append(make_debug("error", "ws:message", "WS", WS_URL, None, None, message[:500], None, e))
            return

        # Heartbeat {"oa": 60} — ignore, do not treat as error / done
        if isinstance(data, dict) and "oa" in data and "action" not in data:
            return

        action = data.get("action")
        debug.append(make_debug("recv", f"ws:{action}", "WS", WS_URL, None, None,
                                 json.dumps(data, ensure_ascii=False)[:1500]))

        if action == "authenticated":
            req = {"action": "getMoments", "id": None, "pageToken": page_token,
                   "clientTarget": client_target, "_client": {"os": "Windows"}}
            debug.append(make_debug("send", "ws:getMoments", "WS", WS_URL, req))
            ws.send(json.dumps(req))
            return

        if action == "pong":
            return

        if action in ("getMoments", "getNewMoments"):
            result["frames"].append(data)
            # Only finish when we got a list payload (even empty). Prefer waiting a
            # little longer if first frame is empty — server sometimes sends an
            # empty getMoments then a filled getNewMoments.
            raw = data.get("data")
            has_list = isinstance(raw, list)
            has_items = has_list and len(raw) > 0
            if has_items or (has_list and action == "getNewMoments"):
                result["done"].set()
                try:
                    ws.close()
                except Exception:
                    pass
            return

        if action == "error" or data.get("error"):
            result["done"].set()
            try:
                ws.close()
            except Exception:
                pass

    def on_error(ws, error):
        debug.append(make_debug("error", "ws:error", "WS", WS_URL, None, None, None, None, error))
        result["done"].set()

    def on_close(ws, code, msg):
        result["done"].set()

    ws = websocket.WebSocketApp(
        WS_URL,
        header=["Origin: https://locket.binhake.dev", f"User-Agent: {s.ua}", f"Cookie: user_id={s.local_id}"],
        on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close,
    )
    # protocol-level pings + our app-level ping loop (binhake expects action=ping)
    wst = threading.Thread(
        target=lambda: ws.run_forever(ping_interval=20, ping_timeout=10),
        daemon=True,
    )
    wst.start()

    deadline = time.time() + max(15, timeout)
    while time.time() < deadline and not result["done"].is_set():
        result["done"].wait(timeout=1.0)
        # app-level ping every 25s (matches binhake posts.html)
        w = ws_holder[0]
        if w and (time.time() - last_app_ping[0]) >= 25:
            try:
                w.send(json.dumps({"action": "ping", "_client": {"os": "Windows"}}))
                last_app_ping[0] = time.time()
                debug.append(make_debug("send", "ws:ping", "WS", WS_URL, {"action": "ping"}))
            except Exception:
                break

    if ws_holder[0]:
        try:
            ws_holder[0].close()
        except Exception:
            pass
    wst.join(timeout=2)

    # Merge all frames that carried data
    items: List[Dict[str, Any]] = []
    seen = set()
    for frame in result["frames"]:
        raw = frame.get("data")
        if not isinstance(raw, list):
            raw = [raw] if raw else []
        for item in raw:
            if not isinstance(item, dict):
                continue
            flat = flatten_firestore(item)
            k = _moment_key(flat)
            if k in seen:
                continue
            seen.add(k)
            items.append(flat)

    if not items and result["frames"]:
        logger.warning("moments WS frames=%d but no items (timeout=%.0fs)", len(result["frames"]), timeout)
    elif not result["frames"]:
        logger.warning("moments WS returned no frames within %.0fs", timeout)
    else:
        logger.info("moments WS ok: %d items from %d frames", len(items), len(result["frames"]))
    return items, debug


def _moment_key(m: Dict[str, Any]) -> str:
    cid = m.get("canonical_uid") or m.get("id")
    if cid:
        return str(cid)
    name = m.get("name")
    if name:
        return str(name).rsplit("/", 1)[-1]
    key = m.get("md5") or m.get("thumbnail_url") or m.get("url") or m.get("video_url")
    if key:
        return str(key)
    d = m.get("date") or {}
    ts = (d.get("_seconds") if isinstance(d, dict) else None) or m.get("timestamp") or ""
    uid = m.get("user") if isinstance(m.get("user"), str) else (m.get("user") or {}).get("uid") or ""
    return f"{uid}:{ts}"


def _splice_feed(existing: List[Dict[str, Any]], fresh: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not fresh:
        return existing
    floor = min(_moment_timestamp(m) for m in fresh)
    older = [m for m in existing if _moment_timestamp(m) < floor]
    return _merge_moments(fresh, older)


def _merge_moments(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = {_moment_key(m) for m in existing}
    out = list(existing)
    for m in incoming:
        k = _moment_key(m)
        if k not in seen:
            seen.add(k)
            out.append(m)
    out.sort(key=_moment_timestamp, reverse=True)
    return out


def _live_moments_loop(s: LocketSession, state: Dict[str, Any]):
    """Long-lived WS: auth once, then keep listening for getNewMoments frames."""
    if not websocket:
        return
    stop: threading.Event = state["stop"]

    def on_open(ws):
        state["ws"] = ws
        ws.send(json.dumps({"action": "auth", "token": s.id_token, "_client": {"os": "Windows"}}))
        logger.info("LIVE WS open for %s", s.local_id[:8])

    def on_message(ws, message):
        try:
            data = json.loads(message)
        except Exception:
            return
        # Keep-alive {"oa": N} from binhake — ignore
        if isinstance(data, dict) and "oa" in data and "action" not in data:
            return
        action = data.get("action")
        if action == "authenticated":
            req = {"action": "getMoments", "id": None, "pageToken": None,
                   "clientTarget": "all", "_client": {"os": "Windows"}}
            ws.send(json.dumps(req))
            state["last_ws_pull"] = time.time()
            state["last_app_ping"] = time.time()
        elif action == "pong":
            return
        elif action in ("getMoments", "getNewMoments"):
            raw = data.get("data")
            if not isinstance(raw, list):
                raw = [raw] if raw else []
            items = [flatten_firestore(x) for x in raw if isinstance(x, dict)]
            if items:
                with state["lock"]:
                    state["items"] = _merge_moments(state.get("items") or [], items)
                    state["updated_at"] = time.time()
                    state["last_fetch_at"] = time.time()
                    state["bootstrapped"] = True
                logger.info("LIVE WS +%d moments for %s (total %d)",
                            len(items), s.local_id[:8], len(state["items"]))
        elif action == "error" or data.get("error"):
            logger.warning("LIVE WS error frame: %s", _snip(str(data), 200))

    def on_error(ws, error):
        logger.warning("LIVE WS error %s: %s", s.local_id[:8], error)

    def on_close(ws, code, msg):
        logger.info("LIVE WS closed %s code=%s", s.local_id[:8], code)
        state["ws"] = None

    while not stop.is_set():
        try:
            # refresh token periodically so long-lived WS can re-auth
            refresh_id_token(s)
            ws = websocket.WebSocketApp(
                WS_URL,
                header=["Origin: https://locket.binhake.dev", f"User-Agent: {s.ua}",
                        f"Cookie: user_id={s.local_id}"],
                on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close,
            )
            state["ws"] = ws
            wst = threading.Thread(target=lambda: ws.run_forever(ping_interval=25, ping_timeout=8), daemon=True)
            wst.start()
            # stay up until stop or socket dies; every ~45s re-pull getMoments so we
            # recover moments missed when the server skipped a push frame
            while not stop.is_set() and wst.is_alive():
                stop.wait(5)
                try:
                    now = time.time()
                    # App-level ping (binhake posts.html does this every ~25s)
                    last_ping = state.get("last_app_ping") or 0
                    if now - last_ping >= 25 and state.get("ws"):
                        state["last_app_ping"] = now
                        state["ws"].send(json.dumps({"action": "ping", "_client": {"os": "Windows"}}))
                    last_pull = state.get("last_ws_pull") or 0
                    if now - last_pull >= 45 and state.get("ws"):
                        state["last_ws_pull"] = now
                        state["ws"].send(json.dumps({
                            "action": "getMoments", "id": None, "pageToken": None,
                            "clientTarget": "all", "_client": {"os": "Windows"},
                        }))
                except Exception as e:
                    logger.debug("LIVE WS periodic pull skip: %s", e)
            try:
                ws.close()
            except Exception:
                pass
            wst.join(timeout=3)
        except Exception as e:
            logger.warning("LIVE WS loop exception: %s", e)
        if not stop.is_set():
            stop.wait(8)  # reconnect backoff


def _history_path(local_id: str) -> str:
    safe = "".join(c for c in local_id if c.isalnum())[:40] or "anon"
    return os.path.join(FEED_HISTORY_DIR, f"locket_feed_{safe}.json")


def _history_load(local_id: str) -> Tuple[List[Dict[str, Any]], set]:
    try:
        with open(_history_path(local_id), encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return [], set()
    raw_items = data.get("items") if isinstance(data, dict) else data
    done = set(data.get("done") or []) if isinstance(data, dict) else set()
    items = [m for m in raw_items or [] if isinstance(m, dict)
             and m.get("source") == "official"
             and (m.get("thumbnail_url") or m.get("video_url"))]
    items.sort(key=_moment_timestamp, reverse=True)
    return items[:FEED_MAX_ITEMS], done


def _history_save(local_id: str, items: List[Dict[str, Any]], done: set) -> None:
    keep = [m for m in items if m.get("source") == "official"][:FEED_MAX_ITEMS]
    path = _history_path(local_id)
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"items": keep, "done": sorted(done)}, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        logger.debug("history save skipped: %s", e)


def _ensure_moments_state(local_id: str) -> Dict[str, Any]:
    with _MOMENTS_LOCK:
        st = _MOMENTS_CACHE.get(local_id)
        if st is None:
            saved_items, saved_done = _history_load(local_id)
            st = {
                "items": saved_items,
                "done_uids": saved_done,
                "window": FEED_WINDOW_START,
                "lock": threading.Lock(),
                "stop": threading.Event(),
                "thread": None,
                "ws": None,
                "updated_at": 0,
                "bootstrapped": False,
                "fetching": False,
                "last_fetch_at": 0,
                "last_crawl_at": 0.0,
                "source": "",
                "warnings": [],
                "diag": {},
                "uids": [],
                "uids_at": 0.0,
                "sync_token": None,
                "deleted_ids": set(),
                "hist_ok": None,
                "hist_fail_at": 0.0,
                "hist_next": None,
                "hist_started": False,
            }
            if st["items"]:
                st["source"] = "official"
            _MOMENTS_CACHE[local_id] = st
        return st


def _binhake_feed_fallback(s: LocketSession, timeout: int) -> List[Dict[str, Any]]:
    raw_items, _ = get_moments_http(s, timeout=min(30, max(12, timeout)))
    out: List[Dict[str, Any]] = []
    for m in raw_items:
        if not isinstance(m, dict):
            continue
        flat = flatten_firestore(m) if "fields" in m else m
        norm = _normalize_moment_official(flat)
        if norm.get("thumbnail_url") or norm.get("video_url"):
            norm["source"] = "binhake"
            out.append(norm)
    out.sort(key=_moment_timestamp, reverse=True)
    return out


def _friend_uids(s: LocketSession, st: Dict[str, Any]) -> List[str]:
    now = time.time()
    with st["lock"]:
        if st["uids"] and now - st["uids_at"] < FEED_UIDS_TTL_SEC:
            return list(st["uids"])
        previous = list(st["uids"])
    try:
        uids, _ = list_friend_uids_firestore(s)
    except Exception as e:
        logger.warning("friend uid list failed: %s", e)
        uids = []
    if uids:
        with st["lock"]:
            st["uids"] = list(uids)
            st["uids_at"] = now
        return list(uids)
    return previous


def _moment_id(m: Dict[str, Any]) -> str:
    return str(m.get("canonical_uid") or m.get("id") or "")


def _fetch_and_store_moments(s: LocketSession, st: Dict[str, Any], timeout: int = 55
                              ) -> List[Dict[str, Any]]:
    with st["lock"]:
        started_here = not st.get("fetching")
        if started_here:
            st["fetching"] = True

    if not started_here:
        deadline = time.time() + 15
        while time.time() < deadline:
            with st["lock"]:
                if not st.get("fetching"):
                    return list(st.get("items") or [])
            time.sleep(0.2)
        with st["lock"]:
            return list(st.get("items") or [])

    now = time.time()
    warnings: List[str] = []
    diag: Dict[str, Any] = {"at": now, "header_profile": _LOCKET_HEADER_PROFILE["name"]}
    crawled: List[Dict[str, Any]] = []
    history_items: List[Dict[str, Any]] = []
    empty_uids: set = set()
    crawl_ok = False
    fallback: List[Dict[str, Any]] = []

    try:
        with st["lock"]:
            last_crawl = st["last_crawl_at"]
            known_ids = {_moment_id(m) for m in st["items"]}
            token = st["sync_token"]
            have_items = bool(st["items"])
        with st["lock"]:
            hist_ok = st["hist_ok"]
            hist_failed_recently = now - st["hist_fail_at"] < FEED_HISTORY_RETRY_SEC
        if FEED_USE_HISTORY and not (hist_ok is False and hist_failed_recently):
            try:
                page, nxt = fetch_history_page(s, None, FEED_HISTORY_PAGE)
                try:
                    with st["lock"]:
                        st["deleted_ids"] |= fetch_deleted_moment_ids(s)
                except Exception as de:
                    diag["deleted_error"] = str(de)[:120]
                with st["lock"]:
                    dropped = st["deleted_ids"]
                page = [m for m in page if _moment_id(m) not in dropped]
                diag["deleted_known"] = len(dropped)
                history_items = page
                with st["lock"]:
                    st["hist_ok"] = True
                    if not st["hist_started"]:
                        st["hist_next"] = nxt
                        st["hist_started"] = True
                diag["history"] = "ok"
                diag["history_page"] = len(page)
                diag["self_in_history"] = any(_moment_author(m) == s.local_id for m in page)
            except Exception as e:
                history_items = []
                with st["lock"]:
                    st["hist_ok"] = False
                    st["hist_fail_at"] = time.time()
                diag["history"] = "failed"
                diag["history_error"] = str(e)[:200]
                warnings.append(f"Firestore history unavailable, using per-friend fallback: {str(e)[:120]}")
        age = now - last_crawl if last_crawl else None
        use_history = bool(history_items) or diag.get("history") == "ok"
        need_crawl = (not use_history) and ((not have_items) or age is None or age > FEED_RECRAWL_SEC)
        if not use_history and not need_crawl and age > FEED_PRECHECK_MIN_SEC:
            try:
                raw, _ = _official_feed_page(s, token, [], quiet=True, timeout=15)
                newer = [e for e in raw if isinstance(e, dict) and _moment_id(e) not in known_ids]
                diag["precheck_new"] = len(newer)
                need_crawl = bool(newer)
            except Exception as e:
                diag["precheck_error"] = str(e)
                need_crawl = True
        diag["crawled"] = need_crawl

        if need_crawl:
            uids = _friend_uids(s, st)
            diag["friends"] = len(uids)
            result = crawl_latest_per_user(s, uids)
            crawled = result["items"]
            diag.update({
                "crawl_targets": result["targets"],
                "crawl_found": len(crawled),
                "crawl_empty": result["empty"],
                "crawl_skipped": result["skipped"],
                "crawl_errors": len(result["errors"]),
                "crawl_ms": int(result["elapsed"] * 1000),
                "self_found": any(_moment_author(m) == s.local_id for m in crawled),
            })
            if result["errors"]:
                warnings.append(
                    f"{len(result['errors'])} of {result['targets']} official lookups failed: {result['errors'][0][:120]}")
            crawl_ok = bool(crawled) or not result["errors"]
            empty_uids = set(result["empty_uids"])
            if not crawled and result["errors"]:
                with st["lock"]:
                    have_official = any(m.get("source") == "official" for m in st["items"])
                if not have_official:
                    fallback = _binhake_feed_fallback(s, timeout)
                    if fallback:
                        warnings.append("Official lookups failed, showing binhake fallback (latest moment per friend)")
    except Exception as e:
        warnings.append(f"Feed refresh failed: {e}")
        logger.error("feed refresh failed: %s", e)

    with st["lock"]:
        before_ids = {_moment_id(m) for m in st["items"]}
        st["fetching"] = False
        st["last_fetch_at"] = time.time()
        st["warnings"] = warnings
        st["diag"] = {**st.get("diag", {}), **diag}
        st["done_uids"] |= empty_uids
        if history_items:
            crawled = history_items
            crawl_ok = True
        if st["deleted_ids"]:
            st["items"] = [m for m in st["items"] if _moment_id(m) not in st["deleted_ids"]]
        if crawled:
            kept = [m for m in st["items"] if m.get("source") != "binhake"]
            st["items"] = _merge_moments(kept, crawled)[:FEED_MAX_ITEMS]
            st["source"] = "official"
            st["sync_token"] = _moment_id(max(st["items"], key=_moment_timestamp))
        elif fallback and not any(m.get("source") == "official" for m in st["items"]):
            st["items"] = fallback
            st["source"] = "binhake"
        if crawl_ok:
            st["last_crawl_at"] = time.time()
        after_ids = {_moment_id(m) for m in st["items"]}
        if after_ids != before_ids:
            st["updated_at"] = time.time()
        st["bootstrapped"] = True
        out = list(st["items"])
        changed = after_ids != before_ids
    if changed:
        _history_save(s.local_id, out, st["done_uids"])
    return out


def _author_stats(items: List[Dict[str, Any]]) -> Dict[str, Tuple[int, float, str]]:
    stats: Dict[str, Tuple[int, float, str]] = {}
    for m in items:
        uid = _moment_author(m)
        if not uid:
            continue
        ts = _moment_timestamp(m)
        count, oldest_ts, oldest_id = stats.get(uid, (0, float("inf"), ""))
        if ts < oldest_ts:
            oldest_ts, oldest_id = ts, _moment_id(m)
        stats[uid] = (count + 1, oldest_ts, oldest_id)
    return stats


def _needy_friends(st: Dict[str, Any]) -> List[Tuple[str, str, float]]:
    items = sorted(st["items"], key=_moment_timestamp, reverse=True)
    window = st["window"]
    cut = _moment_timestamp(items[window - 1]) if len(items) >= window else 0.0
    needy = []
    for uid, (count, oldest_ts, oldest_id) in _author_stats(items).items():
        if uid in st["done_uids"] or count >= FEED_PER_FRIEND or oldest_ts <= cut:
            continue
        needy.append((uid, oldest_id, oldest_ts))
    needy.sort(key=lambda x: x[2], reverse=True)
    return needy


def feed_has_more(st: Dict[str, Any]) -> bool:
    with st["lock"]:
        if st["hist_ok"]:
            return bool(st["hist_next"]) or len(_apply_cap(st["items"])) > st["window"]
        if len(st["items"]) > st["window"]:
            return True
        if _HIST["method"] == "none":
            return False
        stats = _author_stats(st["items"])
        return any(uid not in st["done_uids"] and count < FEED_PER_FRIEND
                   for uid, (count, _, _) in stats.items())


def deepen_feed(s: LocketSession, st: Dict[str, Any], budget: float = FEED_DEEPEN_BUDGET_SEC
                ) -> Dict[str, Any]:
    with st["lock"]:
        use_history = bool(st["hist_ok"])
    if use_history:
        return deepen_history(s, st, budget)
    t0 = time.time()
    rounds = added = 0
    calls = 0
    before_ids: set = set()
    with st["lock"]:
        before_ids = {_moment_id(m) for m in st["items"]}
        targets = list(st["uids"]) + [s.local_id]
    while rounds < FEED_MAX_ROUNDS and time.time() - t0 < budget:
        with st["lock"]:
            needy = _needy_friends(st)
        if not needy:
            break
        method = detect_history_method(s, [(u, i) for u, i, _ in needy], targets)
        if method == "none":
            break
        rounds += 1

        def work(entry: Tuple[str, str, float]):
            uid, oldest_id, _ = entry
            if time.time() - t0 > budget:
                return uid, None
            try:
                if method == "firestore":
                    return uid, fetch_user_history_firestore(s, uid, FEED_PER_FRIEND)
                return uid, fetch_older_via_sync(s, uid, targets, oldest_id)
            except Exception as e:
                logger.debug("deepen %s failed: %s", uid[:8], e)
                return uid, []

        results: List[Tuple[str, Optional[List[Dict[str, Any]]]]] = []
        with ThreadPoolExecutor(max_workers=FEED_CRAWL_WORKERS) as pool:
            results = list(pool.map(work, needy))
        calls += len(needy)
        gained: List[Dict[str, Any]] = []
        with st["lock"]:
            known = {_moment_id(m) for m in st["items"]}
            for uid, got in results:
                if got is None:
                    continue
                fresh = [m for m in got if _moment_id(m) not in known]
                if not fresh or method == "firestore":
                    st["done_uids"].add(uid)
                gained.extend(fresh)
            if gained:
                st["items"] = _merge_moments(st["items"], gained)[:FEED_MAX_ITEMS]
        added += len(gained)
        if not gained and method == "sync":
            continue
    with st["lock"]:
        after_ids = {_moment_id(m) for m in st["items"]}
        changed = after_ids != before_ids
        if changed:
            st["updated_at"] = time.time()
        out = list(st["items"])
        done = set(st["done_uids"])
        st["diag"] = {**st["diag"], "history_method": _HIST["method"], "history_tried": dict(_HIST["tried"]),
                      "deepen_rounds": rounds, "deepen_calls": calls, "deepen_added": added,
                      "deepen_ms": int((time.time() - t0) * 1000), "window": st["window"],
                      "items_total": len(st["items"])}
    if changed:
        _history_save(s.local_id, out, done)
    return {"rounds": rounds, "added": added, "calls": calls}


def _apply_cap(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = sorted(items, key=_moment_timestamp, reverse=True)
    if FEED_PER_FRIEND <= 0:
        return ordered
    seen: Dict[str, int] = {}
    out: List[Dict[str, Any]] = []
    for m in ordered:
        uid = _moment_author(m)
        seen[uid] = seen.get(uid, 0) + 1
        if seen[uid] <= FEED_PER_FRIEND:
            out.append(m)
    return out


def moments_window(st: Dict[str, Any], items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _apply_cap(items)[:st["window"]]


def deepen_history(s: LocketSession, st: Dict[str, Any], budget: float) -> Dict[str, Any]:
    t0 = time.time()
    rounds = added = 0
    error: Optional[str] = None
    while rounds < FEED_MAX_ROUNDS and time.time() - t0 < budget:
        with st["lock"]:
            token = st["hist_next"]
            enough = len(_apply_cap(st["items"])) >= st["window"]
        if enough or not token:
            break
        try:
            page, nxt = fetch_history_page(s, token, FEED_HISTORY_PAGE_MORE)
        except Exception as e:
            error = str(e)[:200]
            break
        rounds += 1
        with st["lock"]:
            st["hist_next"] = nxt
            before = len(st["items"])
            page = [m for m in page if _moment_id(m) not in st["deleted_ids"]]
            st["items"] = _merge_moments(st["items"], page)[:FEED_MAX_ITEMS]
            added += len(st["items"]) - before
    with st["lock"]:
        if added:
            st["updated_at"] = time.time()
        out = list(st["items"])
        done = set(st["done_uids"])
        st["diag"] = {**st["diag"], "history_rounds": rounds, "history_added": added,
                      "history_more_error": error, "history_has_next": bool(st["hist_next"]),
                      "deepen_ms": int((time.time() - t0) * 1000), "window": st["window"],
                      "items_total": len(st["items"])}
    if added:
        _history_save(s.local_id, out, done)
    return {"rounds": rounds, "added": added, "calls": rounds}


def get_moments_cached(s: LocketSession, force: bool = False) -> List[Dict[str, Any]]:
    st = _ensure_moments_state(s.local_id)
    with st["lock"]:
        has_items = bool(st.get("items"))
        last = st.get("last_fetch_at") or 0
        if st.get("bootstrapped") and not force and has_items and (time.time() - last) < 25:
            return list(st["items"])
    return _fetch_and_store_moments(s, st, timeout=55)


def poll_new_moments(s: LocketSession, since: float = 0) -> Tuple[List[Dict[str, Any]], float]:
    st = _ensure_moments_state(s.local_id)
    now = time.time()
    with st["lock"]:
        last = st.get("last_fetch_at") or 0
        has_data = bool(st.get("items"))
        fetching = bool(st.get("fetching"))
    if not fetching and (not has_data or now - last > FEED_PRECHECK_MIN_SEC + 25):
        try:
            get_moments_cached(s, force=True)
        except Exception as e:
            logger.warning("poll refresh failed: %s", e)
    with st["lock"]:
        updated = st.get("updated_at") or 0
        items = list(st.get("items") or [])
        if updated <= since:
            return [], updated
        return items, updated


def stop_moments_live(local_id: str):
    with _MOMENTS_LOCK:
        st = _MOMENTS_CACHE.pop(local_id, None)
    if not st:
        return
    st["stop"].set()
    ws = st.get("ws")
    if ws:
        try:
            ws.close()
        except Exception:
            pass


def _random_storage_name(ext: str = "webp") -> str:
    # Official client uses 20-char alphanumeric ids (e.g. f1q6Vca6JuumOlGtunpQ.webp)
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(alphabet[b % len(alphabet)] for b in os.urandom(20)) + f".{ext}"


def _locket_api_headers(s: LocketSession) -> Dict[str, str]:
    # Shape matches Android 1.237.0 HAR (okhttp). Firebase-Instance-ID-Token is
    # present on every real client call; a stable placeholder is enough for
    # postMomentV2 (auth is the Bearer idToken).
    return {
        "Authorization": f"Bearer {s.id_token}",
        "Content-Type": "application/json; charset=utf-8",
        "Accept-Encoding": "gzip",
        "User-Agent": "okhttp/4.12.0",
        "Connection": "Keep-Alive",
        "Firebase-Instance-ID-Token": f"web_{s.local_id[:16]}:locket-mini",
    }


def _firebase_storage_headers(s: LocketSession, content_type: str) -> Dict[str, str]:
    # App Check failed on the captured emulator but uploads still succeeded with
    # this placeholder token — include it so the request shape matches production.
    return {
        "Authorization": f"Firebase {s.id_token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": IOS_UA,
        "X-Firebase-Storage-Version": "Android/21.0.1",
        "X-Goog-Upload-Command": "start",
        "X-Goog-Upload-Header-Content-Type": content_type,
        "X-Goog-Upload-Protocol": "resumable",
        "x-firebase-gmpid": FIREBASE_GMPID,
        "x-firebase-appcheck": "eyJlcnJvciI6IlVOS05PV05fRVJST1IifQ==",
    }


def _to_webp_or_jpeg(data: bytes, content_type: str) -> Tuple[bytes, str, str]:
    """Prefer webp (matches official Android client). Fall back to JPEG."""
    ct = (content_type or "").lower()
    if "webp" in ct and data[:4] == b"RIFF":
        return data, "image/webp", "webp"
    try:
        img = Image.open(io.BytesIO(data))
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        # Official moments are square thumbs ~1080; keep our compress size.
        w, h = img.size
        if w != h:
            side = min(w, h)
            left, top = (w - side) // 2, (h - side) // 2
            img = img.crop((left, top, left + side, top + side))
        max_side = globals().get('_LOCKET_MAX_SIDE', 1080)
        if img.size[0] > max_side:
            img = img.resize((max_side, max_side), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="WEBP", quality=95, method=5)
        out = buf.getvalue()
        if out:
            return out, "image/webp", "webp"
    except Exception as e:
        logger.debug("webp convert skipped: %s", e)
    # JPEG path
    if data[:3] == b"\xff\xd8\xff":
        return data, "image/jpeg", "jpg"
    try:
        img = Image.open(io.BytesIO(data)).convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=88, optimize=True)
        return buf.getvalue(), "image/jpeg", "jpg"
    except Exception:
        return data, content_type or "application/octet-stream", "bin"


def upload_to_firebase_storage(s: LocketSession, data: bytes, content_type: str,
                                object_name: str, timeout: int = 60
                                ) -> Tuple[str, str, List[Dict[str, Any]]]:
    """Resumable upload to locket-img. Returns (download_url, md5_hex, debug).

    Flow from Android HAR:
      POST .../o?name=...&uploadType=resumable  (X-Goog-Upload-Command: start)
      POST same URL with upload_id             (upload, finalize) + binary body
      Response carries downloadTokens → public URL with ?alt=media&token=...
    """
    debug: List[Dict[str, Any]] = []
    from urllib.parse import quote
    name_q = quote(object_name, safe="")
    start_url = f"{FIREBASE_STORAGE_BASE}?name={name_q}&uploadType=resumable"
    headers = _firebase_storage_headers(s, content_type)
    meta = {"contentType": content_type, "cacheControl": "public, max-age=604800"}

    debug.append(make_debug("send", "storage:start", "POST", start_url,
                             {"name": object_name, "contentType": content_type}))
    t0 = time.time()
    r = requests.post(start_url, headers=headers, json=meta, timeout=timeout)
    dt = time.time() - t0
    console_log("STORAGE START", "POST", start_url, meta, r.status_code, r.text, dt)
    debug.append(make_debug("recv" if r.ok else "error", "storage:start", "POST", start_url,
                             None, r.status_code, r.text, dt))
    r.raise_for_status()
    upload_url = r.headers.get("X-Goog-Upload-URL") or r.headers.get("x-goog-upload-url")
    if not upload_url:
        raise LocketError("Firebase Storage did not return X-Goog-Upload-URL")

    put_headers = {
        "Authorization": f"Firebase {s.id_token}",
        "Content-Type": content_type,
        "Content-Length": str(len(data)),
        "X-Goog-Upload-Command": "upload, finalize",
        "X-Goog-Upload-Offset": "0",
        "X-Goog-Upload-Protocol": "resumable",
        "User-Agent": IOS_UA,
    }
    debug.append(make_debug("send", "storage:upload", "POST", upload_url,
                             {"bytes": len(data), "contentType": content_type}))
    t1 = time.time()
    r2 = requests.post(upload_url, headers=put_headers, data=data, timeout=timeout)
    dt2 = time.time() - t1
    console_log("STORAGE UPLOAD", "POST", upload_url[:120],
                {"bytes": len(data)}, r2.status_code, r2.text[:400], dt2)
    debug.append(make_debug("recv" if r2.ok else "error", "storage:upload", "POST", upload_url,
                             None, r2.status_code, r2.text, dt2))
    r2.raise_for_status()
    info = r2.json() if r2.text else {}
    token = info.get("downloadTokens") or ""
    if not token:
        raise LocketError("Storage upload missing downloadTokens")
    # Public media URL (same shape official postMomentV2 expects)
    media_url = (
        f"https://firebasestorage.googleapis.com/v0/b/{STORAGE_BUCKET}/o/"
        f"{name_q}?alt=media&token={token}"
    )
    md5_hex = hashlib.md5(data).hexdigest()
    return media_url, md5_hex, debug


def _locket_analytics() -> Dict[str, Any]:
    """Minimal analytics block matching Android 1.237.0 postMomentV2 HAR.
    Server ignores most fields; omitting the whole object caused intermittent 4xx
    on some builds, so we always send a stable shape."""
    return {
        "google_analytics": {"app_instance_id": "locket-mini-web"},
        "amplitude": {
            "device_id": "locket-mini-web",
            "session_id": int(time.time() * 1000),
        },
        "android_version": "1.237.0",
        "android_build": "584",
        "platform": "android",
        "experiments": {
            "android_flag_2": 800,
            "android_flag_1": 0,
            "android_flag_9": 0,
            "android_flag_8": 807,
            "android_flag_7": 200,
            "android_flag_6": 200,
            "android_flag_5": 403,
            "android_flag_4": 100,
            "android_flag_10": 0,
            "android_flag_3": 0,
        },
    }


def post_moment_v2(s: LocketSession, thumbnail_url: str, md5_hex: str,
                   caption: str = "", recipients: Optional[List[str]] = None,
                   timeout: int = 30) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Official Locket post endpoint (api.locketcamera.com/postMomentV2).

    Payload shape captured from Android 1.237.0 (2026-08-26 HAR):
      data.thumbnail_url  — public Firebase Storage URL (?alt=media&token=…)
      data.recipients     — [] = all friends
      data.overlays       — caption overlay (type=standard) when text present
      data.md5            — md5 hex of uploaded bytes
      data.analytics      — required-ish wrapper the real client always sends
    """
    debug: List[Dict[str, Any]] = []
    overlays: List[Dict[str, Any]] = []
    cap = (caption or "").strip()
    if cap:
        overlays.append({
            "overlay_id": "caption:standard",
            "overlay_type": "caption",
            "data": {
                "text_color": "#FFFFFFE6",
                "text": cap,
                "type": "standard",
                "max_lines": 4,
                "background": {"colors": [], "material_blur": "regular"},
            },
            "alt_text": cap,
        })
    payload = {
        "data": {
            "analytics": _locket_analytics(),
            "thumbnail_url": thumbnail_url,
            "recipients": recipients if recipients is not None else [],
            "overlays": overlays,
            "md5": md5_hex,
        }
    }
    url = f"{LOCKET_API}/postMomentV2"
    headers = _locket_api_headers(s)
    debug.append(make_debug("send", "locket:postMomentV2", "POST", url,
                             {**payload, "data": {**payload["data"], "thumbnail_url": thumbnail_url[:80] + "…"}}))
    t0 = time.time()
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    dt = time.time() - t0
    console_log("LOCKET postMomentV2", "POST", url,
                {"md5": md5_hex, "caption": cap[:40], "thumb": thumbnail_url[:80]},
                r.status_code, r.text, dt)
    debug.append(make_debug("recv" if r.ok else "error", "locket:postMomentV2", "POST", url,
                             None, r.status_code, r.text, dt))
    r.raise_for_status()
    return r.json(), debug


def upload_media_official(s: LocketSession, data: bytes, content_type: str = "image/jpeg",
                           caption: str = "", timeout: int = 90
                           ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Photo post via official Firebase Storage + postMomentV2 (binhake fallback)."""
    debug: List[Dict[str, Any]] = []
    blob, ct, ext = _to_webp_or_jpeg(data, content_type)
    object_name = f"users/{s.local_id}/moments/thumbnails/{_random_storage_name(ext)}"
    media_url, md5_hex, d1 = upload_to_firebase_storage(s, blob, ct, object_name, timeout=timeout)
    debug.extend(d1)
    result, d2 = post_moment_v2(s, media_url, md5_hex, caption=caption, timeout=min(30, timeout))
    debug.extend(d2)
    return result, debug


def upload_media(s: LocketSession, data: bytes, filename: str, content_type: str,
                  caption: str = "", recipients: str = "all", timeout: int = 90,
                  thumb_data: Optional[bytes] = None, thumb_name: Optional[str] = None,
                  thumb_type: Optional[str] = None, crop_payload: Optional[str] = None,
                  ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """action=uploadMedia — binhake expects BOTH multipart fields:
    - thumb  (preview / square jpeg)
    - media  (full media; for photos same bytes as thumb)
    Missing either field returns MISSING_FIELDS 400.

    crop_payload: JSON string matching binhake's own web client, e.g. for video
    {"type":"video","crop":{"x":..,"y":..,"w":..,"h":..},"video":{"videoWidth":..,"videoHeight":..},"view":{"scale":1}}
    (captured from a real browser upload — see HAR notes). Falls back to "null"
    when not supplied, which is what photos already used successfully.
    """
    debug = []
    form = {
        "action": "uploadMedia",
        "captionID": "defaultCaption",
        "captionText": caption or "",
        "payload": "null",
        "mediaCropPayload": crop_payload if crop_payload else "null",
        "recipients": recipients,
        "mode": '{"restoreToggle":false,"restoreDate":null,"exceptGroupToggle":false}',
    }
    # Real binhake upload (HAR): sends both "thumb" and "media" as binary parts.
    t_data = thumb_data if thumb_data is not None else data
    t_name = thumb_name or filename
    t_type = thumb_type or content_type
    files = {
        "thumb": (t_name, t_data, t_type),
        "media": (filename, data, content_type),
    }
    h = _web_headers(s.id_token, s.ua)
    h.pop("Content-Type", None)
    cookies = {"user_id": s.local_id}

    debug.append(make_debug("send", "binhake:uploadMedia", "POST", BINHAKE_API,
                             {**form, "thumb": f"<{len(t_data)}b {t_type}>",
                              "media": f"<{len(data)}b {content_type}>"}))
    t0 = time.time()
    r = requests.post(BINHAKE_API, headers=h, data=form, files=files, cookies=cookies, timeout=timeout)
    dt = time.time() - t0
    console_log("LOCK uploadMedia", "POST", "api://lock/uploadMedia",
                {**form, "thumb": t_name, "media": filename}, r.status_code, r.text, dt)
    debug.append(make_debug("recv" if r.ok else "error", "binhake:uploadMedia", "POST", BINHAKE_API,
                             None, r.status_code, r.text, dt))
    r.raise_for_status()
    try:
        return r.json(), debug
    except ValueError:
        raise LocketError("uploadMedia returned non-JSON body")


# Locket-style limits: square ~1080, keep visual quality high, shrink if over size cap
_LOCKET_MAX_SIDE = 1080
_LOCKET_MAX_BYTES = 1_500_000  # ~1.5MB soft limit before quality steps down
# Ngân sách thực tế (chỉnh bằng biến môi trường). Locket không công bố giới hạn;
# 1080px vuông là kích thước của app chính thức, nên giữ nguyên kích thước và dồn chất lượng vào encode.
_LOCKET_IMG_BUDGET = int(os.environ.get("LOCKET_IMG_MAX_BYTES", "2000000"))
# Vercel giới hạn body request 4.5MB -> video phải nhỏ hơn mức này (đã trừ phần multipart/thumb).
_VIDEO_MAX_BYTES = int(os.environ.get("LOCKET_VIDEO_MAX_BYTES", "3800000"))


def _new_upload_name() -> str:
    return f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}.jpg"


def compress_image(raw_bytes: bytes) -> Tuple[bytes, str]:
    """Crop to 1:1, resize to 1080, JPEG with high quality. Step quality down only if over size limit.

    Fast path: the client-side cropper already ships a square 1080 JPEG under the size cap in the
    common case, so we probe the header (cheap — no full decode) and pass those bytes through
    untouched instead of re-decoding/re-encoding an image that's already correct.
    """
    if len(raw_bytes) <= _LOCKET_MAX_BYTES and raw_bytes[:3] == b"\xff\xd8\xff":
        try:
            probe = Image.open(io.BytesIO(raw_bytes))
            if probe.format == "JPEG" and probe.mode == "RGB" and probe.size == (_LOCKET_MAX_SIDE, _LOCKET_MAX_SIDE):
                return raw_bytes, _new_upload_name()
        except Exception:
            pass

    img = Image.open(io.BytesIO(raw_bytes))
    # JPEG draft decode: have libjpeg decode straight to a lower DCT scale instead of full
    # resolution then downsampling in Python — several times faster on large source photos
    # and the exact same end quality once we resize down to _LOCKET_MAX_SIDE anyway.
    if img.format == "JPEG":
        try:
            img.draft("RGB", (_LOCKET_MAX_SIDE, _LOCKET_MAX_SIDE))
        except Exception:
            pass
    if img.mode not in ("RGB",):
        img = img.convert("RGB")
    w, h = img.size
    if w != h:
        side = min(w, h)
        left, top = (w - side) // 2, (h - side) // 2
        img = img.crop((left, top, left + side, top + side))
    if img.size[0] != _LOCKET_MAX_SIDE:
        # Plain LANCZOS, no reducing_gap shortcut — draft decode above already did the
        # heavy lifting for speed; this final resize runs at full quality so the upload
        # doesn't lose sharpness. (reducing_gap pre-shrinks with a cheap box filter before
        # LANCZOS, which is faster but visibly softer — not worth it on the one image the
        # user is actually posting.)
        img = img.resize((_LOCKET_MAX_SIDE, _LOCKET_MAX_SIDE), Image.LANCZOS)
    data = None
    for quality in (92, 88, 84, 80, 76, 72):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=True)
        data = buf.getvalue()
        if len(data) <= _LOCKET_MAX_BYTES:
            break
    return data, _new_upload_name()


def _photo_name(ext: str) -> str:
    return f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}.{ext}"


def prepare_photo(raw: bytes) -> Tuple[bytes, str, str]:
    # Chỉ MỘT lần encode lossy (hoặc lossless cho ảnh chụp màn hình PNG).
    # Trước đây: JPEG 0.93 (4:2:0) -> decode -> WebP q85 = hai lần vỡ nét.
    side = _LOCKET_MAX_SIDE
    budget = _LOCKET_IMG_BUDGET
    is_png = raw[:8] == b"\x89PNG\r\n\x1a\n"
    is_webp = raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"
    img = Image.open(io.BytesIO(raw))
    if (is_webp and not getattr(img, "is_animated", False) and img.size[0] == img.size[1]
            and img.size[0] <= side and len(raw) <= budget):
        return raw, _photo_name("webp"), "image/webp"   # client đã encode đẹp rồi, không encode lại
    if img.format == "JPEG":
        try:
            img.draft("RGB", (side * 2, side * 2))
        except Exception:
            pass
    if getattr(img, "is_animated", False):
        img.seek(0)
    try:
        img = ImageOps.exif_transpose(img) or img
    except Exception:
        pass
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        bg = Image.new("RGB", rgba.size, (0, 0, 0))
        bg.paste(rgba, mask=rgba.split()[-1])
        img = bg
    elif img.mode != "RGB":
        img = img.convert("RGB")
    w, h = img.size
    if w != h:
        s = min(w, h)
        left, top = (w - s) // 2, (h - s) // 2
        img = img.crop((left, top, left + s, top + s))
    if img.size[0] > side:
        img = img.resize((side, side), Image.LANCZOS)   # không phóng to ảnh nhỏ hơn 1080
    try:
        out_bytes = None
        if is_png:
            buf = io.BytesIO()
            img.save(buf, format="WEBP", lossless=True, quality=90, method=4)
            if buf.tell() <= budget:
                out_bytes = buf.getvalue()
        if out_bytes is None:
            for q in (97, 94, 91, 88, 84, 80, 75, 70):
                buf = io.BytesIO()
                img.save(buf, format="WEBP", quality=q, method=5)
                out_bytes = buf.getvalue()
                if len(out_bytes) <= budget:
                    break
        return out_bytes, _photo_name("webp"), "image/webp"
    except Exception as e:
        logger.warning("webp encode failed (%s) - JPEG 4:4:4 fallback", e)
        out_bytes = b""
        for q in (95, 92, 88, 84, 80, 75):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=q, subsampling=0, optimize=True)
            out_bytes = buf.getvalue()
            if len(out_bytes) <= budget:
                break
        return out_bytes, _photo_name("jpg"), "image/jpeg"


_ALLOWED_IMG_HOSTS = (
    "firebasestorage.googleapis.com",
    "storage.googleapis.com",
    "lh3.googleusercontent.com",
    "googleusercontent.com",
    "cdn.locketcamera.com",
    "locketcamera.com",
    "locket.binhake.dev",
)
# cache key = (url, max_side) → (ts, bytes, mime)
from collections import OrderedDict as _OD
_img_cache: "_OD[Tuple[str, int, bool], Tuple[float, bytes, str]]" = _OD()
_img_cache_bytes = 0
_img_cache_lock = threading.Lock()
_IMG_CACHE_TTL = 3600
_IMG_CACHE_MAX = 600
_IMG_CACHE_BYTES = int(os.environ.get("LOCKET_IMG_CACHE_MB", "96")) * 1024 * 1024
_img_inflight: Dict[Tuple[str, int, bool], threading.Event] = {}


def _img_cache_get(key):
    with _img_cache_lock:
        hit = _img_cache.get(key)
        if hit and time.time() - hit[0] < _IMG_CACHE_TTL:
            _img_cache.move_to_end(key)
            return hit
        if hit:
            _drop_locked(key)
    return None


def _drop_locked(key):
    global _img_cache_bytes
    old = _img_cache.pop(key, None)
    if old:
        _img_cache_bytes -= len(old[1])


def _img_cache_put(key, data: bytes, mime: str):
    global _img_cache_bytes
    if len(data) > _IMG_CACHE_BYTES // 4:
        return
    with _img_cache_lock:
        _drop_locked(key)
        _img_cache[key] = (time.time(), data, mime)
        _img_cache_bytes += len(data)
        while _img_cache and (len(_img_cache) > _IMG_CACHE_MAX or _img_cache_bytes > _IMG_CACHE_BYTES):
            _drop_locked(next(iter(_img_cache)))

def _img_host_ok(url: str) -> bool:
    from urllib.parse import urlparse
    host = (urlparse(url).hostname or "").lower()
    return bool(host) and any(host == h or host.endswith("." + h) for h in _ALLOWED_IMG_HOSTS)


def _client_needs_jpeg(request) -> bool:
    """Detect browsers that cannot decode WebP (mainly Safari < 14 / iOS < 14)."""
    ua = (request.headers.get("User-Agent") or "").lower()
    accept = (request.headers.get("Accept") or "").lower()
    if "image/webp" in accept:
        return False
    if "safari/" in ua and "chrome/" not in ua and "crios/" not in ua:
        import re
        m = re.search(r"version/(\d+)", ua)
        if m and int(m.group(1)) >= 14:
            return False
        return True
    if "iphone os " in ua:
        import re
        m = re.search(r"iphone os (\d+)_", ua)
        if m and int(m.group(1)) >= 14:
            return False
        return True
    if "chrome/" in ua or "crios/" in ua or "firefox/" in ua or "edg/" in ua:
        return False
    return True


def fetch_image_as_jpeg(url: str, max_side: int = 720, timeout: int = 15, force_convert: bool = False) -> Tuple[bytes, str]:
    """Single-flight wrapper: N identical concurrent requests -> 1 download + 1 encode."""
    max_side = max(64, min(int(max_side or 720), 1280))
    key = (url, max_side, force_convert)
    hit = _img_cache_get(key)
    if hit:
        return hit[1], hit[2]
    with _img_cache_lock:
        ev = _img_inflight.get(key)
        leader = ev is None
        if leader:
            ev = _img_inflight[key] = threading.Event()
    if not leader:
        ev.wait(timeout + 5)
        hit = _img_cache_get(key)
        if hit:
            return hit[1], hit[2]
    try:
        return _fetch_image_impl(url, max_side, timeout, force_convert)
    finally:
        if leader:
            with _img_cache_lock:
                _img_inflight.pop(key, None)
            ev.set()


def _fetch_image_impl(url: str, max_side: int = 720, timeout: int = 15, force_convert: bool = False) -> Tuple[bytes, str]:
    """Download remote media. Smart convert: only re-encode to JPEG when the
    browser cannot display the original format (WebP on old Safari) or when
    resize is needed. Otherwise pass through untouched — saves CPU, bandwidth,
    and preserves original quality."""
    max_side = max(64, min(int(max_side or 720), 1280))
    cache_key = (url, max_side, force_convert)
    now = time.time()
    from urllib.parse import urlparse
    host = (urlparse(url).hostname or "").lower()
    if not host or not any(host == h or host.endswith("." + h) for h in _ALLOWED_IMG_HOSTS):
        raise LocketError("image host not allowed")
    r = _http.get(url, timeout=timeout, headers={
        "User-Agent": IOS_UA,
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
    })
    r.raise_for_status()
    raw = r.content
    ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if ctype.startswith("video/"):
        raise LocketError("video not proxyable as image")

    is_jpeg = ctype in ("image/jpeg", "image/jpg") or url.lower().endswith((".jpg", ".jpeg"))
    is_png = ctype == "image/png" or url.lower().endswith(".png")
    is_webp = ctype == "image/webp" or url.lower().endswith(".webp")

    # Fast path: no resize needed AND format is already JPEG/PNG AND no force → pass through
    if not force_convert and not is_webp:
        try:
            img = Image.open(io.BytesIO(raw))
            w0, h0 = img.size
            needs_resize = max(w0, h0) > max_side
            if not needs_resize:
                _img_cache_put(cache_key, raw, ctype)
                return raw, ctype
        except Exception:
            pass

    # Decode and optionally resize + convert
    try:
        img = Image.open(io.BytesIO(raw))
        if getattr(img, "is_animated", False):
            img.seek(0)
        w0, h0 = img.size
        needs_resize = max(w0, h0) > max_side

        if needs_resize and img.format == "JPEG":
            try:
                img.draft("RGB", (max_side, max_side))
            except Exception:
                pass
        img.load()
        if img.mode in ("RGBA", "LA", "P"):
            bg = Image.new("RGB", img.size, (0, 0, 0))
            rgba = img.convert("RGBA")
            bg.paste(rgba, mask=rgba.split()[-1])
            img = bg
        elif img.mode != "RGB":
            img = img.convert("RGB")
        if needs_resize and max(img.size) > max_side:
            img.thumbnail((max_side, max_side), Image.BILINEAR, reducing_gap=2.0)
        quality = 65 if max_side <= 400 else (72 if max_side <= 540 else (78 if max_side <= 800 else (85 if max_side < 1000 else 92)))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality, optimize=False, subsampling=(0 if max_side >= 1000 else -1))
        data = buf.getvalue()
        out_mime = "image/jpeg"
    except Exception as e:
        logger.warning("JPEG convert fail (%s): %s — serving original", ctype, e)
        if ctype in ("image/jpeg", "image/jpg", "image/png", "image/gif"):
            data, out_mime = raw, ctype
        else:
            raise LocketError(f"cannot convert image ({ctype}): {e}")
    _img_cache_put(cache_key, data, out_mime)
    return data, out_mime


HTML = """\
<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<title>Locket Mini</title>
<script>
(function(){
  function get(){var t=null;try{t=localStorage.getItem('locket_theme')}catch(e){}
    if(t!=='light'&&t!=='dark'){t=(window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches)?'dark':'light'}
    return t}
  function apply(t){
    document.documentElement.setAttribute('data-theme',t);
    var m=document.getElementById('metaTheme');if(m)m.setAttribute('content',t==='dark'?'#0A0A0A':'#FFF3D0');
    var b=document.getElementById('themeBtn');
    if(b)b.innerHTML=t==='dark'?'<i class="bi bi-sun-fill"></i>':'<i class="bi bi-moon-stars-fill"></i>';
  }
  apply(get());
  document.addEventListener('DOMContentLoaded',function(){apply(get())});
  window.toggleTheme=function(){
    var t=document.documentElement.getAttribute('data-theme')==='dark'?'light':'dark';
    try{localStorage.setItem('locket_theme',t)}catch(e){}
    apply(t);
  };
})();
</script>
<meta name="description" content="Locket Mini — xem và đăng khoảnh khắc với bạn bè, nhanh gọn trên mọi thiết bị.">
<meta name="theme-color" id="metaTheme" content="#FFF3D0">
<meta name="application-name" content="Locket Mini">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="default">
<meta name="apple-mobile-web-app-title" content="Locket Mini">
<meta name="mobile-web-app-capable" content="yes">
<meta property="og:title" content="Locket Mini">
<meta property="og:description" content="Pics from your best friends — web mini client.">
<meta property="og:type" content="website">
<link rel="manifest" href="/manifest.webmanifest">
<link rel="icon" href="{{ favicon_url }}">
<link rel="apple-touch-icon" href="{{ favicon_url }}">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/cropperjs/1.6.2/cropper.min.css">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<link rel="preconnect" href="https://cdnjs.cloudflare.com">
<link rel="preconnect" href="https://cdn.jsdelivr.net">
<style>
/* LUMEN-UI v3 — Locket web design language: warm cream/gold, SF-Rounded-style type, big radii, pill buttons, floating stickers */
@font-face{
  font-family:'ProximaSoft';
  src:url('{{ font_url }}') format('opentype');
  font-weight:400 900;
  font-display:swap;
}
:root,html[data-theme=light]{
  --bg:#FFFBF1; --bg-rgb:255,251,241; --fg-rgb:26,18,0;
  --hero:#FFE091; --topbar-bg:rgba(255,240,200,.74);
  --accent:#FFB800; --accent-hover:#FFC857; --accent-rgb:255,184,0; --accent-ink:#B87A00; --on-accent:#1A1200;
  --text:#1A1200; --text2:rgba(26,18,0,.58); --muted:rgba(26,18,0,.38);
  --border:rgba(26,18,0,.08); --border-strong:rgba(26,18,0,.14);
  --surface:#FFF3D6; --surface2:#FFE9B5; --field:#FFFFFF; --glass:rgba(255,251,241,.82);
  --ring:rgba(26,18,0,.16);
  --shadow:0 12px 40px rgba(122,74,0,.16); --shadow-sm:0 4px 16px rgba(122,74,0,.12);
  --danger:#E5484D; --ok:#1FA971; --info:#2F7BFF;
  --nav-h:68px; --top-h:56px;
  color-scheme:light;
}
html[data-theme=dark]{
  --bg:#0A0A0A; --bg-rgb:10,10,10; --fg-rgb:245,245,245;
  --hero:#2C2000; --topbar-bg:rgba(20,16,4,.74);
  --accent-ink:#FFB800;
  --text:#F5F5F5; --text2:rgba(245,245,245,.62); --muted:rgba(245,245,245,.40);
  --border:rgba(255,255,255,.09); --border-strong:rgba(255,255,255,.16);
  --surface:#161616; --surface2:#222222; --field:#1C1C1C; --glass:rgba(22,22,22,.82);
  --ring:rgba(255,255,255,.2);
  --shadow:0 12px 40px rgba(0,0,0,.6); --shadow-sm:0 4px 16px rgba(0,0,0,.45);
  color-scheme:dark;
}
/* Full-screen media surfaces are always dark, whatever the page theme is */
.viewer,.crop-stage{
  --text:#F5F5F5; --text2:rgba(245,245,245,.62); --muted:rgba(245,245,245,.40);
  --border:rgba(255,255,255,.09); --surface:#161616; --surface2:#222222; --field:#1C1C1C;
  --ring:rgba(255,255,255,.2); --accent-ink:#FFB800; --fg-rgb:245,245,245;
  color:var(--text);
}

*{box-sizing:border-box;-webkit-tap-highlight-color:transparent;touch-action:manipulation}
html{height:100%;height:-webkit-fill-available}
html,body{margin:0;padding:0}
body{
  font-family:'ProximaSoft',ui-rounded,'SF Pro Rounded',-apple-system,'Segoe UI',sans-serif;
  background:var(--bg);color:var(--text);
  -webkit-font-smoothing:antialiased;overscroll-behavior-y:none;
  min-height:100vh;min-height:-webkit-fill-available;overflow-x:hidden;
}
body:before{
  content:'';position:fixed;top:0;left:0;right:0;height:560px;z-index:0;pointer-events:none;
  background:linear-gradient(180deg,var(--hero) 0%,rgba(var(--bg-rgb),0) 100%);
}
::selection{background:rgba(var(--accent-rgb),.35)}
.hidden{display:none !important}
.app{position:relative;z-index:1;width:100%;margin:0;min-height:100vh;min-height:-webkit-fill-available;padding-bottom:calc(var(--nav-h) + 30px + env(safe-area-inset-bottom))}

/* ---------- Topbar (translucent, like the web header) ---------- */
.topbar{
  position:-webkit-sticky;position:sticky;top:0;z-index:50;
  padding:calc(10px + env(safe-area-inset-top)) 16px 10px;
  display:flex;align-items:center;justify-content:space-between;
  background:var(--topbar-bg);
  -webkit-backdrop-filter:blur(18px) saturate(1.4);backdrop-filter:blur(18px) saturate(1.4);
}
.brand{display:flex;align-items:center;font-weight:800;letter-spacing:-.02em}
.brand img{width:32px;height:32px;border-radius:10px;margin-right:9px;box-shadow:0 2px 8px rgba(var(--accent-rgb),.45)}
.brand .wordmark{font-size:23px;line-height:1}
.brand .mini{margin-left:7px;font-size:10.5px;font-weight:800;letter-spacing:.03em;text-transform:uppercase;
  padding:3px 8px;border-radius:500px;background:rgba(var(--accent-rgb),.24);color:var(--text)}
.top-actions{display:flex;align-items:center}
.top-actions > * + *{margin-left:8px}
.pill{background:rgba(var(--fg-rgb),.08);color:var(--text);border:none;border-radius:500px;padding:9px 15px;
  font-family:inherit;font-size:14px;font-weight:800;display:inline-flex;align-items:center;cursor:pointer;transition:transform .15s,background .15s}
.pill > * + *{margin-left:6px}
.pill:active{transform:scale(.95);background:rgba(var(--fg-rgb),.14)}
.icon-btn{width:38px;height:38px;border-radius:50%;border:none;background:rgba(var(--fg-rgb),.08);color:var(--text);
  display:inline-flex;align-items:center;justify-content:center;font-size:16px;cursor:pointer;transition:transform .15s}
.icon-btn:active{transform:scale(.92)}

@media (min-width:768px){.topbar{background:linear-gradient(180deg,var(--topbar-bg) 55%,rgba(var(--bg-rgb),0) 100%);-webkit-backdrop-filter:none;backdrop-filter:none;padding-bottom:22px}}

/* ---------- Pages ---------- */
.page{padding:12px 16px 8px;display:none;max-width:560px;margin:0 auto;width:100%}
.page.active{display:block;animation:fadeIn .35s ease both}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
.page-title{margin:4px 0 14px;font-size:28px;font-weight:800;letter-spacing:-.03em;line-height:1.1}
.page-sub{margin:-8px 0 16px;color:var(--text2);font-size:14px;font-weight:700}

/* ---------- Buttons / fields / cards ---------- */
.btn{width:100%;padding:16px;border:none;border-radius:500px;background:var(--accent);color:var(--on-accent);
  font-family:inherit;font-weight:800;font-size:17px;letter-spacing:-.01em;cursor:pointer;
  transition:transform .12s,background .15s,opacity .2s;box-shadow:0 10px 28px rgba(var(--accent-rgb),.38)}
.btn:hover{background:var(--accent-hover)}
.btn:active{transform:scale(.97)}
.btn:disabled{opacity:.55;box-shadow:none}
.btn-ghost{background:rgba(var(--fg-rgb),.08);color:var(--text);box-shadow:none}
.btn-ghost:hover{background:rgba(var(--fg-rgb),.12)}
.btn-ghost.active{background:var(--accent);color:var(--on-accent)}
.input{width:100%;padding:15px 18px;border:none;border-radius:20px;background:var(--field);color:var(--text);
  font-family:inherit;font-size:16px;font-weight:700;outline:none;box-shadow:inset 0 0 0 1.5px var(--border);
  transition:box-shadow .2s}
.input::placeholder{color:var(--muted);font-weight:600}
.input:focus{box-shadow:inset 0 0 0 2px var(--accent),0 0 0 5px rgba(var(--accent-rgb),.2)}
.label{font-size:11px;font-weight:800;color:var(--text2);margin:0 0 7px 4px;display:block;text-transform:uppercase;letter-spacing:.7px}
.card{background:var(--surface);border-radius:28px;padding:18px;margin-bottom:14px}

/* ---------- Login ---------- */
.login-hero{text-align:center;padding:4px 0 8px}
.lh-stage{position:relative;height:240px;margin:0 -6px}
.lh-icon{position:absolute;left:50%;top:50%;width:104px;height:104px;margin:-58px 0 0 -52px;border-radius:30px;
  box-shadow:0 18px 50px rgba(var(--accent-rgb),.55);animation:pop .8s cubic-bezier(.2,1.4,.3,1) both}
@keyframes pop{from{opacity:0;transform:scale(.6)}to{opacity:1;transform:scale(1)}}
.stk{position:absolute;display:block;pointer-events:none;transition:transform .35s ease-out;animation:stkIn .9s cubic-bezier(.2,1.5,.4,1) backwards}
.stk-r{display:block}
.stk img{display:block;width:100%;height:auto;filter:drop-shadow(0 6px 10px rgba(122,74,0,.28));animation:floaty 6s ease-in-out infinite}
.stk.s1{left:3%;top:6%;width:96px;animation-delay:.15s}.stk.s1 .stk-r{-webkit-transform:rotate(-8deg);transform:rotate(-8deg)}
.stk.s2{right:5%;top:2%;width:62px;animation-delay:.3s}.stk.s2 .stk-r{-webkit-transform:rotate(10deg);transform:rotate(10deg)}
.stk.s3{left:2%;bottom:6%;width:60px;animation-delay:.45s}.stk.s3 .stk-r{-webkit-transform:rotate(-12deg);transform:rotate(-12deg)}
.stk.s4{right:3%;bottom:10%;width:70px;animation-delay:.6s}.stk.s4 .stk-r{-webkit-transform:rotate(8deg);transform:rotate(8deg)}
.stk.s5{left:27%;top:-2%;width:48px;animation-delay:.75s}.stk.s5 .stk-r{-webkit-transform:rotate(6deg);transform:rotate(6deg)}
.stk.s6{right:27%;bottom:-3%;width:54px;animation-delay:.9s}.stk.s6 .stk-r{-webkit-transform:rotate(-6deg);transform:rotate(-6deg)}
.stk.s7{right:24%;top:3%;width:46px;animation-delay:1.05s}.stk.s7 .stk-r{-webkit-transform:rotate(14deg);transform:rotate(14deg)}
.stk.s2 img{animation-delay:-1.2s}.stk.s3 img{animation-delay:-2.4s}.stk.s4 img{animation-delay:-3.1s}
.stk.s5 img{animation-delay:-4s}.stk.s6 img{animation-delay:-4.8s}.stk.s7 img{animation-delay:-5.5s}
@keyframes floaty{0%,100%{transform:translateY(0)}50%{transform:translateY(-9px)}}
@keyframes stkIn{from{opacity:0;-webkit-transform:translateY(-60px) scale(.4);transform:translateY(-60px) scale(.4)}to{opacity:1;-webkit-transform:none;transform:none}}
.lh-word{margin:6px 0 4px;font-size:46px;font-weight:800;letter-spacing:-.04em;line-height:1}
.lh-tag{margin:0 auto;color:var(--text2);font-size:18px;font-weight:700;max-width:280px;line-height:1.3}
.login-card{margin-top:22px;padding:20px;box-shadow:var(--shadow)}
.remember-row{display:flex;align-items:center;margin-top:14px;font-size:14px;font-weight:700;color:var(--text2);cursor:pointer;user-select:none}
.remember-row input{width:18px;height:18px;accent-color:#FFB800;margin:0 9px 0 2px}
.login-note{text-align:center;color:var(--muted);font-size:12px;font-weight:700;margin:14px 8px 0;line-height:1.5}

/* ---------- Dashboard ---------- */
.hello{display:flex;align-items:center;margin:2px 0 18px}
.hello .dash-greet{flex:1;min-width:0;margin-left:14px}
.dash-greet .hi{font-size:14px;color:var(--text2);font-weight:700;margin:0 0 3px}
.dash-greet .name{font-size:28px;font-weight:800;margin:0;letter-spacing:-.03em;line-height:1.05;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.streak-card{position:relative;overflow:hidden;isolation:isolate;-webkit-transform:translateZ(0);transform:translateZ(0);-webkit-mask-image:-webkit-radial-gradient(white,black);display:flex;align-items:center;justify-content:space-between;
  padding:20px 22px;margin-bottom:12px;border-radius:32px;color:var(--on-accent);
  background:linear-gradient(135deg,#FFCB3D 0%,#FFB800 60%,#FFA800 100%);box-shadow:0 14px 36px rgba(var(--accent-rgb),.4)}
.streak-card .sc-label{font-size:12px;font-weight:800;text-transform:uppercase;letter-spacing:.7px;opacity:.7}
.streak-card .sc-num{font-size:54px;font-weight:800;line-height:1;letter-spacing:-.04em;margin-top:4px}
.streak-card .sc-num small{font-size:16px;font-weight:800;letter-spacing:0;margin-left:6px;opacity:.65}
.streak-card .sc-img{width:92px;height:auto;margin:-6px -4px -10px 0;-webkit-transform:rotate(10deg);transform:rotate(10deg);
  filter:drop-shadow(0 8px 12px rgba(122,74,0,.35))}
.tiles{display:grid;grid-template-columns:1fr 1fr;grid-gap:12px;gap:12px}
.tile{position:relative;overflow:hidden;text-align:left;border:none;border-radius:30px;padding:16px 16px 18px;
  background:var(--surface);color:var(--text);font-family:inherit;cursor:pointer;transition:transform .15s,box-shadow .2s;
  display:block;width:100%;min-height:168px}
.tile:active{transform:scale(.97)}
.tile img{display:block;height:68px;width:auto;margin:0 0 12px;filter:drop-shadow(0 4px 8px rgba(122,74,0,.22))}
.tile h3{margin:0 0 4px;font-size:18px;font-weight:800;letter-spacing:-.02em}
.tile p{margin:0;font-size:13px;font-weight:700;color:var(--text2);line-height:1.35}
.tile.tile-primary{grid-column:1 / -1;min-height:0;background:var(--field);box-shadow:var(--shadow-sm);
  display:flex;align-items:center;justify-content:space-between;padding:18px 20px}
.tile.tile-primary img{height:86px;margin:-6px -2px -6px 10px;-webkit-transform:rotate(8deg);transform:rotate(8deg)}
.tile.tile-primary h3{font-size:21px}
.tile-cta{display:inline-block;margin-top:12px;padding:9px 18px;border-radius:500px;background:var(--accent);color:var(--on-accent);font-weight:800;font-size:14px}
.contact-card{margin-top:14px}
.contact-card .label-row{font-size:12px;font-weight:800;color:var(--text2);text-transform:uppercase;letter-spacing:.7px;margin-bottom:12px}
.contact-links{display:block}
.contact-link{display:flex;align-items:center;padding:13px 16px;border-radius:20px;background:rgba(var(--fg-rgb),.05);
  color:var(--text);text-decoration:none;font-weight:800;font-size:15px;margin-bottom:8px}
.contact-link:last-child{margin-bottom:0}
.contact-link:active{background:rgba(var(--fg-rgb),.1)}
.contact-link .bi{margin-right:12px;font-size:22px}
.contact-link span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.contact-link .cl-handle{color:var(--text2);font-weight:700;font-size:13px;margin-left:auto}
.dash-foot{text-align:center;padding:26px 0 6px}
.dash-foot .df-brand{display:inline-flex;align-items:center;font-weight:800;font-size:19px;letter-spacing:-.02em}
.dash-foot .df-brand img{width:26px;height:26px;border-radius:8px;margin-right:8px}
.app-version{text-align:center;color:var(--muted);font-size:12px;font-weight:700;letter-spacing:.02em;padding:8px 0 0}

/* ---------- Settings switches ---------- */
.set-card{background:var(--surface);border-radius:28px;padding:2px 18px;margin-bottom:14px}
.switch-row{display:flex;align-items:center;justify-content:space-between;padding:14px 0;border-bottom:1px solid var(--border)}
.switch-row:last-child{border-bottom:none}
.switch-row .sw-text{font-size:15px;font-weight:800;line-height:1.3;padding-right:14px}
.switch-row .sw-sub{font-size:12px;color:var(--text2);font-weight:700;margin-top:3px;line-height:1.35}
.switch{position:relative;width:52px;height:32px;flex-shrink:0;display:inline-block}
.switch input{position:absolute;opacity:0;width:52px;height:32px;margin:0;z-index:2;cursor:pointer}
.switch .slider{position:absolute;top:0;left:0;right:0;bottom:0;background:rgba(var(--fg-rgb),.18);border-radius:500px;cursor:pointer;transition:background .2s}
.switch .slider:before{content:'';position:absolute;width:26px;height:26px;left:3px;top:3px;background:#fff;border-radius:50%;
  box-shadow:0 2px 6px rgba(0,0,0,.28);-webkit-transition:-webkit-transform .2s;transition:transform .2s}
.switch input:checked+.slider{background:var(--accent)}
.switch input:checked+.slider:before{-webkit-transform:translateX(20px);transform:translateX(20px)}
.offline-banner{display:none;padding:12px 16px;margin-bottom:12px;border-radius:20px;
  background:rgba(var(--accent-rgb),.2);font-size:13px;font-weight:800;color:var(--text)}
.offline-banner.show{display:block}

/* ---------- Avatar ---------- */
.avatar-wrap{position:relative;flex-shrink:0;overflow:visible}
.avatar-ring{position:absolute;top:0;left:0;width:100%;height:100%}
.avatar-img{position:absolute;border-radius:50%;overflow:hidden;background:var(--surface2)}
.avatar-img img{width:100%;height:100%;object-fit:cover;display:block}
.avatar-badge{position:absolute;bottom:-1px;right:-1px;pointer-events:none}
.fm-contentPunch{-webkit-mask-image:radial-gradient(circle at 100% 100%,transparent 11px,black 12px);
  mask-image:radial-gradient(circle at 100% 100%,transparent 11px,black 12px)}

/* ---------- Moments ---------- */
.moments-head{display:flex;align-items:center;justify-content:space-between;padding:4px 16px 14px}
.moments-head h2{margin:0;font-size:28px;font-weight:800;letter-spacing:-.03em}
.moments-refresh-btn{background:rgba(var(--fg-rgb),.08);border:none;color:var(--text);font-size:17px;width:38px;height:38px;
  cursor:pointer;display:flex;align-items:center;justify-content:center;border-radius:50%;flex-shrink:0}
.moments-refresh-btn:active{background:rgba(var(--fg-rgb),.16)}
.icon-spin{animation:spin .7s linear infinite}
.mv-seg{display:flex;margin-left:auto;margin-right:8px;border-radius:500px;overflow:hidden;background:rgba(var(--fg-rgb),.08);padding:3px}
.mv-seg button{border:none;background:transparent;color:var(--text2);font-family:inherit;font-weight:800;font-size:13px;
  min-width:34px;height:32px;padding:0 10px;cursor:pointer;border-radius:500px;-webkit-appearance:none;appearance:none}
.mv-seg button.active{background:var(--accent);color:var(--on-accent)}
.moments-new-pill{position:fixed;top:calc(68px + env(safe-area-inset-top));left:50%;z-index:85;
  transform:translateX(-50%) translateY(-10px);background:var(--accent);color:var(--on-accent);border:none;border-radius:500px;padding:10px 20px;
  font-family:inherit;font-size:14px;font-weight:800;display:none;align-items:center;cursor:pointer;
  box-shadow:0 8px 24px rgba(var(--accent-rgb),.5);opacity:0;transition:opacity .2s,transform .2s}
.moments-new-pill.show{display:-webkit-box;display:-webkit-flex;display:flex;opacity:1;transform:translateX(-50%) translateY(0)}
.moments-top-btn{position:fixed;right:16px;bottom:calc(var(--nav-h) + 28px + env(safe-area-inset-bottom));z-index:80;
  width:52px;height:52px;border-radius:50%;border:none;background:var(--accent);color:var(--on-accent);
  display:none;align-items:center;justify-content:center;cursor:pointer;padding:0;box-shadow:0 8px 24px rgba(var(--accent-rgb),.5)}
.moments-top-btn.show{display:-webkit-box;display:-webkit-flex;display:flex}
.moments-top-btn:active{-webkit-transform:scale(.92);transform:scale(.92)}
.moments-top-btn i{color:var(--on-accent);pointer-events:none}
.moments-more{text-align:center;padding:16px;color:var(--muted);font-size:13px;font-weight:800}
.feed-skel{padding:12px;display:flex;flex-direction:column;align-items:center}
.feed-skel .skeleton{width:90%;height:0;padding-bottom:90%;border-radius:28px;margin-bottom:10px}
.moments-grid{display:grid;grid-template-columns:repeat(3,1fr);grid-gap:10px;gap:10px;padding:0 14px}
.moments-grid .skeleton{border-radius:22px !important}
.moment-card{position:relative;border-radius:22px;overflow:hidden;background:var(--surface2);cursor:pointer;width:100%;height:0;padding-bottom:100%;
  box-shadow:var(--shadow-sm);transition:transform .15s}
.moment-card:active{transform:scale(.97)}
.moment-card img,.moment-card video{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover}
.moment-video-badge{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);z-index:2;
  width:36px;height:36px;border-radius:50%;background:rgba(0,0,0,.42);-webkit-backdrop-filter:blur(4px);backdrop-filter:blur(4px);
  display:flex;align-items:center;justify-content:center;color:#fff;font-size:16px;pointer-events:none}
.feed-slide .moment-video-badge{width:56px;height:56px;font-size:24px}
.moment-overlay{position:absolute;top:0;left:0;width:100%;height:100%;
  background:linear-gradient(180deg,rgba(0,0,0,.32) 0%,transparent 28%,transparent 58%,rgba(0,0,0,.5) 100%);pointer-events:none}
.moment-top{position:absolute;top:7px;left:7px;right:7px;z-index:3;display:flex;align-items:center;
  background:rgba(0,0,0,.45);-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px);
  border-radius:500px;padding:3px 9px 3px 3px;max-width:calc(100% - 14px);color:#fff}
.moment-top .avatar-wrap{margin-right:5px}
.moment-top .mname{font-size:11px;font-weight:800;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0;flex:1}
.moment-top .mtime{font-size:10px;font-weight:700;color:rgba(255,255,255,.75);margin-left:auto;flex-shrink:0;padding-left:6px}
.moments-empty,.friends-empty{text-align:center;padding:56px 20px;color:var(--muted);font-weight:800}
.moments-empty i,.friends-empty i{opacity:.55;margin-bottom:10px;display:inline-block;color:var(--accent-ink)}
.moments-empty-retry{margin-top:14px;padding:10px 22px;border-radius:500px;border:none;
  background:var(--accent);color:var(--on-accent);font-family:inherit;font-size:14px;font-weight:800;cursor:pointer}
.moments-more .moments-empty-retry{background:rgba(var(--fg-rgb),.08);color:var(--text);margin-top:0}

.moments-feed{display:none;overflow-y:auto;-webkit-overflow-scrolling:touch}
.moments-feed .feed-slide{display:block;padding:6px 14px 18px;box-sizing:border-box;text-align:center;max-width:480px;margin-left:auto;margin-right:auto}
.moments-feed .feed-card{position:relative;display:block !important;width:100% !important;max-width:none !important;
  height:0;padding-bottom:100% !important;border-radius:30px;overflow:hidden;background:var(--surface2);box-shadow:var(--shadow)}
.moments-feed .feed-card img,.moments-feed .feed-card video{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover}
.moments-feed .feed-meta{width:100% !important;max-width:none !important;margin:12px 0 0 !important;padding:0 6px;text-align:left}
.moments-feed .feed-name{font-size:15px;font-weight:800;display:flex;align-items:center;color:var(--text)}
.moments-feed .feed-name .avatar-wrap{margin-right:10px}
body.mv-grid .moments-grid{display:grid !important}
body.mv-grid .moments-feed{display:none !important}
body.mv-feed .moments-grid{display:none !important}
body.mv-feed .moments-feed{display:block !important}
.moments-grid.cols-2{grid-template-columns:repeat(2,1fr) !important}
.moments-grid.cols-3{grid-template-columns:repeat(3,1fr) !important}
.moments-grid.cols-4{grid-template-columns:repeat(4,1fr) !important}
.moments-grid.dense .moment-caption{display:none}
.moments-grid.dense .moment-top .mtime{display:none}
.moments-grid.cols-4 .moment-top .mname{display:none}
.moments-grid.cols-4 .moment-top{padding-right:3px}
@media (max-width:520px){
  .moments-grid{display:none}
  .moments-feed{display:block}
  #page-moments.active{
    position:fixed;left:0;right:0;top:0;bottom:0;max-width:560px;margin:0 auto;
    padding-top:calc(var(--top-h) + 8px + env(safe-area-inset-top));
    padding-bottom:calc(var(--nav-h) + 40px + env(safe-area-inset-bottom));
    overflow-y:auto;-webkit-overflow-scrolling:touch;z-index:40;background:transparent;
  }
  #page-upload.active{padding-bottom:8px}
  .preview-box{max-height:46vh}
}

/* Caption pill: long text scrolls sideways */
.moment-caption{position:absolute;left:6%;right:6%;bottom:12px;z-index:3;text-align:center;pointer-events:none;display:block}
.cap-pill{display:inline-block;max-width:100%;vertical-align:bottom;background:rgba(0,0,0,.5);border-radius:500px;
  -webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}
.cap-clip{overflow:hidden;margin:0 16px;padding:8px 0;white-space:nowrap;font-size:13px;font-weight:800;line-height:1.35;color:#fff;text-align:left}
.cap-in{display:inline-block}
.cap-clip.marq{-webkit-mask-image:linear-gradient(90deg,transparent 0,#000 12px,#000 calc(100% - 12px),transparent 100%);
  mask-image:linear-gradient(90deg,transparent 0,#000 12px,#000 calc(100% - 12px),transparent 100%)}
.cap-clip.marq .cap-in{padding-right:36px}
.cap-track{display:inline-block;white-space:nowrap;will-change:transform;-webkit-animation:capMarq 10s linear 1s infinite;animation:capMarq 10s linear 1s infinite}
@-webkit-keyframes capMarq{from{-webkit-transform:translate3d(0,0,0)}to{-webkit-transform:translate3d(-50%,0,0)}}
@keyframes capMarq{from{transform:translate3d(0,0,0)}to{transform:translate3d(-50%,0,0)}}
@media (prefers-reduced-motion:reduce){
  .cap-track{display:inline;-webkit-animation:none;animation:none}
  .cap-clip.marq{-webkit-mask-image:none;mask-image:none;text-overflow:ellipsis}
  .cap-clip.marq .cap-in{display:inline;padding:0}
  .cap-track .cap-in + .cap-in{display:none}
  .stk img,.lh-icon{animation:none}
}

/* ---------- Fullscreen viewer + side panel (always dark) ---------- */
.viewer{position:fixed;top:0;left:0;right:0;bottom:0;background:#000;z-index:10000;display:none;flex-direction:row}
.viewer.open,.viewer:not(.hidden){display:flex}
.viewer-main{position:relative;flex:1 1 auto;min-width:0;min-height:0;background:#000;overflow:hidden}
.viewer-media{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:contain;background:#000}
.viewer-head{position:absolute;top:0;left:0;right:0;z-index:3;display:flex;align-items:center;
  padding:calc(16px + env(safe-area-inset-top)) 16px 30px;background:linear-gradient(180deg,rgba(0,0,0,.7),transparent);pointer-events:none}
.viewer-head > *{pointer-events:auto}
.viewer-close{margin-left:auto;width:40px;height:40px;border-radius:50%;background:rgba(255,255,255,.18);
  -webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px);border:none;color:#fff;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center}
.viewer-caption{position:absolute;bottom:24px;left:16px;right:16px;z-index:2;background:rgba(0,0,0,.55);
  -webkit-backdrop-filter:blur(10px);backdrop-filter:blur(10px);border-radius:22px;padding:13px 18px;font-size:15px;font-weight:800;
  text-align:center;white-space:normal;word-break:break-word;line-height:1.4;max-height:30vh;overflow-y:auto;color:#fff}
.viewer-error{position:absolute;top:50%;left:0;right:0;transform:translateY(-50%)}
.viewer-side{flex:0 0 360px;width:360px;max-width:42vw;display:flex;flex-direction:column;min-height:0;background:var(--surface);
  border-left:1px solid var(--border);padding-bottom:env(safe-area-inset-bottom);z-index:4}
.vs-head{display:flex;align-items:center;justify-content:space-between;flex-shrink:0;
  padding:calc(16px + env(safe-area-inset-top)) 18px 14px;border-bottom:1px solid var(--border);cursor:default}
.vs-title{display:flex;align-items:center;font-weight:800;font-size:17px;min-width:0;color:var(--text)}
.vs-title .bi{color:var(--accent);margin-right:8px;font-size:18px}
.vs-meta{display:flex;align-items:center;flex-shrink:0;margin-left:10px}
.vs-chip{background:rgba(255,255,255,.1);border-radius:500px;padding:3px 10px;font-size:12px;font-weight:800;margin-left:6px;color:var(--text)}
.vs-chev{display:none;margin-left:10px;font-size:16px;color:var(--text2);transition:transform .2s}
.viewer-side .viewer-list{flex:1 1 auto;min-height:0;overflow-y:auto;-webkit-overflow-scrolling:touch;padding:4px 18px 18px}
.viewer-side .viewer-reaction{font-size:20px}
.vs-retry{margin-top:8px;padding:8px 18px;border-radius:500px;border:none;background:var(--accent);color:var(--on-accent);
  font-family:inherit;font-weight:800;font-size:13px;cursor:pointer}
.viewer-list{overflow-y:auto;padding:4px 18px 18px}
.viewer-row{display:flex;align-items:center;padding:10px 0;border-bottom:1px solid var(--border)}
.viewer-row:last-child{border-bottom:none}
.viewer-row .avatar-wrap{margin-right:12px;flex:0 0 auto}
.viewer-info{flex:1 1 auto;min-width:0}
.viewer-name{font-weight:800;font-size:15px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--text)}
.viewer-user{font-size:12px;color:var(--text2);font-weight:700}
.viewer-reaction{font-size:18px;margin-right:10px}
.viewer-time{font-size:12px;color:var(--text2);flex:0 0 auto;font-weight:700}
.viewer-time.mtime{margin-left:0}
@media (max-width:768px){
  .viewer{flex-direction:column}
  .viewer-side{flex:0 0 auto;width:100%;max-width:none;max-height:46vh;border-left:none;border-top:1px solid var(--border);
    border-radius:28px 28px 0 0;box-shadow:0 -8px 30px rgba(0,0,0,.5)}
  .vs-head{padding:16px 18px 12px;cursor:pointer;position:relative}
  .vs-head:before{content:'';position:absolute;top:6px;left:50%;margin-left:-18px;width:36px;height:4px;border-radius:4px;background:rgba(255,255,255,.22)}
  .vs-chev{display:inline-block}
  .viewer-side.collapsed .viewer-list{display:none}
  .viewer-side.collapsed .vs-chev{transform:rotate(180deg)}
}
@media (max-width:900px) and (orientation:landscape) and (min-width:769px){
  .viewer-side{flex-basis:300px;width:300px}
}

/* ---------- Friends ---------- */
.fa-card{background:var(--surface);border-radius:28px;padding:6px 16px 14px;margin:4px 0 14px}
.fa-tabs{display:flex;margin:8px 0 4px;background:rgba(var(--fg-rgb),.07);border-radius:500px;padding:3px}
.fa-tabs button{flex:1;background:none;border:none;border-radius:500px;color:var(--text2);
  font-family:inherit;font-weight:800;font-size:13px;padding:10px 4px;cursor:pointer;-webkit-appearance:none}
.fa-tabs button.active{background:var(--accent);color:var(--on-accent)}
.fa-count{display:inline-block;min-width:20px;margin-left:6px;padding:1px 7px;border-radius:500px;background:rgba(var(--fg-rgb),.12);font-size:11px;font-weight:800}
.fa-tabs button.active .fa-count{background:rgba(26,18,0,.18)}
.fa-sum{font-size:13px;color:var(--text2);font-weight:700;padding:12px 2px 2px}
.fa-sum b{color:var(--accent-ink)}
.fa-list{max-height:380px;overflow-y:auto;-webkit-overflow-scrolling:touch}
.fa-time{flex-shrink:0;margin-left:10px;font-size:12px;font-weight:800;color:var(--text2);text-align:right}
.fa-tag{display:inline-block;margin-left:6px;padding:2px 8px;border-radius:500px;font-size:10px;font-weight:800;background:rgba(var(--accent-rgb),.25);color:var(--text)}
.fa-empty{text-align:center;color:var(--muted);font-size:14px;font-weight:800;padding:24px 8px}
.fa-foot{font-size:11.5px;color:var(--muted);font-weight:700;line-height:1.5;padding:10px 2px 0}
.fa-foot a{color:var(--accent-ink)}
.friends-count{font-size:14px;color:var(--text2);font-weight:800;margin:2px 4px 12px}
.friends-count b{color:var(--accent-ink)}
#friendsList{background:var(--surface);border-radius:28px;padding:2px 16px}
#friendsList:empty{display:none}
.friend-row{display:flex;align-items:center;padding:13px 0;border-bottom:1px solid var(--border)}
.friend-row:last-child{border-bottom:none}
.friend-row .avatar-wrap{margin-right:14px}
.friend-info{min-width:0;flex:1}
.friend-name{font-weight:800;font-size:17px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;letter-spacing:-.01em}
.friend-user{font-size:13px;color:var(--text2);font-weight:700;margin-top:2px}
.friend-streak{margin-left:auto;flex-shrink:0;font-size:14px;font-weight:800;color:var(--accent-ink);display:flex;align-items:center}
.people-search{display:flex;margin:0 0 12px}
.people-search .input{flex:1 1 auto;min-width:0}
.people-go{width:52px;flex:0 0 52px;margin-left:8px;padding:0;display:flex;align-items:center;justify-content:center;font-size:19px;border-radius:50%;height:52px}
.people-msg{font-size:13px;color:var(--text2);padding:8px 4px;font-weight:700}
.people-msg a{color:var(--accent-ink)}
.person-card{display:flex;align-items:center;flex-wrap:wrap;padding:13px 14px;margin-bottom:10px;background:var(--surface);border-radius:26px}
.person-card .avatar-wrap{margin-right:12px;flex:0 0 auto}
.person-info{flex:1 1 120px;min-width:0}
.person-name{font-weight:800;font-size:16px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.person-user{font-size:12.5px;color:var(--text2);font-weight:700}
.person-actions{flex:0 0 auto;display:flex;flex-direction:column;align-items:flex-end;margin-left:8px}
.p-btn{border:none;border-radius:500px;padding:10px 17px;font-family:inherit;font-weight:800;font-size:13px;cursor:pointer;background:var(--accent);color:var(--on-accent)}
.p-btn.ghost{background:rgba(var(--fg-rgb),.1);color:var(--text)}
.p-btn:disabled{opacity:.5}
.p-row{display:flex;margin-top:6px}
.p-row .p-btn + .p-btn{margin-left:6px}
.p-share{font-size:12px;color:var(--text2);display:flex;align-items:center;font-weight:700}
.p-share input{margin-right:6px;accent-color:#FFB800}
.p-tag{font-size:12px;font-weight:800;color:var(--text2);padding:8px 4px}
.req-head{font-size:13px;font-weight:800;color:var(--text2);margin:16px 4px 8px}
.req-head b{color:var(--accent-ink)}
.person-slots{font-size:12px;font-weight:800;color:var(--accent-ink);margin-top:2px}
.person-slots.full{color:var(--danger)}
.p-adv{margin:10px 4px 6px;font-size:13px;color:var(--text2)}
.p-adv summary{cursor:pointer;font-weight:800}
.p-adv summary span{font-weight:700;margin-left:6px;color:var(--accent-ink)}
.p-adv-body{display:flex;margin-top:8px}
.p-adv-body .input{flex:1 1 auto;min-width:0;margin-right:8px}

/* ---------- Upload ---------- */
.preview-box{width:100%;height:0;padding-bottom:100%;background:var(--surface);border-radius:32px;overflow:hidden;position:relative;
  box-shadow:inset 0 0 0 2.5px rgba(var(--accent-rgb),.55);cursor:pointer}
.preview-box img,.preview-box video{position:absolute;top:0;left:0;width:100%;height:100%;object-fit:cover}
.preview-hint{position:absolute;top:50%;left:0;right:0;-webkit-transform:translateY(-50%);transform:translateY(-50%);
  text-align:center;color:var(--text2);font-weight:800;font-size:14px;padding:0 20px;line-height:1.5}
.preview-box .preview-hint .ph-img{position:static;top:auto;left:auto;width:84px;height:auto;object-fit:contain;display:block;margin:0 auto 10px;filter:drop-shadow(0 6px 10px rgba(122,74,0,.25))}
.preview-hint i{opacity:.6;margin-bottom:10px;display:inline-block}
.pb-queue-btn{position:absolute;top:12px;right:12px;z-index:5;width:38px;height:38px;border-radius:50%;
  background:rgba(0,0,0,.5);border:none;color:#fff;display:flex;align-items:center;justify-content:center;font-size:16px;cursor:pointer;
  -webkit-backdrop-filter:blur(6px);backdrop-filter:blur(6px)}
#fileInput,#cameraInput{position:absolute;width:1px;height:1px;opacity:0;overflow:hidden;clip:rect(0,0,0,0)}
.upload-actions{display:flex;flex-wrap:wrap;margin:12px -4px 0}
.upload-actions .btn-ghost{flex:1 1 auto;min-width:0;font-size:14px;padding:12px 8px;margin:0 4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.upload-actions .btn-ghost.icon-only{flex:0 0 auto;width:50px;padding:12px 0;text-align:center}
.send-row{margin-top:16px}
.queue-box{margin-top:14px;border-radius:28px;background:var(--surface);overflow:hidden}
.queue-box .qb-head{display:flex;align-items:center;justify-content:space-between;padding:13px 18px;border-bottom:1px solid var(--border);
  font-size:12px;font-weight:800;color:var(--text2);text-transform:uppercase;letter-spacing:.6px}
.queue-item{display:flex;align-items:center;padding:12px 18px;border-bottom:1px solid var(--border)}
.queue-item:last-child{border-bottom:none}
.queue-item img{width:48px;height:48px;border-radius:14px;object-fit:cover;background:var(--surface2);flex-shrink:0;margin-right:12px}
.queue-item .qi-info{min-width:0;flex:1}
.queue-item .qi-title{font-size:14px;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.queue-item .qi-status{font-size:12px;font-weight:800;color:var(--text2);margin-top:2px}
.queue-item .qi-status.pending{color:var(--accent-ink)}
.queue-item .qi-status.uploading{color:var(--info)}
.queue-item .qi-status.error{color:var(--danger)}
.queue-item .qi-status.done{color:var(--ok)}
.queue-item .qi-del{flex-shrink:0;margin-left:10px;width:34px;height:34px;border-radius:50%;border:none;background:rgba(var(--fg-rgb),.08);
  color:var(--text2);font-size:18px;line-height:1;display:flex;align-items:center;justify-content:center;cursor:pointer;padding:0}
.queue-item .qi-del:active{background:rgba(229,72,77,.2);color:var(--danger)}
.queue-item .qi-del:disabled{opacity:.4;pointer-events:none}

/* ---------- Live camera: dark capture panel ---------- */
.live-cam{width:100%;margin:0 0 4px;background:#0B0B0B;border-radius:32px;overflow:hidden;padding-bottom:14px;box-shadow:var(--shadow)}
.lc-frame{width:100%;position:relative;overflow:hidden;background:#0a0a0a;-webkit-transform:translateZ(0);transform:translateZ(0);height:0;padding-bottom:100%}
.lc-frame.sized{height:auto;padding-bottom:0}
.lc-frame video{position:absolute;top:0;left:0;right:0;bottom:0;width:100%;height:100%;max-width:none;max-height:none;
  object-fit:cover;-webkit-object-fit:cover;object-position:center center;display:block;background:#0a0a0a;-webkit-transform:translateZ(0)}
.lc-frame video.mirrored{-webkit-transform:scaleX(-1) translateZ(0);transform:scaleX(-1) translateZ(0)}
.lc-hint{position:absolute;top:50%;left:0;right:0;-webkit-transform:translateY(-50%);transform:translateY(-50%);text-align:center;color:rgba(255,255,255,.7);font-weight:800;font-size:14px;padding:0 20px;z-index:2}
.lc-hint .spinner{margin-bottom:8px}
.lc-flash-btn{position:absolute;top:14px;right:14px;z-index:3;width:40px;height:40px;border-radius:50%;background:rgba(0,0,0,.38);border:none;color:#fff;
  display:flex;align-items:center;justify-content:center;cursor:pointer;-webkit-appearance:none;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px);font-size:16px;
  transition:background .15s,color .15s,transform .1s}
.lc-flash-btn.on{color:#1A1200;background:var(--accent)}
.lc-flash-btn:active{-webkit-transform:scale(.92);transform:scale(.92)}
.lc-flash-btn.hidden{display:none}
.lc-flash-overlay{position:absolute;top:0;left:0;right:0;bottom:0;background:#fff;opacity:0;pointer-events:none;z-index:4}
.lc-flash-overlay.fire{opacity:.85;transition:opacity .12s ease-out}
.lc-zoom-row{position:absolute;left:0;right:0;bottom:14px;z-index:3;display:none;align-items:center;justify-content:center;pointer-events:none}
.lc-zoom-row.show{display:-webkit-box;display:-webkit-flex;display:flex}
.lc-zoom-row > * + *{margin-left:8px}
.lc-zoom-btn{pointer-events:auto;-webkit-appearance:none;border:none;cursor:pointer;background:rgba(0,0,0,.48);color:#fff;font-family:inherit;font-size:12px;font-weight:800;
  min-width:40px;height:32px;border-radius:500px;padding:0 12px;display:flex;align-items:center;justify-content:center;
  transition:background .15s,color .15s,transform .1s;-webkit-backdrop-filter:blur(8px);backdrop-filter:blur(8px)}
.lc-zoom-btn.active{background:var(--accent);color:#1A1200}
.lc-zoom-btn:active{-webkit-transform:scale(.92);transform:scale(.92)}
.lc-focus-ring{position:absolute;width:72px;height:72px;margin:-36px 0 0 -36px;z-index:5;border:2px solid #ffd60a;border-radius:6px;pointer-events:none;opacity:0;transform:scale(1.25);
  box-shadow:0 0 0 1px rgba(0,0,0,.35),0 0 12px rgba(255,214,10,.35)}
.lc-focus-ring.pulse{animation:lcFocusPulse .7s cubic-bezier(.2,.8,.3,1) forwards}
@keyframes lcFocusPulse{0%{opacity:0;transform:scale(1.25)}12%{opacity:1;transform:scale(1)}80%{opacity:1;transform:scale(1)}100%{opacity:0;transform:scale(1)}}
.lc-exposure-col{display:none !important}
.lc-bar{display:grid;grid-template-columns:1fr auto 1fr;align-items:center;padding:16px 20px 4px}
.lc-bar .lc-side:first-child{justify-self:start}
.lc-bar .lc-side:last-child,.lc-bar .lc-side.flip{justify-self:end}
.lc-side{width:46px;height:46px;border-radius:50%;background:rgba(255,255,255,.12);border:none;color:#fff;display:flex;align-items:center;justify-content:center;
  cursor:pointer;-webkit-appearance:none;flex-shrink:0;font-size:18px;transition:background .15s,transform .1s}
.lc-side:active{-webkit-transform:scale(.9);transform:scale(.9);background:rgba(255,255,255,.22)}
.lc-side.spacer{visibility:hidden;pointer-events:none}
.lc-shutter{width:78px;height:78px;border-radius:50%;border:4px solid #fff;background:transparent;padding:5px;cursor:pointer;-webkit-appearance:none;flex-shrink:0;justify-self:center;
  display:flex;align-items:center;justify-content:center;transition:transform .12s,opacity .12s;box-sizing:border-box}
.lc-shutter span{display:block;width:100%;height:100%;border-radius:50%;background:#fff;transition:background .15s,border-radius .18s,width .15s,height .15s}
.lc-shutter:active{-webkit-transform:scale(.92);transform:scale(.92)}
.lc-shutter:disabled{opacity:.45}
.lc-shutter.recording{border-color:#ff3b30}
.lc-shutter.recording span{border-radius:8px;width:42%;height:42%;background:#ff3b30}
.lc-mode-row{display:flex;align-items:center;justify-content:center;padding:12px 0 0}
.lc-mode-row > * + *{margin-left:24px}
.lc-mode-row.hidden{display:none}
.lc-mode-btn{background:none;border:none;cursor:pointer;color:rgba(255,255,255,.5);font-family:inherit;font-size:13px;font-weight:800;letter-spacing:.05em;text-transform:uppercase;
  padding:4px 2px;border-bottom:2px solid transparent;transition:color .15s,border-color .15s}
.lc-mode-btn.active{color:var(--accent);border-color:var(--accent)}
.lc-record-time{position:absolute;top:14px;left:50%;transform:translateX(-50%);z-index:4;display:none;align-items:center;background:rgba(0,0,0,.55);color:#fff;
  font-size:13px;font-weight:800;padding:5px 12px;border-radius:999px}
.lc-record-time.show{display:-webkit-box;display:-webkit-flex;display:flex}
.lc-record-dot{width:8px;height:8px;border-radius:50%;background:#ff3b30;margin-right:6px;animation:lcRecDot 1s infinite}
@keyframes lcRecDot{0%,100%{opacity:1}50%{opacity:.25}}

/* ---------- Crop stages (dark) ---------- */
.crop-stage{position:fixed !important;top:0;left:0;right:0;bottom:0;width:100%;height:100%;background:#000;z-index:9999;display:none;flex-direction:column}
.crop-stage.open{display:-webkit-flex !important;display:flex !important}
.crop-header{flex-shrink:0;padding:calc(18px + env(safe-area-inset-top)) 16px 12px;display:flex;align-items:center;justify-content:space-between;background:#000}
.crop-header span{font-weight:800;font-size:16px;color:#fff}
.crop-header button{background:none;border:none;color:var(--accent);font-family:inherit;font-weight:800;font-size:16px;cursor:pointer;padding:8px 10px}
.crop-header button.cancel{color:rgba(255,255,255,.62)}
.crop-area{flex:1;min-height:0;overflow:hidden;background:#000;position:relative}
.crop-area img{display:block;max-width:100%}
.cropper-container{max-height:100% !important}
.cropper-view-box,.cropper-face{border-radius:0}
.cropper-point{background:var(--accent);width:9px;height:9px}
.cropper-line{background:var(--accent)}
.fit-seg{display:flex;gap:6px;justify-content:center;margin-bottom:10px}
.fit-seg button{flex:1;max-width:130px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.08);color:rgba(255,255,255,.78);font-family:inherit;font-weight:700;font-size:13px;padding:9px 6px;border-radius:12px;cursor:pointer}
.fit-seg button.on{background:var(--accent);border-color:var(--accent);color:#fff}
.fit-preview{display:none;position:absolute;top:0;bottom:0;left:0;right:0;margin:auto;border-radius:6px;box-shadow:0 0 0 1px rgba(255,255,255,.12)}
.crop-footer{flex-shrink:0;padding:12px 16px calc(20px + env(safe-area-inset-bottom));background:#000}
.vcrop-area{flex:1;min-height:0;position:relative;background:#000;display:flex;align-items:center;justify-content:center;overflow:hidden}
.vcrop-area video{position:absolute;top:0;left:0;width:100%;height:100%;max-width:none;max-height:none;object-fit:contain;background:#000;display:block}
.vcrop-box{position:absolute;border:2px solid var(--accent);border-radius:6px;box-shadow:0 0 0 9999px rgba(0,0,0,.62);touch-action:none;cursor:move;box-sizing:border-box}
.vcrop-box::before{content:'';position:absolute;top:0;left:0;right:0;bottom:0;border:1px solid rgba(255,255,255,.35);pointer-events:none}

/* ---------- Video speed ---------- */
.video-crop-row{margin-top:14px;background:var(--surface);border-radius:28px;padding:14px 18px 16px}
.speed-head{display:flex;align-items:center;justify-content:space-between;margin:0 0 10px}
.speed-head .label{margin:0}
.speed-value{font-size:16px;font-weight:800;color:var(--accent-ink);letter-spacing:.02em;min-width:3.2em;text-align:right}
.speed-seg{display:flex;width:100%;border-radius:500px;overflow:hidden;background:rgba(var(--fg-rgb),.07);padding:3px;margin-bottom:12px}
.speed-chip{flex:1 1 0;min-width:0;border:none;border-radius:500px;background:transparent;color:var(--text2);padding:10px 2px;font-size:13px;font-weight:800;
  font-family:inherit;cursor:pointer;-webkit-appearance:none;appearance:none;line-height:1.1}
.speed-chip.active{background:var(--accent);color:var(--on-accent)}
.speed-chip:active{opacity:.85}
.speed-range-wrap{padding:0 2px 2px}
.speed-range{-webkit-appearance:none;appearance:none;width:100%;height:30px;background:transparent;margin:0;padding:0;touch-action:none}
.speed-range:focus{outline:none}
.speed-range::-webkit-slider-runnable-track{height:8px;border-radius:500px;background:rgba(var(--fg-rgb),.14)}
.speed-range::-webkit-slider-thumb{-webkit-appearance:none;appearance:none;width:26px;height:26px;border-radius:50%;background:var(--accent);
  border:3px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,.3);margin-top:-9px;cursor:pointer}
.speed-range::-moz-range-track{height:8px;border-radius:500px;background:rgba(var(--fg-rgb),.14);border:none}
.speed-range::-moz-range-thumb{width:22px;height:22px;border-radius:50%;background:var(--accent);border:3px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,.3);cursor:pointer}
.speed-range-ends{display:flex;justify-content:space-between;font-size:11px;font-weight:800;color:var(--muted);margin-top:2px}
.speed-hint{font-size:12px;color:var(--text2);font-weight:700;margin:8px 2px 0;line-height:1.4}
.speed-busy{display:flex;align-items:center;margin-top:10px;padding:11px 14px;border-radius:18px;background:rgba(var(--accent-rgb),.2);font-size:13px;font-weight:800;color:var(--text)}
.speed-busy .spinner{margin-right:8px}
.speed-busy.hidden{display:none}
.noUi-target{background:rgba(var(--fg-rgb),.1);border-radius:500px;border:none;box-shadow:none;height:6px}
.noUi-connect{background:var(--accent)}
.noUi-handle{width:22px !important;height:22px !important;border-radius:50%;background:var(--accent);border:3px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.4);top:-9px !important;right:-11px !important;cursor:grab}
.noUi-handle::before,.noUi-handle::after{display:none}
.noUi-tooltip{background:var(--surface2);color:var(--text);border:none;font-size:12px;font-weight:800;border-radius:10px;padding:2px 8px}

/* ---------- Floating nav ---------- */
.nav{position:fixed;left:50%;bottom:calc(10px + env(safe-area-inset-bottom));-webkit-transform:translateX(-50%);transform:translateX(-50%);
  width:calc(100% - 24px);max-width:520px;background:var(--glass);
  -webkit-backdrop-filter:blur(22px) saturate(1.6);backdrop-filter:blur(22px) saturate(1.6);
  border-radius:500px;box-shadow:var(--shadow),inset 0 0 0 1px var(--border);
  display:flex;justify-content:space-between;padding:6px;z-index:60}
.nav button{flex:1 1 0;min-width:0;background:none;border:none;border-radius:500px;font-family:inherit;font-size:10.5px;font-weight:800;color:var(--muted);
  display:flex;flex-direction:column;align-items:center;cursor:pointer;padding:8px 2px 7px;transition:background .2s,color .2s}
.nav button i{font-size:20px;line-height:1;margin-bottom:4px}
.nav button.active{background:rgba(var(--accent-rgb),.24);color:var(--text)}
.nav button.active i{color:var(--accent-ink)}
.nav button:active{-webkit-transform:scale(.95);transform:scale(.95)}

/* ---------- Toast / spinner / skeleton / progress ---------- */
.toast{position:fixed;bottom:calc(var(--nav-h) + 30px + env(safe-area-inset-bottom));left:50%;transform:translateX(-50%) translateY(20px);
  background:var(--text);color:var(--bg);padding:12px 22px;border-radius:500px;font-size:14px;font-weight:800;opacity:0;
  transition:all .25s;z-index:300;white-space:nowrap;max-width:88vw;overflow:hidden;text-overflow:ellipsis;box-shadow:var(--shadow)}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
.spinner{width:18px;height:18px;border:2.5px solid rgba(26,18,0,.22);border-top-color:rgba(26,18,0,.8);border-radius:50%;animation:spin .7s linear infinite;display:inline-block;vertical-align:-3px}
.spinner.light{border:2.5px solid rgba(var(--fg-rgb),.14);border-top-color:var(--accent-ink)}
.lc-hint .spinner.light,.crop-header .spinner.light{border-color:rgba(255,255,255,.2);border-top-color:var(--accent)}
@keyframes spin{to{transform:rotate(360deg)}}
.skeleton{border-radius:20px;background:linear-gradient(90deg,rgba(var(--fg-rgb),.06) 25%,rgba(var(--fg-rgb),.12) 50%,rgba(var(--fg-rgb),.06) 75%);
  background-size:200% 100%;animation:shimmer 1.3s infinite}
@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
.progress-pill{position:fixed;left:50%;bottom:calc(var(--nav-h) + 26px + env(safe-area-inset-bottom));z-index:90;
  -webkit-transform:translateX(-50%) translateY(12px);transform:translateX(-50%) translateY(12px);
  background:var(--glass);-webkit-backdrop-filter:blur(18px);backdrop-filter:blur(18px);color:var(--text);
  border-radius:500px;padding:10px 18px 10px 14px;font-size:13px;font-weight:800;display:none;align-items:center;
  box-shadow:var(--shadow),inset 0 0 0 1px rgba(var(--accent-rgb),.5);max-width:92vw;white-space:nowrap;opacity:0;
  transition:opacity .2s,-webkit-transform .2s,transform .2s;pointer-events:none}
.progress-pill.show{display:-webkit-box;display:-webkit-flex;display:flex;opacity:1;-webkit-transform:translateX(-50%) translateY(0);transform:translateX(-50%) translateY(0)}
.progress-pill .pp-spin{width:15px;height:15px;border:2.5px solid rgba(var(--accent-rgb),.3);border-top-color:var(--accent);border-radius:50%;
  -webkit-animation:spin .7s linear infinite;animation:spin .7s linear infinite;margin-right:10px;flex-shrink:0}
.progress-pill .pp-text{overflow:hidden;text-overflow:ellipsis;min-width:0}
.progress-pill .pp-bar{display:block;height:4px;background:rgba(var(--fg-rgb),.12);border-radius:2px;margin-top:6px;overflow:hidden}
.progress-pill .pp-bar > i{display:block;height:100%;width:0%;background:var(--accent);border-radius:2px;-webkit-transition:width .25s ease;transition:width .25s ease}
@media (max-width:520px){ .progress-pill{font-size:12.5px;padding:9px 16px 9px 12px} }
@media (max-width:375px){
  .page.active{animation:none}
  .skeleton{animation:none}
  .stk img{animation:none}
}

/* ================= RESPONSIVE (desktop layout) + MOTION ================= */
.topbar.scrolled{box-shadow:0 8px 30px rgba(122,74,0,.12)}
.nav{overflow:visible}
.nav-ind{position:absolute;top:0;left:0;width:0;height:0;border-radius:500px;background:rgba(var(--accent-rgb),.28);opacity:0;z-index:0;pointer-events:none;
  transition:transform .5s cubic-bezier(.3,1.35,.5,1),width .5s cubic-bezier(.3,1.35,.5,1),height .3s,opacity .2s}
.nav.has-ind button{position:relative;z-index:1}
.nav.has-ind button.active{background:none}
.nav button i{transition:transform .4s cubic-bezier(.3,1.6,.5,1)}
.nav button.active i{-webkit-transform:translateY(-1px) scale(1.12);transform:translateY(-1px) scale(1.12)}

/* staggered spring entrance for page content */
#page-dash.active > *:not(.tiles),#page-upload.active > *,#page-friends.active > *,#page-login.active > *,
#page-dash.active .tile{animation:rise .7s cubic-bezier(.22,1,.36,1) backwards}
@keyframes rise{from{opacity:0;-webkit-transform:translateY(26px) scale(.98);transform:translateY(26px) scale(.98)}to{opacity:1;-webkit-transform:none;transform:none}}
.page.active > *:nth-child(2){animation-delay:.06s}.page.active > *:nth-child(3){animation-delay:.12s}
.page.active > *:nth-child(4){animation-delay:.18s}.page.active > *:nth-child(5){animation-delay:.24s}
.page.active > *:nth-child(6){animation-delay:.3s}.page.active > *:nth-child(n+7){animation-delay:.36s}
#page-dash.active .tile:nth-child(1){animation-delay:.2s}#page-dash.active .tile:nth-child(2){animation-delay:.28s}#page-dash.active .tile:nth-child(3){animation-delay:.36s}
/* cards pop in as they are rendered */
.moment-card,.feed-slide{animation:cardIn .6s cubic-bezier(.22,1,.36,1) backwards}
.friend-row,.person-card,.queue-item,.viewer-row,.fa-card{animation:cardIn .5s cubic-bezier(.22,1,.36,1) backwards}
.moments-grid > :nth-child(6n+2){animation-delay:.05s}.moments-grid > :nth-child(6n+3){animation-delay:.1s}
.moments-grid > :nth-child(6n+4){animation-delay:.15s}.moments-grid > :nth-child(6n+5){animation-delay:.2s}.moments-grid > :nth-child(6n){animation-delay:.25s}
@keyframes cardIn{from{opacity:0;-webkit-transform:translateY(20px) scale(.95);transform:translateY(20px) scale(.95)}to{opacity:1;-webkit-transform:none;transform:none}}
/* streak card: sheen sweep + heartbeat */
.streak-card:after{content:'';position:absolute;top:0;left:-60%;width:36%;height:100%;pointer-events:none;
  background:linear-gradient(100deg,transparent,rgba(255,255,255,.38),transparent);-webkit-transform:skewX(-20deg);transform:skewX(-20deg);
  animation:sheen 5s ease-in-out 1.2s infinite}
@keyframes sheen{0%{left:-60%}35%,100%{left:140%}}
.streak-card .sc-img{animation:beat 2.6s ease-in-out infinite}
@keyframes beat{0%,100%{transform:rotate(10deg) scale(1)}10%{transform:rotate(10deg) scale(1.14)}20%{transform:rotate(10deg) scale(1)}30%{transform:rotate(10deg) scale(1.09)}42%{transform:rotate(10deg) scale(1)}}
.tile img,.tile.tile-primary img{transition:transform .55s cubic-bezier(.3,1.6,.5,1)}
.btn,.p-btn,.pill,.icon-btn,.tile,.nav button{transition-timing-function:cubic-bezier(.3,1.5,.5,1)}
.toast{transition:opacity .25s,transform .45s cubic-bezier(.3,1.6,.5,1)}

@media (hover:hover){
  .tile:hover{-webkit-transform:translateY(-5px);transform:translateY(-5px);box-shadow:var(--shadow)}
  .tile:hover img{-webkit-transform:rotate(-9deg) scale(1.12);transform:rotate(-9deg) scale(1.12)}
  .tile.tile-primary:hover img{-webkit-transform:rotate(-4deg) scale(1.12);transform:rotate(-4deg) scale(1.12)}
  .moment-card:hover{-webkit-transform:translateY(-5px) scale(1.02);transform:translateY(-5px) scale(1.02);box-shadow:var(--shadow)}
  .btn:hover{-webkit-transform:translateY(-2px);transform:translateY(-2px)}
  .btn:active{-webkit-transform:scale(.97);transform:scale(.97)}
  .contact-link{transition:transform .3s cubic-bezier(.3,1.5,.5,1),background .2s}
  .contact-link:hover{-webkit-transform:translateX(4px);transform:translateX(4px);background:rgba(var(--fg-rgb),.09)}
  .pill:hover,.icon-btn:hover{background:rgba(var(--fg-rgb),.14)}
  .nav button:hover{color:var(--text)}
  .streak-card:hover .sc-img{-webkit-transform:rotate(-6deg) scale(1.15);transform:rotate(-6deg) scale(1.15)}
}

@media (min-width:768px){
  .page{padding:20px 28px 8px}
  .topbar{padding-left:28px;padding-right:28px}
  .moments-grid{grid-gap:14px;gap:14px}
}
@media (min-width:900px){
  .app{padding-bottom:48px}
  .page{max-width:1120px}
  #page-login{max-width:640px;padding-top:32px}
  #page-upload,#page-friends{max-width:640px}
  .topbar{padding-top:calc(14px + env(safe-area-inset-top));padding-bottom:22px;
    background:linear-gradient(180deg,var(--topbar-bg) 55%,rgba(var(--bg-rgb),0) 100%);-webkit-backdrop-filter:none;backdrop-filter:none}
  .topbar.scrolled{box-shadow:none}
  /* nav moves to the top, centred, like the web header */
  .nav{top:calc(14px + env(safe-area-inset-top));bottom:auto;width:auto;max-width:none;padding:5px}
  .nav button{flex:0 0 auto;flex-direction:row;align-items:center;padding:10px 18px;font-size:14px}
  .nav button i{font-size:17px;margin:0 8px 0 0}
  .toast{bottom:32px}
  .progress-pill{bottom:28px}
  .moments-top-btn{bottom:32px;right:28px}
  .moments-new-pill{top:calc(84px + env(safe-area-inset-top))}
  /* dashboard becomes a 4-column board */
  #page-dash.active{display:grid;grid-template-columns:repeat(4,1fr);grid-gap:18px;gap:18px;align-content:start}
  #page-dash .hello{grid-column:1 / -1;margin:0}
  #page-dash .streak-card{grid-column:span 2;margin:0;padding:26px 30px}
  #page-dash .streak-card .sc-num{font-size:72px}
  #page-dash .streak-card .sc-img{width:128px}
  #page-dash .tiles{display:contents}
  #page-dash .tile{grid-column:span 2;min-height:200px;padding:22px 24px}
  #page-dash .tile img{height:84px}
  #page-dash .tile.tile-primary{padding:24px 28px}
  #page-dash .tile.tile-primary img{height:120px}
  #page-dash .tile h3{font-size:21px}
  #page-dash .tile.tile-primary h3{font-size:26px}
  #page-dash .contact-card{grid-column:span 2;margin:0}
  #page-dash .dash-foot{grid-column:span 2;align-self:center;padding:0}
  #page-dash .app-version{grid-column:1 / -1}
  .dash-greet .name{font-size:36px}
  /* login hero scales up */
  .lh-stage{height:330px}
  .lh-icon{width:132px;height:132px;margin:-74px 0 0 -66px;border-radius:38px}
  .stk.s1{width:150px}.stk.s2{width:96px}.stk.s3{width:94px}.stk.s4{width:110px}.stk.s5{width:74px}.stk.s6{width:84px}.stk.s7{width:72px}
  .lh-word{font-size:60px}.lh-tag{font-size:21px;max-width:360px}
  .login-card{max-width:460px;margin-left:auto;margin-right:auto}
  .moments-head h2,.page-title{font-size:34px}
  .moments-head{padding:4px 0 18px}
  .moments-grid{padding:0}
}
@media (min-width:1120px){
  .topbar{padding-left:calc((100% - 1064px)/2);padding-right:calc((100% - 1064px)/2)}
}
@media (prefers-reduced-motion:reduce){
  *,*:before,*:after{animation-duration:.001s !important;animation-iteration-count:1 !important;transition-duration:.001s !important}
  .spinner,.pp-spin,.icon-spin{animation-duration:.7s !important;animation-iteration-count:infinite !important}
}
</style>
<style id="perf-boost">
/* Skip layout/paint work for cards that are off-screen */
.moment-card{content-visibility:auto;contain-intrinsic-size:auto 180px;contain:layout paint style}
.feed-slide{content-visibility:auto;contain-intrinsic-size:auto 520px}
.moment-card img,.feed-card img{transform:translateZ(0);image-rendering:auto}
/* Heavy blur on scrolling content is the #1 cause of jank on phones */
@media (max-width:820px){
  .moment-top,.moment-overlay,.moment-video-badge{-webkit-backdrop-filter:none!important;backdrop-filter:none!important}
}
@media (prefers-reduced-motion:reduce){
  *,*::before,*::after{animation-duration:.01ms!important;animation-iteration-count:1!important;transition-duration:.01ms!important;scroll-behavior:auto!important}
}
</style>
</head>
<body>
<div class="app">
  <header class="topbar">
    <div class="brand"><img src="{{ favicon_url }}" alt=""><span class="wordmark">Locket</span><span class="mini">Mini</span></div>
    <div class="top-actions">
      {% if logged_in %}<button type="button" class="pill" onclick="showPage('friends')" title="Bạn bè" aria-label="Bạn bè">
        <i class="bi bi-people-fill" style="font-size:15px"></i>
        <span id="friendCountPill">–</span>
      </button>{% endif %}
      <button type="button" class="icon-btn" id="themeBtn" onclick="toggleTheme()" title="Đổi giao diện sáng/tối" aria-label="Đổi giao diện"><i class="bi bi-moon-stars-fill"></i></button>
    </div>
  </header>

  {% if not logged_in %}
  <div class="page active" id="page-login">
    <div class="login-hero">
      <div class="lh-stage">
        <span class="stk s1"><span class="stk-r"><img src="/s/bff.webp" alt=""></span></span>
        <span class="stk s2"><span class="stk-r"><img src="/s/i_heart_locket.webp" alt=""></span></span>
        <span class="stk s3"><span class="stk-r"><img src="/s/megaphone.webp" alt=""></span></span>
        <span class="stk s4"><span class="stk-r"><img src="/s/locket_red.webp" alt=""></span></span>
        <span class="stk s5"><span class="stk-r"><img src="/s/hearts_duo.webp" alt=""></span></span>
        <span class="stk s6"><span class="stk-r"><img src="/s/heart_3d.webp" alt=""></span></span>
        <span class="stk s7"><span class="stk-r"><img src="/s/locket_tile.webp" alt=""></span></span>
        <img class="lh-icon" src="{{ favicon_url }}" alt="">
      </div>
      <h1 class="lh-word">Locket</h1>
      <p class="lh-tag">Best friends first.<br>Ảnh từ bạn thân, ngay trên web.</p>
    </div>
    <div class="card login-card">
      <label class="label">Email</label>
      <input class="input" id="loginEmail" type="email" placeholder="your@email.com" autocomplete="email" inputmode="email">
      <label class="label" style="margin-top:14px">Password</label>
      <input class="input" id="loginPass" type="password" placeholder="Password" autocomplete="current-password">
      <label class="remember-row"><input type="checkbox" id="loginRemember" checked> Ghi nhớ đăng nhập</label>
      <button class="btn" style="margin-top:18px" onclick="doLogin()" id="loginBtn">Log In</button>
    </div>
    <div class="login-note">Đăng nhập bằng tài khoản Locket của bạn.<br>Mật khẩu chỉ dùng để xác thực với Firebase.</div>
  </div>
  {% else %}

  <div class="page active" id="page-dash">
    <div class="hello">
      <div id="dashAvatar"></div>
      <div class="dash-greet">
        <p class="hi" id="greetLine">Xin chào</p>
        <p class="name" id="dashName">{{ boot_name or '…' }}</p>
      </div>
    </div>
    <div class="streak-card">
      <div class="sc-text">
        <div class="sc-label">Locket Streak</div>
        <div class="sc-num"><span id="streakNum">–</span><small>ngày</small></div>
      </div>
      <img class="sc-img" src="/s/heart_3d.webp" alt="">
    </div>
    <div class="tiles">
      <button type="button" class="tile tile-primary" onclick="showPage('upload')">
        <div>
          <h3>Đăng khoảnh khắc</h3>
          <p>Chụp hoặc chọn ảnh/video rồi gửi cho bạn bè.</p>
          <span class="tile-cta">Mở camera</span>
        </div>
        <img src="/s/career_2.webp" alt="">
      </button>
      <button type="button" class="tile" onclick="showPage('moments')">
        <img src="/s/hearts_duo.webp" alt="">
        <h3>Moments</h3>
        <p>Xem khoảnh khắc mới từ bạn bè.</p>
      </button>
      <button type="button" class="tile" onclick="showPage('friends')">
        <img src="/s/parent_2.webp" alt="">
        <h3>Bạn bè</h3>
        <p>Tìm bạn, lời mời &amp; hoạt động.</p>
      </button>
    </div>
    <div class="card contact-card">
      <div class="label-row">Liên hệ chủ web</div>
      <div class="contact-links">
        <a class="contact-link" href="https://www.threads.net/@anhztuan.1710" target="_blank" rel="noopener">
          <i class="bi bi-threads"></i>
          <span>Threads</span>
          <span class="cl-handle">@anhztuan.1710</span>
        </a>
        <a class="contact-link" href="https://www.instagram.com/anhztuan.1710" target="_blank" rel="noopener">
          <i class="bi bi-instagram"></i>
          <span>Instagram</span>
          <span class="cl-handle">@anhztuan.1710</span>
        </a>
      </div>
    </div>
    <div class="dash-foot"><span class="df-brand"><img src="{{ favicon_url }}" alt="">Locket</span></div>
    <div class="app-version">{{ app_version }}</div>
  </div>

  <div class="page" id="page-upload">
    <h2 class="page-title">Khoảnh khắc mới</h2>
    <div class="offline-banner" id="offlineBanner">
      <i class="bi bi-wifi-off" style="font-size:16px"></i>
      Đang offline — ảnh sẽ lưu máy và tự đăng khi có mạng
    </div>
    <div class="set-card">
      <div class="switch-row">
        <div>
          <div class="sw-text">Camera trực tiếp</div>
          <div class="sw-sub">Camera trong web — không lưu vào thư viện máy</div>
        </div>
        <label class="switch"><input type="checkbox" id="liveCameraSwitch" onchange="toggleLiveCamera(this.checked)"><span class="slider"></span></label>
      </div>
      <div class="switch-row">
        <div>
          <div class="sw-text">Tự mở camera khi vào web</div>
          <div class="sw-sub">Vào trang → tab Đăng → mở camera</div>
        </div>
        <label class="switch"><input type="checkbox" id="autoCameraSwitch" onchange="toggleAutoCamera(this.checked)"><span class="slider"></span></label>
      </div>
      <div class="switch-row">
        <div>
          <div class="sw-text">Chế độ máy yếu</div>
          <div class="sw-sub">Giảm độ phân giải camera &amp; ảnh — mượt hơn trên máy cũ</div>
        </div>
        <label class="switch"><input type="checkbox" id="lowPowerSwitch" onchange="toggleLowPower(this.checked)"><span class="slider"></span></label>
      </div>
    </div>
    <div class="preview-box" id="previewBox" onclick="openCapture()">
      <div class="preview-hint" id="uploadPlaceholder">
        <img class="ph-img" src="/s/megaphone.webp" alt="">
        Chạm để chọn ảnh/video<br><span style="opacity:.7;font-size:12px">hoặc dán ảnh (Ctrl+V)</span>
      </div>
      <img id="previewImg" class="hidden" alt="preview">
      <video id="previewVid" class="hidden" playsinline muted loop autoplay onloadedmetadata="onPreviewVidMeta.call(this)"></video>
      <button type="button" class="pb-queue-btn hidden" id="queueOnlyBtn" onclick="event.stopPropagation();doSaveQueueOnly()" title="Lưu vào hàng đợi, không gửi ngay" aria-label="Lưu vào hàng đợi"><i class="bi bi-inbox"></i></button>
    </div>
    <div class="video-crop-row hidden" id="videoCropRow">
      <div class="speed-head">
        <span class="label" style="margin:0">Tốc độ video</span>
        <span class="speed-value" id="videoSpeedLabel">1x</span>
      </div>
      <div class="speed-seg" id="speedPresets" role="group" aria-label="Tốc độ">
        <button type="button" class="speed-chip" data-speed="0.5" onclick="pickVideoSpeed(0.5)">0.5×</button>
        <button type="button" class="speed-chip active" data-speed="1" onclick="pickVideoSpeed(1)">1×</button>
        <button type="button" class="speed-chip" data-speed="2" onclick="pickVideoSpeed(2)">2×</button>
        <button type="button" class="speed-chip" data-speed="3" onclick="pickVideoSpeed(3)">3×</button>
        <button type="button" class="speed-chip" data-speed="4" onclick="pickVideoSpeed(4)">4×</button>
        <button type="button" class="speed-chip" data-speed="8" onclick="pickVideoSpeed(8)">8×</button>
      </div>
      <div class="speed-range-wrap">
        <input type="range" id="videoSpeedRange" class="speed-range" min="0.5" max="8" step="0.1" value="1"
          oninput="onSpeedRangeInput(this.value)" onchange="onSpeedRangeCommit(this.value)" aria-label="Tốc độ chi tiết">
        <div class="speed-range-ends"><span>0.5×</span><span>8×</span></div>
      </div>
      <div class="speed-hint" id="speedHint">Chạm preset hoặc kéo thanh để xem trước. Video đăng lên sẽ được xuất lại cho mượt.</div>
      <div class="speed-busy hidden" id="speedBusy"><span class="spinner light"></span> <span id="speedBusyText">Đang xuất…</span></div>
    </div>
    <div class="live-cam hidden" id="liveCam">
      <div class="lc-frame">
        <video id="lcVideo" playsinline webkit-playsinline muted autoplay></video>
        <div class="lc-hint hidden" id="lcHint"><span class="spinner light"></span><br>Đang mở camera…</div>
        <div class="lc-flash-overlay" id="lcFlashOverlay"></div>
        <button type="button" class="lc-flash-btn hidden" id="lcFlashBtn" onclick="toggleTorch(event)" aria-label="Đèn flash"><i class="bi bi-lightning-charge-fill"></i></button>
        <div class="lc-zoom-row" id="lcZoomRow"></div>
        <div class="lc-exposure-col" id="lcExposureCol">
          <i class="bi bi-brightness-high-fill"></i>
          <input type="range" class="lc-exposure-range" id="lcExposureRange" min="0" max="1" step="0.01" value="0.5" oninput="onLcExposureInput(this.value)" aria-label="Độ sáng">
          <i class="bi bi-brightness-low"></i>
        </div>
        <div class="lc-record-time" id="lcRecordTime"><span class="lc-record-dot"></span><span id="lcRecordTimeText">0:00</span></div>
      </div>
      <div class="lc-mode-row hidden" id="lcModeRow">
        <button type="button" class="lc-mode-btn active" data-mode="photo" onclick="setLcCaptureMode('photo')">Ảnh</button>
        <button type="button" class="lc-mode-btn" data-mode="video" onclick="setLcCaptureMode('video')">Video</button>
      </div>
      <div class="lc-bar">
        <div class="lc-side spacer" aria-hidden="true"></div>
        <button type="button" class="lc-shutter" id="lcShutterBtn" onclick="onLcShutterTap(event)" aria-label="Chụp"><span></span></button>
        <button type="button" class="lc-side flip" onclick="flipLiveCamera(event)" aria-label="Đổi camera"><i class="bi bi-arrow-repeat"></i></button>
      </div>
    </div>
    <div class="upload-actions hidden" id="uploadActions">
      <button class="btn-ghost btn" onclick="openCapture()">Đổi ảnh</button>
      <button class="btn-ghost btn icon-only" onclick="downloadCapturedMedia()" aria-label="Tải xuống"><i class="bi bi-download"></i></button>
      <button class="btn-ghost btn" onclick="clearUpload()">Xoá</button>
    </div>
    <input type="file" id="fileInput" accept="image/*,video/*" onchange="onFilePick(event)">
    <input type="file" id="cameraInput" accept="image/*" capture="environment" class="hidden" onchange="onFilePick(event)">
    <label class="label" style="margin-top:18px">Chú thích</label>
    <input class="input" id="caption" placeholder="Viết gì đó..." maxlength="200">
    <div class="send-row"><button class="btn" onclick="doUpload()" id="uploadBtn">Gửi cho tất cả bạn bè</button></div>
    <div class="queue-box hidden" id="queueBox">
      <div class="qb-head"><span>Hàng đợi đăng</span><span id="queueCount">0</span></div>
      <div id="queueList"></div>
    </div>
  </div>

  <div class="page" id="page-moments">
    <div class="moments-head">
      <h2>Moments</h2>
      <div class="mv-seg" id="mvSeg" role="group" aria-label="Chế độ xem">
        <button type="button" data-mv="feed" title="Danh sách" aria-label="Danh sách"><i class="bi bi-view-stacked"></i></button>
        <button type="button" data-mv="2" title="2 cột" aria-label="2 cột">2</button>
        <button type="button" data-mv="3" title="3 cột" aria-label="3 cột">3</button>
        <button type="button" data-mv="4" title="4 cột" aria-label="4 cột">4</button>
      </div>
      <button type="button" class="moments-refresh-btn" id="momentsRefreshBtn" onclick="refreshMomentsNow();return false;" title="Làm mới Moments" aria-label="Làm mới Moments">
        <i class="bi bi-arrow-clockwise" id="momentsRefreshIcon"></i>
      </button>
    </div>
    <button type="button" class="moments-new-pill" id="momentsNewPill" onclick="applyPendingMoments();return false;"></button>
    <div class="moments-grid" id="momentsGrid"></div>
    <div class="moments-feed" id="momentsFeed"></div>
    <div id="momentsMoreSentinel" style="height:1px"></div>
    <div id="momentsMore" class="moments-more hidden">Đang tải thêm…</div>
    <div class="moments-empty hidden" id="momentsEmpty">
      <i class="bi bi-collection" style="font-size:48px" id="momentsEmptyIcon"></i>
      <div id="momentsEmptyText">Chưa có khoảnh khắc nào</div>
      <button type="button" class="moments-empty-retry hidden" id="momentsEmptyRetry" onclick="loadMoments(true);return false;">Thử lại</button>
    </div>
  </div>
  <button type="button" class="moments-top-btn" id="momentsTopBtn" onclick="scrollMomentsTop();return false;" ontouchend="scrollMomentsTop();return false;" title="Moments mới nhất" aria-label="Lên đầu">
    <i class="bi bi-chevron-up" style="font-size:22px"></i>
  </button>

  <div class="page" id="page-friends">
    <h2 class="page-title">Bạn bè</h2>
    <div class="people-search">
      <input class="input" id="peopleQuery" type="text" placeholder="Find @username, invite link or UID" autocapitalize="off" autocorrect="off" autocomplete="off" onkeydown="if(event.key==='Enter'){searchPeople();return false}">
      <button type="button" class="btn people-go" onclick="searchPeople();return false;" aria-label="Search"><i class="bi bi-search"></i></button>
    </div>
    <div id="myInvite" class="people-msg"></div>
    <div id="peopleMsg" class="people-msg"></div>
    <div id="peopleResult"></div>
    <div id="peopleRequests"></div>
    <details class="p-adv">
      <summary>App Check token <span id="appcheckState"></span></summary>
      <div class="p-adv-body">
        <input class="input" id="appcheckToken" type="text" placeholder="Paste X-Firebase-AppCheck value" autocapitalize="off" autocorrect="off" autocomplete="off">
        <button type="button" class="p-btn" onclick="saveAppCheck();return false;">Save</button>
      </div>
    </details>
    <div class="fa-card" id="friendActivity">
      <div class="fa-tabs">
        <button type="button" class="active" data-fa="new">Bạn mới<span class="fa-count" id="faNewCount">0</span></button>
        <button type="button" data-fa="gone">Đã xoá kết bạn<span class="fa-count" id="faGoneCount">0</span></button>
      </div>
      <div id="faBody"><div class="fa-empty">Đang tải…</div></div>
      <div class="fa-foot" id="faFoot"></div>
    </div>
    <div class="friends-count" id="friendsCountLine">Đang tải...</div>
    <div id="friendsList"></div>
    <div class="friends-empty hidden" id="friendsEmpty">
      <img src="/s/help_2.webp" alt="" style="width:90px;height:auto;display:block;margin:0 auto 10px">
      <div>Chưa có bạn bè</div>
    </div>
  </div>

  <nav class="nav">
    <button class="active" onclick="showPage('dash')" id="nav-dash"><i class="bi bi-house-door-fill"></i>Trang chủ</button>
    <button onclick="showPage('upload')" id="nav-upload"><i class="bi bi-camera-fill"></i>Đăng</button>
    <button onclick="showPage('moments')" id="nav-moments"><i class="bi bi-grid-3x3-gap-fill"></i>Moments</button>
    <button onclick="showPage('friends')" id="nav-friends"><i class="bi bi-people-fill"></i>Bạn bè</button>
    <button onclick="doLogout()"><i class="bi bi-box-arrow-right"></i>Thoát</button>
  </nav>

  <div class="crop-stage" id="cropStage">
    <div class="crop-header">
      <button class="cancel" onclick="cancelCrop()">Huỷ</button>
      <span>Crop ảnh 1:1</span>
      <button onclick="confirmCrop()">Xong</button>
    </div>
    <div class="crop-area" id="cropArea"><img id="cropImg" alt=""><canvas id="fitPreview" class="fit-preview" width="640" height="640"></canvas></div>
    <div class="crop-footer"><div class="fit-seg" id="fitSeg"><button type="button" data-m="crop" onclick="setCropMode('crop')">Cắt vuông</button><button type="button" data-m="black" onclick="setCropMode('black')">Giữ nguyên · đen</button><button type="button" data-m="blur" onclick="setCropMode('blur')">Giữ nguyên · mờ</button></div><div id="cropHint" style="text-align:center;color:rgba(255,255,255,.62);font-size:12px">Kéo, phóng to/nhỏ để căn ảnh vuông</div></div>
  </div>

  <div class="crop-stage" id="videoCropStage">
    <div class="crop-header">
      <button class="cancel" onclick="cancelVideoCrop()">Huỷ</button>
      <span>Crop video 1:1</span>
      <button onclick="confirmVideoCrop()">Xong</button>
    </div>
    <div class="vcrop-area" id="vcropArea">
      <video id="vcropVideo" playsinline muted loop autoplay></video>
      <div class="vcrop-box" id="vcropBox"></div>
    </div>
    <div class="crop-footer"><div style="text-align:center;color:rgba(255,255,255,.62);font-size:12px">Kéo khung vuông để chọn phần video muốn giữ</div></div>
  </div>

  <div class="viewer hidden" id="viewerStage"></div>

  {% endif %}
</div>
<div class="toast" id="toast"></div>
<div class="progress-pill" id="progressPill" aria-live="polite">
  <span class="pp-spin"></span>
  <div style="min-width:0;flex:1">
    <div class="pp-text" id="progressPillText">Đang tải…</div>
    <div class="pp-bar"><i id="progressPillBar"></i></div>
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/cropperjs/1.6.2/cropper.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.7.1/nouislider.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.7.1/nouislider.min.js"></script>
<script>
const $=id=>document.getElementById(id);
const GOLD_BADGE="{{ gold_badge }}", CELEB_BADGE="{{ celeb_badge }}";
const BOOT_NAME={{ (boot_name or '')|tojson }};
const BOOT_UID={{ (boot_uid or '')|tojson }};
const BOOT_PHOTO={{ (boot_photo or '')|tojson }};
var VIDEO_BUDGET={{ video_budget|int }};
let croppedBlob=null, originalFile=null, isVideo=false, cropper=null;
let videoCropPayload=null, videoThumbBlob=null;
var videoCropOriginal='', _bestPng=true;
let originalVideoBlob=null, videoSpeedFactor=1, videoSpeedSliderObj=null;
var CAN_RENDER_VIDEO_SPEED = (typeof MediaRecorder!=='undefined' &&
  typeof document.createElement('canvas').captureStream==='function');
/* Crop metadata shape confirmed against a real captured request from
   binhake's own web client (HAR capture, Aug 2026): exactly {type, crop,
   video} — no extra "view" key. x/y/w/h are in the video's own native pixels
   (not CSS/displayed pixels) — the crop modal below does that conversion. */
function videoCropJson(vw, vh, x, y, side){
  side=Math.max(2, Math.min(Math.round(side), vw, vh)); side-=side%2;
  x=Math.max(0, Math.min(Math.round(x), vw-side)); x-=x%2;
  y=Math.max(0, Math.min(Math.round(y), vh-side)); y-=y%2;
  return JSON.stringify({type:'video', crop:{x:x,y:y,w:side,h:side}, video:{videoWidth:vw,videoHeight:vh}});
}
function cropFromJson(json, vw, vh){
  try{
    var o=JSON.parse(json), c=o&&o.crop;
    if(!c) return null;
    var ow=(o.video&&o.video.videoWidth)||vw, oh=(o.video&&o.video.videoHeight)||vh;
    var s=Math.max(2, Math.min(Math.round(c.w*vw/ow), vw, vh));
    var x=Math.max(0, Math.min(Math.round(c.x*vw/ow), vw-s));
    var y=Math.max(0, Math.min(Math.round(c.y*vh/oh), vh-s));
    return {x:x, y:y, s:s};
  }catch(e){ return null; }
}
/* Preview vuông dùng object-fit:cover -> phải dịch object-position theo vùng crop, nếu không luôn hiện giữa khung */
function applyPreviewCrop(json){
  var pv=$('previewVid'); if(!pv) return;
  var px=50, py=50;
  try{
    var o=JSON.parse(json), c=o.crop, vw=o.video.videoWidth, vh=o.video.videoHeight;
    if(vw>vh && vw-c.w>0) px=Math.max(0, Math.min(100, c.x/(vw-c.w)*100));
    else if(vh>vw && vh-c.h>0) py=Math.max(0, Math.min(100, c.y/(vh-c.h)*100));
  }catch(e){}
  pv.style.objectPosition=px+'% '+py+'%';
}
function fixVideoName(name, blob){
  var t=(blob&&blob.type)||'';
  var ext=t.indexOf('mp4')>=0?'mp4':(t.indexOf('webm')>=0?'webm':(t.indexOf('quicktime')>=0?'mov':''));
  if(!ext) return name||'video.mp4';
  var base=String(name||'video'), i=base.lastIndexOf('.');
  if(i>0) base=base.slice(0, i);
  return base+'.'+ext;
}
function centerVideoCropJson(vw, vh){
  var side=Math.min(vw, vh);
  return videoCropJson(vw, vh, (vw-side)/2, (vh-side)/2, side);
}
function onPreviewVidMeta(){
  // Some iOS versions leave a freshly-assigned <video src> paused on a black
  // frame until something explicitly calls play() — autoplay alone isn't
  // always enough right after a programmatic src change. This is the actual
  // fix for the preview going black.
  var v=this;
  var p=v.play();
  if(p && p.catch) p.catch(function(){});
  const speedRow=$('videoCropRow');
  if(speedRow) speedRow.classList.toggle('hidden', !(isVideo && CAN_RENDER_VIDEO_SPEED));
}

/* ===================== Video crop stage (custom — Cropper.js only handles
   <img>, there's no real off-the-shelf library for cropping a live <video>
   element, so this is a small hand-built drag-to-position overlay). ===== */
var _vcrop = {rect:null, dragging:false, startX:0, startY:0, boxLeft:0, boxTop:0, onDone:null, blob:null};
function getVideoContentRect(video){
  var cw=video.clientWidth, ch=video.clientHeight;
  var vw=video.videoWidth, vh=video.videoHeight;
  if(!cw||!ch||!vw||!vh) return null;
  var scale=Math.min(cw/vw, ch/vh);
  var rw=vw*scale, rh=vh*scale;
  return {left:(cw-rw)/2, top:(ch-rh)/2, width:rw, height:rh, scale:scale, vw:vw, vh:vh};
}
function openVideoCropStage(blob, onDone){
  _vcrop.blob=blob; _vcrop.onDone=onDone;
  const v=$('vcropVideo');
  v.src=URL.createObjectURL(blob);
  v.onloadedmetadata=function(){
    var p=v.play(); if(p&&p.catch) p.catch(function(){});
    if(v.videoWidth>0 && v.videoWidth===v.videoHeight){
      // Already square — skip the crop UI entirely, per request.
      finishVideoCropStage(centerVideoCropJson(v.videoWidth, v.videoHeight));
      return;
    }
    $('videoCropStage').classList.add('open');
    document.body.style.overflow='hidden';
    requestAnimationFrame(function(){ requestAnimationFrame(initVcropBox); });
  };
  v.onerror=function(){ toast('Không đọc được video'); if(_vcrop.onDone) _vcrop.onDone(centerVideoCropJson(1,1)); };
}
function initVcropBox(){
  const v=$('vcropVideo'), box=$('vcropBox');
  if(!v || !box) return;
  const cr=getVideoContentRect(v);
  if(!cr || cr.width < 8 || cr.height < 8){
    // Layout not ready (iOS) — retry once
    setTimeout(initVcropBox, 80);
    return;
  }
  const side=Math.min(cr.width, cr.height);
  box.style.width=side+'px';
  box.style.height=side+'px';
  box.style.left=(cr.left+(cr.width-side)/2)+'px';
  box.style.top=(cr.top+(cr.height-side)/2)+'px';
  bindVcropDrag();
}
function _vcropOnResize(){
  if(!$('videoCropStage') || !$('videoCropStage').classList.contains('open')) return;
  initVcropBox();
}
if(typeof window !== 'undefined'){
  window.addEventListener('resize', _vcropOnResize);
  window.addEventListener('orientationchange', function(){ setTimeout(_vcropOnResize, 200); });
}
function bindVcropDrag(){
  const box=$('vcropBox');
  if(box._bound) return;
  box._bound=true;
  function start(clientX, clientY){
    _vcrop.dragging=true;
    _vcrop.startX=clientX; _vcrop.startY=clientY;
    _vcrop.boxLeft=parseFloat(box.style.left)||0;
    _vcrop.boxTop=parseFloat(box.style.top)||0;
  }
  function move(clientX, clientY){
    if(!_vcrop.dragging) return;
    const v=$('vcropVideo');
    const cr=getVideoContentRect(v);
    if(!cr) return;
    const side=box.clientWidth;
    let left=_vcrop.boxLeft+(clientX-_vcrop.startX);
    let top=_vcrop.boxTop+(clientY-_vcrop.startY);
    left=Math.max(cr.left, Math.min(cr.left+cr.width-side, left));
    top=Math.max(cr.top, Math.min(cr.top+cr.height-side, top));
    box.style.left=left+'px';
    box.style.top=top+'px';
  }
  function end(){ _vcrop.dragging=false; }
  box.addEventListener('touchstart', function(e){ var t=e.touches[0]; start(t.clientX,t.clientY); e.preventDefault(); }, {passive:false});
  box.addEventListener('touchmove', function(e){ var t=e.touches[0]; move(t.clientX,t.clientY); e.preventDefault(); }, {passive:false});
  box.addEventListener('touchend', end, {passive:true});
  box.addEventListener('mousedown', function(e){ start(e.clientX,e.clientY); e.preventDefault(); });
  window.addEventListener('mousemove', function(e){ if(_vcrop.dragging) move(e.clientX,e.clientY); });
  window.addEventListener('mouseup', end);
}
function cancelVideoCrop(){
  $('videoCropStage').classList.remove('open');
  document.body.style.overflow='';
  try{ $('vcropVideo').pause(); }catch(e){}
  $('fileInput').value='';
  if($('cameraInput')) $('cameraInput').value='';
  _vcrop.blob=null; _vcrop.onDone=null;
}
function confirmVideoCrop(){
  const v=$('vcropVideo'), box=$('vcropBox');
  const cr=getVideoContentRect(v);
  let cropJson;
  if(cr){
    const boxLeft=parseFloat(box.style.left)||cr.left, boxTop=parseFloat(box.style.top)||cr.top;
    const nativeX=(boxLeft-cr.left)/cr.scale, nativeY=(boxTop-cr.top)/cr.scale;
    const nativeSide=box.clientWidth/cr.scale;
    cropJson=videoCropJson(v.videoWidth, v.videoHeight, nativeX, nativeY, nativeSide);
  }else{
    cropJson=centerVideoCropJson(v.videoWidth||1, v.videoHeight||1);
  }
  $('videoCropStage').classList.remove('open');
  document.body.style.overflow='';
  try{ v.pause(); }catch(e){}
  finishVideoCropStage(cropJson);
}
function finishVideoCropStage(cropJson){
  const blob=_vcrop.blob, cb=_vcrop.onDone;
  _vcrop.blob=null; _vcrop.onDone=null;
  if(cb) cb(cropJson, blob);
}

/* ===================== Video speed / time-lapse =====================
   Mobile-first:
   - Preview: HTMLVideoElement.playbackRate only (instant, no encode).
   - Bake for upload: continuous play at playbackRate + canvas captureStream.
     Seek-per-frame was dropped — on phones Safari snaps to keyframes so the
     result skipped chunks and looked broken.
   - Downscale on phone (max 720) so MediaRecorder keeps up without dropping.
*/
var _speedRenderToken = 0;
var _speedBaking = false;

function updateSpeedChipUI(x){
  var row = $('speedPresets');
  if(!row) return;
  var chips = row.querySelectorAll('.speed-chip');
  var best = null, bestDist = 1e9;
  for(var i=0;i<chips.length;i++){
    var s = parseFloat(chips[i].getAttribute('data-speed'));
    var d = Math.abs(s - x);
    if(d < bestDist){ bestDist = d; best = chips[i]; }
  }
  for(var j=0;j<chips.length;j++){
    // Strict match for discrete chips so 2x doesn't light up 3x
    var s2 = parseFloat(chips[j].getAttribute('data-speed'));
    chips[j].classList.toggle('active', Math.abs(s2 - x) < 0.05);
  }
}

function setSpeedLabel(x){
  var lbl = $('videoSpeedLabel');
  if(!lbl) return;
  var t = Math.round(x * 10) / 10;
  lbl.textContent = (Math.abs(t - Math.round(t)) < 0.05 ? Math.round(t) : t.toFixed(1)) + '×';
}

function setSpeedBusy(on, msg){
  var box = $('speedBusy');
  var txt = $('speedBusyText');
  if(txt && msg) txt.textContent = msg;
  if(box) box.classList.toggle('hidden', !on);
  _speedBaking = !!on;
}

function syncSpeedRange(x){
  var r = $('videoSpeedRange');
  if(r) r.value = String(x);
}

function applyPreviewSpeed(x){
  var pv = $('previewVid');
  if(!pv || !isVideo) return;
  try{
    pv.playbackRate = Math.min(Math.max(x, 0.0625), 16);
  }catch(e){}
  // Keep looping so user can judge the speed
  try{ if(pv.paused){ var p=pv.play(); if(p&&p.catch) p.catch(function(){}); } }catch(e){}
}

function onSpeedRangeInput(v){
  var x = parseFloat(v);
  if(!isFinite(x)) return;
  videoSpeedFactor = x;
  setSpeedLabel(x);
  updateSpeedChipUI(x);
  applyPreviewSpeed(x);
}

function onSpeedRangeCommit(v){
  setVideoSpeed(parseFloat(v));
}

function initVideoSpeedSlider(){
  // Native range — more reliable touch UI than noUiSlider on iOS
  syncSpeedRange(videoSpeedFactor || 1);
  setSpeedLabel(videoSpeedFactor || 1);
  updateSpeedChipUI(videoSpeedFactor || 1);
  // Keep legacy handle null so old noUi paths no-op
  videoSpeedSliderObj = null;
}

function pickVideoSpeed(x){
  x = Number(x);
  if(!isFinite(x) || x <= 0) return;
  syncSpeedRange(x);
  setSpeedLabel(x);
  updateSpeedChipUI(x);
  setVideoSpeed(x);
}

function setVideoSpeed(x){
  if(!isVideo || !originalVideoBlob) return;
  x = Math.min(Math.max(Number(x)||1, 0.5), 8);
  videoSpeedFactor = x;
  setSpeedLabel(x);
  updateSpeedChipUI(x);
  syncSpeedRange(x);
  applyPreviewSpeed(x);

  if(Math.abs(x - 1) < 0.05){
    // Restore pristine original for upload
    _speedRenderToken++;
    setSpeedBusy(false);
    croppedBlob = originalVideoBlob;
    videoCropPayload = videoCropOriginal;
    refreshVideoThumb();
    var pv = $('previewVid');
    if(pv){
      var cur = pv.currentTime || 0;
      // Only reset src if we previously swapped in a baked blob
      if(pv.dataset && pv.dataset.baked === '1'){
        pv.src = URL.createObjectURL(originalVideoBlob);
        pv.dataset.baked = '0';
        pv.onloadedmetadata = function(){
          try{ pv.currentTime = Math.min(cur, (pv.duration||0)*0.99); }catch(e){}
          applyPreviewSpeed(1);
        };
      } else {
        applyPreviewSpeed(1);
      }
    }
    return;
  }

  if(!CAN_RENDER_VIDEO_SPEED){
    toast('Máy này chỉ xem trước tốc độ — đăng lên vẫn là 1× (cần iOS 14.3+ / Chrome)');
    return;
  }
  // Preview stays live via playbackRate; bake in background for the upload blob
  renderVideoAtSpeed(x);
}

var _bakeSeq=0;
function evenInt(n){ n=Math.round(n); return n-(n%2); }
function pickBakeMime(){
  var c=['video/mp4;codecs=avc1','video/mp4'];
  if(!IS_PHONE) c.push('video/webm;codecs=vp9');
  c.push('video/webm;codecs=vp8','video/webm');
  for(var i=0;i<c.length;i++){ try{ if(MediaRecorder.isTypeSupported(c[i])) return c[i]; }catch(e){} }
  return '';
}
function recordOnce(v, cr, side, bps, rate, outDur, fps, cancelled){
  return new Promise(function(res, rej){
    var canvas=document.createElement('canvas'); canvas.width=side; canvas.height=side;
    var ctx=canvas.getContext('2d', {alpha:false});
    try{ ctx.imageSmoothingEnabled=true; ctx.imageSmoothingQuality='high'; }catch(e){}
    var stream, rec, chunks=[], mime=pickBakeMime();
    try{
      stream=canvas.captureStream(fps);
      var o={videoBitsPerSecond:bps}; if(mime) o.mimeType=mime;
      rec=new MediaRecorder(stream, o);
    }catch(e){ rej(new Error('Trình duyệt không hỗ trợ xuất video')); return; }
    var drawing=false, raf=0, vfc=0, settled=false, poll=null, kill=null;
    function paint(){ try{ ctx.drawImage(v, cr.x, cr.y, cr.s, cr.s, 0, 0, side, side); }catch(e){} }
    function loopRaf(){ if(!drawing) return; paint(); raf=requestAnimationFrame(loopRaf); }
    function loopVfc(){
      if(!drawing) return;
      paint();
      try{ vfc=v.requestVideoFrameCallback(loopVfc); }catch(e){ loopRaf(); }
    }
    function stop(){
      drawing=false;
      try{ if(raf) cancelAnimationFrame(raf); }catch(e){}
      try{ if(vfc && v.cancelVideoFrameCallback) v.cancelVideoFrameCallback(vfc); }catch(e){}
      try{ v.pause(); }catch(e){}
      try{ if(rec.state==='recording') rec.stop(); }catch(e){}
    }
    function clear(){ clearTimeout(kill); clearInterval(poll); v.onended=null; v.onplay=null; }
    rec.ondataavailable=function(ev){ if(ev.data && ev.data.size) chunks.push(ev.data); };
    rec.onerror=function(){ if(settled) return; settled=true; clear(); stop(); rej(new Error('Lỗi khi ghi video')); };
    rec.onstop=function(){
      try{ stream.getTracks().forEach(function(t){ t.stop(); }); }catch(e){}
      if(settled) return;
      settled=true; clear();
      var type=(mime && mime.indexOf('mp4')>=0)?'video/mp4':'video/webm';
      res(new Blob(chunks, {type:type}));
    };
    v.onended=function(){ paint(); setTimeout(stop, 60); };
    kill=setTimeout(stop, Math.min(60000, Math.ceil(outDur*1000)+4000));
    poll=setInterval(function(){
      if(cancelled && cancelled() && !settled){ settled=true; clear(); stop(); rej(new Error('cancelled')); }
    }, 300);
    v.onplay=function(){
      drawing=true;
      try{ rec.start(250); }catch(e){ if(!settled){ settled=true; clear(); rej(new Error('Không bắt đầu ghi được')); } return; }
      if(typeof v.requestVideoFrameCallback==='function'){ try{ vfc=v.requestVideoFrameCallback(loopVfc); }catch(e){ loopRaf(); } }
      else loopRaf();
    };
    try{ v.currentTime=0; }catch(e){}
    try{ v.playbackRate=rate; }catch(e){}
    var p=v.play();
    if(p && p.catch) p.catch(function(){ if(!settled){ settled=true; clear(); rej(new Error('Không phát được video để xuất')); } });
  });
}
/* Xuất video vuông: đúng vùng crop người dùng chọn + tốc độ + vừa ngân sách byte (thử lại tối đa 2 lần). */
function bakeVideo(blob, cropJson, speed, budget, cancelled){
  return new Promise(function(resolve, reject){
    var url=URL.createObjectURL(blob);
    var v=document.createElement('video');
    v.muted=true; v.playsInline=true; v.loop=false; v.preload='auto';
    v.setAttribute('playsinline',''); v.setAttribute('webkit-playsinline','');
    var finished=false;
    function cleanup(){
      try{ v.pause(); }catch(e){}
      try{ v.removeAttribute('src'); v.load(); }catch(e){}
      try{ URL.revokeObjectURL(url); }catch(e){}
    }
    function fail(msg){ if(finished) return; finished=true; cleanup(); reject(new Error(msg)); }
    function done(r){ if(finished) return; finished=true; cleanup(); resolve(r); }
    v.onerror=function(){ fail('Không đọc được video gốc'); };
    v.onloadedmetadata=function(){
      var vw=v.videoWidth, vh=v.videoHeight, dur=v.duration;
      if(!vw || !vh || !dur || !isFinite(dur)){ fail('Không đọc được video gốc'); return; }
      var cr=cropFromJson(cropJson, vw, vh);
      if(!cr){ var s0=Math.min(vw, vh); cr={x:(vw-s0)/2, y:(vh-s0)/2, s:s0}; }
      var rate=Math.min(Math.max(Number(speed)||1, 0.0625), 16);
      var outDur=Math.max(dur/rate, 0.5);
      var fps=IS_PHONE?24:30;
      var side=evenInt(Math.min(IS_PHONE?720:1080, cr.s));
      var bps=Math.min(Math.floor(budget*8*0.8/outDur), IS_PHONE?6000000:12000000);
      while(bps/(fps*side*side)<0.06 && side>480) side=evenInt(side*0.8);
      var attempt=0;
      (function next(){
        if(cancelled && cancelled()){ fail('cancelled'); return; }
        recordOnce(v, cr, side, bps, rate, outDur, fps, cancelled).then(function(o){
          if(o.size>budget && attempt<2){
            attempt++;
            bps=Math.floor(bps*budget/o.size*0.85);
            while(bps/(fps*side*side)<0.05 && side>480) side=evenInt(side*0.8);
            next(); return;
          }
          done({blob:o, cropJson:videoCropJson(side, side, 0, 0, side), over:o.size>budget});
        }).catch(function(err){ fail((err&&err.message)||'Không xuất được video'); });
      })();
    };
    v.src=url;
  });
}
function renderVideoAtSpeed(x){
  var token=++_speedRenderToken;
  setSpeedBusy(true, 'Đang xuất ' + (Math.round(x*10)/10) + '×…');
  bakeVideo(originalVideoBlob, videoCropOriginal, x, VIDEO_BUDGET, function(){ return token!==_speedRenderToken; })
    .then(function(r){
      if(token!==_speedRenderToken) return;
      setSpeedBusy(false);
      croppedBlob=r.blob; videoCropPayload=r.cropJson;
      refreshVideoThumb();
      applyPreviewSpeed(x);
      toast(r.over ? 'Đã xuất nhưng vẫn lớn hơn giới hạn gửi' : 'Đã xuất ' + (Math.round(x*10)/10) + '× — sẵn sàng đăng');
    }).catch(function(err){
      if(token!==_speedRenderToken) return;
      setSpeedBusy(false);
      if(err && err.message==='cancelled') return;
      toast((err&&err.message)||'Không xuất được video');
    });
}
/* Video lớn hơn ngân sách (Vercel chỉ nhận ~4.5MB/request): tự cắt đúng vùng + nén lại trước khi gửi. */
function ensureVideoFit(){
  if(!isVideo || !croppedBlob || !originalVideoBlob) return Promise.resolve();
  if(croppedBlob.size<=VIDEO_BUDGET) return Promise.resolve();
  if(!CAN_RENDER_VIDEO_SPEED){ toast('Video lớn hơn giới hạn gửi và máy này không nén được'); return Promise.resolve(); }
  var token=++_speedRenderToken;
  setSpeedBusy(true, 'Đang tối ưu video cho vừa giới hạn…');
  return bakeVideo(originalVideoBlob, videoCropOriginal, videoSpeedFactor||1, VIDEO_BUDGET, function(){ return token!==_speedRenderToken; })
    .then(function(r){
      setSpeedBusy(false);
      if(token!==_speedRenderToken) return;
      croppedBlob=r.blob; videoCropPayload=r.cropJson;
      refreshVideoThumb();
      if(r.over) toast('Video vẫn lớn hơn giới hạn, có thể bị từ chối');
    }).catch(function(err){
      setSpeedBusy(false);
      toast((err&&err.message)||'Không tối ưu được video');
    });
}

/* Locket's real API expects an actual JPEG still (a "thumb" field) alongside the
   video, matching binhake's own web client. Without a real image here the field
   silently fell back to the raw video bytes, which the real API rejects — this
   is what was actually breaking video posts, not a network issue. Captures a
   center-square frame from whichever <video> currently holds the footage. */
function captureVideoThumb(videoEl, cropJson){
  return new Promise(function(resolve){
    try{
      if(!videoEl){ resolve(null); return; }

      function paint(){
        try{
          var vw=videoEl.videoWidth, vh=videoEl.videoHeight;
          if(!vw || !vh){ resolve(null); return; }
          var side=Math.min(vw,vh), sx=(vw-side)/2, sy=(vh-side)/2;
          var cr=cropFromJson(cropJson, vw, vh);
          if(cr){ side=cr.s; sx=cr.x; sy=cr.y; }
          var c=document.createElement('canvas');
          c.width=CAPTURE_SIZE; c.height=CAPTURE_SIZE;
          var ctx=c.getContext('2d');
          ctx.imageSmoothingEnabled=true;
          try{ ctx.imageSmoothingQuality='high'; }catch(e){}
          ctx.drawImage(videoEl, sx, sy, side, side, 0, 0, CAPTURE_SIZE, CAPTURE_SIZE);
          c.toBlob(function(b){ resolve(b||null); }, 'image/jpeg', CAPTURE_JPEG_Q);
        }catch(e){ resolve(null); }
      }

      var ready = videoEl.readyState >= 2 && videoEl.videoWidth > 0;
      if(ready){
        var dur = videoEl.duration;
        // Avoid pure-black first frame: nudge slightly into the clip when at 0
        if((!videoEl.currentTime || videoEl.currentTime < 0.05) && isFinite(dur) && dur > 0.2){
          var target = Math.min(0.15, dur * 0.05);
          var done = false;
          var finish = function(){
            if(done) return;
            done = true;
            try{ videoEl.removeEventListener('seeked', onSeek); }catch(e){}
            paint();
          };
          var onSeek = function(){ finish(); };
          videoEl.addEventListener('seeked', onSeek);
          try{ videoEl.currentTime = target; }catch(e){ finish(); }
          setTimeout(finish, 450);
        } else {
          paint();
        }
        return;
      }

      var settled = false;
      var onReady = function(){
        if(settled) return;
        settled = true;
        try{ videoEl.removeEventListener('loadeddata', onReady); }catch(e){}
        try{ videoEl.removeEventListener('loadedmetadata', onReady); }catch(e){}
        paint();
      };
      videoEl.addEventListener('loadeddata', onReady);
      videoEl.addEventListener('loadedmetadata', onReady);
      setTimeout(onReady, 900);
    }catch(e){ resolve(null); }
  });
}

/** Re-grab JPEG still from the preview <video> after crop / speed bake / restore 1x. */
function refreshVideoThumb(){
  var pv = $('previewVid');
  if(!pv || !isVideo) return Promise.resolve(null);
  return captureVideoThumb(pv, videoCropOriginal).then(function(b){
    if(b) videoThumbBlob = b;
    return b;
  });
}

/** Ensure we have a real still before queue/upload (never ship null → 16x16 black server fallback). */
function ensureVideoThumb(){
  if(!isVideo) return Promise.resolve(null);
  if(videoThumbBlob) return Promise.resolve(videoThumbBlob);
  return refreshVideoThumb();
}
let friendsCache=null, momentsLoaded=false, meLoaded=false;
let momentsCache=[], momentsUpdatedAt=0, momentsPollTimer=null;

/* ---------- iPhone 6 friendly limits ---------- */
var IS_PHONE = (typeof window !== 'undefined' && window.innerWidth <= 520);
var IMG_CONCURRENCY = IS_PHONE ? 1 : 4;   // iPhone 6: 1 concurrent decode — avoid Safari kill
var MOMENT_BATCH = IS_PHONE ? 2 : 8;      // DOM nodes per paint wave
var PREFETCH_COUNT = IS_PHONE ? 2 : 10;   // eager Image() ahead of scroll
var THUMB_W = IS_PHONE ? 320 : 480;       // feed thumb max side (grid)
var FEED_THUMB_W = IS_PHONE ? 540 : 720;  // vertical feed card
var AVATAR_W = 96;
var LOCAL_MOMENTS_CAP = IS_PHONE ? 30 : 80;
var LOCAL_FRIENDS_CAP = 200;

/* ---------- Low-power mode: manual toggle + auto-detect old iOS ---------- */
var OLD_IOS = (function(){
  var m = navigator.userAgent.match(/OS (\\d+)_/);
  return !!(m && parseInt(m[1],10) <= 12 && /iPhone|iPad|iPod/.test(navigator.userAgent));
})();
function isLowPower(){
  try{
    var v = localStorage.getItem('locket_low_power');
    if(v==='1') return true;
    if(v==='0') return false;
  }catch(e){}
  return OLD_IOS; // unset = auto, based on device
}
function setLowPower(on){
  try{ localStorage.setItem('locket_low_power', on?'1':'0'); }catch(e){}
  LOW_POWER = on;
}
var LOW_POWER = isLowPower();
var LC_WIDTH_IDEAL = LOW_POWER ? 960 : 1920;
var LC_HEIGHT_IDEAL = LOW_POWER ? 960 : 1080;
var LC_FRAMERATE_IDEAL = LOW_POWER ? 15 : 30;
var CAPTURE_SIZE = LOW_POWER ? 720 : 1080;
var CAPTURE_JPEG_Q = LOW_POWER ? 0.82 : 0.93;
/* WebP support detection — chỉ convert sang JPEG khi thực sự cần */
var WEBP_SUPPORTED = false;
(function(){
  var img = new Image();
  img.onload = function(){ WEBP_SUPPORTED = true; };
  img.onerror = function(){ WEBP_SUPPORTED = false; };
  img.src = 'data:image/webp;base64,UklGRi4AAABXRUJQVlA4TCEAAAAvAUAAEB8wAiMwAgSSNtse/cXjxyCCmrYNrpwmfXgJzU3f';
})();

/* Khi xây URL ảnh: nếu browser hỗ trợ WebP và ảnh gốc là WebP, 
   bỏ qua proxy JPEG để giữ chất lượng gốc + tiết kiệm CPU */
function mediaSrc(u, w){
  u=normalizeMediaUrl(u);
  if(!u)return'';
  if(u.indexOf('/api/img')===0||u.indexOf('blob:')===0||u.indexOf('data:')===0)return u;
  var isWebp = /\\.webp$/i.test(u);
  // Nếu browser support WebP và ảnh là WebP → serve trực tiếp, không qua proxy JPEG
  if(WEBP_SUPPORTED && isWebp && (!w || w <= 1080)){
    return u;
  }
  var q='/api/img?u='+encodeURIComponent(u);
  if(w) q+='&w='+w;
  // Gửi flag cho server biết browser có hỗ trợ WebP không
  if(WEBP_SUPPORTED) q += '&webp=1';
  return q;
}
var TINY_PIXEL = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

/* Image load queue — prevents Safari memory death on iPhone 6 */
var _imgQ = [];
var _imgActive = 0;
var _imgDone = 0;
var _imgTotal = 0;
var _imgProgTimer = null;

function toast(msg){const t=$('toast');t.textContent=msg;t.classList.add('show');clearTimeout(t._h);t._h=setTimeout(()=>t.classList.remove('show'),2600)}
function esc(s){const d=document.createElement('div');d.textContent=s==null?'':String(s);return d.innerHTML}

function showProgress(text, done, total){
  var pill = $('progressPill');
  if(!pill) return;
  var txt = $('progressPillText');
  var bar = $('progressPillBar');
  if(txt) txt.textContent = text || 'Đang tải…';
  if(bar && total > 0){
    var pct = Math.min(100, Math.round((done / total) * 100));
    bar.style.width = pct + '%';
  } else if(bar){
    bar.style.width = '0%';
  }
  pill.classList.add('show');
}
function hideProgress(delay){
  var pill = $('progressPill');
  if(!pill) return;
  clearTimeout(pill._hide);
  pill._hide = setTimeout(function(){ pill.classList.remove('show'); }, delay == null ? 400 : delay);
}
function bumpImgProgress(){
  _imgDone++;
  if(_imgTotal <= 0) return;
  showProgress('Đang tải ảnh ' + Math.min(_imgDone, _imgTotal) + '/' + _imgTotal, _imgDone, _imgTotal);
  if(_imgDone >= _imgTotal && _imgActive === 0 && _imgQ.length === 0){
    showProgress('Xong ' + _imgTotal + ' ảnh', _imgTotal, _imgTotal);
    hideProgress(900);
  }
}
function enqueueImg(imgEl, src){
  if(!imgEl || !src) return;
  // already has real src
  if(imgEl.getAttribute('data-src-loaded') === src) return;
  imgEl.setAttribute('data-src-pending', src);
  _imgQ.push({el: imgEl, src: src});
  _imgTotal++;
  pumpImgQueue();
}
function pumpImgQueue(){
  while(_imgActive < IMG_CONCURRENCY && _imgQ.length){
    var job = _imgQ.shift();
    if(!job || !job.el) continue;
    // element may have been removed / recycled
    if(!job.el.parentNode){ bumpImgProgress(); continue; }
    _imgActive++;
    (function(el, src){
      var done = function(){
        _imgActive = Math.max(0, _imgActive - 1);
        bumpImgProgress();
        pumpImgQueue();
      };
      var tmp = new Image();
      tmp.onload = function(){
        try{
          el.src = src;
          el.setAttribute('data-src-loaded', src);
          el.removeAttribute('data-src-pending');
        }catch(e){}
        done();
      };
      tmp.onerror = function(){
        try{ el.style.opacity = '0.3'; }catch(e){}
        done();
      };
      tmp.src = src;
    })(job.el, job.src);
  }
  if(_imgTotal > 0 && (_imgActive > 0 || _imgQ.length > 0)){
    showProgress('Đang tải ảnh ' + Math.min(_imgDone, _imgTotal) + '/' + _imgTotal, _imgDone, _imgTotal);
  }
}
function resetImgProgress(){
  _imgQ = [];
  _imgActive = 0;
  _imgDone = 0;
  _imgTotal = 0;
}

async function api(url,opts){
  try{
    const r=await fetch(url,opts);
    return await r.json();
  }catch(err){
    var name = err && err.name;
    var msg = (err && err.message) || '';
    if(name === 'AbortError' || /aborted|network changed|ERR_NETWORK/i.test(msg)){
      // Expected: timeout, navigation, or wifi hop — keep quiet
      throw err;
    }
    console.error('api',url,err);
    throw err;
  }
}
// Same as api(), but aborts after `ms` so a hung request can never permanently wedge a
// single-flight flag (was letting one dead request block every future reload silently).
function apiTimeout(url, opts, ms){
  var ctrl = (typeof AbortController!=='undefined') ? new AbortController() : null;
  var o = Object.assign({}, opts||{});
  if(ctrl) o.signal = ctrl.signal;
  var timer = ctrl ? setTimeout(function(){ try{ ctrl.abort(); }catch(e){} }, ms) : null;
  var p = api(url, o).catch(function(err){
    // AbortError from timeout / navigation / network switch is expected — don't spam console as a real failure.
    var name = err && err.name;
    var msg = (err && err.message) || '';
    if(name === 'AbortError' || /aborted|AbortError|network changed|ERR_NETWORK/i.test(msg)){
      return Promise.reject({ aborted: true, name: 'AbortError', message: msg || 'aborted' });
    }
    return Promise.reject(err);
  });
  if(timer && p.finally) p = p.finally(function(){ clearTimeout(timer); });
  return p;
}

{% if not logged_in %}
/* ===================== Login ===================== */
function doLogin(){
  const e=$('loginEmail').value.trim(),p=$('loginPass').value,btn=$('loginBtn');
  const remember=!!$('loginRemember')&&$('loginRemember').checked;
  if(!e||!p){toast('Nhập email và mật khẩu');return}
  btn.innerHTML='<span class="spinner"></span>';btn.disabled=true;
  apiTimeout('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:e,password:p,remember:remember})}, 20000)
  .then(d=>{
    btn.innerHTML='Log In';btn.disabled=false;
    if(d.ok){location.reload()}else{toast(d.error||'Đăng nhập thất bại')}
  }).catch(()=>{btn.innerHTML='Log In';btn.disabled=false;toast('Lỗi mạng')})
}
{% else %}
/* ===================== Nav / paging ===================== */
var _curPage='dash';
function _showPage(name){
  _curPage=name;
  ['dash','upload','moments','friends'].forEach(p=>{
    $(`page-${p}`).classList.remove('active');
    const nb=$(`nav-${p}`); if(nb)nb.classList.remove('active');
  });
  $(`page-${name}`).classList.add('active');
  const nb=$(`nav-${name}`); if(nb)nb.classList.add('active');
  const topBtn=$('momentsTopBtn');
  if(topBtn) topBtn.classList.toggle('show', name==='moments');
  if(name==='dash'&&!meLoaded) loadMe();
  if(name==='moments'){
    _momentsBootLock = true;
    scrollMomentsTop();
    ensureMomentsFromCache();
    var age = (Date.now()/1000) - (momentsUpdatedAt || 0);
    // Online + cache older than 25s (or never refreshed after offline) → force network
    if(navigator.onLine && (age > 25 || !momentsUpdatedAt)){
      loadMoments(true);
    } else {
      loadMoments(false);
    }
    startMomentsPoll();
    setTimeout(function(){ _momentsBootLock = false; scrollMomentsTop(); }, 280);
  } else {
    stopMomentsPoll();
    if(name !== 'friends') hideProgress(0);
  }
  if(name==='friends'){ loadFriends(Date.now()-(friendsFetchedAt||0)>300000); loadPeopleRequests(); }
  if(name==='upload'){
    if(isLiveCamera() && !croppedBlob){
      $('previewBox').classList.add('hidden');
      $('uploadPlaceholder').classList.add('hidden');
      $('liveCam').classList.remove('hidden');
      startLiveCamera();
      // Scroll camera into comfortable view (switches sit above; user should not have to drag)
      setTimeout(function(){ scrollUploadCameraIntoView(); }, 60);
      setTimeout(function(){ scrollUploadCameraIntoView(); }, 280);
    } else {
      // still jump near top of upload form so preview is usable
      setTimeout(function(){
        try{
          var page=$('page-upload');
          if(page) page.scrollIntoView({block:'start'});
          window.scrollTo(0, Math.max(0, (page?page.offsetTop:0) - 8));
        }catch(e){}
      }, 40);
    }
  } else {
    stopLiveCamera();
  }
}

/* ===================== Avatar (ring + notch badge) ===================== */
function getInitials(name){
  if(!name)return'?';
  return name.trim().split(/\\s+/).map(s=>s[0]).join('').toUpperCase().slice(0,2);
}
function stringToHsl(str,s,l){
  let hash=0;
  for(let i=0;i<str.length;i++)hash=str.charCodeAt(i)+((hash<<5)-hash);
  return `hsl(${Math.abs(hash%360)},${s}%,${l}%)`;
}
function renderAvatar(u,size=52){
  u=u||{};
  const uid=(u.uid||u.username||Math.random().toString(36).slice(2)).toString().replace(/[^a-zA-Z0-9]/g,'');
  const hasGold=(u.badge==='locket_gold'||u.badge==='Locket Gold');
  const isCeleb=!!u.celebrity;
  const showBadge=hasGold||isCeleb;
  const ring=showBadge?'#FFB800':'var(--ring)';
  const badgeSrc=hasGold?GOLD_BADGE:CELEB_BADGE;
  const inset=Math.max(2,Math.round(size*0.06));
  const r=size/2;
  const initials=getInitials(u.first_name||u.username||'?');
  const bg=stringToHsl(uid||initials,60,42);
  const pic=mediaSrc(u.profile_picture_url||'', 128);

  let inner;
  if(pic){
    inner=`<img src="${esc(pic)}" alt="" onerror="this.style.display='none';this.nextElementSibling.style.display='block'">
      <div style="display:none;width:100%;height:100%;line-height:${size-inset*2}px;text-align:center;background:${bg};color:#fff;font-weight:800;font-size:${Math.round(size*0.38)}px">${esc(initials)}</div>`;
  }else{
    inner=`<div style="width:100%;height:100%;line-height:${size-inset*2}px;text-align:center;background:${bg};color:#fff;font-weight:800;font-size:${Math.round(size*0.38)}px">${esc(initials)}</div>`;
  }

  return `<div class="avatar-wrap" style="width:${size}px;height:${size}px">
    <svg class="avatar-ring" viewBox="0 0 ${size} ${size}">
      <defs><mask id="m-${uid}-${size}">
        <rect width="${size}" height="${size}" fill="white"/>
        <circle cx="${size/2}" cy="${size/2}" r="${r-inset-2}" fill="black"/>
        ${showBadge?`<circle cx="${size*0.82}" cy="${size*0.82}" r="${size*0.16}" fill="black"/>`:''}
      </mask></defs>
      <circle cx="${size/2}" cy="${size/2}" r="${r-inset}" style="fill:${ring}" mask="url(#m-${uid}-${size})"/>
    </svg>
    <div class="avatar-img ${showBadge?'fm-contentPunch':''}" style="top:${inset}px;left:${inset}px;right:${inset}px;bottom:${inset}px">${inner}</div>
    ${showBadge?`<img class="avatar-badge" src="${badgeSrc}" style="width:${size*0.34}px;height:${size*0.34}px" alt="">`:''}
  </div>`;
}

/* ===================== Dash ===================== */
function greetingLine(){
  const h=new Date().getHours();
  if(h<11)return'Chào buổi sáng';
  if(h<14)return'Chào buổi trưa';
  if(h<18)return'Chào buổi chiều';
  return'Chào buổi tối';
}
function loadMe(){
  $('greetLine').textContent=greetingLine();
  // Paint immediately from session bootstrap (login already returned displayName + photo)
  if(BOOT_NAME) $('dashName').textContent=BOOT_NAME;
  if(BOOT_PHOTO||BOOT_NAME){
    $('dashAvatar').innerHTML=renderAvatar({first_name:BOOT_NAME,profile_picture_url:mediaSrc(BOOT_PHOTO,128)},56);
  }else{
    $('dashAvatar').innerHTML=renderAvatar({},56);
  }
  apiTimeout('/api/me', null, 15000).then(d=>{
    if(!d.ok){toast(d.error||'Không tải được thông tin');return}
    meLoaded=true;
    const me=d.me||{};
    const fullName=((me.first_name||'')+' '+(me.last_name||'')).trim()||me.username||BOOT_NAME||'Bạn';
    $('dashName').textContent=fullName;
    $('dashAvatar').innerHTML=renderAvatar(me,56);
    $('streakNum').textContent=(me.streak!=null&&me.streak!=='')?me.streak:'–';
  }).catch(()=>toast('Lỗi mạng khi tải trang chủ'));
}

/* ===================== Friends (progressive render) ===================== */
const FRIENDS_CACHE_KEY='locket_friends_cache_v2';
const FRIENDS_TTL_MS=6*60*60*1000; // 6 hours — avoid re-fetch every visit
const MOMENTS_LS_KEY='locket_moments_cache_v3';try{localStorage.removeItem('locket_moments_cache_v2')}catch(e){}
const MOMENTS_TTL_MS=45*1000; // 45s soft TTL — after offline, online handler zeros ts anyway
let friendsFetchedAt=0;
/* friends progressive render — smaller batches on weak devices */
const FRIENDS_BATCH = IS_PHONE ? 8 : 16;

function readFriendsLocal(){
  try{
    const raw=localStorage.getItem(FRIENDS_CACHE_KEY);
    if(!raw) return null;
    const obj=JSON.parse(raw);
    if(!obj||!Array.isArray(obj.friends)) return null;
    return obj;
  }catch(e){return null}
}
function writeFriendsLocal(friends,count){
  try{
    localStorage.setItem(FRIENDS_CACHE_KEY, JSON.stringify({friends,count,ts:Date.now()}));
  }catch(e){}
}
function paintFriends(friends,count){
  friendsCache=friends||[];
  try{ renderFriendActivity(); }catch(e){}
  const list=$('friendsList');
  if($('friendCountPill')) $('friendCountPill').textContent=count!=null?count:friendsCache.length;
  if($('friendsCountLine')) $('friendsCountLine').innerHTML=`Tổng cộng <b>${count!=null?count:friendsCache.length}</b> bạn bè`;
  if(!list) return;
  list.innerHTML='';
  if(!friendsCache.length){$('friendsEmpty').classList.remove('hidden');return}
  $('friendsEmpty').classList.add('hidden');
  if(friendsCache.length > 30){
    showProgress('Đang hiện bạn bè 0/' + friendsCache.length, 0, friendsCache.length);
  }
  renderFriendBatch(list,friendsCache,0,FRIENDS_BATCH);
}
function loadFriends(force){
  const list=$('friendsList');
  const local=readFriendsLocal();
  const fresh=local && (Date.now()-(local.ts||0)<FRIENDS_TTL_MS);
  // Instant paint from local cache
  if(local && local.friends && local.friends.length){
    paintFriends(local.friends, local.count);
    friendsFetchedAt=local.ts||0;
    if(!force && fresh) return; // still warm — skip network
  }else if(list){
    $('friendsCountLine').textContent='Đang tải...';
    $('friendsEmpty').classList.add('hidden');
    list.innerHTML=Array(5).fill('<div class="skeleton" style="height:68px;margin-bottom:10px"></div>').join('');
  }
  apiTimeout('/api/friends?check=1', null, 20000).then(d=>{
    if(!d.ok){
      if(!friendsCache||!friendsCache.length){
        toast(d.error||'Không tải được bạn bè');
        if(list) list.innerHTML='';
        $('friendsEmpty').classList.remove('hidden');
        $('friendsCountLine').textContent='';
      }
      return;
    }
    const friends=d.friends||[];
    const count=d.count!=null?d.count:friends.length;
    try{ trackFriends(friends, d.complete!==false); }catch(e){ console.warn(e); }
    // Smart skip re-paint if same size and not forced
    const same=friendsCache && friendsCache.length===friends.length && !force;
    writeFriendsLocal(friends, count);
    friendsFetchedAt=Date.now();
    if(!same) paintFriends(friends, count);
    else {
      friendsCache=friends;
      if($('friendCountPill')) $('friendCountPill').textContent=count;
    }
  }).catch(()=>{
    if(!friendsCache||!friendsCache.length){
      toast('Lỗi mạng khi tải bạn bè');
      if(list) list.innerHTML='';
      $('friendsCountLine').textContent='';
    }
  });
}
function preloadFriends(){
  // Background warm on app open — never blocks UI
  const local=readFriendsLocal();
  if(local && local.friends){
    friendsCache=local.friends;
    if($('friendCountPill')) $('friendCountPill').textContent=local.count!=null?local.count:local.friends.length;
  }
  const need=!(local && (Date.now()-(local.ts||0)<FRIENDS_TTL_MS));
  if(need) loadFriends(false);
}
/* ===== Hoạt động bạn bè: bạn mới + ai đã xoá kết bạn =====
   - "Bạn mới": lấy từ thời điểm tạo tài liệu bạn bè trên Firestore (added_at), có dự phòng theo lần phát hiện.
   - "Đã xoá": so sánh danh sách bạn bè với lần trước (lưu trong trình duyệt, theo từng tài khoản).
     Chỉ coi là xoá sau 2 lần kiểm tra cách nhau >= 5 phút để tránh báo nhầm khi API lỗi tạm thời. */
var FSNAP_KEY='locket_friends_snap_v1_'+BOOT_UID, FLOG_KEY='locket_unfriend_log_v1_'+BOOT_UID;
var FA_CONFIRM_GAP=300, FA_MAX_LOG=100;
var _faTab='new', _faFriends=null;
function jread(k, d){ try{ var r=localStorage.getItem(k); return r?JSON.parse(r):d; }catch(e){ return d; } }
function jwrite(k, v){ try{ localStorage.setItem(k, JSON.stringify(v)); }catch(e){} }
function slimUser(u){
  return {uid:u.uid, first_name:u.first_name||'', last_name:u.last_name||'', username:u.username||'',
          profile_picture_url:u.profile_picture_url||'', celebrity:!!u.celebrity, badge:u.badge||''};
}
function hasIdentity(u){ return !!(u && (u.first_name||u.last_name||u.username||u.profile_picture_url)); }
function trackFriends(friends, complete){
  _faFriends=friends||null;
  if(!friends || !friends.length || complete===false){ renderFriendActivity(); return; }
  var now=Math.floor(Date.now()/1000);
  var snap=jread(FSNAP_KEY, null), log=jread(FLOG_KEY, []);
  var cur={};
  friends.forEach(function(f){ if(f && f.uid) cur[f.uid]=slimUser(f); });
  if(!snap || !snap.profiles){
    snap={since:now, ts:now, profiles:cur, pend:{}, fresh:{}};
  } else {
    var prev=snap.profiles, pend=snap.pend||{}, fresh=snap.fresh||{};
    var prevCount=Object.keys(prev).length, curCount=Object.keys(cur).length;
    // Danh sách tụt bất thường (>40%) gần như chắc chắn là lỗi tải, không phải xoá thật
    if(prevCount>=10 && curCount<prevCount*0.6){ renderFriendActivity(); return; }
    var keep={};
    Object.keys(cur).forEach(function(uid){
      var p=cur[uid];
      if(!hasIdentity(p) && prev[uid] && hasIdentity(prev[uid])) p=prev[uid];
      keep[uid]=p;
      if(!prev[uid]) fresh[uid]=fresh[uid]||now;
      delete pend[uid];
    });
    Object.keys(prev).forEach(function(uid){
      if(cur[uid]) return;
      var pe=pend[uid]||{n:0, t:0};
      if(now-pe.t>=FA_CONFIRM_GAP) pe={n:pe.n+1, t:now};
      pend[uid]=pe;
      if(pe.n>=2){
        log.unshift({uid:uid, profile:prev[uid], at:now});
        delete pend[uid];
      } else keep[uid]=prev[uid];
    });
    Object.keys(fresh).forEach(function(uid){ if(!keep[uid] || now-fresh[uid]>60*86400) delete fresh[uid]; });
    snap={since:snap.since||now, ts:now, profiles:keep, pend:pend, fresh:fresh};
    log=log.slice(0, FA_MAX_LOG);
  }
  jwrite(FSNAP_KEY, snap);
  jwrite(FLOG_KEY, log);
  renderFriendActivity();
}
function faTimeLabel(sec){
  if(!sec) return '';
  var t=timeAgo(sec);
  return t==='vừa xong' ? t : (t+' trước');
}
function faDate(sec){
  try{ return new Date(sec*1000).toLocaleDateString('vi-VN'); }catch(e){ return ''; }
}
function faRow(u, right, tag, sub){
  var name=((u.first_name||'')+' '+(u.last_name||'')).trim()||u.username||u.uid||'';
  var line=(u.username?'@'+esc(u.username):'')+(sub?(u.username?' · ':'')+esc(sub):'');
  return '<div class="friend-row">'+renderAvatar(u, 48)+
    '<div class="friend-info"><div class="friend-name">'+esc(name)+(tag?'<span class="fa-tag">'+esc(tag)+'</span>':'')+'</div>'+
    '<div class="friend-user">'+line+'</div></div>'+
    (right?'<div class="fa-time">'+esc(right)+'</div>':'')+'</div>';
}
function renderFriendActivity(){
  var body=$('faBody'); if(!body) return;
  var friends=_faFriends||friendsCache||[];
  var snap=jread(FSNAP_KEY, null)||{}, log=jread(FLOG_KEY, []);
  var now=Math.floor(Date.now()/1000), cut30=now-30*86400, cut7=now-7*86400;
  var fresh=snap.fresh||{}, pend=snap.pend||{}, profiles=snap.profiles||{};
  var curSet={};
  friends.forEach(function(f){ if(f&&f.uid) curSet[f.uid]=1; });

  var recent=friends.map(function(f){ return {u:f, t:f.added_at||fresh[f.uid]||0}; })
    .filter(function(x){ return x.t && x.t>=cut30; })
    .sort(function(a,b){ return b.t-a.t; });
  var n7=recent.filter(function(x){ return x.t>=cut7; }).length;
  var pendList=Object.keys(pend).filter(function(uid){ return !curSet[uid] && profiles[uid]; });

  var cn=$('faNewCount'), cg=$('faGoneCount');
  if(cn) cn.textContent=recent.length;
  if(cg) cg.textContent=log.length;
  var tabs=document.querySelectorAll('#friendActivity [data-fa]');
  for(var i=0;i<tabs.length;i++) tabs[i].classList.toggle('active', tabs[i].getAttribute('data-fa')===_faTab);

  var html='';
  if(_faTab==='new'){
    if(!friends.length){ html='<div class="fa-empty">Đang tải…</div>'; }
    else if(!recent.length){ html='<div class="fa-empty">Chưa có bạn mới trong 30 ngày qua</div>'; }
    else {
      html='<div class="fa-sum">7 ngày qua: <b>'+n7+'</b> người · 30 ngày qua: <b>'+recent.length+'</b> người</div><div class="fa-list">'+
        recent.slice(0, 40).map(function(x){
          return faRow(x.u, faTimeLabel(x.t), x.t>=cut7?'Mới':'', faDate(x.t));
        }).join('')+'</div>';
    }
  } else {
    if(!log.length && !pendList.length){
      html='<div class="fa-empty">Chưa phát hiện ai xoá kết bạn</div>';
    } else {
      html='<div class="fa-sum">Đã xác nhận: <b>'+log.length+'</b> người'+(pendList.length?' · đang xác nhận: '+pendList.length:'')+'</div><div class="fa-list">'+
        log.slice(0, 50).map(function(g){
          var back=!!curSet[g.uid];
          return faRow(g.profile||{uid:g.uid}, faTimeLabel(g.at), back?'Đã kết bạn lại':'', 'phát hiện '+faDate(g.at));
        }).join('')+
        pendList.map(function(uid){
          return faRow(profiles[uid], '', 'Đang xác nhận…', '');
        }).join('')+'</div>';
    }
  }
  body.innerHTML=html;
  var foot=$('faFoot');
  if(foot){
    foot.innerHTML=(snap.since?'Theo dõi từ '+esc(faDate(snap.since))+'. ':'')+
      'So sánh danh sách bạn bè với lần mở trước; không phân biệt được ai xoá ai, chỉ biết người đó không còn trong danh sách. '+
      (log.length?'<a href="#" onclick="clearFriendLog();return false;">Xoá lịch sử</a>':'');
  }
}
function clearFriendLog(){
  jwrite(FLOG_KEY, []);
  var s=jread(FSNAP_KEY, null);
  if(s){ s.pend={}; jwrite(FSNAP_KEY, s); }
  renderFriendActivity();
}
document.addEventListener('click', function(ev){
  var b=ev.target&&ev.target.closest?ev.target.closest('#friendActivity [data-fa]'):null;
  if(!b) return;
  _faTab=b.getAttribute('data-fa')==='gone'?'gone':'new';
  renderFriendActivity();
});

function peoplePost(path, body){
  return apiTimeout(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)},25000);
}
function personButtons(u,rel){
  var uid=esc(u.uid||'');
  if(rel==='self') return '<span class="p-tag">You</span>';
  if(rel==='friend') return '<span class="p-tag">'+(u.celebrity?'Following':'Friends')+'</span>';
  if(rel==='outgoing') return '<button type="button" class="p-btn ghost p-act" data-act="decline" data-dir="outgoing" data-uid="'+uid+'">Requested · Cancel</button>';
  if(rel==='incoming') return '<label class="p-share"><input type="checkbox" class="p-share-cb"> Share my history</label><div class="p-row"><button type="button" class="p-btn p-act" data-act="accept" data-uid="'+uid+'">Accept</button><button type="button" class="p-btn ghost p-act" data-act="decline" data-dir="incoming" data-uid="'+uid+'">Decline</button></div>';
  if(u.celebrity && celebFull(u)) return '<button type="button" class="p-btn ghost" disabled>Full</button>';
  return '<button type="button" class="p-btn p-act" data-act="add" data-uid="'+uid+'">'+(u.celebrity?'Follow':'Add friend')+'</button>';
}
function celebFull(u){
  var c=u&&u.celebrity_data;
  return !!(c&&c.max_friends&&(c.friend_count||0)>=c.max_friends);
}
function celebSlots(u){
  var c=u&&u.celebrity_data;
  if(!u||!u.celebrity) return '';
  if(!c||!c.max_friends) return '<div class="person-slots">Slots unknown</div>';
  var used=c.friend_count||0, left=c.max_friends-used;
  if(left<=0) return '<div class="person-slots full">Full · '+used+' / '+c.max_friends+'</div>';
  return '<div class="person-slots">'+used+' / '+c.max_friends+' · '+left+' slots left</div>';
}
function personCard(u,rel){
  var name=((u.first_name||'')+' '+(u.last_name||'')).trim()||u.username||u.uid||'';
  return '<div class="person-card" data-uid="'+esc(u.uid||'')+'">'+renderAvatar(u,48)+'<div class="person-info"><div class="person-name">'+esc(name)+'</div><div class="person-user">@'+esc(u.username||u.uid||'')+(u.celebrity?' · Celebrity':'')+'</div>'+celebSlots(u)+'</div><div class="person-actions">'+personButtons(u,rel)+'</div></div>';
}
function searchPeople(){
  var q=(($('peopleQuery')||{}).value||'').trim();
  if(!q) return;
  try{ if(_curPage==='friends') history.replaceState(history.state,'','/friends?q='+encodeURIComponent(q)); }catch(e){}
  var box=$('peopleResult');
  box.innerHTML='<div class="skeleton" style="height:68px"></div>';
  apiTimeout('/api/people/search?q='+encodeURIComponent(q),null,25000).then(function(d){
    if(!d.ok){ box.innerHTML='<div class="people-msg">'+esc(d.error||'Not found')+'</div>'; return; }
    box.innerHTML=personCard(d.user,d.relation);
  }).catch(function(){ box.innerHTML='<div class="people-msg">Network error</div>'; });
}
function loadPeopleRequests(){
  var box=$('peopleRequests');
  if(!box) return;
  loadAppCheckState();
  loadMyInvite();
  apiTimeout('/api/people/requests',null,25000).then(function(d){
    if(!d.ok){ box.innerHTML=''; return; }
    var inc=d.incoming||[], out=d.outgoing||[], h='';
    if(inc.length) h+='<div class="req-head">Friend requests <b>'+inc.length+'</b></div>'+inc.map(function(u){return personCard(u,'incoming')}).join('');
    if(out.length) h+='<div class="req-head">Sent <b>'+out.length+'</b></div>'+out.map(function(u){return personCard(u,'outgoing')}).join('');
    box.innerHTML=h;
  }).catch(function(){});
}
function personAfter(refreshFriends){
  loadPeopleRequests();
  if(refreshFriends) loadFriends(true);
  var q=(($('peopleQuery')||{}).value||'').trim();
  if(q && $('peopleResult') && $('peopleResult').innerHTML) searchPeople();
}
document.addEventListener('click',function(ev){
  var b=ev.target&&ev.target.closest?ev.target.closest('.p-act'):null;
  if(!b||b.disabled) return;
  var uid=b.getAttribute('data-uid'), act=b.getAttribute('data-act');
  b.disabled=true;
  var done=function(d,okMsg,refresh){
    b.disabled=false;
    var msg=$('peopleMsg');
    if(!d||!d.ok){ var em=(d&&d.error)||'Request failed'; toast(em.length>80?'Request rejected, see details below':em); if(msg) msg.textContent=em; return; }
    if(msg) msg.textContent='';
    toast(okMsg); personAfter(refresh);
  };
  var fail=function(){ b.disabled=false; toast('Network error'); };
  if(act==='add'){
    peoplePost('/api/people/add',{uid:uid}).then(function(d){
      done(d,(d&&d.kind==='follow')?'Follow request sent':'Friend request sent',false);
    }).catch(fail);
  }else if(act==='accept'){
    var card=b.closest('.person-card'), cb=card?card.querySelector('.p-share-cb'):null;
    peoplePost('/api/people/accept',{uid:uid,share_history:!!(cb&&cb.checked)}).then(function(d){
      done(d,(d&&d.share_history)?'Accepted, history shared':'Accepted',true);
    }).catch(fail);
  }else if(act==='decline'){
    peoplePost('/api/people/decline',{uid:uid,direction:b.getAttribute('data-dir')||'incoming'}).then(function(d){
      done(d,'Request removed',false);
    }).catch(fail);
  }
});
function loadAppCheckState(){
  var el=$('appcheckState');
  if(!el) return;
  apiTimeout('/api/people/appcheck',null,15000).then(function(d){
    if(!d||!d.ok){ el.textContent=''; return; }
    if(!d.has){ el.textContent=d.provider?'Auto (fetched on demand)':'Not set'; return; }
    if(d.expired){ el.textContent=d.provider?'Auto (refreshing)':'Expired'; return; }
    el.textContent=d.expires_in==null?'Set':('Valid for '+Math.max(1,Math.round(d.expires_in/60))+' min');
  }).catch(function(){});
}
function saveAppCheck(){
  var input=$('appcheckToken');
  if(!input) return;
  peoplePost('/api/people/appcheck',{token:(input.value||'').trim()}).then(function(d){
    if(!d||!d.ok){ toast((d&&d.error)||'Could not save'); return; }
    input.value='';
    toast('App Check token saved');
    loadAppCheckState();
  }).catch(function(){ toast('Network error'); });
}
function loadMyInvite(){
  var el=$('myInvite');
  if(!el) return;
  apiTimeout('/api/people/invite',null,20000).then(function(d){
    if(!d||!d.ok||!d.link){ el.innerHTML=''; return; }
    el.innerHTML='Your link: <a href="#" id="myInviteCopy">'+esc(d.link)+'</a>';
    var a=$('myInviteCopy');
    if(a) a.onclick=function(ev){
      ev.preventDefault();
      try{ navigator.clipboard.writeText(d.link); toast('Link copied'); }catch(e){ toast(d.link); }
    };
  }).catch(function(){});
}
function renderFriendBatch(container,friends,start,batch){
  const end=Math.min(start+batch,friends.length);
  var frag=document.createDocumentFragment();
  for(let i=start;i<end;i++){
    const u=friends[i];
    const row=document.createElement('div');
    row.className='friend-row';
    const streak=(u.streak&&u.streak>0)?`<div class="friend-streak">${u.streak}<i class="bi bi-fire" style="margin-left:3px;color:#FFB800;font-size:14px"></i></div>`:'';
    row.innerHTML=`${renderAvatar(u,60)}<div class="friend-info"><div class="friend-name">${esc(u.first_name||'')} ${esc(u.last_name||'')}</div><div class="friend-user">@${esc(u.username||u.uid||'')}</div></div>${streak}`;
    frag.appendChild(row);
  }
  container.appendChild(frag);
  if(friends.length > 30){
    showProgress('Đang hiện bạn bè ' + end + '/' + friends.length, end, friends.length);
  }
  if(end<friends.length){
    // yield to keep scroll responsive on iPhone 6
    setTimeout(function(){ renderFriendBatch(container,friends,end,batch); }, IS_PHONE ? 32 : 16);
  } else {
    hideProgress(600);
  }
}

/* ===================== Moments (cache + live poll + mobile feed) ===================== */
function normalizeMediaUrl(u){
  if(!u)return'';
  return String(u).replace('firebasestorage.googleapis.com:443','firebasestorage.googleapis.com');
}
/* mediaSrc defined once above with WebP-aware path */
function isPhone(){return window.innerWidth<=520 || IS_PHONE}
function extractCaption(m){
  if(m.caption)return m.caption;
  if(m.overlays&&m.overlays.length){
    const cap=m.overlays.find(o=>o.overlay_id==='caption:standard')
      || m.overlays.find(o=>o.overlay_type==='caption'||(o.overlay_id&&String(o.overlay_id).startsWith('caption:')));
    if(cap)return(cap.data&&cap.data.text)||cap.alt_text||'';
  }
  return'';
}
function timeAgo(seconds){
  if(!seconds)return'';
  const diff=Math.max(0,Math.floor(Date.now()/1000-seconds));
  if(diff<60)return'vừa xong';
  if(diff<3600)return Math.floor(diff/60)+' phút';
  if(diff<86400)return Math.floor(diff/3600)+' giờ';
  return Math.floor(diff/86400)+' ngày';
}
function momentProf(m,friendMap){
  const uidRaw=(typeof m.user==='string')?m.user:((m.user||{}).uid||m.user_id||'');
  const prof=friendMap[uidRaw]||{};
  return {
    uid:uidRaw,
    first_name:m.first_name||prof.first_name||'',
    last_name:m.last_name||prof.last_name||'',
    username:prof.username||'',
    profile_picture_url:mediaSrc(m.profile_picture_url||prof.profile_picture_url||'', 128),
    celebrity:!!(m.from_celebrity||prof.celebrity),
    badge:prof.badge||'',
  };
}
let momentsRendered=0;
var _momentsBootLock = false; // true while opening tab — block scroll-triggered append
var _momentsHiddenAt = 0;
function momentsBatchSize(){ return MOMENTS_GRID_MODE ? Math.max(MOMENT_BATCH, momentsCols()*2) : MOMENT_BATCH; }
// The grid (desktop) and feed (phone) are mutually exclusive per the @media(max-width:520px)
// breakpoint in the CSS — only one is ever visible. Previously both were always built for every
// moment, silently doubling DOM nodes, image decodes and network requests on every device,
// which is a big part of why Moments felt heavy on iPhone 6. IS_PHONE mirrors that same 520px
// cutoff, so we now build only the node that will actually be shown.
var MV_KEY='locket_moments_view_v1';
function readMomentsView(){
  try{ var v=localStorage.getItem(MV_KEY); if(v==='feed'||v==='2'||v==='3'||v==='4') return v; }catch(e){}
  return IS_PHONE?'feed':'3';
}
var MOMENTS_VIEW=readMomentsView();
var MOMENTS_GRID_MODE = MOMENTS_VIEW!=='feed';
function momentsCols(){ return MOMENTS_VIEW==='feed'?1:(parseInt(MOMENTS_VIEW,10)||3); }
function applyMomentsView(){
  var b=document.body;
  b.className=(b.className||'').split(' ').filter(function(c){return c&&c.indexOf('mv-')!==0;})
    .concat([MOMENTS_GRID_MODE?'mv-grid':'mv-feed']).join(' ');
  var g=$('momentsGrid');
  if(g){
    var n=momentsCols();
    g.className='moments-grid cols-'+n+((n>=4||(n>=3&&window.innerWidth<=520))?' dense':'');
  }
  var seg=$('mvSeg');
  if(seg){
    var bs=seg.querySelectorAll('button');
    for(var i=0;i<bs.length;i++) bs[i].classList.toggle('active', bs[i].getAttribute('data-mv')===MOMENTS_VIEW);
  }
}
function setMomentsView(v){
  if(v!=='feed'&&v!=='2'&&v!=='3'&&v!=='4') return;
  if(v===MOMENTS_VIEW) return;
  MOMENTS_VIEW=v; MOMENTS_GRID_MODE=(v!=='feed');
  try{ localStorage.setItem(MV_KEY, v); }catch(e){}
  applyMomentsView();
  var items=window._momentItems||momentsCache||[];
  if(items.length) renderMomentsUI(items, true);
}
function initMomentsView(){
  applyMomentsView();
  var seg=$('mvSeg');
  if(seg) seg.addEventListener('click',function(e){
    var b=e.target.closest?e.target.closest('button[data-mv]'):null;
    if(b) setMomentsView(b.getAttribute('data-mv'));
  });
  var rt=null;
  window.addEventListener('resize',function(){ clearTimeout(rt); rt=setTimeout(applyMomentsView,200); });
}
function buildMomentNode(m, idx, friendMap, eager){
  return MOMENTS_GRID_MODE ? buildMomentCard(m, idx, friendMap, eager) : buildFeedSlide(m, idx, friendMap, eager);
}
function momentsActiveContainer(){
  return $(MOMENTS_GRID_MODE ? 'momentsGrid' : 'momentsFeed');
}
function momentKey(m){ return (m&&(m.name||m.thumbnail_url||m.url||m.video_url))||''; }
function bindMomentsClickDelegation(){
  var grid=$('momentsGrid'), feed=$('momentsFeed');
  function onClick(e){
    var el = e.target.closest ? e.target.closest('[data-idx]') : null;
    if(!el) return;
    var idx = parseInt(el.getAttribute('data-idx'), 10);
    if(!isNaN(idx)) openViewer(idx);
  }
  if(grid) grid.addEventListener('click', onClick);
  if(feed) feed.addEventListener('click', onClick);
}

function buildMomentCard(m, idx, friendMap, eager){
  const displayProf=momentProf(m,friendMap);
  const imgUrl=mediaSrc(m.thumbnail_url||m.url||'', THUMB_W);
  const cap=extractCaption(m);
  const t=(m.date&&m.date._seconds)||m.timestamp||m.created_at||0;
  const fullName=((displayProf.first_name||'')+' '+(displayProf.last_name||'')).trim()||displayProf.username||'?';
  const isVid=!!m.video_url;
  const card=document.createElement('div');
  card.className='moment-card';
  card.setAttribute('data-idx', String(idx));
  // placeholder first — real src goes through concurrency queue
  card.innerHTML=
    '<img src="'+TINY_PIXEL+'" data-real="'+esc(imgUrl)+'" alt="" style="background:var(--surface2)">'+
    '<div class="moment-overlay"></div>'+
    (isVid?'<div class="moment-video-badge"><i class="bi bi-play-fill"></i></div>':'')+
    '<div class="moment-top">'+renderAvatar(displayProf,20)+'<span class="mname">'+esc(fullName)+'</span><span class="mtime" data-ts="'+t+'">'+timeAgo(t)+'</span></div>'+
    (cap?captionHtml(cap):'');
  var imgEl = card.querySelector('img');
  if(imgEl){ imgEl.decoding='async'; }
  if(imgEl && imgUrl){
    if(eager) enqueueImg(imgEl, imgUrl);
    else imgEl.setAttribute('data-lazy', imgUrl);
  }
  return card;
}
function buildFeedSlide(m, idx, friendMap, eager){
  const displayProf=momentProf(m,friendMap);
  const imgUrl=mediaSrc(m.thumbnail_url||m.url||'', FEED_THUMB_W);
  const cap=extractCaption(m);
  const t=(m.date&&m.date._seconds)||m.timestamp||m.created_at||0;
  const name=((displayProf.first_name||'')+' '+(displayProf.last_name||'')).trim()||displayProf.username||'?';
  const isVid=!!m.video_url;
  const slide=document.createElement('div');
  slide.className='feed-slide';
  slide.setAttribute('data-idx', String(idx));
  slide.innerHTML=
    '<div class="feed-card">'+
      '<img src="'+TINY_PIXEL+'" data-real="'+esc(imgUrl)+'" alt="" style="background:var(--surface2)">'+
      '<div class="moment-overlay"></div>'+
      (isVid?'<div class="moment-video-badge"><i class="bi bi-play-fill"></i></div>':'')+
      (cap?captionHtml(cap):'')+
    '</div>'+
    '<div class="feed-meta">'+
      '<div class="feed-name">'+renderAvatar(displayProf,28)+'<span>'+esc(name)+'</span><span class="mtime" data-ts="'+t+'" style="margin-left:auto;color:var(--muted);font-size:12px;font-weight:700">'+timeAgo(t)+'</span></div>'+
    '</div>';
  var imgEl = slide.querySelector('img');
  if(imgEl){ imgEl.decoding='async'; }
  if(imgEl && imgUrl){
    if(eager) enqueueImg(imgEl, imgUrl);
    else imgEl.setAttribute('data-lazy', imgUrl);
  }
  return slide;
}
/* ===== Caption pill: tự chạy chữ khi dài hơn khung ===== */
function captionHtml(cap){
  return '<div class="moment-caption"><div class="cap-pill"><div class="cap-clip"><span class="cap-in">'+esc(cap)+'</span></div></div></div>';
}
var _capRaf=0, _capForce=false, _capResizeT=null;
function fitCaptionsSoon(force){
  if(force) _capForce=true;
  if(_capRaf) return;
  _capRaf=requestAnimationFrame(function(){
    _capRaf=0;
    var f=_capForce; _capForce=false;
    fitCaptions(f);
  });
}
function fitCaptions(force){
  var clips=document.querySelectorAll('#momentsGrid .cap-clip, #momentsFeed .cap-clip');
  var todo=[], i, c;
  for(i=0;i<clips.length;i++){
    c=clips[i];
    if(!force && c._fitW && c._fitW===c.clientWidth) continue;
    todo.push(c);
  }
  if(!todo.length) return;
  for(i=0;i<todo.length;i++){
    c=todo[i];
    var tr=c.querySelector('.cap-track');
    if(tr){
      var first=tr.querySelector('.cap-in');
      c.innerHTML=''; c.appendChild(first); c.classList.remove('marq');
    }
  }
  var info=[];
  for(i=0;i<todo.length;i++){
    c=todo[i];
    var inn=c.querySelector('.cap-in');
    info.push({c:c, inn:inn, cw:c.clientWidth, sw:inn?inn.offsetWidth:0, dist:0});
  }
  for(i=0;i<info.length;i++){
    var o=info[i];
    if(!o.cw || !o.inn) continue;
    if(o.sw>o.cw+1){
      var track=document.createElement('span'); track.className='cap-track';
      var b=o.inn.cloneNode(true); b.setAttribute('aria-hidden','true');
      o.c.classList.add('marq');
      o.c.appendChild(track); track.appendChild(o.inn); track.appendChild(b);
      o.dist=o.sw+36;
    }
    o.c._fitW=o.cw;
  }
  for(i=0;i<info.length;i++){
    if(!info[i].dist) continue;
    var t=info[i].c.querySelector('.cap-track');
    if(t) t.style.animationDuration=Math.max(6, info[i].dist/36)+'s';
  }
}
window.addEventListener('resize', function(){
  clearTimeout(_capResizeT);
  _capResizeT=setTimeout(function(){ fitCaptionsSoon(false); }, 250);
});
if(document.fonts && document.fonts.ready){
  document.fonts.ready.then(function(){ fitCaptionsSoon(true); });
}

function appendMomentsBatch(){
  const items=window._momentItems||[];
  if(momentsRendered>=items.length){
    updateMoreLabel();
    if(items.length){
      showProgress('Moments ' + items.length + '/' + items.length, items.length, items.length);
      hideProgress(800);
    }
    return;
  }
  const friendMap={};
  if(friendsCache)friendsCache.forEach(function(f){ friendMap[f.uid]=f; });
  const target=momentsActiveContainer();
  const batch=momentsBatchSize();
  const end=Math.min(momentsRendered+batch, items.length);
  var frag = target ? document.createDocumentFragment() : null;
  for(let idx=momentsRendered; idx<end; idx++){
    // first few: eager so newest shows immediately
    const eager = idx < (IS_PHONE ? 2 : 4);
    if(frag) frag.appendChild(buildMomentNode(items[idx], idx, friendMap, eager));
  }
  if(target && frag) target.appendChild(frag);
  fitCaptionsSoon(false);
  momentsRendered=end;
  showProgress('Moments ' + momentsRendered + '/' + items.length, momentsRendered, items.length);
  updateMoreLabel();
  // kick lazy load for newly painted nodes near viewport
  scheduleLazyLoad();
}
function showMomentsEmpty(isError){
  var box=$('momentsEmpty'); if(!box) return;
  box.classList.remove('hidden');
  var icon=$('momentsEmptyIcon'), text=$('momentsEmptyText'), retry=$('momentsEmptyRetry');
  if(icon) icon.className='bi '+(isError?'bi-wifi-off':'bi-collection');
  if(text) text.textContent=isError?'Không tải được Moments':'Chưa có khoảnh khắc nào';
  if(retry) retry.classList.toggle('hidden', !isError);
}
function renderMomentsUI(items, reset){
  const grid=$('momentsGrid'), feed=$('momentsFeed');
  window._momentItems=items;
  if(reset!==false){
    hideNewMomentsPill();
    if(grid) grid.innerHTML='';
    if(feed) feed.innerHTML='';
    momentsRendered=0;
    resetImgProgress();
    // Force top — clearing content must not keep old scroll position
    _momentsBootLock = true;
    try{
      var page=$('page-moments');
      if(page) page.scrollTop = 0;
      if(feed) feed.scrollTop = 0;
    }catch(e){}
  }
  if(!items.length){showMomentsEmpty(false); hideProgress(0); return}
  $('momentsEmpty').classList.add('hidden');
  showProgress('Moments 0/' + items.length, 0, items.length);
  // Paint ONLY the first batch on open — more loads when user scrolls
  appendMomentsBatch();
  bindMomentsScroll();
  startRelativeTimeTicker();
  // One small follow-up batch for smoother first screen (still at top)
  if(momentsRendered<items.length){
    setTimeout(function(){
      appendMomentsBatch();
      scrollMomentsTop();
      setTimeout(function(){ _momentsBootLock = false; }, 180);
    }, IS_PHONE ? 100 : 50);
  } else {
    hideProgress(700);
    setTimeout(function(){ _momentsBootLock = false; }, 180);
  }
}

/* Count fresh items ahead of the currently-rendered top (contiguous new run) */
function countNewMoments(freshItems){
  var oldItems=window._momentItems||[];
  var oldKeys={};
  for(var i=0;i<oldItems.length;i++) oldKeys[momentKey(oldItems[i])]=true;
  var n=0;
  for(var j=0;j<freshItems.length;j++){
    if(oldKeys[momentKey(freshItems[j])]) break;
    n++;
  }
  return n;
}
/* Insert only the new cards at the top instead of tearing down the whole
   grid/feed — avoids re-decoding already-painted images and keeps scroll
   position untouched. Falls back to caller doing a full render if nothing
   new is found. */
function prependNewMoments(freshItems){
  var n=countNewMoments(freshItems);
  if(n<=0){ window._momentItems=freshItems; return false; }
  var newOnes=freshItems.slice(0, n);
  var friendMap={};
  if(friendsCache)friendsCache.forEach(function(f){ friendMap[f.uid]=f; });
  var target=momentsActiveContainer();
  var nodes=target?target.querySelectorAll('[data-idx]'):[];
  for(var x=0;x<nodes.length;x++){
    var old=parseInt(nodes[x].getAttribute('data-idx'),10);
    nodes[x].setAttribute('data-idx', String(old+n));
  }
  var frag=target?document.createDocumentFragment():null;
  for(var idx=0; idx<n; idx++){
    if(frag) frag.appendChild(buildMomentNode(newOnes[idx], idx, friendMap, true));
  }
  if(target && frag) target.insertBefore(frag, target.firstChild);
  fitCaptionsSoon(false);
  window._momentItems=freshItems;
  momentsRendered+=n;
  scheduleLazyLoad();
  return true;
}
var _pendingMoments=null;
function showNewMomentsPill(freshItems){
  var n=countNewMoments(freshItems);
  var pill=$('momentsNewPill');
  if(n<=0 || !pill){ hideNewMomentsPill(); return; }
  _pendingMoments=freshItems;
  pill.textContent=(n===1?'1 khoảnh khắc mới':n+' khoảnh khắc mới')+' \u2191';
  pill.classList.add('show');
}
function hideNewMomentsPill(){
  _pendingMoments=null;
  var pill=$('momentsNewPill');
  if(pill) pill.classList.remove('show');
}
function applyPendingMoments(){
  if(!_pendingMoments) return;
  var items=_pendingMoments;
  hideNewMomentsPill();
  scrollMomentsTop();
  if(!prependNewMoments(items)) renderMomentsUI(items);
}
function refreshMomentsNow(){
  var icon=$('momentsRefreshIcon');
  if(icon) icon.classList.add('icon-spin');
  apiTimeout('/api/moments?force=1', null, 20000).then(function(d){
    if(icon) icon.classList.remove('icon-spin');
    if(!d.ok){ toast(d.error||'Không làm mới được Moments'); return; }
    notifyMomentsWarning(d);
    var items=(d.moments||[]).filter(function(m){return m&&(m.thumbnail_url||m.video_url||m.url)});
    if(!items.length && momentsCache.length){
      toast('Không có Moments mới');
      return;
    }
    hideNewMomentsPill();
    scrollMomentsTop();
    applyMomentsNetworkResult(items, d.updated_at, false, {notifyIfNoChange:true});
  }).catch(function(){
    if(icon) icon.classList.remove('icon-spin');
    toast('Lỗi mạng khi làm mới Moments');
  });
}

/* Lazy-load images only when near viewport; unload far ones to free RAM */
function scheduleLazyLoad(){
  if(window._lazyT) return;
  window._lazyT = setTimeout(function(){
    window._lazyT = null;
    runLazyLoad();
  }, 60);
}
function runLazyLoad(){
  pumpMore();
  var root = momentsScrollEl() || document;
  var viewH = (root === document || root === window) ? window.innerHeight : root.clientHeight;
  var scrollTop = (root === document || root === window) ? (window.pageYOffset || document.documentElement.scrollTop) : root.scrollTop;
  var margin = viewH * 1.5;
  var nodes = document.querySelectorAll('#momentsGrid img[data-lazy], #momentsFeed img[data-lazy], #momentsGrid img[data-src-loaded], #momentsFeed img[data-src-loaded]');
  for(var i=0; i<nodes.length; i++){
    var img = nodes[i];
    var card = img.closest ? (img.closest('.moment-card') || img.closest('.feed-slide')) : img.parentNode;
    if(!card) continue;
    var rect = card.getBoundingClientRect ? card.getBoundingClientRect() : {top:0,bottom:0};
    // relative to viewport is fine even inside fixed page
    var near = rect.bottom > -margin && rect.top < viewH + margin;
    var lazy = img.getAttribute('data-lazy');
    var loaded = img.getAttribute('data-src-loaded');
    if(near){
      if(lazy && !loaded){
        img.removeAttribute('data-lazy');
        enqueueImg(img, lazy);
      }
    } else if(loaded && IS_PHONE){
      // unload far images on weak devices (LinkedIn technique)
      img.src = TINY_PIXEL;
      img.setAttribute('data-lazy', loaded);
      img.removeAttribute('data-src-loaded');
    }
  }
}
function momentsScrollEl(){
  // On phone the fixed #page-moments is the scroller; elsewhere feed or window
  const page=$('page-moments');
  if(page && page.classList.contains('active') && isPhone()) return page;
  const feed=$('momentsFeed');
  if(feed && feed.scrollHeight>feed.clientHeight+20) return feed;
  return null;
}
function bindMomentsScroll(){
  // scroll listeners here only drive lazy-load of images near the viewport — kept
  // per-container since that math only needs "is this near visible", not "at the end".
  const page=$('page-moments');
  if(page && !page._boundScroll){
    page._boundScroll=true;
    page.addEventListener('scroll',function(){
      if(!page.classList.contains('active'))return;
      scheduleLazyLoad();
    },false);
  }
  const feed=$('momentsFeed');
  if(feed && !feed._boundScroll){
    feed._boundScroll=true;
    feed.addEventListener('scroll',function(){ scheduleLazyLoad(); },false);
  }
  if(!window._momentsWinScroll){
    window._momentsWinScroll=true;
    window.addEventListener('scroll',function(){
      const p=$('page-moments');
      if(!p||!p.classList.contains('active'))return;
      scheduleLazyLoad();
    },false);
  }
  // "Load more" trigger: one IntersectionObserver watching a 1px sentinel right above
  // the "Đang tải thêm…" label. IntersectionObserver measures against the real browser
  // viewport regardless of *which* ancestor actually scrolls (window on desktop/tablet,
  // the fixed full-screen #page-moments on phone), so this works everywhere without the
  // scrollHeight/clientHeight/scrollTop bookkeeping above ever getting out of sync —
  // that per-container math was the reason "more" could get stuck forever on non-phone
  // layouts where window, not #page-moments, is the actual scroller.
  const sentinel=$('momentsMoreSentinel');
  if(sentinel){
    if(window._momentsMoreObserver && window._momentsMoreObserver.disconnect){
      window._momentsMoreObserver.disconnect();
    }
    window._momentsMoreObserver=null;
    const tryLoadMore=function(){ pumpMore(); };
    if(window.IntersectionObserver){
      window._momentsMoreObserver=new IntersectionObserver(function(entries){
        if(entries.some(function(e){return e.isIntersecting})) tryLoadMore();
      },{root:null, rootMargin:'600px 0px', threshold:0});
      window._momentsMoreObserver.observe(sentinel);
    } else {
      // Very old browsers without IntersectionObserver (pre-iOS 12.2 Safari): fall back
      // to a plain rect check driven off the same scroll listeners already bound above.
      window._momentsMoreObserver = true; // marker so we don't bind this twice
      const rectCheck=function(){
        const r=sentinel.getBoundingClientRect();
        if(r.top < (window.innerHeight || 800) + 600) tryLoadMore();
      };
      if(page) page.addEventListener('scroll', rectCheck, false);
      if(feed) feed.addEventListener('scroll', rectCheck, false);
      window.addEventListener('scroll', rectCheck, false);
    }
  }
}
function scrollMomentsTop(){
  // iOS 12 does not support scrollTo({behavior:'smooth'}) — use scrollTop=0
  function jump(el){
    if(!el) return;
    try{ el.scrollTop=0; }catch(e){}
    try{ if(el.scrollTo) el.scrollTo(0,0); }catch(e){}
  }
  jump(momentsScrollEl());
  jump($('page-moments'));
  jump($('momentsFeed'));
  jump(document.documentElement);
  jump(document.body);
  try{ window.scrollTo(0,0); }catch(e){}
}
let _timeTicker=null;
function startRelativeTimeTicker(){
  if(_timeTicker)return;
  _timeTicker=setInterval(()=>{
    if(document.hidden) return;
    document.querySelectorAll('.mtime[data-ts]').forEach(el=>{
      const ts=Number(el.getAttribute('data-ts'));
      if(ts) el.textContent=timeAgo(ts);
    });
  },30000);
}
function readMomentsLocal(){
  try{
    const raw=localStorage.getItem(MOMENTS_LS_KEY);
    if(!raw) return null;
    const obj=JSON.parse(raw);
    if(!obj||!Array.isArray(obj.moments)) return null;
    return obj;
  }catch(e){return null}
}
function writeMomentsLocal(moments, updatedAt){
  function leanMap(list, cap){
    return (list||[]).slice(0, cap).map(function(m){
      return {
        name:m.name, user:m.user, user_id:m.user_id, thumbnail_url:m.thumbnail_url, url:m.url, video_url:m.video_url,
        date:m.date, timestamp:m.timestamp, created_at:m.created_at, caption:m.caption, overlays:m.overlays,
        first_name:m.first_name, last_name:m.last_name, profile_picture_url:m.profile_picture_url,
        from_celebrity:m.from_celebrity
      };
    });
  }
  var caps = [LOCAL_MOMENTS_CAP, 24, 12, 6];
  for(var i=0;i<caps.length;i++){
    try{
      localStorage.setItem(MOMENTS_LS_KEY, JSON.stringify({
        moments: leanMap(moments, caps[i]),
        updated_at: updatedAt||(Date.now()/1000),
        ts: Date.now()
      }));
      return;
    }catch(e){
      try{ localStorage.removeItem(MOMENTS_LS_KEY); }catch(e2){}
    }
  }
  try{ console.warn('moments local cache unavailable'); }catch(e){}
}
function prefetchMomentImages(items, count){
  // Soft prefetch only a few URLs into the concurrency queue — never flood
  if(!items||!items.length) return;
  var n = Math.min(count || PREFETCH_COUNT, items.length, PREFETCH_COUNT);
  for(var i=0;i<n;i++){
    var m = items[i];
    var u = mediaSrc(m.thumbnail_url||m.url||'', FEED_THUMB_W);
    if(!u) continue;
    // warm browser HTTP cache without attaching to DOM
    var tmp = new Image();
    tmp.src = u;
  }
}
/* Paint memory or localStorage without network. Never clears existing DOM if we have data. */
function ensureMomentsFromCache(){
  if(momentsCache && momentsCache.length){
    // Already in memory — only re-paint if grid/feed is empty (tab was left and DOM wiped)
    var grid=$('momentsGrid'), feed=$('momentsFeed');
    var hasDom = (grid && grid.children.length) || (feed && feed.children.length);
    if(!hasDom){
      renderMomentsUI(momentsCache, true);
      prefetchMomentImages(momentsCache, PREFETCH_COUNT);
    }
    return true;
  }
  var local=readMomentsLocal();
  if(local && local.moments && local.moments.length){
    momentsCache=local.moments;
    momentsUpdatedAt=local.updated_at||(local.ts/1000)||0;
    momentsLoaded=true;
    renderMomentsUI(momentsCache, true);
    prefetchMomentImages(momentsCache, PREFETCH_COUNT);
    return true;
  }
  return false;
}

var _momentsFetchInFlight = false;
var _momentsFetchQueued = false;

function applyMomentsNetworkResult(items, updatedAt, preferPrepend, opts){
  opts = opts || {};
  if(!items || !items.length){
    // Empty network response must NEVER wipe a healthy local cache (binhake glitch / timeout)
    if(momentsCache && momentsCache.length){
      loggerSoft('Moments network returned empty — keeping local cache of ' + momentsCache.length);
      if(opts.notifyIfNoChange) toast('Không có Moments mới');
      return;
    }
    showMomentsEmpty(false);
    hideProgress(0);
    return;
  }
  var hadCache = momentsCache && momentsCache.length > 0;
  momentsCache = items;
  momentsUpdatedAt = updatedAt || (Date.now()/1000);
  momentsLoaded = true;
  writeMomentsLocal(items, momentsUpdatedAt);

  var onMomentsPage = $('page-moments') && $('page-moments').classList.contains('active');
  if(!onMomentsPage){
    prefetchMomentImages(items, PREFETCH_COUNT);
    return;
  }

  // Compare against what's ACTUALLY painted on screen (window._momentItems), not momentsCache —
  // momentsCache can get silently advanced by pollMomentsOnce() while the user is scrolled down
  // (it shows a tappable pill instead of repainting), which used to leave momentsCache pointing
  // at newer data than the DOM. Comparing against momentsCache made this function think "nothing
  // changed" and bail out, so opening the tab again or pressing Reload appeared to do nothing even
  // though a real update was sitting unpainted. window._momentItems is only ever updated when
  // something was actually painted, so it's the correct baseline.
  var painted = window._momentItems || [];
  var prevTop = painted[0] && momentKey(painted[0]);
  var newTop = items[0] && momentKey(items[0]);
  var fullyPainted = momentsRendered >= painted.length;

  if(hadCache && fullyPainted && prevTop && prevTop === newTop && Math.abs(painted.length - items.length) <= 2){
    // Genuinely nothing new and everything visible is already on screen.
    window._momentItems = items;
    prefetchMomentImages(items, PREFETCH_COUNT);
    if(opts.notifyIfNoChange) toast('Đã là Moments mới nhất');
    return;
  }

  if(preferPrepend && hadCache && prependNewMoments(items)){
    prefetchMomentImages(items, PREFETCH_COUNT);
    return;
  }
  // Full paint only when structure actually changed or first load
  renderMomentsUI(items, true);
  prefetchMomentImages(items, PREFETCH_COUNT);
}

function loggerSoft(msg){ try{ console.log('[moments]', msg); }catch(e){} }
var _lastMomentsWarn='';
function notifyMomentsWarning(d){
  try{
    var w=d&&d.warnings&&d.warnings[0];
    if(w&&w!==_lastMomentsWarn){ _lastMomentsWarn=w; toast(String(w)); }
  }catch(e){}
}


var _momentsMoreBusy=false, _momentsHasMore=true, _momentsAutoRounds=2;
function momentTs(m){ return (m&&(m.timestamp||(m.date&&m.date._seconds)))||0; }
var _momentsEmptyStreak=0;
function fetchMoreMoments(auto){
  if(_momentsMoreBusy||!_momentsHasMore||navigator.onLine===false){ updateMoreLabel(); return; }
  _momentsMoreBusy=true; updateMoreLabel();
  var have=(window._momentItems||[]).length;
  apiTimeout('/api/moments/more?have='+have, null, 45000).then(function(d){
    _momentsMoreBusy=false;
    if(!d||!d.ok){ updateMoreLabel(); return; }
    _momentsHasMore=d.has_more!==false;
    var items=(d.moments||[]).filter(function(m){return m&&(m.thumbnail_url||m.video_url||m.url)});
    var cur=window._momentItems||[];
    if(items.length<=cur.length){
      if(++_momentsEmptyStreak>=2) _momentsHasMore=false;
      updateMoreLabel(); return;
    }
    momentsCache=items;
    momentsUpdatedAt=d.updated_at||momentsUpdatedAt;
    writeMomentsLocal(items, momentsUpdatedAt);
    var scroller=momentsScrollEl();
    var top=(scroller&&scroller.scrollTop)||window.pageYOffset||0;
    var gotNew=true;
    if(top<250){
      renderMomentsUI(items, true);
    } else {
      var lastTs=cur.length?momentTs(cur[cur.length-1]):0;
      var seen={};
      cur.forEach(function(m){ seen[momentKey(m)]=1; });
      var older=items.filter(function(m){ return momentTs(m)<lastTs && !seen[momentKey(m)]; });
      gotNew=older.length>0;
      if(gotNew){ window._momentItems=cur.concat(older); appendMomentsBatch(); }
    }
    if(gotNew) _momentsEmptyStreak=0;
    else if(++_momentsEmptyStreak>=2) _momentsHasMore=false;
    updateMoreLabel();
    if(gotNew) pumpMore();
    if(auto && _momentsAutoRounds>0 && _momentsHasMore){
      _momentsAutoRounds--;
      setTimeout(function(){ fetchMoreMoments(true); }, 300);
    }
  }).catch(function(){ _momentsMoreBusy=false; updateMoreLabel(); });
}

function loadMoments(force){
  // 1) Always try to show something immediately
  var painted = ensureMomentsFromCache();

  // Skeleton ONLY when we truly have nothing
  if(!painted && !(momentsCache && momentsCache.length)){
    $('momentsEmpty').classList.add('hidden');
    showProgress('Đang tải Moments…', 0, 1);
    var grid=$('momentsGrid'), feed=$('momentsFeed');
    if(grid && !grid.children.length){
      grid.innerHTML=Array(IS_PHONE?2:4).fill('<div class="skeleton" style="width:100%;height:0;padding-bottom:100%;border-radius:14px"></div>').join('');
    }
    if(feed && !feed.children.length){
      feed.innerHTML='<div class="feed-skel"><div class="skeleton"></div><div class="skeleton" style="height:18px;width:60%;border-radius:8px;padding-bottom:0"></div></div>';
    }
  }

  // Soft memory is fresh (<25s) and caller didn't force → skip network
  if(!force && momentsCache.length && (Date.now()/1000 - (momentsUpdatedAt||0)) < 25){
    return;
  }

  // Single-flight: don't stack parallel force fetches on slow iPhone 6
  if(_momentsFetchInFlight){
    _momentsFetchQueued = true;
    return;
  }
  _momentsFetchInFlight = true;

  var q = '?force=1';
  apiTimeout('/api/moments'+q, null, 65000).then(function(d){
    _momentsFetchInFlight = false;
    var queued = _momentsFetchQueued;
    _momentsFetchQueued = false;
    if(!d.ok){
      if(!momentsCache.length){
        toast(d.error||'Không tải được moments');
        showMomentsEmpty(true);
        hideProgress(0);
      } else {
        // Keep old UI — never clear
        toast(d.error||'Không làm mới được Moments');
      }
      if(queued) loadMoments(true);
      return;
    }
    notifyMomentsWarning(d);
    var items=(d.moments||[]).filter(function(m){return m&&(m.thumbnail_url||m.video_url||m.url)});
    applyMomentsNetworkResult(items, d.updated_at, true);
    _momentsHasMore=d.has_more!==false;
    _momentsEmptyStreak=0;
    _momentsAutoRounds=2;
    setTimeout(function(){ fetchMoreMoments(true); }, 500);
    if(queued) loadMoments(true);
  }).catch(function(err){
    _momentsFetchInFlight = false;
    var queued = _momentsFetchQueued;
    _momentsFetchQueued = false;
    if(err && err.aborted){
      // Timeout / tab switch / wifi hop — silent if we already have cache
      if(!momentsCache.length){
        showMomentsEmpty(true);
        hideProgress(0);
      }
      if(queued) setTimeout(function(){ loadMoments(true); }, 1500);
      return;
    }
    if(!momentsCache.length){
      toast('Lỗi mạng khi tải moments');
      showMomentsEmpty(true);
      hideProgress(0);
    } else {
      toast('Lỗi mạng — đang hiện Moments đã lưu');
    }
    if(queued) setTimeout(function(){ loadMoments(true); }, 1200);
  });
}
function preloadMoments(){
  ensureMomentsFromCache();
  var local=readMomentsLocal();
  var need=!(local && (Date.now()-(local.ts||0)<MOMENTS_TTL_MS) && local.moments && local.moments.length);
  var run = function(){
    if(_momentsFetchInFlight) return;
    apiTimeout('/api/moments?force=1', null, 20000).then(function(d){
      if(!d.ok) return;
      var items=(d.moments||[]).filter(function(m){return m&&(m.thumbnail_url||m.video_url||m.url)});
      // Silent: only update memory/localStorage; do not force full UI re-render unless on page
      if(!items.length && momentsCache.length) return;
      momentsCache=items.length ? items : momentsCache;
      if(items.length){
        momentsUpdatedAt=d.updated_at||(Date.now()/1000);
        momentsLoaded=true;
        writeMomentsLocal(items, momentsUpdatedAt);
        prefetchMomentImages(items, PREFETCH_COUNT);
        if($('page-moments') && $('page-moments').classList.contains('active')){
          applyMomentsNetworkResult(items, momentsUpdatedAt, true);
        }
      }
    }).catch(function(){});
  };
  if(need || !local){
    run();
  }else{
    setTimeout(run, 2800);
  }
}
var _pollBusy=false;
function pollMomentsOnce(){
  if(document.hidden||_pollBusy) return;            // don't poll in background tabs / overlap requests
  _pollBusy=true; setTimeout(function(){_pollBusy=false},1500);
  try{ pumpMore(); updateMoreLabel(); }catch(e){}
  apiTimeout('/api/moments/poll?since='+encodeURIComponent(momentsUpdatedAt||0), null, 20000).then(function(d){
    if(!d.ok||!d.changed)return;
    const items=(d.moments||[]).filter(function(m){return m&&(m.thumbnail_url||m.video_url||m.url)});
    if(!items.length)return;
    // Never accept empty wipe from poll
    momentsCache=items;
    momentsUpdatedAt=d.updated_at||momentsUpdatedAt;
    try{ writeMomentsLocal(items, momentsUpdatedAt); }catch(e){}
    if(!($('page-moments')&&$('page-moments').classList.contains('active'))) return;
    // Near the top: splice the new cards in without tearing down the grid —
    // cheap and doesn't disturb scroll. Scrolled down browsing older moments:
    // surface a tappable pill instead of silently swallowing the update (the
    // old behaviour — user had to reload the page to ever see them).
    var nearTop=true;
    try{
      nearTop = isPhone()
        ? (!$('page-moments') || $('page-moments').scrollTop < 200)
        : ((window.pageYOffset||document.documentElement.scrollTop||0) < 200);
    }catch(e){}
    if(nearTop) prependNewMoments(items);
    else showNewMomentsPill(items);
  }).catch(function(){});
}
function startMomentsPoll(){
  stopMomentsPoll();
  momentsPollTimer=setInterval(pollMomentsOnce, 8000); // 8s — pairs with server poll that refreshes when cache >40s old
}
function stopMomentsPoll(){
  if(momentsPollTimer){clearInterval(momentsPollTimer);momentsPollTimer=null}
  hideNewMomentsPill();
}
function viewerMediaError(el, msg){
  try{
    el.style.display='none';
    if(!el.parentNode.querySelector('.viewer-error')){
      el.insertAdjacentHTML('afterend',
        '<div class="viewer-error moments-empty" style="position:static">'+
        '<i class="bi bi-exclamation-triangle" style="font-size:36px"></i><div>'+esc(msg)+'</div></div>');
    }
  }catch(e){}
}
/* ===== Viewer + Views realtime (single-flight, tạm dừng khi ẩn tab, không rò rỉ) ===== */
var _viewsToken=0, _viewsTimer=null, _viewsAbort=null, _viewsBusy=false;
var _viewsCache={}, _viewsCacheKeys=[], _viewsActive=null, _viewsSig='';
var _viewsIdle=0, _viewsFail=0, _viewsLastHtml='', _viewsPending=null;
var VIEWS_FAST_MS=4000, VIEWS_SLOW_MS=12000, VIEWS_MAX_CACHE=20;

function viewsDelay(){
  var base=_viewsIdle>=6?VIEWS_SLOW_MS:VIEWS_FAST_MS;
  if(_viewsFail>0) base=Math.min(60000, base*Math.pow(2,_viewsFail));
  return base;
}
function viewsCachePut(mid,d){
  if(!_viewsCache[mid]) _viewsCacheKeys.push(mid);
  _viewsCache[mid]=d;
  while(_viewsCacheKeys.length>VIEWS_MAX_CACHE) delete _viewsCache[_viewsCacheKeys.shift()];
}
function stopViews(){
  _viewsToken++;
  if(_viewsTimer){ clearTimeout(_viewsTimer); _viewsTimer=null; }
  if(_viewsAbort){ try{ _viewsAbort.abort(); }catch(e){} _viewsAbort=null; }
  _viewsBusy=false; _viewsActive=null; _viewsSig=''; _viewsIdle=0; _viewsFail=0;
  _viewsLastHtml=''; _viewsPending=null;
}
function startViews(mid){
  stopViews();
  _viewsActive={mid:mid, token:_viewsToken};
  var c=_viewsCache[mid];
  if(c){ _viewsSig=c.sig||''; renderViews(c); }
  loadViews(!!c);
}
function scheduleViews(){
  if(!_viewsActive||_viewsTimer) return;
  _viewsTimer=setTimeout(function(){ _viewsTimer=null; loadViews(true); }, viewsDelay());
}
function loadViews(silent){
  var act=_viewsActive; if(!act) return;
  if(_viewsTimer){ clearTimeout(_viewsTimer); _viewsTimer=null; }
  if(document.hidden || navigator.onLine===false || _viewsBusy) return;
  var token=act.token, mid=act.mid;
  var ctrl=(typeof AbortController!=='undefined')?new AbortController():null;
  _viewsAbort=ctrl; _viewsBusy=true;
  var killer=setTimeout(function(){ if(ctrl){ try{ ctrl.abort(); }catch(e){} } }, 20000);
  var url='/api/moments/views?uid='+encodeURIComponent(mid)+(_viewsSig?'&sig='+encodeURIComponent(_viewsSig):'');
  var opts={credentials:'same-origin'}; if(ctrl) opts.signal=ctrl.signal;
  fetch(url, opts).then(function(r){ return r.json(); }).then(function(d){
    clearTimeout(killer);
    if(token!==_viewsToken) return;
    _viewsBusy=false; _viewsAbort=null;
    if(!d||!d.ok){
      if(d && d.error==='Not logged in') return;
      _viewsFail++;
      if(!_viewsCache[mid]) renderViewsError((d&&d.error)||'Không tải được lượt xem', mid);
      scheduleViews(); return;
    }
    _viewsFail=0;
    if(d.unchanged){ _viewsIdle++; scheduleViews(); return; }
    _viewsIdle=0; _viewsSig=d.sig||'';
    viewsCachePut(mid, d);
    renderViews(d);
    scheduleViews();
  }).catch(function(){
    clearTimeout(killer);
    if(token!==_viewsToken) return;
    _viewsBusy=false; _viewsAbort=null; _viewsFail++;
    if(!_viewsCache[mid]) renderViewsError('Lỗi mạng', mid);
    scheduleViews();
  });
}
function renderViewsError(msg, mid){
  var list=$('viewerList'); if(!list) return;
  _viewsLastHtml='';
  list.innerHTML=`<div class="people-msg">${esc(msg)}<br><button type="button" class="vs-retry" data-mid="${esc(mid)}" onclick="startViews(this.getAttribute('data-mid'))">Thử lại</button></div>`;
}
function renderViews(d){
  var list=$('viewerList'); if(!list) return;
  var viewers=d.viewers||[], count=d.count||viewers.length;
  var c=$('vsCount'); if(c) c.textContent='Lượt xem · '+count;
  var tally={};
  viewers.forEach(function(v){ if(v.reaction) tally[v.reaction]=(tally[v.reaction]||0)+1; });
  var r=$('vsReact');
  if(r) r.innerHTML=Object.keys(tally).map(function(e){
    return '<span class="vs-chip">'+esc(e)+' '+tally[e]+'</span>';
  }).join('');
  var side=$('viewerSide');
  if(side && side.classList.contains('collapsed')){ _viewsPending=d; return; }
  _viewsPending=null;
  var html=viewers.length
    ? viewers.map(viewerRow).join('')
    : '<div class="people-msg" style="text-align:center;padding:28px 0">Chưa có ai xem</div>';
  if(html===_viewsLastHtml) return;
  _viewsLastHtml=html;
  var keep=list.scrollTop;
  list.innerHTML=html;
  list.scrollTop=keep;
}
function toggleViewsSide(){
  var s=$('viewerSide'); if(!s) return;
  s.classList.toggle('collapsed');
  if(!s.classList.contains('collapsed') && _viewsPending) renderViews(_viewsPending);
}
if(!window._viewsListeners){
  window._viewsListeners=true;
  document.addEventListener('visibilitychange',function(){
    if(!_viewsActive) return;
    if(document.hidden){ if(_viewsTimer){ clearTimeout(_viewsTimer); _viewsTimer=null; } }
    else { _viewsIdle=0; loadViews(true); }
  });
  window.addEventListener('online',function(){ if(_viewsActive){ _viewsFail=0; loadViews(true); } });
}

var _viewerOpenId=null;
function openViewer(idx, fromRoute){
  const m=(window._momentItems||[])[idx]; if(!m)return;
  const v=$('viewerStage');
  const isVid=!!m.video_url;
  const raw=normalizeMediaUrl(m.video_url||m.thumbnail_url||m.url);
  const src=isVid?raw:mediaSrc(raw, 1080);
  const poster=(isVid&&m.thumbnail_url)?mediaSrc(m.thumbnail_url,1080):'';
  const cap=extractCaption(m);
  const mid=m.canonical_uid||m.id||'';
  const showSide=!!(BOOT_UID && m.user===BOOT_UID && mid);
  stopViews();

  const media=isVid
    ? `<video class="viewer-media" src="${esc(src)}" ${poster?`poster="${esc(poster)}"`:''} controls autoplay playsinline onerror="viewerMediaError(this,'Không phát được video')"></video>`
    : `<img class="viewer-media" src="${esc(src)}" alt="" onerror="viewerMediaError(this,'Không tải được ảnh')">`;

  const side=showSide?`
    <aside class="viewer-side" id="viewerSide">
      <div class="vs-head" onclick="toggleViewsSide()">
        <div class="vs-title"><i class="bi bi-eye-fill"></i><span id="vsCount">Lượt xem</span></div>
        <div class="vs-meta"><span id="vsReact"></span><i class="bi bi-chevron-down vs-chev"></i></div>
      </div>
      <div class="viewer-list" id="viewerList">
        <div class="skeleton" style="height:48px;margin:8px 0"></div>
        <div class="skeleton" style="height:48px;margin:8px 0"></div>
        <div class="skeleton" style="height:48px;margin:8px 0"></div>
      </div>
    </aside>`:'';

  v.innerHTML=`
    <div class="viewer-main">
      <div class="viewer-head"><button type="button" class="viewer-close" onclick="closeViewer()" aria-label="Đóng">✕</button></div>
      ${media}
      ${cap?`<div class="viewer-caption">${esc(cap)}</div>`:''}
    </div>${side}`;
  v.classList.remove('hidden');
  document.body.style.overflow='hidden';
  _viewerOpenId=mid||('i'+idx);
  if(!fromRoute && mid) pushRoute('/moments/'+encodeURIComponent(mid), {page:'moments', viewer:1});
  if(showSide) startViews(mid);
}
function closeViewer(fromPop){
  stopViews();
  const v=$('viewerStage');
  if(v){ v.classList.add('hidden'); v.innerHTML=''; }
  document.body.style.overflow='';
  _viewerOpenId=null;
  if(fromPop!==true){
    try{
      if(history.state && history.state.viewer) history.back();
      else if(location.pathname.indexOf('/moments/')===0) history.replaceState({page:'moments'},'','/moments');
    }catch(e){}
  }
}
document.addEventListener('keydown',function(e){
  var v=$('viewerStage');
  if(e.key==='Escape' && v && !v.classList.contains('hidden')) closeViewer();
});

/* ===== Router: mỗi tab / ảnh có địa chỉ riêng ===== */
var ROUTES={dash:'/',upload:'/upload',moments:'/moments',friends:'/friends'};
var PAGE_TITLES={dash:'Locket Mini',upload:'Đăng khoảnh khắc · Locket Mini',moments:'Moments · Locket Mini',friends:'Bạn bè · Locket Mini'};
function routeFromPath(path){
  path=String(path||'/');
  while(path.length>1 && path.charAt(path.length-1)==='/') path=path.slice(0,-1);
  if(path==='/upload') return {page:'upload'};
  if(path==='/friends') return {page:'friends'};
  if(path==='/moments') return {page:'moments'};
  if(path.indexOf('/moments/')===0){
    var id=''; try{ id=decodeURIComponent(path.slice(9)); }catch(e){}
    if(/^[A-Za-z0-9_-]{1,64}$/.test(id)) return {page:'moments', moment:id};
    return {page:'moments'};
  }
  return {page:'dash'};
}
function setRouteTitle(name){ try{ document.title=PAGE_TITLES[name]||'Locket Mini'; }catch(e){} }
function pushRoute(path, state){
  try{
    if(location.pathname+location.search===path) return;
    history.pushState(state||{}, '', path);
  }catch(e){}
}
function showPage(name, opts){
  if(!ROUTES[name]) name='dash';
  var vs=$('viewerStage');
  if(vs && !vs.classList.contains('hidden')) closeViewer(true);
  _showPage(name);
  setRouteTitle(name);
  if(!(opts&&opts.noPush)) pushRoute(ROUTES[name], {page:name});
}
function openMomentById(id, tries){
  if(routeFromPath(location.pathname).moment!==id) return;
  var items=window._momentItems||[];
  for(var i=0;i<items.length;i++){
    if((items[i].canonical_uid||items[i].id)===id){ openViewer(i, true); return; }
  }
  tries=tries||0;
  if(tries<24) setTimeout(function(){ openMomentById(id, tries+1); }, 500);
  else { try{ history.replaceState({page:'moments'},'','/moments'); }catch(e){} }
}
function onPop(){
  var r=routeFromPath(location.pathname);
  var vs=$('viewerStage'), open=!!(vs && !vs.classList.contains('hidden'));
  if(r.page!==_curPage){ if(open){ closeViewer(true); open=false; } _showPage(r.page); setRouteTitle(r.page); }
  if(r.moment) openMomentById(r.moment, 0);
  else if(open) closeViewer(true);
}
function initRouter(){
  var r=routeFromPath(location.pathname);
  var canon=r.moment?('/moments/'+encodeURIComponent(r.moment)):ROUTES[r.page];
  var search=(r.page==='friends')?location.search:'';
  try{ history.replaceState({page:r.page}, '', canon+search); }catch(e){}
  if(r.page!=='dash') _showPage(r.page);
  setRouteTitle(r.page);
  if(r.moment) openMomentById(r.moment, 0);
  if(r.page==='friends' && search){
    try{
      var q=new URLSearchParams(search).get('q');
      if(q && $('peopleQuery')){ $('peopleQuery').value=q; searchPeople(); }
    }catch(e){}
  }
  window.addEventListener('popstate', onPop);
}

/* ===== Load-more không bao giờ đứng: bơm theo vị trí thật của sentinel ===== */
var _pumpRaf=0;
function pumpMore(){
  if(_pumpRaf) return;
  _pumpRaf=requestAnimationFrame(function(){ _pumpRaf=0; pumpMoreNow(); });
}
function sentinelNear(){
  var p=$('page-moments'), s=$('momentsMoreSentinel');
  if(!p||!s||!p.classList.contains('active')) return false;
  return s.getBoundingClientRect().top < (window.innerHeight||800)+600;
}
function pumpMoreNow(){
  if(_momentsBootLock){ setTimeout(pumpMore, 150); return; }
  if(!sentinelNear()) return;
  var items=window._momentItems||[];
  if(momentsRendered<items.length){ appendMomentsBatch(); return; }
  fetchMoreMoments(false);
}
function updateMoreLabel(){
  var more=$('momentsMore'); if(!more) return;
  var items=window._momentItems||[];
  if(!items.length){ more.classList.add('hidden'); return; }
  if(momentsRendered<items.length || _momentsMoreBusy){
    if(more.getAttribute('data-s')!=='busy'){ more.textContent='Đang tải thêm…'; more.setAttribute('data-s','busy'); }
    more.classList.remove('hidden'); return;
  }
  if(_momentsHasMore){
    if(more.getAttribute('data-s')!=='tap'){
      more.innerHTML='<button type="button" class="moments-empty-retry" onclick="forceMore();return false;">Tải thêm</button>';
      more.setAttribute('data-s','tap');
    }
    more.classList.remove('hidden'); return;
  }
  more.classList.add('hidden');
}
function forceMore(){
  _momentsHasMore=true; _momentsEmptyStreak=0;
  var items=window._momentItems||[];
  if(momentsRendered<items.length) appendMomentsBatch(); else fetchMoreMoments(false);
  pumpMore();
}
window.addEventListener('resize', function(){ pumpMore(); });
document.addEventListener('visibilitychange', function(){ if(!document.hidden) pumpMore(); });
function timeAgoShort(sec){
  if(!sec) return '';
  var d=Math.max(0, Math.floor(Date.now()/1000 - sec));
  if(d<60) return 'now';
  if(d<3600) return Math.floor(d/60)+'m';
  if(d<86400) return Math.floor(d/3600)+'h';
  if(d<2592000) return Math.floor(d/86400)+'d';
  return Math.floor(d/2592000)+'mo';
}
function viewerRow(v){
  var name=((v.first_name||'')+' '+(v.last_name||'')).trim()||v.username||v.uid||'';
  return '<div class="viewer-row">'+renderAvatar(v,36)+
    '<div class="viewer-info"><div class="viewer-name">'+esc(name)+'</div>'+
    (v.username?'<div class="viewer-user">@'+esc(v.username)+'</div>':'')+'</div>'+
    (v.reaction?'<span class="viewer-reaction">'+esc(v.reaction)+'</span>':'')+
    '<span class="viewer-time mtime"'+(v.viewed_at?' data-ts="'+v.viewed_at+'"':'')+'>'+(v.viewed_at?timeAgo(v.viewed_at):'')+'</span></div>';
}


/* ===================== Upload: pick / paste / crop ===================== */
function triggerFilePick(){$('fileInput').click()}
function triggerCamera(){const c=$('cameraInput'); if(c) c.click(); else triggerFilePick()}
function onFilePick(e){const f=e.target.files[0];if(f)handleIncomingFile(f); e.target.value=''}
document.addEventListener('paste',(e)=>{
  if(!$('page-upload')||!$('page-upload').classList.contains('active'))return;
  const items=(e.clipboardData||{}).items||[];
  for(const it of items){
    if(it.type&&it.type.startsWith('image/')){
      const f=it.getAsFile(); if(f){handleIncomingFile(f);e.preventDefault();break}
    }
  }
});
/* ===== Ảnh: thu nhỏ từng bước (sắc nét) + encode một lần, tốt nhất trong ngân sách ===== */
function drawSquareHQ(src, sx, sy, sw, sh, size){
  var cur=src, cx=sx, cy=sy, cs=Math.min(sw, sh);
  if(!LOW_POWER){
    while(cs>=size*2){
      var half=Math.max(size, Math.floor(cs/2));
      var t=document.createElement('canvas'); t.width=half; t.height=half;
      var tc=t.getContext('2d');
      tc.imageSmoothingEnabled=true;
      try{ tc.imageSmoothingQuality='high'; }catch(e){}
      tc.drawImage(cur, cx, cy, cs, cs, 0, 0, half, half);
      cur=t; cx=0; cy=0; cs=half;
    }
  }
  var out=document.createElement('canvas'); out.width=size; out.height=size;
  var oc=out.getContext('2d');
  oc.imageSmoothingEnabled=true;
  try{ oc.imageSmoothingQuality='high'; }catch(e){}
  oc.drawImage(cur, cx, cy, cs, cs, 0, 0, size, size);
  return out;
}
function encChain(canvas, type, qs, budget, cb){
  var i=0;
  (function step(){
    canvas.toBlob(function(b){
      if(!b || b.type!==type){ cb(null, false); return; }
      if(b.size<=budget){ cb(b, false); return; }
      if(++i>=qs.length){ cb(b, true); return; }
      step();
    }, type, qs[i]);
  })();
}
/* PNG (lossless) cho ảnh chụp màn hình nếu đủ nhỏ -> WebP 0.96+ -> JPEG. Server không encode lại ảnh đã đẹp. */
function bestBlob(canvas, cb){
  var BUDGET=2800000;
  function viaJpeg(){ encChain(canvas, 'image/jpeg', [0.95,0.9,0.85,0.8], BUDGET, function(b){ cb(b); }); }
  function viaWebp(){
    encChain(canvas, 'image/webp', [0.96,0.92,0.88,0.84], BUDGET, function(b, over){
      if(b && !over) cb(b); else viaJpeg();
    });
  }
  if(LOW_POWER){ canvas.toBlob(cb, 'image/jpeg', CAPTURE_JPEG_Q); return; }
  if(_bestPng){
    canvas.toBlob(function(p){
      if(p && p.type==='image/png' && p.size<=BUDGET) cb(p); else viaWebp();
    }, 'image/png');
  } else viaWebp();
}

function handleIncomingFile(f){
  _bestPng=/png/i.test((f&&f.type)||'');
  originalFile=f; isVideo=f.type.startsWith('video');
  const url=URL.createObjectURL(f);
  if(isVideo){
    openVideoCropStage(f, function(cropJson, blob){ finalizeVideoSelection(blob, cropJson, f.name); });
    return;
  }
  // If the picked photo is already a perfect square, skip the crop step
  // entirely — just resize/compress it to CAPTURE_SIZE and go straight to
  // caption. Cropper.js still runs for anything that actually needs cropping.
  const probe=new Image();
  probe.onload=function(){
    if(probe.naturalWidth>0 && probe.naturalWidth===probe.naturalHeight){
      const c=drawSquareHQ(probe, 0, 0, probe.naturalWidth, probe.naturalHeight, CAPTURE_SIZE);
      bestBlob(c, function(b){
        croppedBlob=b;
        $('previewImg').src=URL.createObjectURL(b);
        $('previewImg').classList.remove('hidden');
        $('previewVid').classList.add('hidden');
        $('uploadPlaceholder').classList.add('hidden');
        $('uploadActions').classList.remove('hidden');
        if($('queueOnlyBtn')) $('queueOnlyBtn').classList.remove('hidden');
      }, 'image/jpeg', CAPTURE_JPEG_Q);
      return;
    }
    openCropStage(url);
  };
  probe.onerror=function(){ openCropStage(url); };
  probe.src=url;
}
function finalizeVideoSelection(blob, cropJson, filename){
  isVideo=true;
  if(!originalFile) originalFile={name: filename || ('video_'+Date.now()+'.mp4')};
  croppedBlob=blob;
  originalVideoBlob=blob;
  videoSpeedFactor=1;
  videoCropPayload=cropJson; videoCropOriginal=cropJson; videoThumbBlob=null;
  // Keep prior thumb if caller already captured one (live record); else null until loadeddata/refresh
  if(!videoThumbBlob) videoThumbBlob=null;
  _speedRenderToken++;
  setSpeedBusy(false);
  $('previewImg').classList.add('hidden');
  $('previewVid').classList.remove('hidden');
  var pv=$('previewVid');
  if(pv._u){ try{ URL.revokeObjectURL(pv._u); }catch(e){} }
  pv._u=URL.createObjectURL(blob);
  pv.src=pv._u;
  applyPreviewCrop(cropJson);
  try{ pv.dataset.baked = '0'; }catch(e){}
  try{ pv.playbackRate = 1; }catch(e){}
  $('uploadPlaceholder').classList.add('hidden');
  $('uploadActions').classList.remove('hidden');
  if($('queueOnlyBtn')) $('queueOnlyBtn').classList.remove('hidden');
  initVideoSpeedSlider();
  setSpeedLabel(1);
  updateSpeedChipUI(1);
  syncSpeedRange(1);
  // Always grab a still from the finalized clip (full square, not black)
  refreshVideoThumb();
}

/* ---- Đăng không crop: giữ nguyên ảnh, lấp khung 1:1 bằng nền đen hoặc nền mờ ---- */
var cropMode='crop';
try{ var _cm=localStorage.getItem('lm_cropmode'); if(_cm==='black'||_cm==='blur') cropMode=_cm; }catch(e){}
function composeFit(img, mode, S){
  var w=img.naturalWidth||img.width, h=img.naturalHeight||img.height;
  var f=Math.min(S/w, S/h);
  var dw=Math.max(1,Math.round(w*f)), dh=Math.max(1,Math.round(h*f));
  // 1) thu nhỏ ảnh về đúng kích thước hiển thị (giảm nửa nhiều bước cho nét)
  var cur=img, cw=w, ch=h;
  while(!LOW_POWER && cw/2>=dw && ch/2>=dh){
    var nw=Math.floor(cw/2), nh=Math.floor(ch/2);
    var t=document.createElement('canvas'); t.width=nw; t.height=nh;
    var tc=t.getContext('2d'); tc.imageSmoothingEnabled=true;
    try{ tc.imageSmoothingQuality='high'; }catch(e){}
    tc.drawImage(cur,0,0,cw,ch,0,0,nw,nh);
    cur=t; cw=nw; ch=nh;
  }
  var small=document.createElement('canvas'); small.width=dw; small.height=dh;
  var sc=small.getContext('2d'); sc.imageSmoothingEnabled=true;
  try{ sc.imageSmoothingQuality='high'; }catch(e){}
  sc.drawImage(cur,0,0,cw,ch,0,0,dw,dh);
  // 2) khung vuông
  var out=document.createElement('canvas'); out.width=S; out.height=S;
  var c=out.getContext('2d');
  c.fillStyle='#000'; c.fillRect(0,0,S,S);
  if(mode==='blur' && (dw<S || dh<S)){
    // nền mờ dạng gradient: lấy vùng giữa ảnh, thu cực nhỏ rồi phóng to mượt (chạy được cả Safari)
    var side=Math.min(dw,dh), sx=Math.floor((dw-side)/2), sy=Math.floor((dh-side)/2);
    var tiny=document.createElement('canvas'); tiny.width=tiny.height=16;
    var tn=tiny.getContext('2d'); tn.imageSmoothingEnabled=true;
    try{ tn.imageSmoothingQuality='high'; }catch(e){}
    tn.drawImage(small,sx,sy,side,side,0,0,16,16);
    var mid=document.createElement('canvas'); mid.width=mid.height=Math.max(64,Math.round(S/8));
    var md=mid.getContext('2d'); md.imageSmoothingEnabled=true;
    try{ md.imageSmoothingQuality='high'; }catch(e){}
    md.drawImage(tiny,0,0,mid.width,mid.height);
    c.imageSmoothingEnabled=true;
    try{ c.imageSmoothingQuality='high'; }catch(e){}
    c.drawImage(mid,0,0,S,S);
    c.fillStyle='rgba(0,0,0,.30)'; c.fillRect(0,0,S,S);
  }
  c.drawImage(small,Math.round((S-dw)/2),Math.round((S-dh)/2));
  return out;
}
function setCropMode(m){
  cropMode=m;
  try{ localStorage.setItem('lm_cropmode', m); }catch(e){}
  var seg=$('fitSeg');
  if(seg) Array.prototype.forEach.call(seg.querySelectorAll('button'), function(b){
    b.classList.toggle('on', b.getAttribute('data-m')===m);
  });
  var cont=document.querySelector('#cropStage .cropper-container');
  var pv=$('fitPreview'), area=$('cropArea'), hint=$('cropHint');
  if(m==='crop'){
    if(pv) pv.style.display='none';
    if(cont) cont.style.display='';
    try{ if(cropper) cropper.resize(); }catch(e){}
    if(hint) hint.textContent='Kéo, phóng to/nhỏ để căn ảnh vuông';
    return;
  }
  if(cont) cont.style.display='none';
  if(hint) hint.textContent = m==='black' ? 'Giữ nguyên toàn bộ ảnh, phần thừa tô đen' : 'Giữ nguyên toàn bộ ảnh, phần thừa là nền mờ cùng màu ảnh';
  if(!pv || !area) return;
  var side=Math.max(120, Math.min(area.clientWidth, area.clientHeight));
  pv.style.width=side+'px'; pv.style.height=side+'px';
  pv.style.display='block';
  var src=composeFit($('cropImg'), m, 640);
  pv.getContext('2d').drawImage(src,0,0,640,640);
}

function openCropStage(url){
  $('cropImg').src=url;
  const stage=$('cropStage');
  stage.classList.add('open');
  document.body.style.overflow='hidden';
  if(cropper){cropper.destroy()}
  // delay init so layout is fullscreen first (iOS)
  setTimeout(()=>{
    cropper=new Cropper($('cropImg'),{
      aspectRatio:1,viewMode:1,autoCropArea:1,dragMode:'move',
      guides:true,center:true,highlight:false,background:false,responsive:true,toggleDragModeOnDblclick:false,
      ready:function(){ setCropMode(cropMode); },
    });
  },50);
}
function cancelCrop(){
  $('cropStage').classList.remove('open');
  document.body.style.overflow='';
  $('fileInput').value='';
  if($('cameraInput')) $('cameraInput').value='';
  if(!croppedBlob){$('uploadActions').classList.add('hidden')}
  if(cropper){cropper.destroy();cropper=null}
}
function confirmCrop(){
  if(!cropper)return;
  const btn=document.querySelector('#cropStage .crop-header button:last-child');
  if(btn){btn.disabled=true;btn.innerHTML='<span class="spinner light"></span>'}
  let canvas;
  if(cropMode!=='crop'){
    canvas=composeFit($('cropImg'), cropMode, CAPTURE_SIZE);
  } else {
    const rawCv=cropper.getCroppedCanvas(LOW_POWER?{width:CAPTURE_SIZE,height:CAPTURE_SIZE,imageSmoothingQuality:'high'}:{maxWidth:4096,maxHeight:4096,imageSmoothingQuality:'high'});
    canvas=LOW_POWER?rawCv:drawSquareHQ(rawCv, 0, 0, rawCv.width, rawCv.height, CAPTURE_SIZE);
  }
  bestBlob(canvas, b=>{
    croppedBlob=b;
    $('previewImg').src=URL.createObjectURL(b);
    $('previewImg').classList.remove('hidden');
    $('previewVid').classList.add('hidden');
    $('uploadPlaceholder').classList.add('hidden');
    $('uploadActions').classList.remove('hidden');
    if($('queueOnlyBtn')) $('queueOnlyBtn').classList.remove('hidden');
    $('cropStage').classList.remove('open');
    document.body.style.overflow='';
    if(btn){btn.disabled=false;btn.textContent='Xong'}
    cropper.destroy();cropper=null;
  },'image/jpeg',CAPTURE_JPEG_Q);
}
function clearUpload(){
  croppedBlob=null;originalFile=null;isVideo=false;videoCropPayload=null;videoThumbBlob=null;
  originalVideoBlob=null;videoSpeedFactor=1;
  $('fileInput').value='';
  if($('cameraInput')) $('cameraInput').value='';
  $('previewImg').classList.add('hidden');$('previewVid').classList.add('hidden');
  if($('videoCropRow')) $('videoCropRow').classList.add('hidden');
  if($('queueOnlyBtn')) $('queueOnlyBtn').classList.add('hidden');
  if(videoSpeedSliderObj) videoSpeedSliderObj.set(1);
  $('uploadActions').classList.add('hidden');
  openCapture();
}

/* ===================== Live camera (in-page, giống Locket gốc) ===================== */
let lcStream=null, lcTrack=null, lcFacing='environment', lcTorchOn=false, lcWantActive=false, lcStarting=false;
let lcZoomCaps=null, lcZoomCurrent=1, lcPinchStartDist=0, lcPinchStartZoom=1;
let lcExposureCaps=null;
var _lcTouchStartPt=null, _lcTouchMoved=false, _lcLastTouchTime=0;
let lcCaptureMode='photo', lcRecorder=null, lcRecordedChunks=[], lcRecordTimer=null, lcRecordStartTs=0;
const LC_MAX_RECORD_MS=15000;

function isLiveCamera(){
  try{
    var v = localStorage.getItem('locket_live_camera');
    // Default ON: in-page camera stays in memory (no Gallery save from system Camera app)
    if(v === null || v === undefined || v === '') return true;
    return v === '1';
  }catch(e){ return true; }
}
function toggleLiveCamera(on){
  try{ localStorage.setItem('locket_live_camera', on?'1':'0'); }catch(e){}
  if(!croppedBlob) openCapture();
  if(on) setTimeout(function(){ scrollUploadCameraIntoView(); }, 80);
}
/** Force .lc-frame to true 1:1 at full parent width — iPhone 6 / iOS 12 safe.
 *  Always measure #liveCam (parent), NEVER the frame itself — after the first
 *  shrink, frame.clientWidth would be the shrunk value and lock us at ~220px.
 */
function _sizeLcFrame(){
  try{
    var cam = $('liveCam');
    var frame = cam && cam.querySelector ? cam.querySelector('.lc-frame') : document.querySelector('#liveCam .lc-frame');
    if(!frame || !cam || cam.classList.contains('hidden')) return;
    // The square shape itself comes ONLY from CSS (.lc-frame's padding-bottom:100%
    // trick — width:100%, height:0, padding-bottom:100% always makes height equal
    // width, no JS needed, works on every browser back to IE6). This used to also
    // force explicit pixel width/height here and disable that CSS rule via a
    // '.sized' class, but on at least one iPhone 6 that pixel math produced a
    // portrait (not square) box — the CSS-only version doesn't have that failure
    // mode, so this function now only re-runs the video cover-crop below.
    _coverLcVideo();
  }catch(e){}
}
/** Manually crop-to-cover the live video into its (always-square, CSS-sized)
 *  frame using explicit pixel geometry instead of CSS object-fit. Old WebKit
 *  (iOS 12 / iPhone 6) has a history of getting object-fit:cover wrong on
 *  <video> inside a transformed/absolutely-positioned ancestor — the frame
 *  itself stays square (that part is CSS, not this function's job), but the
 *  video paints at its native aspect, making the picture look letterboxed
 *  rather than a true square crop. Computing width/height/left/top by hand
 *  sidesteps that: this works identically on every browser, old or new.
 */
function _coverLcVideo(){
  try{
    var v=$('lcVideo');
    var frame=document.querySelector('#liveCam .lc-frame');
    if(!v || !frame) return;
    var vw=v.videoWidth, vh=v.videoHeight;
    var fw=frame.clientWidth||frame.offsetWidth||0, fh=frame.clientHeight||frame.offsetHeight||0;
    if(!vw || !vh || !fw || !fh) return;
    var scale=Math.max(fw/vw, fh/vh);
    var w=Math.ceil(vw*scale), h=Math.ceil(vh*scale);
    v.style.position='absolute';
    v.style.width=w+'px';
    v.style.height=h+'px';
    v.style.left=Math.round((fw-w)/2)+'px';
    v.style.top=Math.round((fh-h)/2)+'px';
    v.style.right='auto';
    v.style.bottom='auto';
  }catch(e){}
}
function scrollUploadCameraIntoView(){
  try{
    _sizeLcFrame();
    var target = $('liveCam');
    if(!target || target.classList.contains('hidden')) target = $('previewBox');
    if(!target) return;
    // Prefer centering the square in the visible area above the bottom nav
    var rect = target.getBoundingClientRect();
    var navH = 64 + 8;
    var viewH = (window.innerHeight || document.documentElement.clientHeight) - navH;
    var idealTop = Math.max(0, (viewH - rect.height) / 2);
    var delta = rect.top - idealTop;
    if(Math.abs(delta) > 12){
      var y = (window.pageYOffset || document.documentElement.scrollTop || 0) + delta;
      try{ window.scrollTo(0, Math.max(0, y)); }catch(e){}
      try{ document.documentElement.scrollTop = Math.max(0, y); }catch(e){}
      try{ document.body.scrollTop = Math.max(0, y); }catch(e){}
    }
  }catch(e){}
}
// Decide what the "upload spot" should show right now: live camera, file-pick box, or leave the
// existing preview/caption alone (a photo is already captured and waiting to be sent).
function openCapture(){
  if(croppedBlob){
    $('previewImg').classList.add('hidden');$('previewVid').classList.add('hidden');
    if($('videoCropRow')) $('videoCropRow').classList.add('hidden');
    if($('queueOnlyBtn')) $('queueOnlyBtn').classList.add('hidden');
    $('uploadActions').classList.add('hidden');
    croppedBlob=null;originalFile=null;isVideo=false;videoThumbBlob=null;videoCropPayload=null;
    originalVideoBlob=null;videoSpeedFactor=1;
    if(videoSpeedSliderObj) videoSpeedSliderObj.set(1);
  }
  if(isLiveCamera()){
    $('previewBox').classList.add('hidden');
    $('uploadPlaceholder').classList.add('hidden');
    $('liveCam').classList.remove('hidden');
    startLiveCamera();
    setTimeout(function(){ scrollUploadCameraIntoView(); }, 60);
  } else {
    stopLiveCamera();
    $('liveCam').classList.add('hidden');
    $('previewBox').classList.remove('hidden');
    $('uploadPlaceholder').classList.remove('hidden');
    triggerFilePick();
  }
}
function stopLiveCamera(){
  lcWantActive=false;
  if(lcStream){ lcStream.getTracks().forEach(t=>t.stop()); lcStream=null; }
  lcTrack=null; lcTorchOn=false; lcZoomCaps=null; lcZoomCurrent=1; lcExposureCaps=null;
  lcStopRecording(true);
  lcCaptureMode='photo';
  const fb=$('lcFlashBtn'); if(fb){ fb.classList.remove('on'); fb.classList.add('hidden'); }
  const zr=$('lcZoomRow'); if(zr){ zr.classList.remove('show'); zr.innerHTML=''; }
  const ec=$('lcExposureCol'); if(ec) ec.classList.remove('show');
  const mr=$('lcModeRow'); if(mr) mr.classList.add('hidden');
  const v=$('lcVideo'); if(v) v.srcObject=null;
}
function startLiveCamera(){
  if(lcStarting) return;
  stopLiveCamera();
  lcWantActive=true; lcStarting=true;
  const hint=$('lcHint'); if(hint) hint.classList.remove('hidden');
  _sizeLcFrame();

  // Prefer exact facing + highest practical resolution. Many mobile browsers
  // still downscale the preview track — we re-apply max from capabilities after open.
  var constraints = {
    audio:false,
    video:{
      facingMode: { exact: lcFacing },
      width: { ideal: LC_WIDTH_IDEAL },
      height: { ideal: LC_HEIGHT_IDEAL },
      frameRate: { ideal: LC_FRAMERATE_IDEAL }
    }
  };

  function openCam(c){
    return navigator.mediaDevices.getUserMedia(c);
  }

  openCam(constraints).catch(function(){
    // exact facingMode often fails on desktop / some Android — fall back to ideal
    return openCam({
      audio:false,
      video:{
        facingMode: { ideal: lcFacing },
        width: { ideal: LC_WIDTH_IDEAL },
        height: { ideal: LC_HEIGHT_IDEAL },
        frameRate: { ideal: LC_FRAMERATE_IDEAL }
      }
    });
  }).catch(function(){
    return openCam({
      audio:false,
      video:{ facingMode: lcFacing, width: { ideal: LOW_POWER ? 640 : 1280 }, height: { ideal: LOW_POWER ? 640 : 720 } }
    });
  }).then(function(s){
    lcStarting=false;
    if(!lcWantActive){ s.getTracks().forEach(function(t){t.stop()}); return; }
    lcStream=s; lcTrack=s.getVideoTracks()[0];
    const v=$('lcVideo');
    try{ v.setAttribute('playsinline',''); v.setAttribute('webkit-playsinline',''); v.muted=true; v.playsInline=true; }catch(e){}
    v.srcObject=s;
    v.classList.toggle('mirrored', lcFacing==='user');
    var tryPlay = function(){ try{ var p=v.play(); if(p&&p.catch) p.catch(function(){}); }catch(e){} };
    tryPlay();
    v.onloadedmetadata = function(){ tryPlay(); _sizeLcFrame(); _coverLcVideo(); scrollUploadCameraIntoView(); };
    if(hint) hint.classList.add('hidden');
    _sizeLcFrame();
    setTimeout(function(){ _sizeLcFrame(); _coverLcVideo(); scrollUploadCameraIntoView(); }, 120);

    try{
      const caps=lcTrack.getCapabilities && lcTrack.getCapabilities();
      const settings=lcTrack.getSettings && lcTrack.getSettings();
      const fb=$('lcFlashBtn');
      if(fb) fb.classList.toggle('hidden', !(caps && caps.torch));

      // Push track to sensor max when browser allows (reduces soft upscale on front cam).
      // Skipped entirely in low-power mode — the smaller ideal size above is the point.
      if(!LOW_POWER && caps && caps.width && caps.height && typeof caps.width.max==='number'){
        var wantW = Math.min(caps.width.max, 1920);
        var wantH = Math.min(caps.height.max, 1080);
        var curW = (settings && settings.width) || 0;
        var curH = (settings && settings.height) || 0;
        if(wantW > curW || wantH > curH){
          lcTrack.applyConstraints({
            width: { ideal: wantW },
            height: { ideal: wantH }
          }).catch(function(){});
        }
      }

      // Continuous AF when available (Chrome/Android). iOS WebKit ignores this — OS AF stays on.
      if(caps && caps.focusMode){
        var modes = Array.isArray(caps.focusMode) ? caps.focusMode : [caps.focusMode];
        var want = modes.indexOf('continuous') >= 0 ? 'continuous'
                  : modes.indexOf('single-shot') >= 0 ? 'single-shot' : null;
        if(want){
          lcTrack.applyConstraints({ advanced: [{ focusMode: want }] }).catch(function(){
            try{ lcTrack.applyConstraints({ focusMode: want }); }catch(e2){}
          });
        }
      }

      setupLcZoom(caps);
      setupLcExposure(caps);
      setupLcModeSwitch();
    }catch(e){}
  }).catch(function(err){
    lcStarting=false;
    if(hint) hint.classList.add('hidden');
    toast('Không mở được camera: '+(err&&err.message?err.message:'bị từ chối quyền'));
    $('liveCameraSwitch').checked=false;
    toggleLiveCamera(false);
  });
}
function flipLiveCamera(e){
  if(e)e.preventDefault();
  if(lcRecorder && lcRecorder.state==='recording'){ toast('Không đổi camera được khi đang quay'); return; }
  lcFacing = lcFacing==='environment' ? 'user' : 'environment';
  startLiveCamera();
}
function toggleTorch(e){
  if(e)e.preventDefault();
  if(!lcTrack) return;
  lcTorchOn=!lcTorchOn;
  $('lcFlashBtn').classList.toggle('on', lcTorchOn);
  try{ lcTrack.applyConstraints({advanced:[{torch:lcTorchOn}]}); }catch(err){}
}
/* Zoom: browser zoom units = FOV relative to main lens on most Android Chrome builds
   (0.5 ultrawide, 1.0 main, 2.0 tele). We ALWAYS snap open to main (1.0) and verify
   via getSettings() — otherwise the stream stays on ultrawide while UI says "1x". */
function setupLcZoom(caps){
  const row=$('lcZoomRow');
  lcZoomCaps=null; lcZoomCurrent=1;
  if(row){ row.classList.remove('show'); row.innerHTML=''; }
  if(!caps || typeof caps.zoom!=='object' || caps.zoom===null) return;
  const min=caps.zoom.min, max=caps.zoom.max, step=caps.zoom.step||0.1;
  if(typeof min!=='number' || typeof max!=='number' || max<=min) return;
  // Front camera zoom is usually digital-only and misleading — skip picker
  if(lcFacing==='user') return;

  lcZoomCaps={min:min,max:max,step:step};

  // Main lens = zoom 1.0 when in range; else device default mid/min
  var main = (min <= 1 && max >= 1) ? 1 : (min <= 1.0 ? Math.min(max, Math.max(min, 1)) : min);

  // Discrete levels that match native Camera labels
  var levels=[];
  function addLevel(z){
    z = Math.min(max, Math.max(min, z));
    // snap to step grid lightly
    if(step > 0) z = Math.round(z / step) * step;
    z = Number(z.toFixed(2));
    if(levels.indexOf(z) === -1) levels.push(z);
  }
  if(min < 0.95) addLevel(min);          // ultrawide (0.5x)
  addLevel(main);                         // main (1x)
  if(max >= 1.8) addLevel(Math.min(max, 2));
  if(max >= 2.8) addLevel(Math.min(max, 3));

  if(row && levels.length >= 2){
    row.innerHTML = levels.map(function(z){
      var label;
      if(Math.abs(z - main) < 0.05) label = '1x';
      else if(z < main) label = (Math.round((z / main) * 10) / 10) + 'x';
      else label = (Math.abs(z - Math.round(z)) < 0.05 ? Math.round(z) : z) + 'x';
      // For ultrawide show 0.5x not 0.5 when main is 1
      if(z < main && min < 0.95 && Math.abs(z - min) < 0.08) label = '0.5x';
      return '<button type="button" class="lc-zoom-btn" data-z="'+z+'" onclick="setLcZoom('+z+');return false;">'+label+'</button>';
    }).join('');
    row.classList.add('show');
  }

  // Force main lens, then re-read actual zoom (apply can fail silently)
  applyLcZoom(main, true);
  setTimeout(function(){ syncLcZoomFromTrack(); }, 120);
  setTimeout(function(){ syncLcZoomFromTrack(); }, 400);
}
function syncLcZoomFromTrack(){
  if(!lcTrack || !lcTrack.getSettings) return;
  try{
    var st = lcTrack.getSettings();
    if(typeof st.zoom === 'number'){
      lcZoomCurrent = st.zoom;
      updateLcZoomButtons(st.zoom);
    }
  }catch(e){}
}
function updateLcZoomButtons(actual){
  const row=$('lcZoomRow');
  if(!row) return;
  var btns=row.querySelectorAll('.lc-zoom-btn');
  var best=-1, bestDist=1e9;
  for(var i=0;i<btns.length;i++){
    var z=parseFloat(btns[i].getAttribute('data-z'));
    var d=Math.abs(z-actual);
    if(d<bestDist){ bestDist=d; best=i; }
  }
  for(var j=0;j<btns.length;j++) btns[j].classList.toggle('active', j===best);
}
function applyLcZoom(z, updateUI){
  if(!lcTrack || !lcZoomCaps) return;
  const clamped=Math.min(Math.max(Number(z), lcZoomCaps.min), lcZoomCaps.max);
  lcZoomCurrent=clamped;
  // Prefer plain constraint (Chrome); fall back to advanced (older)
  var applied=false;
  try{
    var p = lcTrack.applyConstraints({ zoom: clamped });
    if(p && p.then){
      p.then(function(){ applied=true; syncLcZoomFromTrack(); })
       .catch(function(){
         lcTrack.applyConstraints({ advanced: [{ zoom: clamped }] })
           .then(function(){ syncLcZoomFromTrack(); })
           .catch(function(){ syncLcZoomFromTrack(); });
       });
    } else {
      applied=true;
    }
  }catch(err){
    try{ lcTrack.applyConstraints({ advanced: [{ zoom: clamped }] }); }catch(e2){}
  }
  if(updateUI!==false) updateLcZoomButtons(clamped);
  // Verify after hardware settles
  setTimeout(function(){ syncLcZoomFromTrack(); }, 180);
}
function setLcZoom(z){ applyLcZoom(z, true); }
function setupLcExposure(caps){
  // Intentionally disabled — vertical range control looked broken and rarely worked on iOS/WebKit.
  lcExposureCaps=null;
  var col=$('lcExposureCol');
  if(col) col.classList.remove('show');
}
function onLcExposureInput(v){ /* no-op */ }
/* Tap-to-focus — shows the same focus square/pulse as a native camera app on every tap,
   and additionally nudges the real focus point when the browser exposes that control
   (mainly Chrome/Android; WebKit doesn't support it, so on iPhone this is visual-only,
   same as iOS's own camera already auto-focusing continuously in the background). */
function lcDoFocusAt(clientX, clientY){
  var frame=document.querySelector('#liveCam .lc-frame');
  if(!frame) return;
  var rect=frame.getBoundingClientRect();
  if(!rect.width || !rect.height) return;
  var relX=Math.min(Math.max((clientX-rect.left)/rect.width,0),1);
  var relY=Math.min(Math.max((clientY-rect.top)/rect.height,0),1);
  showLcFocusRing(clientX-rect.left, clientY-rect.top);

  if(!lcTrack) return;
  try{
    var caps=lcTrack.getCapabilities && lcTrack.getCapabilities();
    if(!caps) return;

    var advanced=[];
    // Ưu tiên: focusMode + pointsOfInterest (Chrome/Android)
    if(caps.focusMode){
      var modes=Array.isArray(caps.focusMode)?caps.focusMode:[caps.focusMode];
      var mode = modes.indexOf('single-shot')>=0 ? 'single-shot'
               : modes.indexOf('continuous')>=0 ? 'continuous' : null;
      if(mode) advanced.push({focusMode: mode});
    }
    if(caps.pointsOfInterest){
      advanced.push({pointsOfInterest: [{x:relX, y:relY}]} );
    }
    // Fallback: exposure compensation theo vùng chạm (nếu có)
    if(caps.exposureCompensation){
      advanced.push({exposureCompensation: 0});
    }

    if(advanced.length){
      lcTrack.applyConstraints({advanced: advanced}).catch(function(){});
    }
  }catch(e){}
}
function showLcFocusRing(x,y){
  var frame=document.querySelector('#liveCam .lc-frame');
  if(!frame) return;
  var ring=frame.querySelector('.lc-focus-ring');
  if(!ring){
    ring=document.createElement('div');
    ring.className='lc-focus-ring';
    frame.appendChild(ring);
  }
  ring.style.left=x+'px'; ring.style.top=y+'px';
  ring.classList.remove('pulse');
  void ring.offsetWidth; // restart the CSS animation
  ring.classList.add('pulse');
}
/* Pinch-to-zoom (two fingers) and tap-to-focus (one finger, minimal movement) share the
   same touch sequence, so they're disambiguated here by touch count + movement threshold.
   Both branches bail out cheaply when the device doesn't support the underlying capability. */
function lcTouchDist(t){
  var dx=t[0].clientX-t[1].clientX, dy=t[0].clientY-t[1].clientY;
  return Math.sqrt(dx*dx+dy*dy);
}
function lcPinchStart(e){
  if(!e.touches) return;
  if(e.touches.length===2 && lcZoomCaps){
    lcPinchStartDist=lcTouchDist(e.touches);
    lcPinchStartZoom=lcZoomCurrent;
    _lcTouchStartPt=null;
  } else if(e.touches.length===1){
    var t=e.target;
    if(t.closest && t.closest('button,input,.lc-zoom-row,.lc-exposure-col')){
      _lcTouchStartPt=null;
      return;
    }
    _lcTouchStartPt={x:e.touches[0].clientX, y:e.touches[0].clientY};
    _lcTouchMoved=false;
  }
}
function lcPinchMove(e){
  if(!e.touches) return;
  if(e.touches.length===2 && lcZoomCaps && lcPinchStartDist){
    e.preventDefault();
    var ratio=lcTouchDist(e.touches)/lcPinchStartDist;
    applyLcZoom(lcPinchStartZoom*ratio, true);
    return;
  }
  if(e.touches.length===1 && _lcTouchStartPt){
    var dx=e.touches[0].clientX-_lcTouchStartPt.x, dy=e.touches[0].clientY-_lcTouchStartPt.y;
    if(Math.sqrt(dx*dx+dy*dy) > 12) _lcTouchMoved=true;
  }
}
function lcPinchEnd(e){
  lcPinchStartDist=0;
  _lcLastTouchTime=Date.now();
  if(_lcTouchStartPt && !_lcTouchMoved){
    lcDoFocusAt(_lcTouchStartPt.x, _lcTouchStartPt.y);
  }
  _lcTouchStartPt=null; _lcTouchMoved=false;
}
function lcFrameClick(e){
  // Ignore the synthetic click that follows a touch tap — already handled in lcPinchEnd.
  if(Date.now() - _lcLastTouchTime < 600) return;
  // Ignore taps on the flash/zoom/exposure controls that live inside the frame —
  // those already have their own handlers and shouldn't also trigger a focus tap.
  if(e.target.closest && e.target.closest('button,input,.lc-zoom-row,.lc-exposure-col')) return;
  lcDoFocusAt(e.clientX, e.clientY);
}
function finishLiveStill(blob){
  var btn=$('lcShutterBtn'); if(btn) btn.disabled=false;
  if(!blob){ toast('Không chụp được ảnh, thử lại'); return; }
  croppedBlob=blob; isVideo=false; originalFile=null;
  $('previewImg').src=URL.createObjectURL(blob);
  $('previewImg').classList.remove('hidden');
  $('previewVid').classList.add('hidden');
  $('uploadActions').classList.remove('hidden');
  if($('queueOnlyBtn')) $('queueOnlyBtn').classList.remove('hidden');
  stopLiveCamera();
  $('liveCam').classList.add('hidden');
  $('previewBox').classList.remove('hidden');
}
function canvasCaptureFromVideo(v){
  /* Crop the SAME region object-fit:cover shows in the square frame — WYSIWYG.
     Square frame + cover ⇒ center crop of the shorter video axis. */
  var vw=v.videoWidth, vh=v.videoHeight;
  if(!vw || !vh) return null;
  var size=Math.min(vw,vh);
  var sx=(vw-size)/2, sy=(vh-size)/2;
  var out=document.createElement('canvas');
  out.width=CAPTURE_SIZE; out.height=CAPTURE_SIZE;
  var ctx=out.getContext('2d');
  if(lcFacing==='user'){ ctx.translate(CAPTURE_SIZE,0); ctx.scale(-1,1); }
  ctx.imageSmoothingEnabled=true;
  try{ ctx.imageSmoothingQuality='high'; }catch(e){}
  ctx.drawImage(v, sx, sy, size, size, 0, 0, CAPTURE_SIZE, CAPTURE_SIZE);
  return out;
}
function captureLivePhoto(e){
  if(e)e.preventDefault();
  _bestPng=false;
  const v=$('lcVideo');
  if(!v || !v.videoWidth) return;
  const btn=$('lcShutterBtn'); btn.disabled=true;
  const overlay=$('lcFlashOverlay');
  if(overlay){ overlay.classList.add('fire'); setTimeout(function(){ overlay.classList.remove('fire'); },120); }

  if(lcTrack && lcTrack.applyConstraints){
    try{
      lcTrack.applyConstraints({ advanced: [{ focusMode: 'single-shot' }] }).catch(function(){
        try{ lcTrack.applyConstraints({ focusMode: 'single-shot' }); }catch(e2){}
      });
    }catch(e){}
  }

  // Always capture from the live video element so the photo matches what the user sees.
  // ImageCapture.takePhoto() often returns a different FOV/lens than the preview track
  // (especially with multi-cam zoom), which caused "preview one way, photo another".
  var settleMs = 160;
  setTimeout(function(){
    try{
      // Prefer grabFrame when available — same track frames as preview, full res
      if(window.ImageCapture && lcTrack){
        try{
          var ic = new ImageCapture(lcTrack);
          if(ic.grabFrame){
            ic.grabFrame().then(function(bmp){
              var side=Math.min(bmp.width, bmp.height);
              var sx=(bmp.width-side)/2, sy=(bmp.height-side)/2;
              var c=document.createElement('canvas');
              c.width=CAPTURE_SIZE; c.height=CAPTURE_SIZE;
              var ctx=c.getContext('2d');
              ctx.imageSmoothingEnabled=true;
              try{ ctx.imageSmoothingQuality='high'; }catch(e){}
              if(lcFacing==='user'){ ctx.translate(CAPTURE_SIZE,0); ctx.scale(-1,1); }
              ctx.drawImage(bmp, sx, sy, side, side, 0, 0, CAPTURE_SIZE, CAPTURE_SIZE);
              try{ bmp.close(); }catch(e){}
              bestBlob(c, function(b){ finishLiveStill(b); });
            }).catch(function(){ fromVideoEl(); });
            return;
          }
        }catch(e){}
      }
      fromVideoEl();
    }catch(err){
      btn.disabled=false;
      toast('Không chụp được ảnh');
    }
  }, settleMs);

  function fromVideoEl(){
    try{
      var canvas=canvasCaptureFromVideo(v);
      if(!canvas){ btn.disabled=false; toast('Camera chưa sẵn sàng'); return; }
      bestBlob(canvas, function(b){ finishLiveStill(b); });
    }catch(err){
      btn.disabled=false;
      toast('Không chụp được ảnh');
    }
  }
}

/* ===================== Live camera: Photo/Video mode switch ===================== */
// MediaRecorder only exists on Safari 14.3+/iOS 14.3+ — iPhone 6 tops out at iOS 12.5.7,
// so it never has it. Hide the Video mode entirely there instead of showing a button that
// would just fail — same "only offer what the device can actually do" approach as zoom/focus.
function setupLcModeSwitch(){
  const row=$('lcModeRow');
  if(!row) return;
  const supported = (typeof MediaRecorder!=='undefined');
  row.classList.toggle('hidden', !supported);
  setLcCaptureMode('photo');
}
function setLcCaptureMode(mode){
  if(lcRecorder && lcRecorder.state==='recording') return; // don't switch mid-recording
  lcCaptureMode = mode;
  const row=$('lcModeRow');
  if(row){
    var btns=row.querySelectorAll('.lc-mode-btn');
    for(var i=0;i<btns.length;i++) btns[i].classList.toggle('active', btns[i].getAttribute('data-mode')===mode);
  }
  const shutter=$('lcShutterBtn');
  if(shutter) shutter.setAttribute('aria-label', mode==='video' ? 'Quay video' : 'Chụp');
}
function onLcShutterTap(e){
  if(e)e.preventDefault();
  if(lcCaptureMode==='video'){
    if(lcRecorder && lcRecorder.state==='recording') lcStopRecording(false);
    else lcStartRecording();
  } else {
    captureLivePhoto(e);
  }
}
function pickLcMimeType(){
  var candidates=['video/mp4;codecs=avc1,mp4a', 'video/mp4', 'video/webm;codecs=vp9,opus', 'video/webm;codecs=vp8,opus', 'video/webm'];
  for(var i=0;i<candidates.length;i++){
    try{ if(MediaRecorder.isTypeSupported(candidates[i])) return candidates[i]; }catch(e){}
  }
  return '';
}
function lcStartRecording(){
  if(!lcStream || lcRecorder) return;
  var mime=pickLcMimeType();
  try{
    var _bps=LOW_POWER?1500000:4000000;
    lcRecorder = mime ? new MediaRecorder(lcStream, {mimeType:mime, videoBitsPerSecond:_bps}) : new MediaRecorder(lcStream, {videoBitsPerSecond:_bps});
  }catch(e){
    toast('Trình duyệt không hỗ trợ quay video'); return;
  }
  lcRecordedChunks=[];
  lcRecorder.ondataavailable=function(ev){ if(ev.data && ev.data.size) lcRecordedChunks.push(ev.data); };
  lcRecorder.onstop=function(){ finishVideoCapture(); };
  lcRecorder.start(250);
  lcRecordStartTs=Date.now();
  const shutter=$('lcShutterBtn'); if(shutter) shutter.classList.add('recording');
  const rt=$('lcRecordTime'); if(rt) rt.classList.add('show');
  lcRecordTimer=setInterval(lcTickRecordTimer, 200);
  lcTickRecordTimer();
}
function lcTickRecordTimer(){
  var ms=Date.now()-lcRecordStartTs;
  var s=Math.floor(ms/1000);
  var txt=$('lcRecordTimeText');
  if(txt) txt.textContent='0:'+(s<10?'0':'')+s;
  if(ms>=LC_MAX_RECORD_MS) lcStopRecording(false);
}
function lcStopRecording(discard){
  if(lcRecordTimer){ clearInterval(lcRecordTimer); lcRecordTimer=null; }
  const shutter=$('lcShutterBtn'); if(shutter) shutter.classList.remove('recording');
  const rt=$('lcRecordTime'); if(rt) rt.classList.remove('show');
  if(!lcRecorder) return;
  if(discard){
    try{ if(lcRecorder.state!=='inactive') lcRecorder.stop(); }catch(e){}
    lcRecordedChunks=[];
    lcRecorder=null;
    return;
  }
  try{
    if(lcRecorder.state!=='inactive') lcRecorder.stop();
  }catch(e){ lcRecorder=null; }
}
function finishVideoCapture(){
  var mime=(lcRecorder && lcRecorder.mimeType) || 'video/webm';
  var blob=new Blob(lcRecordedChunks, {type:mime});
  lcRecordedChunks=[];
  lcRecorder=null;
  if(!blob.size){ toast('Không quay được video, thử lại nhé'); return; }
  const ext = mime.indexOf('mp4')>=0 ? 'mp4' : 'webm';
  const filename='lc_video_'+Date.now()+'.'+ext;
  // Grab the thumb frame from the still-live feed before stopLiveCamera() tears
  // down the stream — drawImage() below captures synchronously, so this is safe
  // even though the actual JPEG encode (toBlob) finishes asynchronously after.
  stopLiveCamera();
  $('liveCam').classList.add('hidden');
  $('previewBox').classList.remove('hidden');
  originalFile={name:filename};
  openVideoCropStage(blob, function(cropJson, croppedBlobOut){
    finalizeVideoSelection(croppedBlobOut, cropJson, filename);
  });
}

/* ===================== Offline upload queue (IndexedDB) ===================== */
const QDB_NAME='locket_upload_queue_v1', QDB_STORE='jobs';
let _qdb=null, _queueFlushing=false;
function openQdb(){
  return new Promise((resolve,reject)=>{
    if(_qdb){resolve(_qdb);return}
    const req=indexedDB.open(QDB_NAME,1);
    req.onupgradeneeded=()=>{const db=req.result; if(!db.objectStoreNames.contains(QDB_STORE)) db.createObjectStore(QDB_STORE,{keyPath:'id'});};
    req.onsuccess=()=>{
      _qdb=req.result;
      _qdb.onclose=function(){ _qdb=null; };
      _qdb.onversionchange=function(){ try{ _qdb.close(); }catch(e){} _qdb=null; };
      resolve(_qdb);
    };
    req.onerror=()=>reject(req.error);
  });
}
function qWithRetry(run){
  return run().catch(err=>{
    // Safari/WKWebView can force-close an idle IndexedDB connection while the
    // tab is backgrounded; the cached handle then throws on the next use.
    // Drop it and reopen once instead of failing the whole upload.
    _qdb=null;
    return run();
  });
}
function qAll(){
  return qWithRetry(()=>openQdb().then(db=>new Promise((resolve,reject)=>{
    const tx=db.transaction(QDB_STORE,'readonly');
    const req=tx.objectStore(QDB_STORE).getAll();
    req.onsuccess=()=>resolve((req.result||[]).sort((a,b)=>a.created-b.created));
    req.onerror=()=>reject(req.error);
  })));
}
function qPut(job){
  return qWithRetry(()=>openQdb().then(db=>new Promise((resolve,reject)=>{
    const tx=db.transaction(QDB_STORE,'readwrite');
    tx.objectStore(QDB_STORE).put(job);
    tx.oncomplete=()=>resolve();
    tx.onerror=()=>reject(tx.error);
  })));
}
function qDel(id){
  return qWithRetry(()=>openQdb().then(db=>new Promise((resolve,reject)=>{
    const tx=db.transaction(QDB_STORE,'readwrite');
    tx.objectStore(QDB_STORE).delete(id);
    tx.oncomplete=()=>resolve();
    tx.onerror=()=>reject(tx.error);
  })));
}
function blobToDataUrl(blob){
  return new Promise((resolve,reject)=>{
    const r=new FileReader();
    r.onload=()=>resolve(r.result);
    r.onerror=()=>reject(r.error);
    r.readAsDataURL(blob);
  });
}
function dataUrlToBlob(dataUrl){
  const parts=String(dataUrl).split(',');
  const mime=(parts[0].match(/:(.*?);/)||[])[1]||'image/jpeg';
  const bin=atob(parts[1]);
  const arr=new Uint8Array(bin.length);
  for(let i=0;i<bin.length;i++) arr[i]=bin.charCodeAt(i);
  return new Blob([arr],{type:mime});
}
function statusLabel(st){
  if(st==='pending') return 'Chờ mạng…';
  if(st==='uploading') return 'Đang đăng…';
  if(st==='error') return 'Lỗi — sẽ thử lại';
  if(st==='done') return 'Đã đăng';
  return st||'';
}
async function renderQueue(){
  const box=$('queueBox'), list=$('queueList'), count=$('queueCount');
  if(!box) return;
  let jobs=[];
  try{ jobs=await qAll(); }catch(e){ console.warn(e); }
  // hide completed after a while conceptually — we delete on success
  if(!jobs.length){ box.classList.add('hidden'); return; }
  box.classList.remove('hidden');
  if(count) count.textContent=jobs.length;
  list.innerHTML='';
  jobs.forEach(j=>{
    const row=document.createElement('div');
    row.className='queue-item';
    const thumb=j.preview||j.thumbDataUrl||'';
    const busy = j.status==='uploading';
    row.innerHTML=`
      ${thumb?`<img src="${esc(thumb)}" alt="">`:`<div style="width:44px;height:44px;border-radius:10px;background:#222"></div>`}
      <div class="qi-info">
        <div class="qi-title">${esc(j.caption||'(không chú thích)')}</div>
        <div class="qi-status ${esc(j.status)}">${esc(statusLabel(j.status))}${j.error?': '+esc(j.error):''}</div>
      </div>
      <button type="button" class="qi-del" data-qid="${esc(j.id)}" title="Xóa khỏi hàng đợi" aria-label="Xóa khỏi hàng đợi" ${busy?'disabled':''}>×</button>`;
    list.appendChild(row);
  });
  // One delegated listener — avoid rebinding on every re-render
  if(!list._qiDelBound){
    list._qiDelBound=true;
    list.addEventListener('click', function(ev){
      var btn=ev.target.closest && ev.target.closest('.qi-del');
      if(!btn) return;
      ev.preventDefault();
      var id=btn.getAttribute('data-qid');
      if(!id) return;
      btn.disabled=true;
      removeQueueJob(id);
    });
  }
}
async function removeQueueJob(id){
  try{
    // Cancel in-flight mark so flushQueue will not restart it mid-delete
    _inFlightJobs.delete(id);
    await qDel(id);
    toast('Đã xóa khỏi hàng đợi');
    await renderQueue();
  }catch(e){
    console.warn(e);
    toast('Không xóa được');
    await renderQueue();
  }
}
async function enqueueUpload(blob, caption, filename, contentType){
  const id='j_'+Date.now()+'_'+Math.random().toString(36).slice(2,8);
  let preview='';
  try{ if(blob.type&&blob.type.startsWith('image')) preview=await blobToDataUrl(blob); }catch(e){}
  let dataUrl='';
  try{ dataUrl=await blobToDataUrl(blob); }catch(e){ toast('Không lưu được ảnh offline'); throw e; }
  const job={id, kind:'photo', created:Date.now(), caption:caption||'', filename, contentType, dataUrl, preview, status:'pending', error:''};
  await qPut(job);
  await renderQueue();
  return job;
}
/* Videos skip the base64 round trip entirely — IndexedDB's structured clone can
   store a Blob natively, so there's no atob() memory spike here the way there
   would be if this went through blobToDataUrl() like photos do. Only the (tiny)
   JPEG thumb gets base64'd, since that's cheap regardless. Old iOS (<14) has a
   history of silently corrupting Blobs stored this way, so this is skipped
   entirely in that case — caller falls back to direct-send-only. */
async function enqueueVideoUpload(blob, caption, filename, contentType, cropPayload, thumbBlob){
  if(OLD_IOS) throw new Error('blob-store-unsupported');
  filename=fixVideoName(filename, blob); contentType=(blob&&blob.type)||contentType;
  const id='j_'+Date.now()+'_'+Math.random().toString(36).slice(2,8);
  let thumbDataUrl='';
  if(thumbBlob){ try{ thumbDataUrl=await blobToDataUrl(thumbBlob); }catch(e){} }
  const job={id, kind:'video', created:Date.now(), caption:caption||'', filename, contentType,
             videoBlob:blob, cropPayload:cropPayload||'', thumbDataUrl, status:'pending', error:''};
  await qPut(job);
  await renderQueue();
  return job;
}
var QUEUE_BACKOFF_MS = [5000, 15000, 30000, 60000, 120000, 300000];
var _inFlightJobs = new Set();
async function postJob(job){
  if(_inFlightJobs.has(job.id)) return false;
  _inFlightJobs.add(job.id);
  try{
    if(navigator.locks && navigator.locks.request){
      return await navigator.locks.request('locket-job-'+job.id, {ifAvailable:true}, async lock=>{
        if(!lock) return false; // another tab already holds this job's lock
        return await postJobInner(job);
      });
    }
    return await postJobInner(job);
  }finally{
    _inFlightJobs.delete(job.id);
  }
}
async function postJobInner(job){
    job.status='uploading'; job.error='';
    await qPut(job); await renderQueue();
    const isVideoJob = job.kind==='video';
    const blob = isVideoJob ? job.videoBlob : dataUrlToBlob(job.dataUrl);
    const fd=new FormData();
    fd.append('file', blob, job.filename||(isVideoJob?'video.mp4':'moment.jpg'));
    fd.append('caption', job.caption||'');
    fd.append('client_id', job.id);
    if(isVideoJob && job.cropPayload) fd.append('cropPayload', job.cropPayload);
    if(isVideoJob && job.thumbDataUrl){
      try{ fd.append('thumb', dataUrlToBlob(job.thumbDataUrl), 'thumb.jpg'); }catch(e){}
    }
    try{
      const d=await apiTimeout('/api/upload',{method:'POST',body:fd}, isVideoJob?60000:25000);
      if(d&&d.ok){
        await qDel(job.id);
        momentsUpdatedAt=0;
        momentsLoaded=false;
        await renderQueue();
        return true;
      }
      job.attempts=(job.attempts||0)+1;
      job.status='error'; job.error=(d&&d.error)||'Đăng thất bại';
      job.nextTry=Date.now()+QUEUE_BACKOFF_MS[Math.min(job.attempts-1, QUEUE_BACKOFF_MS.length-1)];
      await qPut(job); await renderQueue();
      return false;
    }catch(err){
      job.status='pending'; job.error=''; job.nextTry=0;
      await qPut(job); await renderQueue();
      return false;
    }
}
async function flushQueue(){
  if(_queueFlushing) return;
  _queueFlushing=true;
  try{
    const jobs=await qAll();
    const now=Date.now();
    for(const j of jobs){
      if(j.status==='done'){ await qDel(j.id); continue; }
      if(_inFlightJobs.has(j.id)) continue;
      if(j.nextTry && j.nextTry>now) continue;
      const ok=await postJob(j);
      if(!ok && j.status==='error') continue;
    }
  }finally{
    _queueFlushing=false;
    await renderQueue();
  }
}
var _queueTicker=null;
function startQueueTicker(){
  if(_queueTicker) return;
  _queueTicker=setInterval(function(){
    if(document.visibilityState==='visible') flushQueue();
  }, 20000);
}
function updateOnlineUI(){
  const b=$('offlineBanner');
  if(b) b.classList.toggle('show', !navigator.onLine);
}
function toggleAutoCamera(on){
  try{ localStorage.setItem('locket_auto_camera', on?'1':'0'); }catch(e){}
  // If user turns it on while already on the site, jump to camera now
  if(on){
    setTimeout(function(){
      showPage('upload');
      if(!isLiveCamera()) setTimeout(function(){ triggerCamera(); }, 280);
      else setTimeout(function(){ scrollUploadCameraIntoView(); }, 200);
    }, 50);
  }
}
function isAutoCamera(){
  try{ return localStorage.getItem('locket_auto_camera')==='1'; }catch(e){ return false; }
}
function toggleLowPower(on){
  setLowPower(on);
  LC_WIDTH_IDEAL = on ? 960 : 1920;
  LC_HEIGHT_IDEAL = on ? 960 : 1080;
  LC_FRAMERATE_IDEAL = on ? 15 : 30;
  CAPTURE_SIZE = on ? 720 : 1080;
  CAPTURE_JPEG_Q = on ? 0.82 : 0.93;
  toast(on ? 'Đã bật chế độ máy yếu' : 'Đã tắt chế độ máy yếu');
  // Re-open the live camera with the new constraints if it's currently running
  if(lcStream) startLiveCamera();
}
function maybeAutoCamera(){
  if(!isAutoCamera()) return;
  if(location.pathname!=='/' && location.pathname!=='') return;
  // Always switch to Đăng. Live camera mode opens in-page stream;
  // otherwise open the system camera picker (capture=environment).
  setTimeout(function(){
    showPage('upload');
    if(!isLiveCamera()){
      setTimeout(function(){ triggerCamera(); }, 350);
    } else {
      setTimeout(function(){ scrollUploadCameraIntoView(); }, 200);
      setTimeout(function(){ scrollUploadCameraIntoView(); }, 500);
    }
  }, 350);
}

/* ===================== Upload: send ===================== */
function downloadCapturedMedia(){
  if(!croppedBlob){toast('Chưa có ảnh/video để tải');return}
  try{
    const ext = isVideo ? ((croppedBlob.type||'').indexOf('mp4')>=0?'mp4':'webm') : 'jpg';
    const a=document.createElement('a');
    a.href=URL.createObjectURL(croppedBlob);
    a.download='locket_'+Date.now()+'.'+ext;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(()=>URL.revokeObjectURL(a.href), 4000);
  }catch(e){ toast('Không tải xuống được'); }
}
/* Save-only: writes the job to the offline queue and stops there — no
   flushQueue() call, so this never touches the network at all. Meant for
   rapid back-to-back captures (chụp nhiều liên tục) where waiting on a send
   attempt between shots would slow things down; everything queued this way
   still goes out automatically via the ticker/focus/online listeners the next
   time the tab is open and connected. */
function doSaveQueueOnly(){
  if(_speedBaking){toast('Đang xuất tốc độ video, đợi xong rồi lưu');return}
  if(!croppedBlob){toast('Chọn ảnh hoặc video trước đã');return}
  const cap=$('caption').value;
  const filename=isVideo?(originalFile?originalFile.name:'video.mp4'):'moment.jpg';
  const ct=(croppedBlob.type)||(isVideo?'video/mp4':'image/jpeg');
  const btn=$('queueOnlyBtn');
  if(btn) btn.disabled=true;
  const afterThumb = function(){
    const p = isVideo
      ? enqueueVideoUpload(croppedBlob, cap, filename, ct, videoCropPayload, videoThumbBlob)
      : enqueueUpload(croppedBlob, cap, filename, ct);
    p.then(job=>{
      if(btn) btn.disabled=false;
      toast('Đã lưu vào hàng đợi');
      $('caption').value=''; clearUpload();
    }).catch(err=>{
      console.error(err);
      if(btn) btn.disabled=false;
      toast('Không lưu được vào hàng đợi');
    });
  };
  if(isVideo){
    ensureVideoFit().then(function(){ return ensureVideoThumb(); }).then(afterThumb, afterThumb);
  } else {
    afterThumb();
  }
}
function doUpload(){
  if(_speedBaking){toast('Đang xuất tốc độ video, đợi xong rồi đăng');return}
  if(!croppedBlob){toast('Chọn ảnh hoặc video trước đã');return}
  const cap=$('caption').value,btn=$('uploadBtn');
  const filename=isVideo?(originalFile?originalFile.name:'video.mp4'):'moment.jpg';
  const ct=(croppedBlob.type)||(isVideo?'video/mp4':'image/jpeg');

  const finishFail=(msg)=>{
    btn.innerHTML='Gửi cho tất cả bạn bè';btn.disabled=false;
    toast(msg||'Đăng thất bại');
  };

  const directVideoUpload=()=>{
    // Fallback path only: old iOS (Blob-in-IndexedDB is unreliable there) or the
    // queue write itself failed. Goes straight over the network, so a dropped
    // connection here does lose the video — that's the honest tradeoff on
    // devices too old to safely persist it locally.
    btn.innerHTML='<span class="spinner"></span> Đang xử lý...';btn.disabled=true;
    const sendDirect=function(){
    const fd=new FormData();
    fd.append('file',croppedBlob,fixVideoName(filename, croppedBlob));
    fd.append('caption',cap);
    fd.append('client_id', 'd_'+Date.now()+'_'+Math.random().toString(36).slice(2,8));
    if(videoCropPayload) fd.append('cropPayload', videoCropPayload);
    if(videoThumbBlob) fd.append('thumb', videoThumbBlob, 'thumb.jpg');
    apiTimeout('/api/upload',{method:'POST',body:fd}, 60000).then(d=>{
      if(d&&d.ok){
        btn.innerHTML='Gửi cho tất cả bạn bè';btn.disabled=false;
        toast('Đã đăng!');
        momentsLoaded=false; momentsUpdatedAt=0;
        $('caption').value=''; clearUpload();
      }else{
        finishFail(d&&d.error||'Đăng thất bại');
      }
    }).catch(()=>finishFail(navigator.onLine?'Lỗi mạng khi đăng, thử lại':'Mất mạng — video này không lưu được offline trên máy này, thử lại khi có mạng'));
    };
    ensureVideoFit().then(function(){ return ensureVideoThumb(); }).then(sendDirect, sendDirect);
  };

  if(isVideo){
    // Same as photos: save first, send in the background. A dropped connection
    // (or no connection at all) no longer loses the video or shows a bare error —
    // it sits in the queue and the ticker/focus/online listeners retry it.
    btn.disabled=true;
    ensureVideoFit().then(function(){ return ensureVideoThumb(); }).then(function(){
      return enqueueVideoUpload(croppedBlob, cap, filename, ct, videoCropPayload, videoThumbBlob);
    }).then(job=>{
      btn.innerHTML='Gửi cho tất cả bạn bè';btn.disabled=false;
      toast('Đã lưu — đang gửi...');
      $('caption').value=''; clearUpload();
      flushQueue();
    }).catch(err=>{
      console.error(err);
      directVideoUpload();
    });
    return;
  }

  // Photos: hand straight to the offline queue and give the button back
  // immediately — no spinner, no waiting on the network round trip. Sending
  // now happens in the background via flushQueue() (kicked off right below,
  // plus the ticker and the focus/visibility/online listeners as backup), and
  // actual delivery status shows up in the queue list, not on this button.
  btn.disabled=true;
  enqueueUpload(croppedBlob, cap, filename, ct).then(job=>{
    btn.innerHTML='Gửi cho tất cả bạn bè';btn.disabled=false;
    toast('Đã lưu — đang gửi...');
    $('caption').value=''; clearUpload();
    flushQueue();
  }).catch(err=>{
    console.error(err);
    finishFail('Không lưu được, thử lại');
  });
}

function wipeLocalData(){
  try{ localStorage.removeItem(MOMENTS_LS_KEY); localStorage.removeItem(FRIENDS_CACHE_KEY); }catch(e){}
  try{
    if(window.caches) caches.keys().then(function(ks){
      ks.forEach(function(k){ if(k.indexOf('locket-mini')===0) caches.delete(k); });
    });
  }catch(e){}
}
function doLogout(){
  stopViews();
  var go=function(){ wipeLocalData(); location.href='/'; };
  apiTimeout('/api/logout', null, 10000).then(go).catch(go);
}

/* boot */
loadMe();
preloadFriends();
preloadMoments();
bindMomentsClickDelegation();
initMomentsView();
initRouter();
(function bindPreviewVidThumb(){
  var pv=$('previewVid');
  if(!pv) return;
  pv.addEventListener('loadeddata', function(){
    if(isVideo && !videoThumbBlob){
      captureVideoThumb(pv, videoCropOriginal).then(function(b){ videoThumbBlob=b; });
    }
  });
})();
(function bindLcPinchZoom(){
  var frame=document.querySelector('#liveCam .lc-frame');
  if(!frame) return;
  frame.addEventListener('touchstart', lcPinchStart, {passive:true});
  frame.addEventListener('touchmove', lcPinchMove, {passive:false});
  frame.addEventListener('touchend', lcPinchEnd, {passive:true});
  frame.addEventListener('touchcancel', lcPinchEnd, {passive:true});
  frame.addEventListener('click', lcFrameClick);
})();
updateOnlineUI();
renderQueue().then(()=>flushQueue());
startQueueTicker();
window.addEventListener('focus',function(){ flushQueue(); });
window.addEventListener('online',function(){
  updateOnlineUI();
  toast('Đã có mạng — đang đăng hàng đợi');
  flushQueue();
  // Coming back online: soft cache is untrusted (may be hours old while offline).
  // Invalidate timers and force a real fetch so Moments are not stuck on stale local data.
  momentsUpdatedAt = 0;
  try{
    var ml0=readMomentsLocal();
    if(ml0){
      ml0.ts = 0;
      // keep items for instant paint, but mark age expired
      localStorage.setItem(MOMENTS_LS_KEY, JSON.stringify({
        moments: ml0.moments||[],
        updated_at: 0,
        ts: 0
      }));
    }
  }catch(e){}
  preloadMoments();
  if($('page-moments') && $('page-moments').classList.contains('active')){
    ensureMomentsFromCache();
    loadMoments(true);
  }
});
window.addEventListener('offline',function(){ updateOnlineUI(); toast('Mất mạng — ảnh mới sẽ lưu máy'); });
document.addEventListener('visibilitychange',function(){
  if(document.visibilityState==='visible'){
    flushQueue();
    const gap = _momentsHiddenAt ? (Date.now() - _momentsHiddenAt) : 0;
    const local=readFriendsLocal();
    if(!local || Date.now()-(local.ts||0)>FRIENDS_TTL_MS) loadFriends(false);

    var ml=readMomentsLocal();
    var cacheAge = ml ? (Date.now()-(ml.ts||0)) : 999999;
    // After long background / offline stretch → force network; keep painting local meanwhile
    if(navigator.onLine && (cacheAge > 30000 || gap > 60000)){
      momentsUpdatedAt = 0;
      preloadMoments();
    } else if(!ml || Date.now()-(ml.ts||0)>MOMENTS_TTL_MS) {
      preloadMoments();
    }

    const momentsActive = $('page-moments') && $('page-moments').classList.contains('active');
    if(momentsActive){
      ensureMomentsFromCache();
      var grid=$('momentsGrid'), feed=$('momentsFeed');
      var hasDom = (grid && grid.children.length) || (feed && feed.children.length);
      if(gap > 15000 || !hasDom || cacheAge > 30000){
        loadMoments(true);
        bindMomentsScroll();
      } else {
        pollMomentsOnce();
      }
    }
    if($('page-upload') && $('page-upload').classList.contains('active') && isLiveCamera() && !croppedBlob && !lcStream){
      startLiveCamera();
    }
  } else {
    _momentsHiddenAt = Date.now();
    stopLiveCamera();
  }
});
window.addEventListener('pageshow', function(e){
  if(e.persisted){
    flushQueue();
    // BFCache restore: luôn force fetch moments nếu online
    if(navigator.onLine){
      momentsUpdatedAt = 0; // invalidate soft cache
    }
    const momentsActive = $('page-moments') && $('page-moments').classList.contains('active');
    if(momentsActive){
      ensureMomentsFromCache();
      loadMoments(true); // force
      bindMomentsScroll();
    } else {
      ensureMomentsFromCache();
    }
  }
});
window.addEventListener('pagehide', stopLiveCamera);
window.addEventListener('resize', function(){
  if($('liveCam') && !$('liveCam').classList.contains('hidden')) _sizeLcFrame();
});
window.addEventListener('orientationchange', function(){
  setTimeout(function(){
    if($('liveCam') && !$('liveCam').classList.contains('hidden')){
      _sizeLcFrame();
      scrollUploadCameraIntoView();
    }
  }, 200);
});
(function initAutoCamera(){
  const sw=$('autoCameraSwitch');
  if(sw) sw.checked=isAutoCamera();
  maybeAutoCamera();
})();
(function initLiveCamera(){
  const sw=$('liveCameraSwitch');
  if(sw) sw.checked=isLiveCamera();
})();
(function initLowPower(){
  const sw=$('lowPowerSwitch');
  if(sw) sw.checked=isLowPower();
})();
if('serviceWorker' in navigator){
  setTimeout(function(){
    navigator.serviceWorker.register('/sw.js').catch(function(){});
  }, 1500);
}
{% endif %}
</script>
<script>
/* LUMEN-MOTION */
(function(){
  try{
    var tb=document.querySelector('.topbar');
    function onS(){var y=window.pageYOffset||document.documentElement.scrollTop||0;if(tb)tb.classList.toggle('scrolled',y>6)}
    window.addEventListener('scroll',onS,{passive:true});onS();

    var nav=document.querySelector('.nav');
    if(nav){
      var ind=document.createElement('span');ind.className='nav-ind';
      nav.insertBefore(ind,nav.firstChild);nav.classList.add('has-ind');
      var place=function(){
        var a=nav.querySelector('button.active');
        if(!a){ind.style.opacity=0;return}
        ind.style.opacity=1;ind.style.width=a.offsetWidth+'px';ind.style.height=a.offsetHeight+'px';
        var tr='translate('+a.offsetLeft+'px,'+a.offsetTop+'px)';
        ind.style.webkitTransform=tr;ind.style.transform=tr;
      };
      if(window.MutationObserver)new MutationObserver(place).observe(nav,{attributes:true,subtree:true,attributeFilter:['class']});
      window.addEventListener('resize',place);
      setTimeout(place,60);setTimeout(place,450);
      if(document.fonts&&document.fonts.ready)document.fonts.ready.then(place);
    }

    var sn=document.getElementById('streakNum');
    if(sn&&window.MutationObserver&&!(window.matchMedia&&matchMedia('(prefers-reduced-motion: reduce)').matches)){
      var busy=false,mo=new MutationObserver(function(){
        if(busy)return;
        var v=parseInt(sn.textContent,10);
        if(isNaN(v)||v<2)return;
        busy=true;var t0=null,dur=1000;
        var step=function(ts){
          if(t0===null)t0=ts;
          var p=Math.min(1,(ts-t0)/dur),e=1-Math.pow(1-p,3);
          sn.textContent=String(Math.round(v*e));
          if(p<1)requestAnimationFrame(step);else mo.disconnect();
        };
        sn.textContent='0';requestAnimationFrame(step);
      });
      mo.observe(sn,{childList:true,characterData:true,subtree:true});
    }

    var st=document.querySelector('.lh-stage');
    if(st&&window.matchMedia&&matchMedia('(hover:hover)').matches){
      var items=st.querySelectorAll('.stk');
      document.addEventListener('pointermove',function(e){
        var x=e.clientX/window.innerWidth-.5,y=e.clientY/window.innerHeight-.5;
        for(var i=0;i<items.length;i++){
          var d=(i%3+1)*14,tr='translate('+(-x*d)+'px,'+(-y*d)+'px)';
          items[i].style.webkitTransform=tr;items[i].style.transform=tr;
        }
      });
    }
  }catch(e){}
})();
</script>
</body>
</html>
"""


# ============================================================
# Flask app + routes
# ============================================================

from datetime import timedelta

app = Flask(__name__)
def _load_secret() -> str:
    # Secret ổn định giữa các lần khởi động để không bị đăng xuất hàng loạt.
    env = os.environ.get("SECRET_KEY")
    if env:
        return env
    path = os.path.join(FEED_HISTORY_DIR, "locket_secret.key")
    try:
        with open(path, encoding="utf-8") as f:
            val = f.read().strip()
        if len(val) >= 32:
            return val
    except Exception:
        pass
    val = os.urandom(32).hex()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(val)
    except Exception as e:
        logger.warning("cannot persist secret key: %s", e)
    return val


app.secret_key = _load_secret()
app.config["MAX_CONTENT_LENGTH"] = 60 * 1024 * 1024
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("LOCKET_HTTPS", "0") == "1"

_CSP_ON = os.environ.get("LOCKET_CSP", "1") != "0"
_CSP_CONNECT_EXTRA = "".join(" " + h for h in os.environ.get("LOCKET_CSP_CONNECT", "").split()
                             if h.startswith("https://") or h.startswith("wss://"))
_CSP = ("default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; "
        "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://cdn.jsdelivr.net; "
        "font-src 'self' data: https://raw.githubusercontent.com https://cdn.jsdelivr.net; "
        "img-src 'self' data: blob: https:; media-src 'self' blob: https:; "
        "connect-src 'self'" + _CSP_CONNECT_EXTRA + "; worker-src 'self'; manifest-src 'self'; "
        "object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'")
_EXTRA_HOSTS = {h.strip().lower() for h in os.environ.get("LOCKET_ALLOWED_HOSTS", "").split(",") if h.strip()}
_TRUST_PROXY = os.environ.get("LOCKET_TRUST_PROXY", "0") == "1"

_LOGIN_FAILS: Dict[str, List[float]] = {}
_LOGIN_LOCK = threading.Lock()
_LOGIN_MAX_FAILS = 8
_LOGIN_WINDOW_SEC = 600


def _client_ip() -> str:
    if _TRUST_PROXY:
        fwd = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
        if fwd:
            return fwd
    return request.remote_addr or "?"


def _login_blocked() -> bool:
    ip, now = _client_ip(), time.time()
    with _LOGIN_LOCK:
        hits = [t for t in _LOGIN_FAILS.get(ip, []) if now - t < _LOGIN_WINDOW_SEC]
        if hits:
            _LOGIN_FAILS[ip] = hits
        else:
            _LOGIN_FAILS.pop(ip, None)
        return len(hits) >= _LOGIN_MAX_FAILS


def _login_fail() -> None:
    ip, now = _client_ip(), time.time()
    with _LOGIN_LOCK:
        if len(_LOGIN_FAILS) > 500:
            for k in [k for k, v in _LOGIN_FAILS.items() if not v or now - v[-1] > _LOGIN_WINDOW_SEC]:
                _LOGIN_FAILS.pop(k, None)
        _LOGIN_FAILS.setdefault(ip, []).append(now)


def _login_clear() -> None:
    with _LOGIN_LOCK:
        _LOGIN_FAILS.pop(_client_ip(), None)


@app.before_request
def _csrf_origin_check():
    # Chặn POST cross-site tới /api/* (bổ sung cho SameSite=Lax).
    if request.method in ("POST", "PUT", "PATCH", "DELETE") and request.path.startswith("/api/"):
        origin = request.headers.get("Origin") or request.headers.get("Referer")
        if origin:
            from urllib.parse import urlsplit
            host = (urlsplit(origin).netloc or "").lower()
            ok = {(request.host or "").lower(),
                  (request.headers.get("X-Forwarded-Host") or "").split(",")[0].strip().lower()} | _EXTRA_HOSTS
            if host not in ok:
                return jsonify({"ok": False, "error": "Forbidden origin"}), 403
    return None


@app.after_request
def _security_headers(resp):
    h = resp.headers
    h.setdefault("X-Content-Type-Options", "nosniff")
    h.setdefault("X-Frame-Options", "DENY")
    h.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    h.setdefault("Permissions-Policy", "camera=(self), microphone=(), geolocation=()")
    if request.path.startswith("/api/") and "Cache-Control" not in h:
        h["Cache-Control"] = "no-store"
    if (resp.mimetype or "") == "text/html":
        h.setdefault("Cache-Control", "no-cache, private")
        if _CSP_ON:
            h.setdefault("Content-Security-Policy", _CSP)
    return resp


def get_session() -> Optional[LocketSession]:
    tok = session.get("token")
    lid = session.get("local_id")
    if not tok or not lid:
        return None
    return LocketSession(
        id_token=tok, refresh_token=session.get("refresh_token"), local_id=lid,
        email=session.get("email", ""), display_name=session.get("display_name", ""),
        photo_url=session.get("photo_url", ""), ua=session.get("ua") or WEB_UA_POOL[0],
    )


# ---- performance layer -------------------------------------------------------
import gzip as _gzip
import hashlib as _hashlib
from flask import Response as _Response

try:                                   # Flask >= 2.2
    app.json.sort_keys = False         # sorting 1500 moments per response is wasted CPU
    app.json.compact = True
    app.json.ensure_ascii = False      # smaller payload for Vietnamese text
except Exception:
    app.config["JSON_SORT_KEYS"] = False
app.config["TEMPLATES_AUTO_RELOAD"] = False
app.jinja_env.auto_reload = False
app.jinja_env.cache_size = 0

# render_template_string() re-parses + re-compiles the whole 230 KB template on EVERY
# request. Compile once at import time instead.
_HTML_TPL = app.jinja_env.from_string(HTML)
_PAGE_CACHE: "_OD[str, Tuple[str, bytes, str]]" = _OD()   # key -> (etag, gzip bytes, raw html)
_PAGE_CACHE_LOCK = threading.Lock()
_GZIP_TYPES = ("text/", "application/json", "application/javascript",
               "application/manifest+json", "image/svg+xml")


@app.after_request
def _compress(resp):
    try:
        if (resp.status_code != 200 or resp.direct_passthrough or "Content-Encoding" in resp.headers
                or "gzip" not in (request.headers.get("Accept-Encoding") or "")):
            return resp
        ctype = (resp.mimetype or "")
        if not ctype.startswith(_GZIP_TYPES):
            return resp
        body = resp.get_data()
        if len(body) < 700:
            return resp
        resp.set_data(_gzip.compress(body, compresslevel=4))
        resp.headers["Content-Encoding"] = "gzip"
        resp.headers["Content-Length"] = str(len(resp.get_data()))
        resp.headers.add("Vary", "Accept-Encoding")
    except Exception:
        pass
    return resp


@app.route("/")
@app.route("/login")
@app.route("/upload")
@app.route("/moments")
@app.route("/moments/<mid>")
@app.route("/friends")
def index(mid=None):
    s = get_session()
    ctx = dict(
        logged_in=bool(s),
        gold_badge=GOLD_BADGE, celeb_badge=CELEB_BADGE,
        font_url=FONT_URL, favicon_url=FAVICON_URL,
        boot_name=(s.display_name if s else ""),
        boot_uid=(s.local_id if s else ""),
        boot_photo=(s.photo_url if s else ""),
        boot_email=(s.email if s else ""),
        app_version=APP_VERSION_STRING,
        video_budget=_VIDEO_MAX_BYTES,
    )
    key = repr(sorted(ctx.items()))
    with _PAGE_CACHE_LOCK:
        hit = _PAGE_CACHE.get(key)
        if hit:
            _PAGE_CACHE.move_to_end(key)
    if not hit:
        html = _HTML_TPL.render(**ctx)
        etag = '"%s"' % _hashlib.md5(html.encode("utf-8")).hexdigest()[:16]
        hit = (etag, html.encode("utf-8"))
        with _PAGE_CACHE_LOCK:
            _PAGE_CACHE[key] = hit
            while len(_PAGE_CACHE) > 64:
                _PAGE_CACHE.popitem(last=False)
    etag, body = hit
    if request.headers.get("If-None-Match") == etag:
        r = _Response(status=304)
    else:
        r = _Response(body, mimetype="text/html")
    r.headers["ETag"] = etag
    r.headers["Cache-Control"] = "private, no-cache"
    return r


@app.route("/manifest.webmanifest")
def web_manifest():
    from flask import Response
    import json as _json
    data = {
        "name": "Locket Mini",
        "short_name": "Locket Mini",
        "description": "Xem và đăng khoảnh khắc với bạn bè",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#000000",
        "theme_color": "#000000",
        "orientation": "portrait",
        "icons": [
            {"src": FAVICON_URL, "sizes": "180x180", "type": "image/png", "purpose": "any maskable"},
        ],
    }
    return Response(_json.dumps(data), mimetype="application/manifest+json")


@app.route("/sw.js")
def service_worker():
    from flask import Response
    # Offline shell + cache API images / static CDN for repeat visits
    js = r"""
const CACHE='locket-mini-v1';
const PRE=[
  '/',
  'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css',
  'https://cdnjs.cloudflare.com/ajax/libs/cropperjs/1.6.2/cropper.min.css',
  'https://cdnjs.cloudflare.com/ajax/libs/cropperjs/1.6.2/cropper.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.7.1/nouislider.min.css',
  'https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.7.1/nouislider.min.js'
];
self.addEventListener('install',e=>{
  e.waitUntil(caches.open(CACHE).then(c=>c.addAll(PRE)).then(()=>self.skipWaiting()));
});
self.addEventListener('activate',e=>{
  e.waitUntil(caches.keys().then(keys=>Promise.all(keys.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim()));
});
self.addEventListener('fetch',e=>{
  const req=e.request;
  const url=new URL(req.url);
  if(req.method!=='GET') return;
  // Cache-first for proxied images (offline-friendly after first view)
  if(url.pathname==='/api/img'){
    e.respondWith(
      caches.open(CACHE).then(async c=>{
        const hit=await c.match(req);
        if(hit) return hit;
        try{
          const res=await fetch(req);
          if(res && res.ok){ c.put(req, res.clone()); c.keys().then(function(ks){ var im=ks.filter(function(k){return k.url.indexOf('/api/img')>-1}); if(im.length>220) c.delete(im[0]); }); }
          return res;
        }catch(err){
          return hit || Response.error();
        }
      })
    );
    return;
  }
  // Network-first for app shell / API JSON; fall back to cache offline
  if(url.origin===self.location.origin){
    e.respondWith(
      fetch(req).then(res=>{
        if(res && res.ok && (url.pathname==='/' || url.pathname.endsWith('.css') || url.pathname.endsWith('.js'))){
          const copy=res.clone();
          caches.open(CACHE).then(c=>c.put(req, copy));
        }
        return res;
      }).catch(()=>caches.match(req).then(r=>r||caches.match('/')))
    );
  }
});
"""
    # Tie the cache name to the app version/build so a deploy automatically
    # evicts everything an old tab had cached, instead of silently serving
    # stale JS to a returning visitor (this bit the video/camera fixes above —
    # same-origin fetches are network-first already, but cached CDN libs and
    # the pre-cache list itself were still keyed to a name that never changed).
    js = js.replace("const CACHE='locket-mini-v1';", "const CACHE='locket-mini-%s-%s';" % (APP_VERSION, APP_BUILD))
    resp = Response(js, mimetype="application/javascript")
    resp.headers["Service-Worker-Allowed"] = "/"
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    remember = bool(data.get("remember"))
    if not email or not password:
        return jsonify({"ok": False, "error": "Nhập email và mật khẩu"})
    if _login_blocked():
        return jsonify({"ok": False, "error": "Thử đăng nhập quá nhiều lần, đợi vài phút rồi thử lại."}), 429
    try:
        s, _dbg = login(email, password)
        _login_clear()
        session.clear()
        session.permanent = remember
        session["token"] = s.id_token
        session["local_id"] = s.local_id
        session["refresh_token"] = s.refresh_token
        session["email"] = s.email
        session["display_name"] = s.display_name
        session["photo_url"] = s.photo_url
        session["ua"] = s.ua
        session["token_issued_at"] = time.time()
        session["remember"] = remember
        return jsonify({
            "ok": True,
            "me": {
                "display_name": s.display_name,
                "photo_url": s.photo_url,
                "email": s.email,
                "local_id": s.local_id,
            },
        })
    except LoginError as e:
        _login_fail()
        logger.error("LOGIN failed: %s", e.friendly)
        return jsonify({"ok": False, "error": e.friendly})
    except Exception as e:
        logger.error("LOGIN unexpected error: %s", e)
        return jsonify({"ok": False, "error": f"Lỗi không xác định: {e}"})


@app.route("/api/logout", methods=["GET", "POST"])
def api_logout():
    lid = session.get("local_id")
    if lid:
        stop_moments_live(lid)
    session.clear()
    return jsonify({"ok": True})


@app.route("/api/me")
def api_me():
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    try:
        s = ensure_fresh_token(s)
        info, _dbg = get_self_info(s)
        info["email"] = s.email
        # Prefer the richer display name we already got at login when getInfo is sparse
        if not info.get("first_name") and s.display_name:
            info["first_name"] = s.display_name
        if not info.get("profile_picture_url") and s.photo_url:
            info["profile_picture_url"] = s.photo_url
        return jsonify({"ok": True, "me": info})
    except Exception as e:
        logger.error("ME error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


_UID_RE = re.compile(r"^[A-Za-z0-9]{28}$")


def _fs_list_uids(s: LocketSession, collection: str, timeout: int = 20) -> List[str]:
    url = (f"https://firestore.googleapis.com/v1/projects/locket-4252a/databases/(default)/"
           f"documents/users/{s.local_id}/{collection}?pageSize=300")
    headers = {"Authorization": f"Bearer {s.id_token}", "Accept": "application/json",
               "User-Agent": "okhttp/4.12.0"}
    r = requests.get(url, headers=headers, timeout=timeout)
    if r.status_code in (403, 404):
        return []
    r.raise_for_status()
    uids: List[str] = []
    for doc in (r.json() or {}).get("documents") or []:
        name = str(doc.get("name") or "")
        uid = name.rsplit("/", 1)[-1]
        if uid and uid not in uids:
            uids.append(uid)
    return uids


def _invite_token_for_username(username: str) -> Optional[str]:
    r = requests.get(f"https://locket.cam/{username}", timeout=12,
                     headers={"User-Agent": "com.locket.Locket/2.68.0 iPhone/27.0"})
    for pattern in (r"locket\.camera%2Finvites%2F([A-Za-z0-9]+)", r"locket\.camera/invites/([A-Za-z0-9]+)"):
        m = re.search(pattern, r.text)
        if m:
            return m.group(1)
    return None


def resolve_person(s: LocketSession, query: str) -> Dict[str, Any]:
    q = (query or "").strip()
    if not q:
        raise LocketError("Enter a username, invite link or UID")
    token: Optional[str] = None
    m = re.search(r"locket\.camera(?:%2F|/)invites(?:%2F|/)([A-Za-z0-9]+)", q)
    if m:
        token = m.group(1)
    else:
        m = re.search(r"locket\.cam/([A-Za-z0-9_.\-]+)", q)
        handle = (m.group(1) if m else q).lstrip("@").strip()
        if _UID_RE.match(handle):
            try:
                profile, _ = fetch_user_v2(s, handle)
                if profile.get("uid"):
                    return profile
            except Exception as e:
                logger.debug("uid lookup failed, trying username: %s", e)
        token = _invite_token_for_username(handle)
    if not token:
        raise LocketError(f"No Locket user found for '{q}'")
    res = _locket_post_api(s, "fetchUserForInviteToken", {"invite_token": token},
                           timeout=20, quiet=True, analytics=False)
    data = res.get("data", res) if isinstance(res, dict) else {}
    user = data.get("user") if isinstance(data, dict) and isinstance(data.get("user"), dict) else data
    raw = user if isinstance(user, dict) else {}
    profile = _normalize_user(raw)
    if not profile.get("uid"):
        raise LocketError(f"No Locket user found for '{q}'")
    profile["friendship_status"] = str(raw.get("friendship_status") or "")
    return _enrich_person(s, profile)


def _enrich_person(s: LocketSession, profile: Dict[str, Any]) -> Dict[str, Any]:
    status = profile.get("friendship_status", "")
    try:
        full, _ = fetch_user_v2(s, profile["uid"])
    except Exception as e:
        logger.debug("profile enrich failed: %s", e)
        full = {}
    merged = {**profile, **{k: v for k, v in full.items() if v not in (None, "", 0)}}
    merged["celebrity"] = bool(profile.get("celebrity") or full.get("celebrity"))
    merged["friendship_status"] = status
    if merged["celebrity"] and not merged.get("celebrity_data"):
        try:
            doc = fetch_user_doc_firestore(s, merged["uid"])
            merged["celebrity_data"] = _norm_celeb_data(doc.get("celebrity_data"))
        except Exception as e:
            logger.debug("celebrity slots lookup failed: %s", e)
    return merged


def _relation_from_status(status: str) -> str:
    value = (status or "").strip().lower()
    if not value:
        return ""
    if "outgoing" in value:
        return "outgoing"
    if "incoming" in value:
        return "incoming"
    if value in ("friend", "friends", "accepted"):
        return "friend"
    return ""


def _norm_celeb_data(raw: Any) -> Optional[Dict[str, int]]:
    if not isinstance(raw, dict):
        return None
    out: Dict[str, int] = {}
    for key in ("friend_count", "max_friends"):
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        out[key] = int(value)
    return out or None


def _relationship(s: LocketSession, uid: str) -> str:
    if uid == s.local_id:
        return "self"

    def load(kind: str) -> set:
        try:
            if kind == "friends":
                return set(list_friend_uids_firestore(s)[0])
            return set(_fs_list_uids(s, kind))
        except Exception as e:
            logger.debug("relationship %s failed: %s", kind, e)
            return set()

    kinds = ("friends", "incoming_friend_requests", "outgoing_friend_requests")
    with ThreadPoolExecutor(max_workers=3) as pool:
        friends, incoming, outgoing = list(pool.map(load, kinds))
    if uid in friends:
        return "friend"
    if uid in incoming:
        return "incoming"
    if uid in outgoing:
        return "outgoing"
    return "none"


def list_friend_requests(s: LocketSession) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {"incoming": [], "outgoing": []}
    for key, collection in (("incoming", "incoming_friend_requests"), ("outgoing", "outgoing_friend_requests")):
        try:
            uids = _fs_list_uids(s, collection)[:60]
        except Exception as e:
            logger.warning("%s list failed: %s", collection, e)
            continue

        def load(uid: str) -> Dict[str, Any]:
            try:
                profile, _ = fetch_user_v2(s, uid)
                profile["uid"] = profile.get("uid") or uid
                return profile
            except Exception:
                return {"uid": uid}

        if uids:
            with ThreadPoolExecutor(max_workers=min(8, len(uids))) as pool:
                result[key] = list(pool.map(load, uids))
    return result


def _invalidate_social_caches(s: LocketSession) -> None:
    st = _profile_state(s.local_id)
    with _PROFILE_LOCK:
        st["friends_at"] = 0
    moments = _MOMENTS_CACHE.get(s.local_id)
    if moments:
        with moments["lock"]:
            moments["uids_at"] = 0.0
            moments["last_crawl_at"] = 0.0


def _write_api(s: LocketSession, endpoint: str, payload: Dict[str, Any],
               needs_appcheck: bool = False) -> Dict[str, Any]:
    if needs_appcheck:
        ensure_appcheck(s)
    try:
        return _locket_post_api(s, endpoint, payload, timeout=25, quiet=False, retries=0,
                                analytics=False)
    except LocketError as e:
        if getattr(e, "status", None) not in (401, 403):
            raise
        state = appcheck_status(s.local_id)
        if state["has"] and state["expired"]:
            hint = "The App Check token has expired."
        elif state["has"]:
            hint = "The App Check token was rejected."
        elif state["provider"]:
            hint = "The App Check provider did not return a token."
        else:
            hint = "No App Check token is set."
        raise LocketError(
            f"Locket rejected {endpoint} (HTTP {e.status}). {hint} Sending a friend request needs a valid "
            "X-Firebase-AppCheck token: set LOCKET_APPCHECK_PROVIDER_URL and LOCKET_APPCHECK_PROVIDER_KEY "
            "for automatic tokens, or paste one under 'App Check token' (it lasts about 1 hour).") from e


def add_person(s: LocketSession, uid: str) -> Dict[str, Any]:
    profile: Dict[str, Any] = {"uid": uid}
    try:
        profile, _ = fetch_user_v2(s, uid)
        profile["uid"] = profile.get("uid") or uid
    except Exception as e:
        logger.debug("profile before add failed: %s", e)
    payload = {"source": "inviteCardPopup", "user_uid": uid}
    res = _write_api(s, "sendFriendRequest", payload, needs_appcheck=True)
    _invalidate_social_caches(s)
    return {"kind": "follow" if profile.get("celebrity") else "friend", "user": profile, "result": res}


def accept_person(s: LocketSession, uid: str, share_history: bool) -> Dict[str, Any]:
    payload = {
        "user_uid": uid,
        "during_signup": False,
        "ios_version": f"{LOCKET_IOS_APP_VERSION}.{LOCKET_IOS_BUILD}",
        "share_history": bool(share_history),
    }
    res = _write_api(s, "acceptFriendRequest", payload)
    _invalidate_social_caches(s)
    return {"uid": uid, "share_history": bool(share_history), "result": res}


def decline_person(s: LocketSession, uid: str, direction: str) -> Dict[str, Any]:
    direction = "outgoing" if direction == "outgoing" else "incoming"
    res = _write_api(s, "deleteFriendRequest", {"user_uid": uid, "direction": direction})
    _invalidate_social_caches(s)
    return {"uid": uid, "direction": direction, "result": res}


def _people_call(fn):
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    try:
        s = ensure_fresh_token(s)
        return jsonify({"ok": True, **fn(s)})
    except Exception as e:
        logger.error("PEOPLE error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


def _uid_arg(data: Dict[str, Any]) -> str:
    uid = str(data.get("uid") or "").strip()
    if not _UID_RE.match(uid):
        raise LocketError("Invalid user id")
    return uid


@app.route("/api/people/search")
def api_people_search():
    q = request.args.get("q", "")

    def run(s):
        profile = resolve_person(s, q)
        relation = "self" if profile["uid"] == s.local_id else _relation_from_status(profile.get("friendship_status", ""))
        return {"user": profile, "relation": relation or _relationship(s, profile["uid"])}
    return _people_call(run)


@app.route("/api/people/requests")
def api_people_requests():
    return _people_call(lambda s: list_friend_requests(s))


@app.route("/api/people/add", methods=["POST"])
def api_people_add():
    data = request.get_json(force=True) or {}
    return _people_call(lambda s: add_person(s, _uid_arg(data)))


@app.route("/api/people/accept", methods=["POST"])
def api_people_accept():
    data = request.get_json(force=True) or {}
    return _people_call(lambda s: accept_person(s, _uid_arg(data), bool(data.get("share_history"))))


@app.route("/api/people/decline", methods=["POST"])
def api_people_decline():
    data = request.get_json(force=True) or {}
    return _people_call(lambda s: decline_person(s, _uid_arg(data), str(data.get("direction") or "incoming")))


@app.route("/api/people/appcheck", methods=["GET", "POST"])
def api_people_appcheck():
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    if request.method == "POST":
        token = str((request.get_json(force=True) or {}).get("token") or "").strip()
        if token and token.count(".") != 2:
            return jsonify({"ok": False, "error": "That does not look like an App Check token"})
        set_appcheck_token(s.local_id, token)
    return jsonify({"ok": True, **appcheck_status(s.local_id)})


@app.route("/api/people/invite")
def api_people_invite():
    def run(s):
        info, _ = get_self_info(s)
        username = str(info.get("username") or "")
        return {"username": username, "link": f"https://locket.cam/{username}" if username else ""}
    return _people_call(run)


def _normalize_viewer_entry(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, str):
        return {"uid": raw, "viewed_at": None}
    if not isinstance(raw, dict):
        return None
    uid = raw.get("uid") or raw.get("user") or raw.get("user_uid") or raw.get("viewer_uid")
    if not uid:
        return None
    ts = raw.get("viewed_at") or raw.get("date") or raw.get("created_at")
    seconds = None
    if isinstance(ts, dict):
        seconds = ts.get("_seconds") or ts.get("seconds")
    elif isinstance(ts, (int, float)):
        seconds = ts if ts < 1e12 else ts / 1000.0
    return {"uid": str(uid), "viewed_at": seconds}


def fetch_moment_reactions(s: LocketSession, moment_uid: str, timeout: int = 10) -> List[Dict[str, Any]]:
    url = (f"https://firestore.googleapis.com/v1/projects/locket-4252a/databases/(default)/"
           f"documents/moments/{moment_uid}/reactions")
    r = requests.get(url, headers={"Authorization": f"Bearer {s.id_token}",
                                    "Accept": "application/json"}, timeout=timeout)
    if r.status_code in (403, 404):
        return []
    r.raise_for_status()
    out: List[Dict[str, Any]] = []
    for doc in (r.json() or {}).get("documents") or []:
        f = flatten_firestore(doc) or {}
        emoji = f.get("string") or f.get("reaction") or f.get("emoji") or ""
        uid = f.get("user") or f.get("user_uid") or ""
        if emoji and uid:
            out.append({"uid": str(uid), "emoji": str(emoji)})
    return out


def _cached_profile(s: LocketSession, uid: str) -> Dict[str, Any]:
    st = _profile_state(s.local_id)
    with _PROFILE_LOCK:
        p = st["profiles"].get(uid)
    if p and (p.get("first_name") or p.get("username") or p.get("profile_picture_url")):
        return dict(p)
    prof, _ = fetch_user_v2(s, uid)
    prof["uid"] = prof.get("uid") or uid
    with _PROFILE_LOCK:
        if len(st["profiles"]) < 2000:
            st["profiles"][uid] = prof
    return dict(prof)


def _get_moment_views_raw(s: LocketSession, moment_uid: str) -> Dict[str, Any]:
    res = _locket_post_api(s, "getMomentViews", {"moment_uid": moment_uid}, timeout=20, quiet=True)
    data = res.get("data", res) if isinstance(res, dict) else {}
    if not isinstance(data, dict):
        data = {}
    raw_viewers = data.get("moment_views") or data.get("viewers") or data.get("views") or []
    viewers: List[Dict[str, Any]] = []
    seen: set = set()
    for entry in raw_viewers if isinstance(raw_viewers, list) else []:
        norm = _normalize_viewer_entry(entry)
        if norm and norm["uid"] not in seen:
            seen.add(norm["uid"])
            viewers.append(norm)

    try:
        reactions = fetch_moment_reactions(s, moment_uid)
    except Exception as e:
        logger.debug("reactions fetch failed for %s: %s", moment_uid[:8], e)
        reactions = []
    reaction_by_uid = {r["uid"]: r["emoji"] for r in reactions}

    total_views = len(viewers)
    viewers = viewers[:150]
    uids = [v["uid"] for v in viewers if v["uid"] != s.local_id]
    profiles: Dict[str, Dict[str, Any]] = {}
    if uids:
        def load(uid: str):
            try:
                prof = _cached_profile(s, uid)
                return uid, prof
            except Exception:
                return uid, {"uid": uid}
        with ThreadPoolExecutor(max_workers=min(8, len(uids))) as pool:
            for uid, prof in pool.map(load, uids):
                profiles[uid] = prof

    out_viewers: List[Dict[str, Any]] = []
    for v in viewers:
        prof = profiles.get(v["uid"], {"uid": v["uid"]})
        out_viewers.append({**prof, "viewed_at": v["viewed_at"], "reaction": reaction_by_uid.get(v["uid"], "")})
    out_viewers.sort(key=lambda x: x.get("viewed_at") or 0, reverse=True)

    count = data.get("views_count") or data.get("count") or total_views
    return {"count": int(count) if isinstance(count, (int, float)) else len(out_viewers),
            "viewers": out_viewers,
            "reaction_count": len(reactions)}


_VIEWS_LOCK = threading.Lock()
_VIEWS_CACHE: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
_VIEWS_INFLIGHT: Dict[Tuple[str, str], threading.Event] = {}
_VIEWS_TTL_SEC = 3.0
_VIEWS_CACHE_MAX = 120


def get_moment_views(s: LocketSession, moment_uid: str) -> Dict[str, Any]:
    # Cache 3s + single-flight: nhiều tab/poll cùng lúc chỉ tốn 1 lần gọi upstream.
    key = (s.local_id, moment_uid)
    now = time.time()
    with _VIEWS_LOCK:
        hit = _VIEWS_CACHE.get(key)
        if hit and now - hit[0] < _VIEWS_TTL_SEC:
            return hit[1]
        ev = _VIEWS_INFLIGHT.get(key)
        leader = ev is None
        if leader:
            ev = threading.Event()
            _VIEWS_INFLIGHT[key] = ev
    if not leader:
        ev.wait(25)
        with _VIEWS_LOCK:
            hit = _VIEWS_CACHE.get(key)
        if hit:
            return hit[1]
        raise LocketError("views busy")
    try:
        data = _get_moment_views_raw(s, moment_uid)
        sig_src = json.dumps([(v.get("uid"), v.get("viewed_at"), v.get("reaction"))
                              for v in data.get("viewers", [])], default=str)
        data["sig"] = hashlib.md5(sig_src.encode("utf-8")).hexdigest()[:16]
        with _VIEWS_LOCK:
            _VIEWS_CACHE[key] = (time.time(), data)
            if len(_VIEWS_CACHE) > _VIEWS_CACHE_MAX:
                for k, _ in sorted(_VIEWS_CACHE.items(), key=lambda kv: kv[1][0])[:_VIEWS_CACHE_MAX // 2]:
                    _VIEWS_CACHE.pop(k, None)
        return data
    finally:
        with _VIEWS_LOCK:
            _VIEWS_INFLIGHT.pop(key, None)
        ev.set()


@app.route("/api/moments/views")
def api_moment_views():
    uid = request.args.get("uid", "")

    def run(s):
        if not uid or not re.match(r"^[A-Za-z0-9_-]{1,64}$", uid):
            raise LocketError("Missing moment id")
        data = get_moment_views(s, uid)
        if request.args.get("sig") and request.args.get("sig") == data.get("sig"):
            return {"unchanged": True, "sig": data["sig"], "count": data.get("count")}
        return data
    return _people_call(run)


@app.route("/api/friends")
def api_friends():
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    try:
        s = ensure_fresh_token(s)
        force = request.args.get("force") == "1"
        if request.args.get("check") == "1" and not force:
            # Rẻ: chỉ 1 request lấy danh sách UID; nếu khác bản cache mới tải lại profile.
            try:
                fresh_uids, _ = list_friend_uids_firestore(s)
                cached = {f.get("uid") for f in (_profile_state(s.local_id)["friends"] or [])}
                if cached and set(fresh_uids) != cached:
                    force = True
            except Exception as e:
                logger.debug("friends check skipped: %s", e)
        friends = get_friends_cached(s, force=force)
        meta = _FRIEND_META.get(s.local_id) or {}
        times = meta.get("items") or {}
        fresh_meta = bool(meta) and (time.time() - float(meta.get("at") or 0)) < 15 * 60
        out_friends = []
        for f in friends:
            item = dict(f)
            ts = times.get(item.get("uid"))
            if ts:
                item["added_at"] = ts
            out_friends.append(item)
        return jsonify({"ok": True, "friends": out_friends, "count": len(out_friends),
                        "complete": bool(fresh_meta and meta.get("complete", False))})
    except Exception as e:
        logger.error("FRIENDS error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/moments")
def api_moments():
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    try:
        s = ensure_fresh_token(s)
        force = request.args.get("force") == "1"
        st = _ensure_moments_state(s.local_id)
        items = _enrich_moments(s, moments_window(st, get_moments_cached(s, force=force)))
        return jsonify({
            "has_more": feed_has_more(st),
            "ok": True,
            "moments": items,
            "count": len(items),
            "updated_at": st.get("updated_at") or time.time(),
            "cached": not force and bool(st.get("bootstrapped")),
            "force": force,
            "source": st.get("source") or "",
            "warnings": list(st.get("warnings") or []),
            "diag": dict(st.get("diag") or {}),
        })
    except Exception as e:
        logger.error("MOMENTS error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/moments/more")
def api_moments_more():
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    try:
        s = ensure_fresh_token(s)
        st = _ensure_moments_state(s.local_id)
        have = int(request.args.get("have") or 0)
        with st["lock"]:
            st["window"] = max(st["window"], have + FEED_WINDOW_STEP)
            empty = not st["items"]
        if empty:
            get_moments_cached(s, force=True)
        deepen_feed(s, st)
        with st["lock"]:
            items = moments_window(st, st["items"])
            updated = st.get("updated_at") or time.time()
        items = _enrich_moments(s, items)
        return jsonify({
            "ok": True,
            "moments": items,
            "count": len(items),
            "updated_at": updated,
            "has_more": feed_has_more(st),
            "diag": dict(st.get("diag") or {}),
        })
    except Exception as e:
        logger.error("MOMENTS MORE error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/moments/poll")
def api_moments_poll():
    """Lightweight live poll — returns full list only when cache was updated after `since`."""
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    try:
        s = ensure_fresh_token(s)
        since = float(request.args.get("since") or 0)
        items, updated = poll_new_moments(s, since=since)
        st = _ensure_moments_state(s.local_id)
        items = _enrich_moments(s, moments_window(st, items))
        return jsonify({
            "ok": True,
            "moments": items,
            "updated_at": updated,
            "changed": bool(items) and updated > since,
            "source": st.get("source") or "",
        })
    except Exception as e:
        logger.error("MOMENTS POLL error: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/img")
def api_img():
    from flask import Response
    from urllib.parse import unquote, urlsplit, urlunsplit, quote
    url = (request.args.get("u") or "").strip()
    if "%252F" in url or "%253F" in url or "%2526" in url:
        url = unquote(url)
    if not url.startswith("https://"):
        return jsonify({"ok": False, "error": "bad url"}), 400
    if not _img_host_ok(url):
        return jsonify({"ok": False, "error": "host not allowed"}), 403
    try:
        max_side = int(request.args.get("w") or 720)
    except Exception:
        max_side = 720
    max_side = max(64, min(max_side, 1280))
    try:
        parts = urlsplit(url)
        path = parts.path or ""
        marker = "/o/"
        if marker in path:
            pre, obj = path.split(marker, 1)
            if "/" in obj and "%2F" not in obj.upper():
                obj = quote(obj, safe="")
            path = pre + marker + obj
            url = urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))
    except Exception:
        pass
    
    # Smart convert: chỉ ép JPEG khi browser không hỗ trợ Webp HOẶC cần resize
    client_supports_webp = request.args.get("webp") == "1"
    needs_resize = max_side < 1080  # nếu xin thumb nhỏ thì vẫn resize
    try:
        if client_supports_webp and not needs_resize and (url.lower().endswith(".webp") or "image/webp" in (request.headers.get("Accept") or "")):
            # Pass-through: redirect hoặc proxy gốc
            _pk = (url, -1, False)
            _hit = _img_cache_get(_pk)
            if _hit:
                body, ctype = _hit[1], _hit[2]
            else:
                r = _http.get(url, timeout=15, headers={"User-Agent": IOS_UA, "Accept": "image/webp,image/*,*/*;q=0.8"})
                r.raise_for_status()
                body = r.content
                ctype = (r.headers.get("Content-Type") or "image/webp").split(";")[0].strip()
                _img_cache_put(_pk, body, ctype)
            resp = Response(body, mimetype=ctype)
            resp.headers["Cache-Control"] = "private, max-age=86400, immutable"
            return resp
    except Exception:
        pass

    try:
        force_convert = not client_supports_webp
        data, mime = fetch_image_as_jpeg(url, max_side=max_side, force_convert=force_convert)
        resp = Response(data, mimetype=mime)
        # A given storage URL never changes content -> let the browser keep it a day.
        resp.headers["Cache-Control"] = "private, max-age=86400, immutable"
        return resp
    except requests.HTTPError as e:
        code_up = getattr(getattr(e, "response", None), "status_code", 0) or 0
        logger.warning("IMG PROXY upstream %s for %s", code_up, url[:120])
        if 400 <= code_up < 500:
            miss = jsonify({"ok": False, "error": "image unavailable"})
            miss.status_code = 404
            miss.headers["Cache-Control"] = "private, max-age=300"
            return miss
        return jsonify({"ok": False, "error": str(e)}), 502
    except Exception as e:
        logger.warning("IMG PROXY fail %s: %s", url[:120], e)
        return jsonify({"ok": False, "error": str(e)}), 502

@app.route("/api/upload", methods=["POST"])
def api_upload():
    s = get_session()
    if not s:
        return jsonify({"ok": False, "error": "Not logged in"})
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "No file"})
    f = request.files["file"]
    thumb_f = request.files.get("thumb")
    caption = request.form.get("caption", "")
    crop_payload = request.form.get("cropPayload") or None
    client_id = request.form.get("client_id") or None
    if f.filename == "":
        return jsonify({"ok": False, "error": "Empty filename"})
    dedupe_key = f"{s.local_id}:{client_id}" if client_id else None
    if dedupe_key:
        with _UPLOAD_DEDUPE_LOCK:
            now = time.time()
            _dedupe_prune(now)
            hit = _UPLOAD_DEDUPE.get(dedupe_key)
            if hit:
                if hit.get("pending"):
                    return jsonify({"ok": False, "error": "Đang đăng ảnh này rồi, đợi chút", "retry": True})
                return jsonify({"ok": True, "result": hit["result"], "deduped": True})
            # Reserve the key for the whole duration of the upload — closes the
            # window where two requests for the same client_id (two tabs, or a
            # retry racing the original) both pass the check before either has
            # a result to dedupe against, and both end up posted to Locket.
            _UPLOAD_DEDUPE[dedupe_key] = {"at": now, "pending": True, "result": None}
    try:
        s = ensure_fresh_token(s)
        raw = f.read()
        ct = f.content_type or "application/octet-stream"
        is_video = "video" in ct
        if is_video:
            ext = f.filename.rsplit(".", 1)[-1] if "." in f.filename else "mp4"
            filename = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}.{ext}"
            # Locket's real client always sends a JPEG "thumb" alongside the video;
            # without one, upload_media() fell back to sending the raw video bytes
            # (with a video/* content-type) as the thumb field, which the real
            # binhake API silently rejects — that was the actual "video won't
            # post" bug, not a network/timeout issue. The client now captures a
            # real still frame before sending; use it when present.
            thumb_data = thumb_name = thumb_type = None
            if thumb_f and thumb_f.filename:
                thumb_data = thumb_f.read()
                thumb_name = "thumb.jpg"
                thumb_type = thumb_f.content_type or "image/jpeg"
            else:
                # Defensive fallback for an old cached page that hasn't picked up
                # the client-side thumb capture yet — still never send raw video
                # bytes as the thumb field.
                buf = io.BytesIO()
                Image.new("RGB", (16, 16), (10, 10, 10)).save(buf, format="JPEG", quality=70)
                thumb_data = buf.getvalue()
                thumb_name = "thumb.jpg"
                thumb_type = "image/jpeg"
            try:
                result, _dbg = upload_media(s, raw, filename, ct, caption=caption, crop_payload=crop_payload,
                                             thumb_data=thumb_data, thumb_name=thumb_name, thumb_type=thumb_type)
            except Exception as e_vid:
                logger.error("binhake video upload failed: %s", e_vid)
                # Official HAR only captured photo/webp posts; video needs a
                # separate media path we don't have yet. Surface a clear error.
                raise LocketError(
                    f"Đăng video qua binhake thất bại: {e_vid}. "
                    "API chính thức cho video chưa được bắt trong HAR — thử ảnh hoặc thử lại sau."
                ) from e_vid
        else:
            data, filename, ptype = prepare_photo(raw)
            # Official path primary (Firebase Storage + postMomentV2 from Android HAR).
            # binhake uploadMedia is broken upstream — keep as silent last-resort only.
            try:
                result, _dbg = upload_media_official(s, data, ptype, caption=caption)
            except Exception as e_off:
                logger.warning("official postMomentV2 failed (%s) — last-resort binhake", e_off)
                try:
                    result, _dbg = upload_media(s, data, filename, ptype, caption=caption)
                except Exception as e_bh:
                    logger.error("binhake uploadMedia also failed: %s", e_bh)
                    raise LocketError(
                        f"Đăng ảnh thất bại (official: {e_off}; binhake: {e_bh})"
                    ) from e_bh
        if dedupe_key:
            with _UPLOAD_DEDUPE_LOCK:
                _UPLOAD_DEDUPE[dedupe_key] = {"at": time.time(), "pending": False, "result": result}
        # Mark cache stale but KEEP items — client paints old posts while force-fetch merges new one
        with _MOMENTS_LOCK:
            st = _MOMENTS_CACHE.get(s.local_id)
            if st:
                st["last_fetch_at"] = 0
                st["last_crawl_at"] = 0
                # bootstrapped stays True so soft path can still serve until force merge
        return jsonify({"ok": True, "result": result})
    except requests.HTTPError as e:
        logger.error("UPLOAD http error: %s", e)
        if dedupe_key:
            with _UPLOAD_DEDUPE_LOCK:
                _UPLOAD_DEDUPE.pop(dedupe_key, None)
        return jsonify({"ok": False, "error": f"Upload bị từ chối ({e})"})
    except Exception as e:
        logger.error("UPLOAD error: %s", e)
        if dedupe_key:
            with _UPLOAD_DEDUPE_LOCK:
                _UPLOAD_DEDUPE.pop(dedupe_key, None)
        return jsonify({"ok": False, "error": str(e)})


# LUMEN-STICKERS-BEGIN
_STICKERS_B64 = {
    "bff": "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",
    "hearts_duo": "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",
    "heart_3d": "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",
    "heart_flat": "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",
    "megaphone": "UklGRqoWAABXRUJQVlA4WAoAAAAQAAAA7wAAygAAQUxQSKcLAAAB8Mb/nyE7/v89X1U9JznvaI3ovbZt27Y3a9u2bdu2jWBt2xsnZzNT9Xo9L8zJ7jnV05WrETEByFWpAW7wcsfe+vrfDeroT166dPMFpgNcIZgydUD/ra75nv9e3zphRUDclIgA0x77Psmoap2YaTRy7GN7AxCZ0vCQ/b8lY1D+e4sN0j7Yvgb4KQopMO/jpEVjF1og+cHGgjaZchBg279IZVebkvG6RQE3peBQHFunslvN+Otx0wBuikBELqYqu7tBDlsHKGQKwONiqrH7VWl3DUbhsk8whMY0zfjjzoBkXg2bTAypkErePgt81jkZ8BmV6WrgiCXgM068v5R1pmyB9R1QZBxWozFxZX17uFwT9HlFNTWa/bMdvOSZw64so7K+LnyWifQbSisBAz+cRSTHatgpWCmofFqQ4eL/9zgjy6k8AD6/CqzfiFYW+35G53JLpLiBDZbUlMeiyC2HuUarlYWRX84Mn1mCsxlZ3sgj4SSrRGb6y1jiYK/1RV45DGGpqFwCLqfE9fuAWqrIo5yTjCqwbYNWKuMTACAuk6TAXQwsef2mnft7wLs8EXHOe++cSJPHyqNUy0Zy3NNbDgKK3BBxzuPfetd8IevsQlPVlIwkvzp6Nojkgzh03nPA4kuuutGKi887oIbmGX+mdkUJNZCfbwG4TPAegJ9hjcNue/vbX0aOHNsxZuQfv3wz/M4zN+h/OJVdaCPO2+w0xqRIbXDSXTPD54CvAf2WPe6Zf9iV+o/xvxsba6K4hyExMpLvr4mi8sQDCx77Ajs1U5u8qrGLTbmJLNRhlhxNqTvBVZsDsPJtP5AxKtMN3BoXMLKMqpN2gpfqkgKY/9GxZIhMus5Dp5lkLKdaYxcUlSUOg06ok0GZeOAlx7K0StsIvqIcsOUXpBmTVw7/qTw0/jkfpJIcel1qDKxe44gZRSrIY9ahtMiSmpWJgVfDS9VIgVW+pRor2dS2hK8Y8dhhAiOrOtqX/xepGOzSQWV1K+9HtRTYrU5llSt3g6uQAht3UFlt9vsscJXhMMu3DKz4yHvbC6kIwTQfU1n11mhsB18RDhcwsvojv55ZXCU47MXIHFRegUr0mOVHWhaQtoRI6xM3zZM05qHxIbjWV8PRrDMXbOwq4ludwxpmlgsMvBq1FicY8DGN2Rj5xSxwLU0EN9OYkZE7omhpbdhLY1YEu6EGaWEeC/1mypxUfjVzKxM/7TNsMC+VC8O1LofzqMzMwC0hLcthw0ZkbirPRMv2GPw1NUNub1lS9HyckRnyaKsSj/3rDean8TXXqrD8OGqWvNmixPd5ncoseVpak8O1VOao8la05Bq20WCZclpLcjLfj6bM0si9RFqPuPbnGZgpNqfzLQgn0pinRj0GkFbjsGbdmLE3Tw3fWgTtn1AzJnDo3PCtRIraHQzMWAt8d1ZIC3EYEqLlDKkcOr1IyxDMN5LKzI28CyIto+0jKrM38Fj4FuFxNpX5qzp6ZfiW4LFxXS2DGPhiu7gW4GXQl1RmcdT1IeUTX7uPgXlsfE1Qfo+DGJnLxk2cK5tgsWDMZuUL8CVzMuADZpTx74XFlUo8rmRkRpkdhlqpvOxEtYxigw8WRZkcBv2iypxWfvZ/lFnanqIyq80mrVamAidTmdkN7lcij7UnquXXuVIaL7N8wcjcjnywvSzi/C0MzG7lW9OVxWFIbDC/jX/NWhLBoqNoOWZhoXI4N83LNOZ45OqlkAIXMDDLlXuWosBmGizXTi6DYK4vqcy1m0pRPE5lphufLkGBY0yZbx+k57HaeLNsI39Jzvmp32JgxnekJk6uY2TWp+awZyPaFJTDvB3MVTOzJDQtjwGfUbNFVRt17b4fkhLf434qM7XBNI0fpyQOp2qwPDH+ssUSy6683hoPU7tH+XBKHhuaGTPVwidrABjwmXWXnZiQYN7RNObsc2sPfpSB3Ru5fjpOZnqDxow1Jd9jd5uNmS0Z5/ydjMxbVVp3BT7cK5kChzBY5tAiu7vBI5Gqw+o0Y+4rxyyZisOCX5sx+yPva/NpCGpvU5n9puNWR5GEK9xVDMx/5ePwSLLA3oyWf2ajl4IkIVjjN1Xmv/JQCFIUTPMNjfnf4JPtLgmH9mcZmf+BH80JhwSlcGdTLf+UPyyGAgmKw46TojH7lT8uCYcUHRYeTWPua+Tni8EjRcEMX1GZ+4F8ZQ7UkKL4XvdQmfkaOPIcD4ckHU5ntJwzMyMfXgEQJOmx0T9Rmc9mZp2ZmWoMRpJvbtkTBdJ0WKCDxrzW2Kmy8/EfXbRwO1AgTfHTvEZlTlvk5K3jr2+GPnTiSgDgBGmKw+1U5rMxnLLp1rvu2bzXblusu+xcvQBARJCoeBxswbKqvgz+a61wSLjAxlRjRivHrVL0qE22KLwTJC2Y7xNG5tVvi8GjtAL/No2Z9e28cKURwTVU5nXku/3L43pgP6plVuBzbZBySCFYbaQqs+shuDKIAGjf6Esqc7vB69CG9D2AJa7+oIPG3DblUaglJwV6bPBaB0llfrFjExSpicNiD5E0NebYn4vBJybAMSOpxjxXftXuJC2P2R4hI3PdeA8KJF1g8U8ZlPluh4tLymGF7xiY9UtCUnJY+Hcqs/6nNqTssdhXVOa88lyk7GTatxiY9ZFrwqWE8xgs65QfTJeSYJ3xasz6Os+HQ0L/+4DGrFcdvRp8Og4H0Zj3gc/WRJIRTPNR9mnYDzUkW8OQoMx75Ud9HdJ1/V5mzDzjnvAJYaUJpnln/KwmSPkk1pn1Fv/ZAEn1fZ4x7yKvcoKU5xxLy7rI7+eCS2pDRua8RW6PAkmfQM065WUQSethWs4pR/QWQdrDsi7w4zkhSPxnZnzkX0ujQOqTMs44bmN4JM98V47cEIIpJov8ahkUKGEj01T5ypwoUMZf8yzSLmuDRyk/p+WXGb/YGhCU84nssmjktQPhBSU9l5pVFowcujrgBWUdYjllkbTXD+gBOJR3acYcMjM1kqzfslEvQFDmmb+k5Y1pDMpOx7+5//weKASl7nk7G9liMTQaRpI67qePb9qyfw1AISj7DgxZYhqDsfnnoQ+cvftqM6JZnKD8i/5FywgzVWvq9N37z9tzo0WmRrNzImiJRdvDDFlgGkNQdm6jX7vlkHUXnrEHmota4QUts4YhwaziTGOjrmwOI797//aTNpurT4FOXa3wDq1VXK/PqVVmMRibR33+3J0n7rBMP0xWnBNBK/Y4kVZlze8/dcURWy43m0Oz804ELV36/EarLNM/d1xhgZl6o9NarfCCKjyYWlnk0WiWovBOUJFOer/DUFGm3/YR70QEleqxycRolWTGDSCoXin8dQxVZIFniKsiOMzwBbWCGnyipzhUssOaHWqVoxw2OxwqWnAEzSpGOXQwBJVd8+ewYWlZySzwzcHwqG4RXM6GpRPrwbRUGvlUfxSocoe2mxiTMTZbiSL1wgIO1S7AJaQmoeTzx+640RuMJVGSn24GEVS9ODmVDAlE8oK+APq/TdP0NBg57siZ4QTVL4KtfmFdu6vBiYcAvlag741kSMk0NALJb84cCHhBDopgkefJ2C2m/Gp9QAA44MBfGSwJM43RSPKHJ46YFoAgFx3aDvuWNLWuMSV585xwgmZxWPRBkmpdZqZqTZ1/9uC52y4AwCEnPTDX+YFkUPsvpoHke1t7FJh8DT23+pBkiPZfTGNoRHauk355/oJ9l5+vfzsAqQnyUjww7wVfkYyNqNaZaWxEku/u3APw+LceqO3w/miS9RBVzcxUY6Me2RzG/fjJo+fstsK0bQ6dSs0L8lMATLv/g9+wOXbK5p/u3bMGiODfiwBY+fo3J/C/jvzw2ZvP3WGtAZi8OCeCXBUPYJ4tz3zkvbHsdOJXj5697YKAOHSlc8DUK+9z3fOf/DyuXp/43YhHLt9vs2UG1tDsvHPSlLtS8wB6zTjnIkttsP56i8w9sBcAXwi62NcASL/+s8w1zzxzD5qhFzptayu8Q34CAFZQOCDcCgAAEDwAnQEq8ADLAD5VKpJGI6KhoSUyGZBwColN3BgAX4ARBtM/5HUMO0/1X9j/zI+ZCoPzn7uf1j9mfkh3v88f3L0svEv0P/Kf3T8mPgj/gPYX+mv977gH8N/lP+h/t/499w7zBfsj+wHuxf4r9mfc1/efUA/nH+Q//PYDegJ+4/plfuJ8Jv7b/tn7Qn/r1kLyb/l/w+/SnkRsM2ihuswLSTNPJaqqqq9buijCW7e0REREKwVhLMFooRG69G05cnE6du7u3y6xMtechGNVyRI1Z/PK9zKOK5APd1y6Une5vkjMbFHgTmcD12bwhsQVR23cFOmWaLrMhbH1M+gFPlVLetEDXUiaUu3pTchn/amID1mImKuo7/UTqMDp3l/4g3HNQCt9C4nbEcn0V1YREGuXVZ8KoKRfoG89VhhqI2UT1cTn9NwprpbSYhuaRy0F4901AM37O5p/fAFgYzUqe1Pa/Bq/5tonKrCqPHj5B439HFRVGridrSQeALJVCc/L8qOCvWP8BbRtYQHRK3+WK/HeDDsVuEiSetBlVPC8VyoKN3vxe6WZ+QagOq1SyAMelgYwL/3wjVRcR1OnyjVjccVi6RX7sTJ1EPLNx1xs6uE34lu96sysQSHfEH5JHrWZpMzUDS6an62zMzGgAP7qLUJoP//2pxSNpQAFt/JOLf16v01TsVXQDMafzOZh7vxH7bX3JhTJrOI6+m8+iDBBQzlI1JBlESmQH1fvuAkEkjpEDhxr9okT4pPrNubzc4J+5zza99XEfyOunsiTEOAEj/JOnf5JwHRu4KemRPR83yCtQDaVXCVTHmN+or8HYMJaXwMj9McrJYj0G3u8HgOsj1Cv+MgSpbptG39eE3Hr/DSlNcVWdgK35zo6FxI3R34T6+AMH7SVZt626n10HOg3C/xsDwu6SbO1ydpY8YV1qCiuEaxgNY19ADL/knBieKzcuOiJquQxAu7iv6c2nRxc0Xto4fO1zMTLA6y0T9gUJJQ25BcIoOE6HCLKKro91NzowU/6lhFxnTskZ9NMpJUVyGe61ERqJcAcbsceIF0MwEQFSeBBL7rvkzHGHwd5OfUxTAQpN8ojxVZcraGBsUiEZpr0cCKR38J9Bc3N5Ek+7SrdwCA2ujI+wwqvW2ciqAfYulke7AhY7jgQOSCjg6nM09zereeWCo9SIdenXf1HYTLacrpvv3DS9zUiI6j1X73rG1s5Bwacpa5WV/Ph4rPYoLMiDuq+tSFYDNBZIPZ1F4Nd7jGJHvjfoYBkCtb3PLvC00Y0GR9vP5v1tgBclQJeAJn+SczOw+GY15WdvrylwfP/WXGnQvLvhSFX/Fxf4cZZX7i/GGKmCJ/+Ib+Mdg3i7f/B+DyO2rZbV/x+O0+h2axgWMuU1iEnYP7/erEdwo2jNzAo+rOGv8UAl7P8JC9ds/CIibKTxyYL4BRaQkui6eKNh07MpJ6w8PFILViGk0X50HYj6Yh/bvVHj74VYuCl2XlhPAn2OCDswRCYIjZrcUj4SM3YvY/F/nkqtN4EiHQ6M85cFfICptag3dABlirXouz8XL/AWpfBzrVDBUWX2y3hYL4WOjzxCq2XUyaRvIbALpePRZs7woOxk9WHX6IyGssDPTvLGYJrj9cKeqBkhJnK0ifegUa1lZ4ElnbmpzttOYYQLF1O2a6/GWmBqGZ+rQYV5fSU+gHfOYLLcaFRwkCBEWd+VCEZ/JOCPdwCrnQYM41wLtVkT+qcjVOrV5ewiSunhtrwYmVb6nUvZSwt7CkGb56wTyYqtHh1WAVt15I0/P6pofOPcvxxHI8Ba8bZLfcMLh4P44agIndPXmcUAy6wvrmMAay1tNAkcSkR6mhl8kfBbuwv45BKK/8I8yWEyyYLypqIUGx458ztQ4eLSKMR97wXQMBYEvo9adym4mW7SGvHAZeCMqdyYbfHSUW/qb9LmW/yMXlb3vAJw8AUbz97ai64LmiDP9l6jIOY74d6Ej/y4V04X+/9eCUKTFt/k/bxrz+QVVbJ5Kl06z4V2RZL1bG4rne1LqgD9uUk7UM6n8k6d/knB/AbZbP3jARCbIGgVajZJMrPPDZBpDfLzg3m/PN0e7sGUOPekcFHXbKwSKJayeYrvaik9jDn01eYEPVM49SiCS0X9iR1uvrP66/CPSl1xd0a4ftDnRNVuLnZnm9vRFPNuGQDWol1PIFxI5V4iVu1e4TN1kqy/zuwjVIDBbfhPsuKfLI+GIH7dtZza28N0vT86GzLOX5ooD6PnhxChVMgyglFb3yLGtD9mBd5ODseoainAJNFv+Z30FPBHuhENAaGiPF9E9uWOFopvpkygP+Nbz2puhorptiZZqG/q2bWp21Ea7ykpQHVRsz28vWff0Atx3/IJ7kZtnjKcwBlFpVp7h/5JwYk21k1JHC+fRAr+ZvB0yajdwPzcaJi4QCpO2JpbprkZG5JdEVBpvE3gyg1o/xhSN7OdgtzMlLwWkhJorQEX7kF/KDoI2Wj68IQvNi7v5zStjFydYttb8C2M16RLhUKdEB+W+XDo1F3QkqMsobUO/D6wv3UF31usDNZX/41FfxBS5N3jFOfWE/LyjtNQwfOFXKobMDVOGo/jy7AwvGM+Vlo0mObr5Yg+zKJjtLCqbLgAE7j////anj/mR9k89thc4QKuqVXfrLjToXl3wpCqrfAudPWpjAa0mLSrw6p9kld8WAdd83TSMwujGZgtNpNuwULZJSqG9I/xOsuBQdPQR2uUGJ7G5ZL/Gf8uJTOV174jg/tfByQwvIpjZ6Sjx4BnkIxqyiRdr9GI2uH4JMSgWUBfL9L+SJqyq/76QpNx1lzRbldIiMxLM9XQPIPWrSd9DsGMjKmAA0mMX5HG8wJkWTnQdO7Y9YM/4BuQfOSVmPEnvZtRY6tkIrF1/Jlfv8mVCESMkMkG80ZtMEGxWv4p0JmHyYY/eqUw+ud1xF/q0lv/Ltj32VkPdDQbeZG0OYnSVClHWOCegXrjv6PULXJgl93WhmvYH6pW0yvFgifboQ6omkmdoRHNq3w3GkeJfKC4vxTyF38WUK8E7fxR//+ANh6BMj8QXlnpew7+zAQ4HUTYce+3/CmGIzWYo0jHPlYXDYi93cgWgMyFOFB9KJrD5dPAdPStuHUD4OMzjDajC7X5oY+i0cjscKIIfyZUF3N2191JkjipkT3e8xNCM8bs38VlQ3f8h13N3VOAvjPvBslNLmJSNdy4BNHHzlYfHu7+c/OlmQiYPjmxLmF/+yPh5NSe/Qy+THiaSNzUOHv4l3Gd+j8woDHpK6v6GdleUnf4UiqLeFBMhGL/bppok4CP9O1oiaN2gvtCNd7EnCj/LhpuKCC3BU4vnHx7zwItm9lNZ112VpFQ2wswf2wXgBuBEhu8GtznkwyOcQlHqrZ74u2ziymBf9tUAuEyispWboYv8uK5oVTqkQQfUa8W0UWLGUrX6Xu9G0ZNXum0mOvwIbzoAU4ZDXyPcLnp8H5By19klpubBuXg19z4LVR4UQ7pRTsAQX8mVD6v0/QuthpdlkHyqy6X7/CtGswlry/Aiaf6pyCypGxv8fWUVmB1NUcbVipy78X+8wGZy6RN1vnf4bDITTooSnaFkoMXLnUo6/TJKP3YOa7lw4t9EKGRmBZbGHkxAYSPZwrSyJcbOSsJ0C0/kyogvrZ1pkof//cP//9ve///t/sgiW89+wEI+X8mWP++FQAAAA=",
    "i_heart_locket": "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",
    "locket_red": "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",
    "locket_tile": "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",
    "help_1": "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",
    "help_2": "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",
    "career_2": "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",
    "parent_2": "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",
}


@app.route("/s/<name>.webp")
def lumen_sticker(name):
    from flask import Response
    data = _STICKERS_B64.get(name)
    if not data:
        return ("", 404)
    resp = Response(base64.b64decode(data), mimetype="image/webp")
    resp.headers["Cache-Control"] = "public, max-age=604800, immutable"
    return resp
# LUMEN-STICKERS-END


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    logger.info("Starting Locket Mini (%s) on http://0.0.0.0:%d", APP_VERSION_STRING, port)
    try:
        from waitress import serve        # pip install waitress  (much faster than Werkzeug's dev server)
        logger.info("Serving with waitress (16 threads)")
        serve(app, host="0.0.0.0", port=port, threads=16, connection_limit=400,
              channel_timeout=120, ident="Lumen")
    except ImportError:
        logger.info("waitress not installed -> Werkzeug threaded server (pip install waitress for speed)")
        app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
