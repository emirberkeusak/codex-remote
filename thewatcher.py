#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import atexit
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# === Yeni: Excel için pandas ===
try:
    import pandas as pd
except Exception:
    pd = None  # pandas yoksa Country eşlemesi devre dışı; Country: '-' gönderilir

# =========================
# Varsayılan Ayarlar
# =========================
DEFAULT_BASE_URL = "https://e38ce8fd14d3d5a75199844a241806d4.chainupcloud.info"
DEPOSIT_API_ENDPOINT = f"{DEFAULT_BASE_URL}/admin-api/depositCrypto"
KEEPALIVE_PAGE_URL = f"{DEFAULT_BASE_URL}/depositCrypto"
GET_USER_INFO_ENDPOINT = f"{DEFAULT_BASE_URL}/admin-api/get_user_info"
TARGET_COOKIE_DOMAIN = "chainupcloud.info"  # tarayıcıdan çekerken filtre
COUNTRY_MAP_XLSX = "country_map.xlsx"       # Aynı dizinde

# Telegram — CLI ile de geçebilirsiniz
DEFAULT_TELEGRAM_BOT_TOKEN = "7895901821:AAEJs3mmWxiWrRyVKcRiAxMN2Rn4IpiyV0o"  # CLI --token ile verin
DEFAULT_TELEGRAM_CHAT_ID = "-4678220102"    # CLI --chat-id ile verin
DEFAULT_TELEGRAM_THREAD_ID: Optional[int] = None  # opsiyonel

# Poll aralığı
POLL_INTERVAL_SECONDS = 60
REQUEST_TIMEOUT = 15

# Retry
RETRY_TOTAL = 5
RETRY_BACKOFF_FACTOR = 0.6
RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)
RETRY_ALLOWED_METHODS = frozenset(["GET", "POST"])

# State & Log
STATE_FILE = "state.json"
LOG_FILE = "deposit_watcher.log"
LOG_LEVEL = logging.INFO
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3

stop_event = threading.Event()
cookie_file_mtime: Optional[float] = None
raw_cookie_header: str = ""  # "name=value; ..." biçiminde
auto_cookie_source: Optional[str] = None  # edge|chrome|firefox|brave|opera

# === Yeni: Country kodu eşlemesi bellekte tutulur ===
_country_code_to_names: Dict[str, List[str]] = {}
_country_map_mtime: Optional[float] = None


def setup_logging() -> None:
    from logging.handlers import RotatingFileHandler
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout); ch.setLevel(LOG_LEVEL); ch.setFormatter(fmt); logger.addHandler(ch)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
    fh.setLevel(LOG_LEVEL); fh.setFormatter(fmt); logger.addHandler(fh)


def load_cookies_from_file(cookie_file: str) -> Tuple[Dict[str, str], str]:
    if not os.path.exists(cookie_file):
        raise FileNotFoundError(f"Cookie dosyası yok: {cookie_file}")
    with open(cookie_file, "r", encoding="utf-8") as f:
        data = f.read().strip()
    if data.lower().startswith("cookie:"):
        data = data[len("cookie:"):].strip()
    # normalize
    data = " ".join(line.strip() for line in data.splitlines())
    data = data.replace("; ", ";").strip().strip(";")
    cookie_dict: Dict[str, str] = {}
    if data:
        for part in data.split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            k, v = part.split("=", 1)
            cookie_dict[k.strip()] = v.strip()
    return cookie_dict, data


def _get_cookie_val(cd: Dict[str, str], *keys: str, default: str = "") -> str:
    for k in keys:
        if k in cd and cd[k]:
            return cd[k]
    return default


def build_session(cookie_dict: Dict[str, str]) -> requests.Session:
    """
    Session + retry kur; hem cookie jar’a yükle hem de kritik admin header’ları set et.
    """
    sess = requests.Session()
    retry = Retry(total=RETRY_TOTAL, backoff_factor=RETRY_BACKOFF_FACTOR,
                  status_forcelist=RETRY_STATUS_FORCELIST, allowed_methods=RETRY_ALLOWED_METHODS,
                  raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    sess.mount("http://", adapter); sess.mount("https://", adapter)

    # Cookie jar
    sess.cookies.clear()
    for k, v in cookie_dict.items():
        sess.cookies.set(k, v, domain="e38ce8fd14d3d5a75199844a241806d4.chainupcloud.info")

    # Header bileşenleri
    csrf_token = _get_cookie_val(cookie_dict, "csrfToken", "csrftoken", default="")
    admin_token = _get_cookie_val(cookie_dict, "admin-token", default="")
    admin_broker = _get_cookie_val(cookie_dict, "admin-broker-id-co", default="")
    admin_source = _get_cookie_val(cookie_dict, "admin-source", default="admin")
    lan = _get_cookie_val(cookie_dict, "lan", default="en_US")
    servicelang = _get_cookie_val(cookie_dict, "servicelanguage", default="en-US")
    admin_language = _get_cookie_val(cookie_dict, "admin-language", default=lan)
    swap_broker_id = _get_cookie_val(cookie_dict, "swap-broker-Id", default="")

    common_headers = {
        # Tarayıcıya benzer başlıklar
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": DEFAULT_BASE_URL,
        "Referer": KEEPALIVE_PAGE_URL,
        "Accept-Language": f"{servicelang},en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",

        # Admin uçlarının beklediği spesifik başlıklar
        "admin-token": admin_token,
        "admin-broker-id-co": admin_broker,
        "admin-source": admin_source,
        "lan": lan,
        "servicelanguage": servicelang,
        "admin-language": admin_language,
        "swap-broker-Id": swap_broker_id,

        # Bazı sistemlerde isim duyarlı olabilir
        "csrfToken": csrf_token,
        "X-CSRF-Token": csrf_token,
    }
    # Boşları ayıkla
    for k in list(common_headers.keys()):
        if common_headers[k] is None or common_headers[k] == "":
            common_headers.pop(k, None)

    sess.headers.update(common_headers)
    return sess


def _api_headers() -> Dict[str, str]:
    """
    Her POST/GET çağrısında ham Cookie’yi ve XMLHttpRequest işaretini garantiye al.
    Session.headers zaten diğer admin başlıklarını içeriyor.
    """
    return {
        "Cookie": raw_cookie_header,
        "X-Requested-With": "XMLHttpRequest",
    }


def try_keepalive(sess: requests.Session) -> None:
    """
    Oturumu sıcak tutmak için UI sayfasına GET at.
    """
    try:
        r = sess.get(KEEPALIVE_PAGE_URL, timeout=REQUEST_TIMEOUT, headers={"Cookie": raw_cookie_header})
        if r.status_code == 401:
            logging.warning("Keepalive 401: oturum düşmüş olabilir.")
    except Exception as e:
        logging.debug(f"Keepalive hata: {e}")


def fetch_deposits(sess: requests.Session) -> List[Dict]:
    """
    Admin API’den en güncel deposit kayıtlarını çek.
    """
    payload = {"page": 1, "size": 200, "pageSize": 200, "limit": 200}
    resp = sess.post(DEPOSIT_API_ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT, headers=_api_headers())
    if resp.status_code == 401:
        raise PermissionError("401 Unauthorized (cookie/oturum).")
    resp.raise_for_status()
    data = resp.json()
    code = data.get("code")
    ok_code = (code == "0" or code == 0)
    if not ok_code:
        raise ValueError(f"API 'code' başarısız: {code}, msg={data.get('msg')}")
    inner = data.get("data") or {}
    lst = inner.get("depositCryptoMapList") or []
    if not isinstance(lst, list):
        raise ValueError("depositCryptoMapList beklenmedik tip")
    return lst


def load_state(state_file: str) -> Dict:
    if not os.path.exists(state_file):
        return {"processed_ids": [], "last_seen_created_at": 0, "bootstrap_done": False}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"processed_ids": [], "last_seen_created_at": 0, "bootstrap_done": False}


def save_state(state_file: str, state: Dict) -> None:
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, state_file)


def epoch_ms_to_local_iso(ms: int) -> str:
    """
    Yeni: timezone adını kaldırdık (sadece YYYY-MM-DD HH:MM:SS)
    """
    try:
        dt = datetime.fromtimestamp(ms / 1000, tz=None).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(ms)


def build_telegram_message(deposit: Dict) -> str:
    """
    Mesaj üretimi. Country satırı eklendi (symbol'ün üstünde).
    deposit içine (akışta) 'countryNameResolved' ve 'countryCode' alanlarını koyuyoruz.
    """
    # Yeni: Country adı satırı (Excel’den çözümlenen)
    country_name = str(deposit.get("countryNameResolved", "-"))

    symbol = str(deposit.get("symbol", "-"))
    amount = str(deposit.get("amount", "-"))
    usdt_amount = str(deposit.get("usdtAmount", "-"))
    uid = str(deposit.get("uid", "-"))
    status_desc = str(deposit.get("statusDesc", "-"))
    created_at = int(deposit.get("createdAt", 0) or 0)
    when_str = epoch_ms_to_local_iso(created_at) if created_at else "-"

    # get_user_info ile doldurulmuş countryCode (yoksa '-')
    country_code_raw = str(deposit.get("countryCode", "-"))

    return "\n".join([
        "🟢 Yeni Deposit",
        "",
        f"Country: {country_name}",
        f"symbol: {symbol}",
        f"amount: {amount}",
        "",
        f"usdtAmount: {usdt_amount}",
        f"uid: {uid}",
        f"statusDesc: {status_desc}",
        f"time: {when_str}",
        f"countryCode: {country_code_raw}",
    ])


def send_telegram_message(bot_token: str, chat_id: str, text: str,
                          thread_id: Optional[int] = None,
                          disable_web_page_preview: bool = True) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": disable_web_page_preview}
    if thread_id is not None:
        payload["message_thread_id"] = int(thread_id)
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logging.error(f"Telegram hata kodu: {r.status_code} - {r.text}")
            return False
        j = r.json()
        if not j.get("ok", False):
            logging.error(f"Telegram 'ok': False -> {j}")
            return False
        return True
    except Exception as e:
        logging.error(f"Telegram gönderim hatası: {e}")
        return False


def bootstrap_state_with_current_list(state: Dict, deposits: List[Dict]) -> Dict:
    if not deposits:
        state["bootstrap_done"] = True; save_state(STATE_FILE, state); return state
    try:
        max_created = max(int(x.get("createdAt", 0) or 0) for x in deposits)
    except Exception:
        max_created = 0
    initial_ids = []
    for item in deposits[:1000]:
        _id = item.get("id")
        try:
            initial_ids.append(int(_id))
        except Exception:
            initial_ids.append(str(_id))
    deduped, seen = [], set()
    for _id in initial_ids:
        if _id in seen: continue
        seen.add(_id); deduped.append(_id)
    state["processed_ids"] = deduped[-5000:]
    state["last_seen_created_at"] = max_created
    state["bootstrap_done"] = True
    save_state(STATE_FILE, state)
    return state


def detect_new_deposits(state: Dict, deposits: List[Dict]) -> List[Dict]:
    last_seen_created = int(state.get("last_seen_created_at", 0) or 0)
    processed_ids = set(state.get("processed_ids", []))
    candidates: List[Dict] = []
    for item in deposits:
        _id = item.get("id")
        created_at = int(item.get("CreatedAt", item.get("createdAt", 0)) or 0)  # CreatedAt guard
        try:
            normalized_id = int(_id)
        except Exception:
            normalized_id = str(_id)
        is_new_by_id = normalized_id not in processed_ids
        is_new_by_time = created_at > last_seen_created
        if is_new_by_id or is_new_by_time:
            candidates.append(item)
    candidates.sort(key=lambda x: int(x.get("createdAt", 0) or 0))
    return candidates


def update_state_after_send(state: Dict, sent_items: List[Dict]) -> Dict:
    if not sent_items: return state
    processed_ids = state.get("processed_ids", [])
    last_seen_created = int(state.get("last_seen_created_at", 0) or 0)
    for item in sent_items:
        _id = item.get("id")
        created_at = int(item.get("createdAt", 0) or 0)
        try:
            normalized_id = int(_id)
        except Exception:
            normalized_id = str(_id)
        processed_ids.append(normalized_id)
        if created_at > last_seen_created:
            last_seen_created = created_at
    if len(processed_ids) > 6000:
        processed_ids = processed_ids[-5000:]
    state["processed_ids"] = processed_ids
    state["last_seen_created_at"] = last_seen_created
    save_state(STATE_FILE, state)
    return state


def handle_signals():
    def _handler(signum, frame):
        logging.info(f"Sinyal alındı ({signum}). Bot durduruluyor...")
        stop_event.set()
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def _compose_cookie_header_from_items(items: List[Tuple[str, str]]) -> str:
    """
    Önemli anahtarları öne alarak deterministik 'Cookie' header’ı üret.
    """
    priority = [
        "JSESSIONID",
        "admin-token",
        "admin-broker-id-co",
        "csrfToken",
        "lan",
        "servicelanguage",
        "admin-language",
        "admin-source",
        "swap-broker-Id",
        "other_lan",
        "info",
        "isShowDownLoadDialogEntrustedQuery",
        "isShowDownLoadDialogAssetsQuery",
    ]
    d: Dict[str, str] = {}
    for k, v in items:
        if not k or v is None: continue
        d[k] = v
    ordered: List[str] = []
    seen = set()
    for k in priority:
        if k in d and k not in seen:
            ordered.append(f"{k}={d[k]}")
            seen.add(k)
    for k, v in d.items():
        if k in seen: continue
        ordered.append(f"{k}={v}")
        seen.add(k)
    return "; ".join(ordered)


def _read_cookies_from_browser(domain_filter: str, source: str) -> Optional[str]:
    """
    browser-cookie3 ile tarayıcıdan cookie oku, hedef domain için header’a çevir.
    source: edge|chrome|firefox|brave|opera
    """
    try:
        import browser_cookie3 as bc3
    except Exception as e:
        logging.error("browser-cookie3 yok veya import edilemedi. 'pip install browser-cookie3' kurun.")
        return None

    try:
        if source == "edge":
            cj = bc3.edge()
        elif source == "chrome":
            cj = bc3.chrome()
        elif source == "firefox":
            cj = bc3.firefox()
        elif source == "brave":
            cj = bc3.brave()
        elif source == "opera":
            cj = bc3.opera()
        else:
            logging.error(f"Geçersiz --auto-cookie kaynağı: {source}")
            return None
    except Exception as e:
        logging.error(f"Tarayıcı cookie okunamadı ({source}): {e}")
        return None

    pairs: List[Tuple[str, str]] = []
    for c in cj:
        dom = (c.domain or "").lstrip(".")
        if domain_filter in dom:
            pairs.append((c.name, c.value))

    if not pairs:
        logging.warning(f"Tarayıcıdan {domain_filter} için cookie bulunamadı.")
        return None

    return _compose_cookie_header_from_items(pairs)


def write_cookies_txt_if_changed(cookie_path: str, new_cookie_header: str) -> bool:
    """
    cookies.txt içeriği değiştiyse yazar ve True döner; değilse False.
    (TEK SATIR yazım garanti)
    """
    old = ""
    if os.path.exists(cookie_path):
        try:
            with open(cookie_path, "r", encoding="utf-8") as f:
                old = f.read().strip()
            if old.lower().startswith("cookie:"):
                old = old[len("cookie:"):].strip()
            old = old.replace("; ", ";").strip().strip(";")
        except Exception:
            old = ""
    new_norm = new_cookie_header.replace("; ", ";").strip().strip(";")
    if old == new_norm:
        return False
    with open(cookie_path, "w", encoding="utf-8") as f:
        f.write(new_norm)
    return True


def reload_cookies_and_session(cookie_path: str) -> requests.Session:
    """
    cookies.txt → cookie_dict + raw header → session + header’lar
    """
    global cookie_file_mtime, raw_cookie_header
    cookie_dict, raw = load_cookies_from_file(cookie_path)
    raw_cookie_header = raw
    cookie_file_mtime = os.path.getmtime(cookie_path)
    sess = build_session(cookie_dict)
    logging.info("Cookie yeniden yüklendi ve session tazelendi.")
    return sess


def maybe_refresh_cookie_from_browser(cookie_path: str, bot_token: str, chat_id: str, thread_id: Optional[int]) -> Optional[requests.Session]:
    """
    --auto-cookie açık ise tarayıcıdan oku; değiştiyse txt’yi yaz, session’ı yenile,
    Telegram’a “Cookie otomatik güncellendi (tarayıcı)” bildirimi yolla. Yeni session döndürür.
    """
    global auto_cookie_source
    if not auto_cookie_source:
        return None
    new_header = _read_cookies_from_browser(TARGET_COOKIE_DOMAIN, auto_cookie_source)
    if not new_header:
        return None
    changed = write_cookies_txt_if_changed(cookie_path, new_header)
    if not changed:
        return None
    sess = reload_cookies_and_session(cookie_path)
    send_telegram_message(bot_token, chat_id, "🔑 Cookie otomatik güncellendi (tarayıcı).", thread_id)
    logging.info("Cookie tarayıcıdan çekildi ve cookies.txt güncellendi (OTOMATIK).")
    return sess


# === Yeni: country_map.xlsx yükleyici ===
def _normalize_code_str(s: str) -> str:
    """
    ' 62 ' , '+62', '62.0' vb. -> '62'
    """
    if s is None:
        return ""
    ss = str(s).strip()
    if not ss:
        return ""
    # '+' ve boşlukları at, kesir varsa noktadan önceyi al
    ss = ss.replace("+", "").strip()
    if "." in ss:
        left = ss.split(".", 1)[0]
        if left.isdigit():
            return left
    # son temizlik
    return "".join(ch for ch in ss if ch.isdigit())


def _load_country_map_if_needed() -> None:
    """
    country_map.xlsx değiştiyse veya daha önce yüklenmediyse belleğe al.
    Beklenen sütun adları: 'COUNTRY' ve 'COUNTRY CODE'
    Büyük/küçük harf duyarsız, boşluklar tolere edilir.
    """
    global _country_code_to_names, _country_map_mtime
    try:
        if pd is None:
            return
        if not os.path.exists(COUNTRY_MAP_XLSX):
            return
        mtime = os.path.getmtime(COUNTRY_MAP_XLSX)
        if _country_map_mtime is not None and mtime == _country_map_mtime:
            return  # zaten güncel
        df = pd.read_excel(COUNTRY_MAP_XLSX)
        # Sütun adlarını normalize et
        cols = {str(c).strip().lower(): c for c in df.columns}
        # Olası isim varyasyonlarını destekle
        cand_country = None
        cand_code = None
        for key, orig in cols.items():
            k = key.replace(" ", "")
            if cand_country is None and (k in ("country",) or "country" == k):
                cand_country = orig
            if cand_code is None and (k in ("countrycode", "code", "dialcode")):
                cand_code = orig
        # Eğer tam eşleşme bulunamazsa kaba arama:
        if cand_country is None:
            for c in df.columns:
                if str(c).strip().lower().startswith("country"):
                    cand_country = c
                    break
        if cand_code is None:
            for c in df.columns:
                if "code" in str(c).strip().lower():
                    cand_code = c
                    break

        if cand_country is None or cand_code is None:
            logging.warning("country_map.xlsx sütunları bulunamadı (COUNTRY / COUNTRY CODE). Country eşlemesi devre dışı.")
            _country_code_to_names = {}
            _country_map_mtime = mtime
            return

        mapping: Dict[str, List[str]] = {}
        for _, row in df.iterrows():
            name = str(row.get(cand_country, "")).strip()
            code_raw = _normalize_code_str(row.get(cand_code, ""))
            if not name or not code_raw:
                continue
            mapping.setdefault(code_raw, [])
            if name not in mapping[code_raw]:
                mapping[code_raw].append(name)

        _country_code_to_names = mapping
        _country_map_mtime = mtime
        logging.info(f"Country map yüklendi ({len(mapping)} kod).")
    except Exception as e:
        logging.warning(f"country_map.xlsx okunamadı: {e}")
        _country_code_to_names = {}


def _parse_second_plus_code(country_code_field: str) -> str:
    """
    '+360+62' -> '62' (ikinci '+' sonraki bölüm)
    '+62' -> '62' (tek + ise tümünü alalım)
    """
    if not country_code_field:
        return ""
    s = str(country_code_field).strip()
    parts = s.split("+")
    # örn ['', '360', '62'] -> ikinci artıdan sonraki = parts[-1] (eğer ≥2 + varsa)
    if len(parts) >= 3:
        tail = parts[-1]
    elif len(parts) == 2:
        tail = parts[-1]
    else:
        tail = s
    return _normalize_code_str(tail)


def resolve_country_names_from_code(code_str: str) -> str:
    """
    '62' -> 'Indonesia' (ör.), birden fazla varsa 'Afghanistan/Pakistan' gibi.
    """
    if not code_str:
        return "-"
    _load_country_map_if_needed()
    if not _country_code_to_names:
        return "-"
    names = _country_code_to_names.get(code_str, [])
    if not names:
        return "-"
    return "/".join(names)


def fetch_user_info(sess: requests.Session, uid: int) -> Dict[str, Optional[str]]:
    """
    get_user_info çağrısı: UID ile countryCode çek.
    - Ana payload olarak {"uid": uid} dener, başarısız olursa {"id": uid} dener.
    - Başlıklar: session.headers + _api_headers() + Referer (userDetail?id=<uid>)
    Dönen yapı: {"countryCode": "...", "ip": "..."}
    """
    headers = _api_headers().copy()
    headers["Referer"] = f"{DEFAULT_BASE_URL}/userDetail?id={uid}"
    payloads = [{"uid": uid}, {"id": uid}]

    last_error: Optional[Exception] = None
    for pl in payloads:
        try:
            r = sess.post(GET_USER_INFO_ENDPOINT, json=pl, timeout=REQUEST_TIMEOUT, headers=headers)
            if r.status_code == 401:
                raise PermissionError("401 Unauthorized (get_user_info).")
            r.raise_for_status()
            data = r.json()
            code = data.get("code")
            if not (code == "0" or code == 0):
                raise ValueError(f"get_user_info 'code' başarısız: {code}, msg={data.get('msg')}")
            inner = data.get("data") or {}
            user = inner.get("user") or {}
            cc = user.get("countryCode")  # örn '+360+62'
            ip = inner.get("ip") or ""
            return {"countryCode": cc, "ip": ip}
        except Exception as e:
            last_error = e
            continue

    if last_error:
        logging.warning(f"get_user_info hatası (uid={uid}): {last_error}")
    return {"countryCode": None, "ip": None}


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="ChainUp Deposit Watcher (auto-cookie + user country enrich)")
    parser.add_argument("--cookie-file", default="cookies.txt", help="Cookie dosyası (tek satır, Request Headers → Cookie)")
    parser.add_argument("--token", default=DEFAULT_TELEGRAM_BOT_TOKEN, help="Telegram bot token")
    parser.add_argument("--chat-id", default=DEFAULT_TELEGRAM_CHAT_ID, help="Telegram chat id")
    parser.add_argument("--thread-id", default=DEFAULT_TELEGRAM_THREAD_ID, type=int, nargs="?", help="Opsiyonel Telegram topic/thread id")
    parser.add_argument("--interval", default=POLL_INTERVAL_SECONDS, type=int, help="Sorgu aralığı saniye")
    parser.add_argument("--auto-cookie", choices=["edge","chrome","firefox","brave","opera"], help="Tarayıcıdan cookie otomatik çek")
    args = parser.parse_args()

    bot_token = args.token
    chat_id = args.chat_id
    thread_id = args.thread_id
    interval = max(10, int(args.interval))
    global auto_cookie_source
    auto_cookie_source = args.auto_cookie

    # Başlangıçta country_map.xlsx yükle
    _load_country_map_if_needed()

    # Eğer --auto-cookie verildiyse önce tarayıcıdan okumayı dene ve dosyaya yaz
    if auto_cookie_source:
        logging.info(f"--auto-cookie aktif ({auto_cookie_source}). Tarayıcıdan cookie çekilecek.")
        try:
            header_from_browser = _read_cookies_from_browser(TARGET_COOKIE_DOMAIN, auto_cookie_source)
            if header_from_browser:
                if write_cookies_txt_if_changed(args.cookie_file, header_from_browser):
                    logging.info("İlk başlatmada cookie tarayıcıdan yazıldı (cookies.txt).")
                    send_telegram_message(bot_token, chat_id, "🔑 Cookie otomatik güncellendi (tarayıcı).", thread_id)
        except Exception as e:
            logging.warning(f"İlk tarayıcı cookie yazımı başarısız: {e}")

    # İlk cookie yükle
    try:
        sess = reload_cookies_and_session(args.cookie_file)
    except Exception as e:
        logging.error(f"Cookie yüklenemedi: {e}")
        sys.exit(1)

    # İlk Country map mtime’ı not et
    global _country_map_mtime

    def on_exit():
        try:
            logging.info("Bot durduruluyor (exit).")
            send_telegram_message(bot_token, chat_id, "⛔ Bot durduruldu", thread_id)
        except Exception:
            pass
    atexit.register(on_exit)

    # Sinyaller
    def _handler(signum, frame):
        logging.info(f"Sinyal alındı ({signum}). Bot durduruluyor...")
        stop_event.set()
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    logging.info("Bot başlatıldı. Depositleri izlemeye başladı.")
    send_telegram_message(bot_token, chat_id, "✅ Bot başlatıldı", thread_id)

    state = load_state(STATE_FILE)

    # İlk bootstrap ve keepalive
    try:
        try_keepalive(sess)
        # Oturumun doğru olduğundan emin olmak için (auto-cookie varsa) bir kez daha tarayıcıdan güncelleme dene
        refreshed = maybe_refresh_cookie_from_browser(args.cookie_file, bot_token, chat_id, thread_id)
        if refreshed is not None:
            sess = refreshed
        initial_list = fetch_deposits(sess)
        logging.info(f"İlk listede {len(initial_list)} kayıt bulundu.")
        if not state.get("bootstrap_done", False):
            state = bootstrap_state_with_current_list(state, initial_list)
            logging.info("Bootstrap tamamlandı; yeni kayıtlar bildirilecek.")
    except PermissionError as e:
        logging.error(f"401 (oturum) - cookie güncel değil: {e}")
        sys.exit(2)
    except ValueError as e:
        logging.error(f"İlk fetch hata: {e} (muhtemelen code=10004 / oturum reddi)")
    except Exception as e:
        logging.error(f"İlk fetch sırasında beklenmeyen hata: {e}")

    while not stop_event.is_set():
        loop_start = time.time()

        # --auto-cookie: tarayıcıdan cookie tazele (her turda dener, değişmişse uygular)
        try:
            refreshed = maybe_refresh_cookie_from_browser(args.cookie_file, bot_token, chat_id, thread_id)
            if refreshed is not None:
                sess = refreshed
        except Exception as e:
            logging.debug(f"Tarayıcıdan cookie tazeleme hatası: {e}")

        # cookies.txt manuel değiştiyse yeniden yükle (ör. sen elle değiştin)
        try:
            current_mtime = os.path.getmtime(args.cookie_file)
            if cookie_file_mtime is None or current_mtime != cookie_file_mtime:
                logging.info("Cookie dosyasında değişiklik algılandı (MANUEL).")
                sess = reload_cookies_and_session(args.cookie_file)
                send_telegram_message(bot_token, chat_id, "📝 Cookie manuel güncellendi (dosya).", thread_id)
        except Exception as e:
            logging.debug(f"Cookie mtime kontrol hatası: {e}")

        # Country map değiştiyse yeniden yükle (opsiyonel izleme)
        try:
            if os.path.exists(COUNTRY_MAP_XLSX):
                cm_m = os.path.getmtime(COUNTRY_MAP_XLSX)
                if _country_map_mtime is None or cm_m != _country_map_mtime:
                    _load_country_map_if_needed()
        except Exception as e:
            logging.debug(f"country_map.xlsx mtime kontrol hatası: {e}")

        try_keepalive(sess)

        new_items_sent: List[Dict] = []
        try:
            deposit_list = fetch_deposits(sess)
            candidates = detect_new_deposits(state, deposit_list)
            if candidates:
                for item in candidates:
                    # === Yeni: UID'den countryCode çek ===
                    uid_val = item.get("uid")
                    try:
                        uid_int = int(uid_val)
                    except Exception:
                        uid_int = None

                    cc_raw = "-"
                    country_name_resolved = "-"

                    if uid_int is not None:
                        info = fetch_user_info(sess, uid_int)  # {"countryCode": "...", "ip": "..."}
                        if info and info.get("countryCode"):
                            cc_raw = str(info.get("countryCode"))
                            dial_tail = _parse_second_plus_code(cc_raw)  # '62'
                            country_name_resolved = resolve_country_names_from_code(dial_tail)

                    # Mesajda kullanılmak üzere item'ı zenginleştir
                    item["countryCode"] = cc_raw
                    item["countryNameResolved"] = country_name_resolved

                    text = build_telegram_message(item)
                    ok = send_telegram_message(bot_token, chat_id, text, thread_id)
                    if ok:
                        logging.info(f"Telegram'a gönderildi | id={item.get('id')} createdAt={item.get('createdAt')}")
                        new_items_sent.append(item)
                    else:
                        logging.error(f"Telegram gönderimi başarısız | id={item.get('id')}")
                if new_items_sent:
                    state = update_state_after_send(state, new_items_sent)
            else:
                logging.info("Yeni deposit yok.")
        except PermissionError as e:
            logging.warning(f"401 alındı, cookie yeniden okunacak: {e}")
            try:
                refreshed = maybe_refresh_cookie_from_browser(args.cookie_file, bot_token, chat_id, thread_id)
                if refreshed is not None:
                    sess = refreshed
                else:
                    sess = reload_cookies_and_session(args.cookie_file)
            except Exception as e2:
                logging.error(f"Cookie yeniden yüklenemedi: {e2}")
        except ValueError as e:
            msg = str(e)
            if "10004" in msg or "not logged in" in msg.lower():
                logging.warning("API '10004 / not logged in' tespit edildi. Cookie tazeleniyor...")
                try:
                    refreshed = maybe_refresh_cookie_from_browser(args.cookie_file, bot_token, chat_id, thread_id)
                    if refreshed is not None:
                        sess = refreshed
                    else:
                        sess = reload_cookies_and_session(args.cookie_file)
                except Exception as e2:
                    logging.error(f"Cookie yeniden yüklenemedi: {e2}")
            else:
                logging.error(f"Deposit sorgusunda hata: {e}")
        except Exception as e:
            logging.error(f"Deposit sorgusunda beklenmeyen hata: {e}")

        # Bekleme (dilimli; stop_event erken çıkabilir)
        elapsed = time.time() - loop_start
        sleep_s = max(1.0, interval - elapsed)
        slept = 0.0
        while slept < sleep_s and not stop_event.is_set():
            chunk = min(1.0, sleep_s - slept)
            time.sleep(chunk)
            slept += chunk

    logging.info("Bot durduruldu.")


if __name__ == "__main__":
    main()
