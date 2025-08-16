#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
from datetime import datetime
from zoneinfo     import ZoneInfo
import hmac
import hashlib
import logging
import threading
import requests
import pandas as pd
import re

from datetime import datetime
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# timestamp in UTC+2
TZ = ZoneInfo("Europe/Tirane")

# ── CONFIG (Binance & Telegram) ─────────────────────────────────────────────────
API_KEY_BINANCE    = 'iJmoTezYi1v82UJ6IFFYUtgoH7Xf5lmmmdbZguxS3BPhm93RMB7VM7slDfnp2TM2'
API_SECRET_BINANCE = 'FXIEwjfjYuYFyAfpFT9ud5gluG3OzsNd68Fj7iPeIeXa1T2na6PwWpMPvcqO3lOy'
BOT_TOKEN  = '7295679982:AAGZfrco1rgSPbGdnkVaJj_FXHptSfyLUgo'

CHAT_ID   = -1002503387372    # grubunuzun chat_id’si
THREAD_ID =  10363            # mesaj_thread_id

SYMBOLS_FILE = r'C:\Users\EmirBerkeUsak\python\symbols.txt'
STATE_FILE   = r'C:\Users\EmirBerkeUsak\python\risk_limits.json'
OUTPUT_DIR   = r'C:\Users\EmirBerkeUsak\python\out'

BASE_URL_BINANCE = 'https://fapi.binance.com'
ENDPOINT_BINANCE = '/fapi/v1/leverageBracket'

# ── CONFIG (Darkex) ──────────────────────────────────────────────────────────────
API_KEY_DARKEX    = "97f0c7f39fd0960985cc6ef2901cdc05"
API_SECRET_DARKEX = "ed52be43f76ccf1f849434071a3a9a29"

BASE_URL_DARKEX               = "https://futures.darkex.com"
PUBLIC_INFO_PATH_DARKEX       = "/fe-co-api/common/public_info"
CONTRACT_INFO_PATH_DARKEX     = "/fe-co-api/common/public_futures_contract_info"
FULL_PUBLIC_INFO_URL_DARKEX   = BASE_URL_DARKEX + PUBLIC_INFO_PATH_DARKEX
FULL_CONTRACT_INFO_URL_DARKEX = BASE_URL_DARKEX + CONTRACT_INFO_PATH_DARKEX

# Zaman dilimi sabiti: Tirane saatine (UTC+2) göre
TZ = ZoneInfo("Europe/Tirane")

# ── LOGGING SETUP ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

# ── Ortak: Telegram’a Metin Mesajı Gönderme ────────────────────────────────────────
def send_telegram(text: str):
    """
    Telegram’a bir mesaj göndermek için kullanılır.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "message_thread_id": THREAD_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, data=payload)
    resp.raise_for_status()


def send_document(file_path: str):
    """
    Telegram’a bir dosya (örneğin Excel .xlsx) göndermek için kullanılır.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(file_path, 'rb') as f:
        files = {'document': (os.path.basename(file_path), f)}
        data = {'chat_id': CHAT_ID, "message_thread_id": THREAD_ID}
        resp = requests.post(url, data=data, files=files)
        resp.raise_for_status()


# ── BINANCE: Risk Limit Kontrol Fonksiyonları ────────────────────────────────────
def load_symbols(path):
    """
    symbols.txt içindeki her satırı büyük harfe çevirip listeye döner.
    """
    with open(path, 'r', encoding='utf-8') as f:
        return [s.strip().upper() for s in f if s.strip()]


def fetch_all_brackets_binance():
    """
    Binance Futures API’dan tüm sembollerin risk limit tablolarını çeker.
    """
    ts = str(int(time.time() * 1000))
    qs = f"timestamp={ts}"
    signature = hmac.new(
        API_SECRET_BINANCE.encode(),
        qs.encode(),
        hashlib.sha256
    ).hexdigest()

    url = f"{BASE_URL_BINANCE}{ENDPOINT_BINANCE}?{qs}&signature={signature}"
    headers = {"X-MBX-APIKEY": API_KEY_BINANCE}

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def load_previous_state(path):
    """
    risk_limits.json dosyasını yükler; eğer yoksa boş dict döner.
    """
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def save_state(path, data):
    """
    risk_limits.json dosyasına güncel durumu yazar.
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def check_and_notify():
    """
    ● symbols.txt’teki sembolleri okur  
    ● Binance’dan güncel risk limit bilgilerini çeker  
    ● Önceki risk_limits.json ile karşılaştırır  
    ● Değişiklik varsa detaylı mesaj gönderir,  
      yoksa “Saat ve değişiklik yok” mesajı atar  
    ● En son durumu risk_limits.json’a yazar  
    """
    symbols = load_symbols(SYMBOLS_FILE)
    all_data = fetch_all_brackets_binance()

    # Sadece symbols.txt içindeki sembolleri al
    current = {
        entry['symbol']: entry['brackets']
        for entry in all_data
        if entry['symbol'] in symbols
    }

    prev = load_previous_state(STATE_FILE)
    now = datetime.now(TZ)
    timestamp_str = now.strftime('%H:%M:%S %d-%m-%Y')

    # Eğer önceki kayıt yoksa, yalnızca kaydet ve çık
    if not prev:
        msg0 = (
            f"⚙️ *Risk Limit Monitor Başlatıldı*\n\n"
            f"⏱️ Saat: {timestamp_str} (UTC+2)\n"
            f"ℹ️ İlk yükleme: Önceki kayıt bulunamadı, mevcut durum kaydedildi."
        )
        logging.info("İlk çalışma, state kaydedildi.")
        try:
            send_telegram(msg0)
            time.sleep(1)
        except Exception:
            pass
        save_state(STATE_FILE, current)
        return

    # Önceki kayıt varsa, her sembolü tek tek kontrol et
    changes = []
    for sym, curr_br in current.items():
        old_br = prev.get(sym)
        if old_br is None:
            # Önceki kayıtta olmayan, yeni eklenmiş sembol → atla
            continue
        if curr_br != old_br:
            changes.append((sym, old_br, curr_br))

    if changes:
        field_names = {
            'initialLeverage':  'Leverage',
            'notionalCap':      'Max Notional',
            'notionalFloor':    'Min Notional',
            'maintMarginRatio': 'Maintenance Margin'
        }

        for sym, old, new in changes:
            tier_diffs = []
            for o, n in zip(old, new):
                diffs = []
                for key, label in field_names.items():
                    if o.get(key) != n.get(key):
                        if key == 'maintMarginRatio':
                            old_pct = o[key] * 100
                            new_pct = n[key] * 100
                            diffs.append(f"{label}: {old_pct:.2f}% → {new_pct:.1f}%")
                        elif key in ('notionalCap', 'notionalFloor'):
                            diffs.append(f"{label}: {o[key]:,} → {n[key]:,}")
                        else:
                            diffs.append(f"{label}: {o[key]} → {n[key]}")
                if diffs:
                    tier_diffs.append((o['bracket'], diffs))

            ts = datetime.now(TZ)
            msg  = f"🔔 *Risk Limit Güncellemesi*\n\n"
            msg += f"   `{sym}`\n\n"
            msg += f"⏱️ Saat: {ts.strftime('%H:%M:%S %d-%m-%Y')} (UTC+2)\n\n"
            msg += "🔄 Güncelleme Detayları:\n\n"
            for tier, diffs in tier_diffs:
                msg += f"• Tier {tier}:\n"
                for diff in diffs:
                    msg += f"  - {diff}\n"

            logging.info(f"Değişiklik tespit edildi: {sym}")
            try:
                send_telegram(msg)
                time.sleep(1)
            except Exception:
                pass

        logging.info("Saat başı güncelleme tamamlandı.")
    else:
        msg_no = (
            f"✅ *Risk Limit Kontrolü*\n\n"
            f"⏱️ Saat: {timestamp_str} (UTC+2)\n"
            f"✅ Herhangi bir değişiklik yok."
        )
        logging.info("Herhangi bir değişiklik bulunamadı.")
        try:
            send_telegram(msg_no)
            time.sleep(1)
        except Exception:
            pass

        logging.info("Saat başı güncelleme tamamlandı.")

    save_state(STATE_FILE, current)


# ── DARKEX: Risk Limit Çekme Fonksiyonları ────────────────────────────────────────
def fetch_all_contracts_darkex(api_key: str, secret_key: str) -> list[dict]:
    timestamp = str(int(time.time() * 1000))
    body_dict = {}
    body_json = json.dumps(body_dict)

    pre_hash = timestamp + "POST" + PUBLIC_INFO_PATH_DARKEX + body_json
    signature = hmac.new(
        secret_key.encode("utf-8"),
        pre_hash.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type":    "application/json",
        "X-CH-APIKEY":     api_key,
        "X-CH-TS":         timestamp,
        "X-CH-SIGN":       signature
    }

    logging.debug(f"Request headers: {headers}")
    logging.debug(f"Request body: {body_json}")
    

    for attempt in range(3):  # 3 kez dene
        try:
            resp = requests.post(
                FULL_PUBLIC_INFO_URL_DARKEX,
                data=body_json,
                headers=headers,
                timeout=20  # Daha uzun timeout
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("succ", False):
                raise RuntimeError(f"public_info failed: {data}")
            return data["data"].get("contractList", [])
        except Exception as e:
            logging.warning(f"[{attempt+1}/3] Darkex contractList çekme hatası: {e}")
            time.sleep(2)

    raise RuntimeError("Darkex contractList 3 kez denenmesine rağmen alınamadı.")

def fetch_risk_limits_for(contract_id: int, api_key: str, secret_key: str) -> list[dict]:
    """
    Darkex’in /public_futures_contract_info endpoint’inden verilen contract_id için
    leverMarginInfo listesini çeker. Debug printler kaldırıldı.
    """
    timestamp = str(int(time.time() * 1000))
    body_dict = {"contractId": contract_id}
    body_json = json.dumps(body_dict)

    pre_hash = timestamp + "POST" + CONTRACT_INFO_PATH_DARKEX + body_json
    signature = hmac.new(
        secret_key.encode("utf-8"),
        pre_hash.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type":    "application/json",
        "X-CH-APIKEY":     api_key,
        "X-CH-TS":         timestamp,
        "X-CH-SIGN":       signature
    }

    resp = requests.post(FULL_CONTRACT_INFO_URL_DARKEX, data=body_json, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("succ", False):
        raise RuntimeError(f"contract_info failed for {contract_id}: {data}")

    info_list = data["data"].get("leverMarginInfo", None)
    return info_list or []


# ── NORMALIZATION HELPER ─────────────────────────────────────────────────────────
def normalize_binance_pair(binance_pair: str) -> str:
    """
    1000PEPEUSDT gibi Binance pair’ini, başındaki sayısal öneki atarak normalize eder.
    Örnek: "1000PEPEUSDT" → "PEPEUSDT"
           "BTCUSDT"      → "BTCUSDT"  
    """
    return re.sub(r'^\d+', '', binance_pair.upper())


def normalize_darkex_contract(cname: str) -> str:
    """
    Darkex contractName'i normalize eder. Örnekler:
      - "1000BONK-USDT"        → ["1000BONK","USDT"] → birleştir → "1000BONKUSDT" → baştaki sayı sil → "BONKUSDT"
      - "USDT1791-RDNT-USDT"   → ["USDT1791","RDNT","USDT"] → son iki → "RDNTUSDT"
      - "BTC-USDT"             → ["BTC","USDT"] → birleştir → "BTCUSDT"
      - Alt çizgi varsa önce tireyle eşitleyip devam eder.
    """
    cname = cname.strip().upper().replace("_", "-")
    parts = cname.split("-")
    if len(parts) >= 3:
        base = parts[-2] + parts[-1]
    else:
        base = "".join(parts)
    return re.sub(r'^\d+', '', base.upper())


# ── TELEGRAM KOMUT İŞLEYİCİSİ ──────────────────────────────────────────────────────
def get_updates(offset=None):
    """
    Telegram bot’un getUpdates endpoint’ine istek atar.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {'timeout': 100}
    if offset:
        params['offset'] = offset
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error getting updates: {e}")
        return None

def handle_commands(update):
    if 'message' in update:
        message = update['message']
        text = message.get('text')
        if text == '/create_excel':
            logging.info("Komut alındı: /create_excel")
            send_telegram("Komut algılandı, Excel hazırlanıyor..")

            excel_path = None  # ← Önceden tanımlıyoruz
            try:
                excel_path = generate_risk_limit_excel()
                send_document(excel_path)
            except Exception as e:
                logging.error(f"Excel oluşturulurken hata: {e}")
                send_telegram(f"❌ Excel oluşturulamadı:\n`{e}`")
            finally:
                try:
                    if excel_path and os.path.exists(excel_path):
                        os.remove(excel_path)
                        logging.info(f"Geçici dosya silindi: {excel_path}")
                except Exception as e:
                    logging.warning(f"Dosya silinirken hata: {e}")

def poll_updates():
    offset = None
    while True:
        try:
            updates = get_updates(offset)
            if updates and updates.get('result'):
                for update in updates['result']:
                    offset = update['update_id'] + 1
                    handle_commands(update)
        except Exception as e:
            logging.error(f"poll_updates içinde hata oluştu: {e}")
        time.sleep(1)

# ── EXCEL ÜRETİMİ: Binance + Darkex Risk Limitlerini Tek Bir Dosyada Birleştir ──
def generate_risk_limit_excel():
    """
    1) symbols.txt’ten sembolleri yükler  
    2) Binance’dan risk limit tablolarını çeker  
    3) Darkex’ten contractList’i alıp normalize eder  
    4) Her pair için Darkex tier’larını çeker  
    5) DataFrame’de “Exchange”, “Pair”, “Type”, “Tier1..Tier15”, “Risk1..Risk15” kolonlarını oluşturur  
    6) Excel’e yazar; sayısal Tier hücreleri binlik ayraçlı (“#,##0”) görünür  
    """
    # 1) Tüm sembolleri oku
    pairs = load_symbols(SYMBOLS_FILE)

    # Binance normalize edilmiş hâli
    normalized_binance = {pair: normalize_binance_pair(pair) for pair in pairs}

    # 2) Binance verisini çek
    all_binance = fetch_all_brackets_binance()
    binance_map = {
        entry['symbol']: entry['brackets']
        for entry in all_binance
        if entry['symbol'] in pairs
    }

    # 3) Darkex contractList’i çek, normalize edip map oluştur
    all_contracts = fetch_all_contracts_darkex(API_KEY_DARKEX, API_SECRET_DARKEX)
    contract_map = {
        normalize_darkex_contract(entry.get("contractName")): entry.get("id")
        for entry in all_contracts
        if entry.get("contractName") and entry.get("id") is not None
    }

    # 4) Her Binance pair için Darkex tier’larını çek
    darkex_map = {}
    for pair, norm_bin in normalized_binance.items():
        cid = contract_map.get(norm_bin)
        if cid is not None:
            d_tiers = fetch_risk_limits_for(cid, API_KEY_DARKEX, API_SECRET_DARKEX)
            time.sleep(1.5)
        else:
            d_tiers = []
        darkex_map[pair] = d_tiers

    # 5) DataFrame’e eklemek üzere satırları hazırla
    rows = []
    tier_columns = [f"Tier{i}" for i in range(1, 16)]
    risk_columns = [f"Risk{i}" for i in range(1, 16)]

    for pair in pairs:
        # ○ Binance tarafı
        b_tiers = binance_map.get(pair, [])
        b_by_tier = {entry.get("bracket"): entry for entry in b_tiers if entry.get("bracket") is not None}

        b_leverage = []
        b_maint    = []
        b_notional = []
        for i in range(1, 16):
            e = b_by_tier.get(i, {})
            b_leverage.append(e.get("initialLeverage", ""))
            mm = e.get("maintMarginRatio")
            b_maint.append(f"{mm * 100:.2f}%" if mm is not None else "")
            cap = e.get("notionalCap")
            b_notional.append(cap if cap is not None else "")
        # ○ Darkex tarafı
        d_tiers = darkex_map.get(pair, [])
        d_by_tier = {}
        for entry in d_tiers:
            lvl = entry.get("level")
            if lvl is not None:
                try:
                    lvl_int = int(lvl)
                except:
                    continue
                d_by_tier[lvl_int] = entry

        d_leverage = []
        d_maint    = []
        d_notional = []
        for i in range(1, 16):
            e = d_by_tier.get(i, {})
            ml = e.get("maxLever", "")
            if isinstance(ml, str) and ml.endswith("x"):
                try:
                    ml_num = int(ml[:-1])
                except:
                    ml_num = ""
            else:
                try:
                    ml_num = int(ml)
                except:
                    ml_num = ""
            d_leverage.append(ml_num)

            mmr = e.get("minMarginRate", "")
            if isinstance(mmr, str) and mmr.endswith("%"):
                d_maint.append(mmr)
            else:
                d_maint.append("")

            mpv = e.get("maxPositionValue", "")
            if isinstance(mpv, str):
                try:
                    mpv_int = int(mpv)
                except:
                    mpv_int = ""
            else:
                mpv_int = mpv
            d_notional.append(mpv_int)

        # ○ Risk hesaplaması (sadece Binance – Leverage satırı için)
        risk_values = []
        for i in range(1, 16):
            b_val = b_leverage[i-1]
            d_val = d_leverage[i-1]
            try:
                b_num = float(b_val) if b_val not in ("", None) else None
            except:
                b_num = None
            try:
                d_num = float(d_val) if d_val not in ("", None) else None
            except:
                d_num = None

            if b_num is None or d_num is None:
                risk_values.append("")
            else:
                if d_num > b_num:
                    risk_values.append("Yüksek")
                elif d_num < b_num:
                    risk_values.append("Düşük")
                else:
                    risk_values.append("Eşit")

        # Satırları ekle
        rows.append({
            "Exchange": "Binance",
            "Pair":     pair,
            "Type":     "Leverage",
            **{f"Tier{i}": b_leverage[i-1] for i in range(1, 16)},
            **{f"Risk{i}":  risk_values[i-1]     for i in range(1, 16)}
        })
        rows.append({
            "Exchange": "Binance",
            "Pair":     pair,
            "Type":     "Maintenance Margin",
            **{f"Tier{i}": b_maint[i-1] for i in range(1, 16)},
            **{f"Risk{i}":  ""           for i in range(1, 16)}
        })
        rows.append({  
            "Exchange": "Binance",
            "Pair":     pair,
            "Type":     "Max Value USDT",
            **{f"Tier{i}": b_notional[i-1] for i in range(1, 16)},
            **{f"Risk{i}":  ""             for i in range(1, 16)}
        })

        rows.append({
            "Exchange": "Darkex",
            "Pair":     pair,
            "Type":     "Leverage",
            **{f"Tier{i}": d_leverage[i-1] for i in range(1, 16)},
            **{f"Risk{i}":  ""            for i in range(1, 16)}
        })
        rows.append({
            "Exchange": "Darkex",
            "Pair":     pair,
            "Type":     "Maintenance Margin",
            **{f"Tier{i}": d_maint[i-1] for i in range(1, 16)},
            **{f"Risk{i}":  ""           for i in range(1, 16)}
        })
        rows.append({
            "Exchange": "Darkex",
            "Pair":     pair,
            "Type":     "Max Value USDT",
            **{f"Tier{i}": d_notional[i-1] for i in range(1, 16)},
            **{f"Risk{i}":  ""            for i in range(1, 16)}
        })

    # 6) DataFrame oluştur ve sütun sırasını ayarla
    all_columns = ["Exchange", "Pair", "Type"] + tier_columns + risk_columns
    df = pd.DataFrame(rows, columns=all_columns)

    # 7) Excel’e yaz fakat binlik ayraçlı format uygula
    now = datetime.now(TZ).strftime('%H%M%S_%d-%m-%Y') 
    filename = f"risk_limits_{now}.xlsx"
    full_path = os.path.join(OUTPUT_DIR, filename)

    # openpyxl kullanarak yazıyoruz
    with pd.ExcelWriter(full_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='RiskLimits', index=False, startrow=0, header=True)
        workbook  = writer.book
        worksheet = writer.sheets['RiskLimits']

        # Hücre kenarlıkları için
        from openpyxl.styles import Border, Side, numbers

        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Binlik ayraç formatı uygulanan sütunlar: Tier1..Tier15
        # Başlık satırı = 1; veri  satırları = 2.. (len(df)+1)
        for row_idx in range(2, 2 + len(df)):
            for col_idx in range(4, 4 + len(tier_columns)):
                cell = worksheet.cell(row=row_idx, column=col_idx)
                value = cell.value
                if isinstance(value, (int, float)):
                    cell.number_format = '#,##0'
                elif isinstance(value, str) and value.endswith('%'):
                    try:
                        # Yüzde veriyi float’a çevir
                        percent_val = float(value.strip('%')) / 100
                        cell.value = percent_val
                        cell.number_format = '0.00%'
                    except:
                        pass
                cell.border = thin_border

        # Risk sütunlarına da basit kenarlık ekle
        for row_idx in range(2, 2 + len(df)):
            for col_idx in range(4 + len(tier_columns), 4 + len(tier_columns) + len(risk_columns)):
                cell = worksheet.cell(row=row_idx, column=col_idx)
                cell.border = thin_border

    logging.info(f"Excel dosyası oluşturuldu: {full_path}")
    return full_path


# ── PROGRAMIN GİRİŞ NOKTASI ───────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    1) Başlangıçta bir kez “bot aktif, saat başı kontroller planlandı” mesajı gönder.
    2) Telegram komutlarını dinlemeye başlamak için poll_updates() ayrı bir thread’de çalıştır.
    3) APScheduler cron trigger ile her saat başı check_and_notify()’u çalıştır.
    """

    # Bot başladığında log
    logging.info("Bot başlatıldı.")

    # 1) Başlangıç bildirimi (sadece bir kez)
    startup_now = datetime.now(TZ)
    startup_msg = (
        f"⚙️ *Risk Limit Kontrol Botu Aktif*\n\n"
        f"⏱️ Saat başı kontroller planlandı.\n"
        f"⏱️ Başlangıç: {startup_now.strftime('%H:%M:%S %d-%m-%Y')} (UTC+2)"
    )
    try:
        send_telegram(startup_msg)
        time.sleep(1)
    except Exception:
        pass

    # 2) Telegram komutlarını asenkron dinlemek için ayrı bir daemon thread başlat
    polling_thread = threading.Thread(target=poll_updates, daemon=True)
    polling_thread.start()

    # 3) Scheduler’ı oluştur ve cron trigger ayarla (her saat başı)
    scheduler = BlockingScheduler(timezone="Europe/Tirane")
    trigger = CronTrigger(hour='*', minute=0, second=0, timezone=TZ)

    scheduler.add_job(
        func=check_and_notify,
        trigger=trigger,
        id='risk_limit_job',
        replace_existing=True,
        misfire_grace_time=300  # Kaçırılan job’lar en fazla 5 dk içinde çalışsın
    )

    try:
        logging.info("Scheduler çalıştırılıyor.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler durduruldu.")
        pass
