#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import threading
import queue
import logging
import math
from urllib.parse import urlencode
from collections import defaultdict

import requests
import pandas as pd
import MetaTrader5 as mt5

# --- Logging setup ---
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === Configuration (Telegram güncellendi) ===
TELEGRAM_BOT_TOKEN = "7895901821:AAEJs3mmWxiWrRyVKcRiAxMN2Rn4IpiyV0o"
CHAT_ID            = "-4678220102"

DARKEX_API_KEY     = "cb658cb61b423d35be9bf855e21a34e7"
DARKEX_SECRET_KEY  = "638beeeb2ac75e229cf716202fc8da6c"
DARKEX_BASE_URL    = "https://futuresopenapi.darkex.com"

MT5_LOGIN     = 810005950
MT5_PASSWORD  = "nDGbT_4O"
MT5_SERVER    = "Taurex-Demo"
MT5_LEVERAGE  = 1  # mt5 lot dönüşümünde kullanılıyor

# Darkex symbol → MT5 symbol eşlemesi
SYMBOL_MAP = {
    "USDT1791-BTC-USDT": "BTCUSD",
    "USDT1791-ETH-USDT": "ETHUSD",
}

# Darkex kontrat başına baz varlık miktarı (ör: BTCUSDT kontrat boyu 0.001 BTC ise)
CONTRACT_SIZE = {
    "USDT1791-BTC-USDT": 0.001,
    "USDT1791-ETH-USDT": 0.001,
}

# Emir kuyruğu (monitor_positions yalnızca buraya yazar; MT5 yürütücü artık snapshot-otoriter)
mt5_queue = queue.Queue()


# === DARKEX REST helpers (VERİ ÇEKME – DEĞİŞTİRİLMEDİ) ===

def generate_signature(timestamp, method, request_path, body, secret_key):
    pre = timestamp + method.upper() + request_path + (body or "")
    import hmac, hashlib
    sig = hmac.new(secret_key.encode(), pre.encode(), hashlib.sha256).hexdigest()
    # Darkex çift URL-escape istiyor; korunmuştur
    return requests.utils.quote(requests.utils.quote(sig))

def get_futures_account_info():
    path = "/fapi/v1/account"
    ts   = str(int(time.time() * 1000))
    sig  = generate_signature(ts, "GET", path, "", DARKEX_SECRET_KEY)
    headers = {
        "X-CH-APIKEY": DARKEX_API_KEY,
        "X-CH-TS":     ts,
        "Content-Type":"application/json",
        "X-CH-SIGN":   sig
    }
    r = requests.get(DARKEX_BASE_URL + path, headers=headers, timeout=10)
    logger.info("Futures Account Info: %s %s", r.status_code, r.text)
    return r.json()

def get_futures_account_info_df():
    try:
        data = get_futures_account_info()
        if "account" in data and isinstance(data["account"], list):
            return pd.DataFrame(data["account"])
    except Exception as e:
        logger.error("get_futures_account_info_df error: %s", e)
    return pd.DataFrame()

def get_futures_account_balance_df():
    try:
        df = get_futures_account_info_df()
        if not df.empty and "totalEquity" in df.columns:
            return df[["totalEquity"]]
    except Exception as e:
        logger.error("get_futures_account_balance_df error: %s", e)
    return pd.DataFrame()

def get_open_orders_df(contract_name=None):
    try:
        path = "/fapi/v1/openOrders"
        params = {}
        if contract_name:
            params["contractName"] = contract_name
        qs  = "?" + urlencode(params) if params else ""
        ts  = str(int(time.time() * 1000))
        sig = generate_signature(ts, "GET", path + qs, "", DARKEX_SECRET_KEY)
        headers = {
            "X-CH-APIKEY": DARKEX_API_KEY,
            "X-CH-TS":     ts,
            "Content-Type":"application/json",
            "X-CH-SIGN":   sig
        }
        r = requests.get(DARKEX_BASE_URL + path, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "status" in df.columns:
            df = df[df["status"] != "PENDING_CANCEL"]
        return df
    except Exception as e:
        logger.error("get_open_orders_df error: %s", e)
        return pd.DataFrame()

def get_open_positions_df():
    path = "/fapi/v1/account"
    ts   = str(int(time.time() * 1000))
    sig  = generate_signature(ts, "GET", path, "", DARKEX_SECRET_KEY)
    headers = {
        "X-CH-APIKEY": DARKEX_API_KEY,
        "X-CH-TS":     ts,
        "Content-Type":"application/json",
        "X-CH-SIGN":   sig
    }
    r = requests.get(DARKEX_BASE_URL + path, headers=headers, timeout=10)
    r.raise_for_status()
    
    data = r.json()
    acct_list = data.get("account")
    if not isinstance(acct_list, list):
        logger.error("get_open_positions_df: unexpected response %s", data)
        raise ValueError("account list missing")
    
    rows = []
    any_positions = False
    for acct in acct_list:
        for vo in acct.get("positionVos", []):
            cname = vo.get("contractName")
            positions = vo.get("positions")
            if positions is None:
                logger.error("get_open_positions_df: positions missing for %s", vo)
                continue
            if not isinstance(positions, list):
                logger.error("get_open_positions_df: positions not list for %s", vo)
                continue
            if positions:
                any_positions = True
            for pos in positions:
                print(pos)  # mevcut davranışı koruyoruz
                vol = float(pos.get("volume", 0))
                if vol <= 0:
                    continue
                rows.append({
                    "contractName": cname,
                    "side":         pos.get("side"),
                    "volume":       vol,           # Darkex kontrat sayısı
                    "openPrice":    float(pos.get("openPrice", 0)),
                    "uPnL":         float(pos.get("unRealizedAmount", 0)),
                })

    if not any_positions:
        logger.debug("get_open_positions_df: snapshot contained no open positions")

    return pd.DataFrame(rows)

def send_text_to_telegram(token, chat_id, text):
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    try:
        r = requests.post(url, data=data, timeout=5)
        r.raise_for_status()
    except Exception as e:
        logger.error("Telegram send failed: %s", e)


# === MT5 Integration (GİRİŞ – DEĞİŞTİRİLMEDİ) ===

def init_mt5(login, password, server):
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    if not mt5.login(login, password, server):
        raise RuntimeError(f"MT5 login failed: {mt5.last_error()}")
    logger.info("MT5 initialized and logged in")

def volume_to_lots(darkex_symbol, contract_count):
    """
    Darkex kontrat sayısını MT5 lot'a çevirir:
      raw_lots = contract_count * Darkex_contract_size * MT5_LEVERAGE / MT5_contract_size
    Sonra yukarı yuvarla (ceil) ve step/min kurallarına uydur.
    """
    mt5_sym = SYMBOL_MAP.get(darkex_symbol)
    if not mt5_sym:
        raise ValueError(f"Mapping bulunamadı: {darkex_symbol}")

    info = mt5.symbol_info(mt5_sym)
    if info is None:
        raise RuntimeError(f"MT5 symbol_info yüklenemedi: {mt5_sym}")

    darkex_ct_size = CONTRACT_SIZE[darkex_symbol]
    mt5_ct_size    = info.trade_contract_size
    lev            = MT5_LEVERAGE

    raw_lots = contract_count * darkex_ct_size * lev / mt5_ct_size

    step    = info.volume_step
    vol_min = info.volume_min
    lots    = math.ceil(raw_lots / step) * step
    if lots < vol_min and lots > 0:
        lots = vol_min

    return lots


# =========================================================
#           EMİR YÖNETİMİ — OTORİTER HEDEF EŞLEME
# =========================================================

# --- Parametreler: snapshot, güvenlik, histerezis ---
_SNAPSHOT_INTERVAL_SEC = 2.0   # Darkex snapshot sıklığı
_MISS_CONFIRM_CYCLES   = 3     # Bir key üst üste bu kadar snapshot'ta yoksa gerçekten kapandı say
_MAX_RETRY_SEND        = 2     # order_send retry
_SLIPPAGE_PIPS         = 10    # deviation
_HYSTERESIS_STEP_FR    = 0.5   # step/2 altı farkları yok say

def _mt5_select_all_symbols():
    for s in SYMBOL_MAP.values():
        try:
            mt5.symbol_select(s, True)
        except Exception:
            pass

def _mt5_positions_by_side(mt5_symbol, side):
    """MT5’ten mevcut pozisyonları (hedging modunda birden fazla olabilir) getirir."""
    all_pos = mt5.positions_get(symbol=mt5_symbol) or []
    if side == "BUY":
        return [p for p in all_pos if p.type == mt5.POSITION_TYPE_BUY]
    else:
        return [p for p in all_pos if p.type == mt5.POSITION_TYPE_SELL]

def _current_side_volume(mt5_symbol, side):
    """Seçili sembol+side için mevcut toplam lot (float)."""
    ps = _mt5_positions_by_side(mt5_symbol, side)
    return sum(p.volume for p in ps)

def _send_mt5_market(sym, side, lots, comment):
    """Market açma (BUY/SELL)."""
    if lots <= 0:
        return True, None
    tick = mt5.symbol_info_tick(sym)
    if tick is None:
        return False, ("tick", "none")

    if side == "BUY":
        order_type = mt5.ORDER_TYPE_BUY
        price      = tick.ask
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price      = tick.bid

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       sym,
        "volume":       lots,
        "type":         order_type,
        "price":        price,
        "deviation":    _SLIPPAGE_PIPS,
        "magic":        123456,
        "comment":      comment,
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    last_err = None
    for _ in range(_MAX_RETRY_SEND + 1):
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            return True, res
        last_err = res
        time.sleep(0.05)
    return False, last_err

def _close_from_positions(mt5_symbol, side, lots_to_close, comment):
    """
    Hedging-uyumlu kapanış: ilgili side'daki pozisyonları sırayla (büyükten küçüğe) tüketir.
    Toplamda lots_to_close kadar kapatmaya çalışır.
    """
    if lots_to_close <= 0:
        return 0.0, 0.0

    remaining = lots_to_close
    closed = 0.0
    positions = sorted(_mt5_positions_by_side(mt5_symbol, side),
                       key=lambda p: p.volume,
                       reverse=True)
    for p in positions:
        if remaining <= 0:
            break
        use = min(remaining, p.volume)

        close_side = "SELL" if side == "BUY" else "BUY"
        tick = mt5.symbol_info_tick(mt5_symbol)
        if tick is None:
            break
        price = tick.bid if close_side == "SELL" else tick.ask

        req = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       mt5_symbol,
            "volume":       use,
            "type":         mt5.ORDER_TYPE_SELL if side == "BUY" else mt5.ORDER_TYPE_BUY,
            "position":     p.ticket,  # DOĞRU: kapanış position ticket ile yapılır
            "price":        price,
            "deviation":    _SLIPPAGE_PIPS,
            "magic":        123456,
            "comment":      comment,
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        ok = False
        last_err = None
        for _ in range(_MAX_RETRY_SEND + 1):
            res = mt5.order_send(req)
            if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                ok = True
                break
            last_err = res
            time.sleep(0.05)

        if ok:
            closed += use
            remaining -= use
        else:
            logger.error("MT5 close failed: %s", last_err)
            break

    return closed, max(0.0, remaining)

def _telegram_confirm_open(darkex_symbol, side, lots, price):
    msg = (f"✅ MT5 OPEN OK\n"
           f"Symbol: {SYMBOL_MAP[darkex_symbol]} ({darkex_symbol})\n"
           f"Side: {side}\n"
           f"Lots: {lots:.4f}\n"
           f"Price: {price:.2f}")
    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, msg)

def _telegram_confirm_close(darkex_symbol, side, lots_closed):
    msg = (f"✅ MT5 CLOSE OK\n"
           f"Symbol: {SYMBOL_MAP[darkex_symbol]} ({darkex_symbol})\n"
           f"Side: {side}\n"
           f"Closed Lots: {lots_closed:.4f}")
    send_text_to_telegram(TELEGRAM_BOT_TOKEN, CHAT_ID, msg)

def mt5_executor():
    """
    OTORİTER HEDEF EŞLEME:
      - Darkex snapshot'ı periyodik alınır, (symbol, side) → desired_contracts_auth belirlenir.
      - Bir pozisyonun kapandığına ancak ardışık _MISS_CONFIRM_CYCLES snapshot’ta görülmezse inanılır.
      - MT5’teki toplam lot, hedef lota tek hamlede yaklaştırılır (histerezis: step/2 altında işlem yok).
      - Monitor'den gelen kuyruk olayları sadece “uyandırıcı” niteliğinde; asıl kaynak snapshot’tır.
    """
    _mt5_select_all_symbols()

    # Otoriter hedefler (Darkex’e göre): key=(darkex_symbol, side) → kontrat
    desired_contracts_auth = defaultdict(float)
    # "Görünmeme sayacı": transient kayıplarda kapanış yapmamak için
    miss_counts = defaultdict(int)

    next_snapshot = 0.0
    empty_snapshot_run = 0

    # Thread-safety için sembol-yanı kilitleri (eşzamanlı emir çakışmasını önler)
    inflight = defaultdict(lambda: threading.Lock())

    while True:
        # 1) Kuyruktan bir şey gelirse sadece uyanık kal (snapshot yine belirleyici)
        try:
            _ = mt5_queue.get(timeout=_SNAPSHOT_INTERVAL_SEC / 3.0)
        except queue.Empty:
            pass

        now = time.time()

        # 2) Darkex snapshot: authoritative hedefleri güncelle
        if now >= next_snapshot:
            next_snapshot = now + _SNAPSHOT_INTERVAL_SEC

            snapshot_ok = False
            try:
                df = get_open_positions_df()
                snapshot_ok = True
            except Exception as e:
                logger.error("Darkex snapshot error: %s", e)
                snapshot_ok = False

            if not snapshot_ok:
                continue

            seen = set()
            if df.empty:
                empty_snapshot_run += 1
                if empty_snapshot_run >= _MISS_CONFIRM_CYCLES:
                    for key in list(desired_contracts_auth.keys()):
                        miss_counts[key] = _MISS_CONFIRM_CYCLES
                        desired_contracts_auth[key] = 0.0
            else:
                empty_snapshot_run = 0
                for _, r in df.iterrows():
                    darkex_symbol = r["contractName"]
                    side          = r["side"]
                    vol_ct        = float(r["volume"])
                    key = (darkex_symbol, side)
                    desired_contracts_auth[key] = max(0.0, vol_ct)
                    miss_counts[key] = 0  # görüldü
                    seen.add(key)

                # Snapshot’ta görülmeyen anahtarlar için miss say
                    for key in list(desired_contracts_auth.keys()):
                        if key not in seen:
                            miss_counts[key] += 1
                            # ancak yeterince yoksa eski hedefi koru (kapanmış sayma)
                            if miss_counts[key] >= _MISS_CONFIRM_CYCLES:
                                desired_contracts_auth[key] = 0.0

        # 3) Reconcile: her anahtar için MT5 lotunu target’a yaklaştır
        #    Ayrıca MT5’te kalmış "yetim" pozisyonlar için de 0 hedef uygulanır
        keys_to_handle = set(desired_contracts_auth.keys())

        # MT5’te var olup auth’ta hiç olmayanlar → miss_counts onlara da işlesin
        for d_sym, mt5_sym in SYMBOL_MAP.items():
            for side in ("BUY", "SELL"):
                positions = _mt5_positions_by_side(mt5_sym, side)
                if positions:
                    k = (d_sym, side)
                    if k not in desired_contracts_auth:
                        # Bu anahtar hiç otoriter hedef set edilmemişse,
                        # “0 hedef” kabul edelim ama transient kapanış riskine karşı
                        # miss confirm koşulunu uygularız:
                        if k not in miss_counts:
                            miss_counts[k] = 1
                            desired_contracts_auth[k] = 0.0
                        else:
                            miss_counts[k] += 1
                            if miss_counts[k] >= _MISS_CONFIRM_CYCLES:
                                desired_contracts_auth[k] = 0.0
                        keys_to_handle.add(k)

        for (darkex_symbol, side) in list(keys_to_handle):
            mt5_symbol = SYMBOL_MAP.get(darkex_symbol)
            if not mt5_symbol:
                continue

            lock = inflight[(darkex_symbol, side)]
            if not lock.acquire(blocking=False):
                continue

            try:
                info = mt5.symbol_info(mt5_symbol)
                if info is None:
                    logger.error("symbol_info None: %s", mt5_symbol)
                    continue

                # Hedef LOT
                target_lots = volume_to_lots(darkex_symbol, desired_contracts_auth[(darkex_symbol, side)])

                # Mevcut LOT (toplam, aynı side)
                curr_lots = _current_side_volume(mt5_symbol, side)

                # Histerezis: fark step/2’den küçükse işlem yapma
                step = info.volume_step
                diff = target_lots - curr_lots
                if abs(diff) < max(step * _HYSTERESIS_STEP_FR, 0.0):
                    continue

                if diff > 0:
                    # ↑ Açılış
                    lots_to_open = math.ceil(diff / step) * step
                    ok, res = _send_mt5_market(mt5_symbol, side, lots_to_open, "Darkex mirror open")
                    if ok:
                        tick = mt5.symbol_info_tick(mt5_symbol)
                        price = (tick.ask if side == "BUY" else tick.bid) if tick else 0.0
                        _telegram_confirm_open(darkex_symbol, side, lots_to_open, price)
                        logger.info("Opened %s %s lots=%.4f (target=%.4f, curr=%.4f)",
                                    mt5_symbol, side, lots_to_open, target_lots, curr_lots)
                    else:
                        logger.error("Open failed %s %s: %s", mt5_symbol, side, res)

                else:
                    # ↓ Kapatış — SADECE otoriter hedef 0’a (veya düşmüş hedefe) işaret ediyorsa
                    lots_to_close_req = math.ceil((-diff) / step) * step
                    lots_to_close_req = min(lots_to_close_req, curr_lots)  # güvenlik

                    # Miss confirm sağlanmadan desired 0 olmaz; bu nedenle burada kapama güvenli
                    closed_lots, remaining_lots = _close_from_positions(
                        mt5_symbol, side, lots_to_close_req, "Darkex mirror close"
                    )
                    if closed_lots > 0:
                        _telegram_confirm_close(darkex_symbol, side, closed_lots)
                        logger.info("Closed %s %s lots=%.4f (target=%.4f, curr_before=%.4f)",
                                    mt5_symbol, side, closed_lots, target_lots, curr_lots)
                    if remaining_lots > 0:
                        logger.warning("Close shortfall %s %s: remaining lots=%.4f (no reopen!)",
                                       mt5_symbol, side, remaining_lots)

            finally:
                lock.release()


# =========================================================
#    (AŞAĞISI: POZİSYON TAKİBİ ve TELEGRAM KOMUTLARI)
#              VERİ/INFO KISMI – DEĞİŞTİRİLMEDİ
# =========================================================

def _pos_key(pos):
    return f"{pos['contractName']}|{pos['side']}"

def monitor_positions(poll_interval=10):
    prev = {}
    while True:
        try:
            df = get_open_positions_df()
            curr = { _pos_key(r): r for _, r in df.iterrows() }

            # Yeni veya artan pozisyonlar → open delta (bilgi amaçlı; yürütme snapshot-otoriter)
            for k, pos in curr.items():
                cnt = pos['volume']  # Darkex kontrat sayısı
                if k not in prev:
                    send_text_to_telegram(
                        TELEGRAM_BOT_TOKEN, CHAT_ID,
                        f"New pos: {pos['contractName']} {pos['side']} cnt:{cnt}"
                    )
                    mt5_queue.put({
                        'symbol':    pos['contractName'],
                        'side':      pos['side'],
                        'contracts': cnt,
                        'action':    'open'
                    })
                else:
                    prev_cnt = prev[k]['volume']
                    if cnt > prev_cnt:
                        diff = cnt - prev_cnt
                        send_text_to_telegram(
                            TELEGRAM_BOT_TOKEN, CHAT_ID,
                            f"Added to pos: {pos['contractName']} {pos['side']} +{diff:.3f}"
                        )
                        mt5_queue.put({
                            'symbol':    pos['contractName'],
                            'side':      pos['side'],
                            'contracts': diff,
                            'action':    'open'
                        })
                    elif cnt < prev_cnt:
                        diff = prev_cnt - cnt
                        send_text_to_telegram(
                            TELEGRAM_BOT_TOKEN, CHAT_ID,
                            f"Partially closed pos: {pos['contractName']} {pos['side']} -{diff:.3f}"
                        )
                        mt5_queue.put({
                            'symbol':    pos['contractName'],
                            'side':      pos['side'],
                            'contracts': diff,
                            'action':    'close'
                        })

            # Tam kapanan pozisyonlar → full close (bilgi amaçlı; yürütme snapshot-otoriter)
            for k, pos in prev.items():
                if k not in curr:
                    cnt = pos['volume']
                    send_text_to_telegram(
                        TELEGRAM_BOT_TOKEN, CHAT_ID,
                        f"Closed pos: {pos['contractName']} {pos['side']} cnt:{cnt}"
                    )
                    mt5_queue.put({
                        'symbol':    pos['contractName'],
                        'side':      pos['side'],
                        'contracts': cnt,
                        'action':    'close'
                    })

            prev = {
                k: {'contractName': p['contractName'],
                    'side':         p['side'],
                    'volume':       p['volume']}
                for k, p in curr.items()
            }
        except Exception as e:
            logger.error("monitor_positions error: %s", e)
        time.sleep(poll_interval)


def get_updates(offset=None):
    url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {'offset': offset} if offset else {}
    try:
        r = requests.get(url, params=params, timeout=5)
        r.raise_for_status()
        return r.json().get('result', [])
    except Exception as e:
        logger.error("get_updates error: %s", e)
        return []

def handle_commands(update):
    msg     = update.get('message', {})
    text    = msg.get('text', '').strip()
    chat_id = msg.get('chat', {}).get('id')
    if chat_id is None:
        return

    if text == '/balance':
        try:
            df       = get_futures_account_balance_df()
            dark_bal = df['totalEquity'].iloc[0] if not df.empty else None
        except:
            dark_bal = None
        try:
            acc      = mt5.account_info()
            mt5_bal  = acc.balance if acc else None
        except:
            mt5_bal  = None

        parts = []
        parts.append(f"Darkex Balance: {dark_bal:.2f} USDT" if dark_bal is not None else "Darkex Balance: ❌")
        parts.append(f"MT5 Balance:    {mt5_bal:.2f} USDT"   if mt5_bal   is not None else "MT5 Balance:    ❌")
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, chat_id, "\n".join(parts))
        return

    elif text == '/positions':
        try:
            # Darkex
            darkex_msg = []
            df = get_open_positions_df()
            if df.empty:
                darkex_msg.append("📭 Darkex açık pozisyon yok.")
            else:
                darkex_msg.append("📈 Darkex Açık Pozisyonlar:")
                for _, r in df.iterrows():
                    darkex_msg.append(
                        f"{r['contractName']} | {r['side']} | Vol: {r['volume']} | Entry: {r['openPrice']} | uPnL: {r['uPnL']:.2f}"
                    )
        except Exception as e:
            logger.error("Darkex position fetch error: %s", e)
            darkex_msg = ["❌ Darkex pozisyonları alınırken hata oluştu."]

        try:
            # MT5
            mt5_msg = []
            positions = mt5.positions_get()
            if not positions:
                mt5_msg.append("📭 MT5 açık pozisyon yok.")
            else:
                mt5_msg.append("📉 MT5 Açık Pozisyonlar:")
                for p in positions:
                    mt5_msg.append(
                        f"{p.symbol} | {'BUY' if p.type==0 else 'SELL'} | Vol: {p.volume} | Entry: {p.price_open:.2f} | uPnL: {p.profit:.2f}"
                    )
        except Exception as e:
            logger.error("MT5 position fetch error: %s", e)
            mt5_msg = ["❌ MT5 pozisyonları alınırken hata oluştu."]

        send_text_to_telegram(TELEGRAM_BOT_TOKEN, chat_id, "\n".join(darkex_msg + [""] + mt5_msg))

    elif text == '/orders':
        try:
            df = get_open_orders_df()
            if df.empty:
                reply = "📭 Açık emir yok."
            else:
                lines = ["📝 Açık Emirler:"]
                for _, r in df.iterrows():
                    price = float(r.get("price", 0))
                    qty   = float(r.get("origQty", r.get("quantity", 0)))
                    lines.append(
                        f"{r.get('contractName', r.get('symbol', ''))} | {r.get('side')} | Price:{price} | Qty:{qty}"
                    )
                reply = "\n".join(lines)
        except:
            reply = "❌ Emirler alınırken hata oluştu."
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, chat_id, reply)


def poll_updates():
    offset = None
    while True:
        for upd in get_updates(offset):
            offset = upd['update_id'] + 1
            handle_commands(upd)
        time.sleep(1)


if __name__ == "__main__":
    init_mt5(MT5_LOGIN, MT5_PASSWORD, MT5_SERVER)
    threading.Thread(target=poll_updates,      daemon=True).start()
    threading.Thread(target=monitor_positions, daemon=True).start()
    threading.Thread(target=mt5_executor,      daemon=True).start()

    logger.info("Bot running…")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down…")
