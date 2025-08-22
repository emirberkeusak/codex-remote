import time
import threading
import queue
import logging
import math
from urllib.parse import urlencode

import requests
import pandas as pd
import MetaTrader5 as mt5

# --- Logging setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === Configuration ===
TELEGRAM_BOT_TOKEN = "8082196969:AAEol89om4b0LNIzWebTLOBXg1j4MW_19DA"
CHAT_ID            = "-4847294401"

DARKEX_API_KEY     = "cb658cb61b423d35be9bf855e21a34e7"
DARKEX_SECRET_KEY  = "638beeeb2ac75e229cf716202fc8da6c"
DARKEX_BASE_URL    = "https://futuresopenapi.darkex.com"

MT5_LOGIN     =  810005950
MT5_PASSWORD  = "nDGbT_4O"
MT5_SERVER    = "Taurex-Demo"
MT5_LEVERAGE  = 1

# Darkex symbol → MT5 symbol
SYMBOL_MAP = {
    "USDT1791-BTC-USDT": "BTCUSD",
    "USDT1791-ETH-USDT": "ETHUSD",
}

# Darkex kontrat başına baz varlık miktarı
# Darkex GUI'de Contract Size=0.001 BTC diyorsa 0.001 bırakın
CONTRACT_SIZE = {
    "USDT1791-BTC-USDT": 0.001,
    "USDT1791-ETH-USDT": 0.001,
}

mt5_queue = queue.Queue()


# === DARKEX REST helpers ===

def generate_signature(timestamp, method, request_path, body, secret_key):
    pre = timestamp + method.upper() + request_path + (body or "")
    import hmac, hashlib
    sig = hmac.new(secret_key.encode(), pre.encode(), hashlib.sha256).hexdigest()
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
    acct_list = r.json().get("account", [])
    rows = []
    for acct in acct_list:
        for vo in acct.get("positionVos", []):
            cname = vo.get("contractName")
            for pos in vo.get("positions", []):
                print(pos)
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
    return pd.DataFrame(rows)

def send_text_to_telegram(token, chat_id, text):
    url  = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    try:
        r = requests.post(url, data=data, timeout=5)
        r.raise_for_status()
    except Exception as e:
        logger.error("Telegram send failed: %s", e)


# === MT5 Integration ===

def init_mt5(login, password, server):
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    if not mt5.login(login, password, server):
        raise RuntimeError(f"MT5 login failed: {mt5.last_error()}")
    logger.info("MT5 initialized and logged in")


def volume_to_lots(darkex_symbol, contract_count):
    """
    Darkex kontrat sayısını MT5 lot'a çevirir:
      raw_lots = contract_count * Darkex_contract_size * MT5_LEVERAGE
                 / MT5_contract_size
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
    if lots < vol_min:
        lots = vol_min

    return lots


def mt5_executor():
    for s in SYMBOL_MAP.values():
        mt5.symbol_select(s, True)

    mt5_tickets = {}
    while True:
        evt = mt5_queue.get()
        sym = SYMBOL_MAP[evt['symbol']]

        if evt['action'] == 'open':
            cnt  = evt['contracts']
            lots = volume_to_lots(evt['symbol'], cnt)
            tick = mt5.symbol_info_tick(sym)
            price = tick.ask if evt['side'] == "BUY" else tick.bid

            req = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "symbol":       sym,
                "volume":       lots,
                "type":         mt5.ORDER_TYPE_BUY if evt['side']=="BUY" else mt5.ORDER_TYPE_SELL,
                "price":        price,
                "deviation":    10,
                "magic":        123456,
                "comment":      "Darkex open",
                "type_time":    mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            res = mt5.order_send(req)
            if res.retcode == mt5.TRADE_RETCODE_DONE:
                key = f"{evt['symbol']}|{evt['side']}"
                mt5_tickets.setdefault(key, []).append(res.order)
                logger.info("MT5 opened ticket=%s lots=%.4f", res.order, lots)
            else:
                logger.error("MT5 open failed: %s", res)

        else:  # close
            rem_contracts = evt['contracts']
            key = f"{evt['symbol']}|{evt['side']}"
            tickets = mt5_tickets.get(key, [])
            if not tickets:
                logger.error("No tickets to close for %s", key)
                continue

            info = mt5.symbol_info(sym)
            lev  = MT5_LEVERAGE

            new_list = []
            tick = mt5.symbol_info_tick(sym)
            for ticket in tickets:
                if rem_contracts <= 0:
                    new_list.append(ticket)
                    continue

                pos = next((p for p in mt5.positions_get(symbol=sym) if p.ticket == ticket), None)
                if not pos:
                    continue

                darkex_ct_size = CONTRACT_SIZE[evt['symbol']]
                mt5_ct_size    = info.trade_contract_size
                available_contracts = pos.volume * mt5_ct_size / darkex_ct_size / lev
                to_close = min(rem_contracts, available_contracts)

                lots = volume_to_lots(evt['symbol'], to_close)
                lots = min(lots, pos.volume)

                close_type = mt5.ORDER_TYPE_SELL if evt['side']=="BUY" else mt5.ORDER_TYPE_BUY
                price      = tick.bid if close_type==mt5.ORDER_TYPE_SELL else tick.ask

                req = {
                    "action":       mt5.TRADE_ACTION_DEAL,
                    "symbol":       sym,
                    "volume":       lots,
                    "type":         close_type,
                    "position":     ticket,
                    "price":        price,
                    "deviation":    10,
                    "magic":        123456,
                    "comment":      "Darkex close",
                    "type_time":    mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }
                res = mt5.order_send(req)
                if res.retcode == mt5.TRADE_RETCODE_DONE:
                    logger.info("MT5 closed ticket=%s lots=%.4f", ticket, lots)
                    rem_contracts -= to_close
                    if available_contracts > to_close:
                        new_list.append(ticket)
                else:
                    logger.error("MT5 close failed: %s", res)
                    new_list.append(ticket)

            if new_list:
                mt5_tickets[key] = new_list
            else:
                mt5_tickets.pop(key, None)


def _pos_key(pos):
    return f"{pos['contractName']}|{pos['side']}"


def monitor_positions(poll_interval=10):
    prev = {}
    while True:
        try:
            df = get_open_positions_df()
            curr = { _pos_key(r): r for _, r in df.iterrows() }

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
            # Fetch Darkex positions
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
            # Fetch MT5 positions
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

        # Combine and send
        full_reply = "\n".join(darkex_msg + [""] + mt5_msg)
        send_text_to_telegram(TELEGRAM_BOT_TOKEN, chat_id, full_reply)

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
