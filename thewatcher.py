#!/usr/bin/env python3
# Gerekli paketler:
# pip install requests openpyxl

import sys
import os
import time
import hmac
import hashlib
from pathlib import Path
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment


# ---------------------------
# Darkex endpointleri
# ---------------------------
URL = "https://www.darkex.com/fe-co-api/common/public_info"
TIER_URL = "https://www.darkex.com/fe-co-api/common/public_futures_contract_info"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json;charset=UTF-8",
    "cookie": "lan=en_US; cusSkin=1; _fbp=fb.1.1745354379248.163616128196321939; _ga=GA1.1.1245577152.1745354379; CHFIT_EXPIRATION=1776890380191; CHFIT_DEVICEID=lkWin7X1M2i0yHLWHdFi00Cbn-s1hP8kuFE_1mQ6oV7P9sQbcCs9Iov7cdoFlD1R; _c_WBKFRo=f9NDVPcQW7DStvrplQolw2uHnp5MAaBy5wc2HS5a; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2232969520%22%2C%22first_id%22%3A%221965f3a4c5934-097b306403171b8-26011c51-2073600-1965f3a4c5a11e0%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk2NWYzYTRjNTkzNC0wOTdiMzA2NDAzMTcxYjgtMjYwMTFjNTEtMjA3MzYwMC0xOTY1ZjNhNGM1YTExZTAiLCIkaWRlbnRpdHlfbG9naW5faWQiOiIzMjk2OTUyMCJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%2232969520%22%7D%7D; _ga_F1W306LH6F=GS2.1.s1747236858$o17$g0$t1747236858$j0$l0$h0; _ga_C8LDVXWDFX=GS2.1.s1747914149$o1$g1$t1747914351$j0$l0$h0; _ym_uid=1751001675844873554; _ym_d=1751001675; __adroll_fpc=06edb2ca75aa9ba5d43e72804323fd14-1751001675526; FP_DEVICE_ID=911fd120906b129f1e5bd990b8bce28c; FP_DEVICE_VISITORID=c5975ddb8cc0c21ee8c522e214aa267f; __ar_v4=EYQ6NBYIZVHR5EJ6S6GMBM%3A20250726%3A9%7CH6G3HUORVBAH7GXUFX35SX%3A20250726%3A9%7CFZFY2R4NYBEW3BCPTLTO7I%3A20250726%3A9; lan.sig=DNIiOSD1Q3ofA_oyppgjDfS2LXe3cDxtJwaaLdF0lFs; JSESSIONID=3D87863CB7063EC16AF9907473CAD0FD; token=bbee1f3b62e0021379a0ea496450e9c15f7963eeb0e54238b969367a3e7b4021; isLogin=true; _gcl_au=1.1.1539652439.1755367154; _tt_enable_cookie=1; _ttp=01K2T0J8SZ9QF8NQHV16R45CW8_.tt.1; _ym_isad=2; ttcsid=1755367154502::kc4hHH0s1XXDchSNse9g.1.1755367557069; ttcsid_D28BJ0JC77UB6AOKCVE0=1755367154501::H1-vqyElou2RbjIxFZxx.1.1755367559055; _ga_4JHJ4YPRL8=GS2.1.s1755371256$o129$g0$t1755371256$j60$l0$h0; _ga_3JN0V1H9P0=GS2.1.s1755371256$o42$g0$t1755371256$j60$l0$h0",
    "device": "c5975ddb8cc0c21ee8c522e214aa267f",
    "exchange-client": "pc",
    "exchange-language": "en_US",
    "exchange-token": "bbee1f3b62e0021379a0ea496450e9c15f7963eeb0e54238b969367a3e7b4021",
    "futures-version": "101",
    "is-sub": "0",
    "origin": "https://www.darkex.com",
    "priority": "u=1, i",
    "referer": "https://www.darkex.com/en_US/futures/futuresData?marginCoin=USDT&type=1&contractId=467",
    "sec-ch-ua": "\"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"138\", \"Google Chrome\";v=\"138\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    # uuid-cu varsa ekleyebilirsiniz: "uuid-cu": "..."
}


# ---------------------------
# Binance (USDⓈ-M Futures) ayarları
# ---------------------------
API_KEY_BINANCE = "iJmoTezYi1v82UJ6IFFYUtgoH7Xf5lmmmdbZguxS3BPhm93RMB7VM7slDfnp2TM2"
API_SECRET_BINANCE = "FXIEwjfjYuYFyAfpFT9ud5gluG3OzsNd68Fj7iPeIeXa1T2na6PwWpMPvcqO3lOy"

BASE_URL_BINANCE = "https://fapi.binance.com"
ENDPOINT_BINANCE = "/fapi/v1/leverageBracket"  # tüm semboller için risk limit


# ===========================
# Mevcut yardımcılar (değişmeden)
# ===========================
def normalize_symbol(c: dict) -> str:
    """
    Darkex sözleşme objesinden base/quote veya symbol alanlarını kullanarak
    standart sembol (ör. BTCUSDT) oluşturur.
    """
    base, quote = c.get("base"), c.get("quote")
    if base and quote:
        return f"{base}{quote}"
    sym = c.get("symbol") or ""
    return sym.replace("-", "")


def extract_symbol_id(data: dict):
    """
    API'den dönen data objesinden (data['contractList']) (symbol, contract_id) listesi çıkarır.
    Alfabetik olarak sembole göre sıralar.
    """
    rows = []
    for c in (data or {}).get("contractList", []):
        sym = normalize_symbol(c)
        cid = c.get("id")
        if sym and cid is not None:
            rows.append((sym, cid))
    return sorted(rows, key=lambda x: x[0])


def get_desktop_path() -> Path:
    """
    Kullanıcının Masaüstü klasörünün yolunu döndürür.
    - Varsayılan: ~ / Desktop
    - Windows'ta özel konumlandırma varsa kayıt defterinden okumayı dener.
    - Masaüstü mevcut değilse ev dizinine döner.
    """
    home = Path.home()
    desktop = home / "Desktop"
    if desktop.exists():
        return desktop

    try:
        if os.name == "nt":
            import winreg
            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders"
            ) as key:
                desktop_dir = winreg.QueryValueEx(key, "Desktop")[0]
                p = Path(desktop_dir)
                if p.exists():
                    return p
    except Exception:
        pass

    return home


def save_to_excel(rows, filepath: Path):
    """
    (symbol, contract_id) satırlarını tek sayfalık bir Excel dosyasına yazar.
    Başlıkları kalın yapar, sütun genişliklerini içerik uzunluğuna göre ayarlar.
    """
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "Contracts"

    headers = ["SYMBOL", "CONTRACT_ID"]
    header_font = Font(bold=True)
    left_align = Alignment(horizontal="left")

    ws.cell(row=1, column=1, value=headers[0]).font = header_font
    ws.cell(row=1, column=2, value=headers[1]).font = header_font

    r = 2
    for sym, cid in rows:
        ws.cell(row=r, column=1, value=str(sym)).alignment = left_align
        ws.cell(row=r, column=2, value=cid)
        r += 1

    max_len_symbol = max(len("SYMBOL"), *(len(str(s)) for s, _ in rows)) if rows else len("SYMBOL")
    max_len_id = max(len("CONTRACT_ID"), *(len(str(cid)) for _, cid in rows)) if rows else len("CONTRACT_ID")

    ws.column_dimensions["A"].width = max_len_symbol + 2
    ws.column_dimensions["B"].width = max_len_id + 2
    ws.freeze_panes = "A2"

    filepath.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(filepath))


# ---------------------------
# Darkex Tier çekme ve yazdırma
# ---------------------------
def build_referer_for_contract(cid: int) -> str:
    return f"https://www.darkex.com/en_US/futures/futuresData?marginCoin=USDT&type=1&contractId={cid}"


def find_contract_mapping_file() -> Path | None:
    """
    contract_id_mapping.xlsx dosyasını önce çalışma dizininde, sonra Masaüstünde arar.
    Bulamazsa None döner.
    """
    cwd = Path.cwd() / "contract_id_mapping.xlsx"
    if cwd.exists():
        return cwd
    desktop = get_desktop_path() / "contract_id_mapping.xlsx"
    if desktop.exists():
        return desktop
    return None


def load_contract_ids_from_excel(filepath: Path):
    """
    contract_id_mapping.xlsx dosyasından (SYMBOL, CONTRACT_ID) listesi okur.
    """
    try:
        wb = load_workbook(filepath, read_only=True, data_only=True)
    except Exception as e:
        print(f"Excel okuma hatası ({filepath}): {e}", file=sys.stderr)
        sys.exit(4)

    ws = wb.active
    headers = [str(cell.value).strip() if cell.value is not None else "" for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    header_map = {h.lower(): idx for idx, h in enumerate(headers)}
    sym_idx = header_map.get("symbol")
    cid_idx = header_map.get("contract_id")
    if cid_idx is None:
        wb.close()
        print("Excel'de 'CONTRACT_ID' sütunu bulunamadı.", file=sys.stderr)
        sys.exit(5)

    rows = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        sym = row[sym_idx] if sym_idx is not None else ""
        cid = row[cid_idx]
        if cid is None:
            continue
        try:
            cid_int = int(str(cid).split(".")[0].strip())
        except Exception:
            continue
        sym_str = str(sym).strip() if sym is not None else ""
        rows.append((sym_str, cid_int))

    wb.close()
    return rows


def fetch_tier_info(session: requests.Session, contract_id: int):
    """
    Darkex: Belirli CONTRACT_ID için tier listesi.
    """
    ua_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {"contractId": contract_id, "uaTime": ua_time}

    hdr = dict(HEADERS)
    hdr["referer"] = build_referer_for_contract(contract_id)

    try:
        r = session.post(TIER_URL, json=payload, headers=hdr, timeout=20)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        return {"ok": False, "error": f"istek/parse hatası: {e}", "contract_id": contract_id}

    if j.get("code") == "0":
        data = j.get("data", {}) or {}
        return {
            "ok": True,
            "contract_id": contract_id,
            "leverMarginInfo": data.get("leverMarginInfo", []) or [],
            "coinAlias": data.get("coinAlias"),
            "mTime": data.get("mTime"),
        }
    else:
        return {"ok": False, "error": f"API hata cevabı: {j}", "contract_id": contract_id}


def format_mtime(ms_value) -> str:
    try:
        ms_int = int(str(ms_value))
        dt = datetime.fromtimestamp(ms_int / 1000.0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def print_tier_block(symbol: str, contract_id, result: dict):
    """
    Tek bir sembol/kontrat için tier listesini terminale alt alta yazdırır.
    contract_id Darkex için int, Binance için 'BINANCE' gibi string olabilir.
    """
    header_line = f"{symbol}  (CONTRACT_ID={contract_id})" if symbol else f"(CONTRACT_ID={contract_id})"
    print(header_line)
    if not result or not result.get("ok"):
        err = (result or {}).get("error", "tier verisi alınamadı")
        print(f"  Hata: {err}")
        print("-" * 48)
        return

    tiers = result.get("leverMarginInfo", []) or []
    coin_alias = result.get("coinAlias")
    mtime_str = format_mtime(result.get("mTime"))
    if coin_alias:
        print(f"  Margin Coin: {coin_alias}")
    if mtime_str:
        print(f"  mTime: {mtime_str}")

    if not tiers:
        print("  Tier verisi yok.")
        print("-" * 48)
        return

    print("  level | maxLever | minPositionValue | maxPositionValue | minMarginRate")
    for t in tiers:
        level = t.get("level")
        maxLever = t.get("maxLever")
        minPos = t.get("minPositionValue")
        maxPos = t.get("maxPositionValue")
        minRate = t.get("minMarginRate")
        print(f"  {str(level):>5} | {str(maxLever):>8} | {str(minPos):>16} | {str(maxPos):>16} | {str(minRate):>12}")
    print("-" * 48)


def batch_fetch_and_print_tiers(rows):
    """
    Darkex: Çoklu CONTRACT_ID için eşzamanlı istek ve yazdırma.
    """
    max_workers = min(12, max(4, (os.cpu_count() or 4) * 2))
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [(sym, cid, ex.submit(fetch_tier_info, session, cid)) for sym, cid in rows]
            for sym, cid, fut in futures:
                res = None
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"ok": False, "error": f"future/unknown: {e}", "contract_id": cid}
                print_tier_block(sym, cid, res)


# ---------------------------
# Binance risk limit (tier) çekme ve yazdırma
# ---------------------------
def fetch_all_brackets_binance(session: requests.Session):
    """
    Binance USDⓈ-M Futures: Tüm semboller için risk limit tablolarını çeker.
    """
    ts = str(int(time.time() * 1000))
    qs = f"timestamp={ts}&recvWindow=5000"
    signature = hmac.new(API_SECRET_BINANCE.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE_URL_BINANCE}{ENDPOINT_BINANCE}?{qs}&signature={signature}"
    headers = {"X-MBX-APIKEY": API_KEY_BINANCE}

    resp = session.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()  # list[ {symbol, brackets:[...]} ]


def binance_brackets_to_tiers_entry(entry: dict):
    """
    Binance 'brackets' girdisini (symbol için) Darkex formatına dönüştürür.
    """
    tiers = []
    for br in entry.get("brackets", []):
        level = br.get("bracket")
        lev = br.get("initialLeverage")
        max_lever = f"{int(lev)}x" if lev is not None else ""
        # notional tercih, yoksa qty:
        min_pos = br.get("notionalFloor", br.get("qtyFloor"))
        max_pos = br.get("notionalCap", br.get("qtyCap"))
        mmr = br.get("maintMarginRatio")
        if mmr is not None:
            try:
                min_rate = f"{float(mmr) * 100:.2f}%"
            except Exception:
                min_rate = str(mmr)
        else:
            min_rate = ""

        tiers.append({
            "level": str(level) if level is not None else "",
            "maxLever": max_lever,
            "minPositionValue": str(min_pos) if min_pos is not None else "",
            "maxPositionValue": str(max_pos) if max_pos is not None else "",
            "minMarginRate": min_rate,
        })
    return tiers


def unique_symbols_in_order(rows):
    """
    rows: [(symbol, contract_id)] -> list of unique symbols preserving order.
    """
    seen = set()
    out = []
    for sym, _ in rows:
        s = str(sym).strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def print_binance_tiers_for_symbols(symbols):
    """
    Binance: Veriyi tek çağrıda alır, istenen semboller için Darkex ile aynı
    tablo formatında (kolon adları ve hizalama) terminale yazar.
    """
    with requests.Session() as session:
        try:
            all_data = fetch_all_brackets_binance(session)
        except Exception as e:
            print(f"Binance risk limit verisi alınamadı: {e}", file=sys.stderr)
            return

    # symbol -> entry map
    by_symbol = {}
    for item in all_data or []:
        sym = item.get("symbol")
        if sym:
            by_symbol[sym] = item

    for sym in symbols:
        entry = by_symbol.get(sym)
        if not entry:
            print_tier_block(sym, "BINANCE", {"ok": False, "error": "Sembol bulunamadı"})
            continue

        tiers = binance_brackets_to_tiers_entry(entry)
        # USDT çiftleri için Margin Coin'i yazdırmak isterseniz:
        coin_alias = "USDT" if sym.endswith("USDT") else None
        result = {"ok": True, "leverMarginInfo": tiers}
        if coin_alias:
            result["coinAlias"] = coin_alias

        print_tier_block(sym, "BINANCE", result)


# ---------------------------
# main
# ---------------------------
def main():
    # 1) contract_id_mapping.xlsx varsa: Darkex tier -> ardından Binance tier yazdır.
    mapping_file = find_contract_mapping_file()
    if mapping_file is not None:
        rows = load_contract_ids_from_excel(mapping_file)
        if not rows:
            print(f"{mapping_file} içinde işlenecek satır bulunamadı.", file=sys.stderr)
            sys.exit(6)

        # Darkex
        batch_fetch_and_print_tiers(rows)

        # Binance (aynı semboller)
        symbols = unique_symbols_in_order(rows)
        if symbols:
            # Araya boş satır
            print()
            print_binance_tiers_for_symbols(symbols)
        return

    # 2) Aksi halde: public_info -> Excel'e yaz ve Masaüstüne kaydet (mevcut davranış korunur)
    ua_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {"uaTime": ua_time}

    try:
        r = requests.post(URL, json=payload, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"İstek hatası: {e}", file=sys.stderr)
        sys.exit(1)

    if data.get("code") != "0":
        print(f"API hata cevabı: {data}", file=sys.stderr)
        sys.exit(2)

    payload_data = data.get("data", {})
    rows = extract_symbol_id(payload_data)

    desktop = get_desktop_path()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"darkex_symbol_contracts_{timestamp}.xlsx"
    save_path = desktop / filename

    try:
        save_to_excel(rows, save_path)
    except Exception as e:
        print(f"Excel yazma/kaydetme hatası: {e}", file=sys.stderr)
        sys.exit(3)

    print(f"{len(rows)} satır Excel dosyasına yazıldı ve kaydedildi: {save_path}")


if __name__ == "__main__":
    main()
