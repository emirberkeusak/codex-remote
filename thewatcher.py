from license_checker import run_license_check
from PySide6.QtWidgets import QMessageBox
import sys
import os
import asyncio
import json
import re
import time
from datetime import datetime
import pandas as pd


import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedError, InvalidStatusCode
from urllib.parse import urlencode

from PySide6 import QtWidgets, QtCore, QtGui, QtCharts
from PySide6.QtWidgets import QHeaderView
from PySide6.QtCharts import (QChart, QChartView, QLineSeries, QDateTimeAxis, QValueAxis)
from PySide6.QtWidgets import QGraphicsSimpleTextItem
from PySide6.QtCore import Qt
import qasync

RE_SWAP_SUFFIX = re.compile(r"-SWAP$")
RE_PERP_SUFFIX = re.compile(r"(-PERP|PERP)$")
RE_AFTER_USDT = re.compile(r"(?<=USDT)_.*$")
RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_TRAILING_DIGITS = re.compile(r"\d+$")
RE_NON_ALNUM = re.compile(r"[^A-Z0-9]")


def resource_path(relative_path):
    """EXE içinden splash.png yolunu çözer"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

# --- Constants & Endpoints ---
EXCHANGES = ["Binance", "OKX", "Bybit", "Bitget", "Gateio"]
# Sadece Binance için Ask/Bid kolonları
AB_EXCHANGES = ["Binance", "OKX", "Bybit", "Bitget", "Gateio"]

# Funding tablosu kolon başlıkları
FUNDING_COLUMNS = [
    "Symbol",
    "Binance",
    "Binance Countdown",
    "OKX",
    "OKX Countdown",
    "Bybit",
    "Bybit Countdown",
    "Bitget",
    "Bitget Countdown",
    "Gateio",
    "Gateio Countdown",
]

# Funding Rate Diff tablosu kolon başlıkları
FUNDING_RATE_DIFF_COLUMNS = [
    "Symbol",
    "Exch",
    "Funding Rate",
    "Countdown",
    "Ask Derinlik",
    "3 Kademe Derinlik",
    "Bid Derinlik",
    "3 Kademe Derinlik",
    "Index Price",
]

BINANCE_URL            = "wss://fstream.binance.com/stream?streams=!markPrice@arr"
BINANCE_REST_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
OKX_WS_URL             = "wss://ws.okx.com:8443/ws/v5/public"
OKX_REST_INSTRUMENTS   = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
BYBIT_WS_URL           = "wss://stream.bybit.com/v5/public/linear"
BYBIT_REST_INSTRUMENTS = "https://api.bybit.com/v5/market/instruments-info?category=linear"
BITGET_WS_URL          = "wss://ws.bitget.com/v2/ws/public"
BITGET_REST_CONTRACTS  = "https://api.bitget.com/api/v2/mix/market/contracts"
GATEIO_WS_URL          = "wss://fx-ws.gateio.ws/v4/ws/usdt"
GATEIO_REST_TICKERS    = "https://api.gateio.ws/api/v4/futures/usdt/tickers"


BYBIT_BATCH_SIZE     = 50
BYBIT_OPEN_TIMEOUT   = 30
BYBIT_CLOSE_TIMEOUT  = 10

FEE_RATE_BUY  = 0.0005  # Commission rate when buying
FEE_RATE_SELL = 0.0005  # Commission rate when selling
DEBUG_BITGET_ORDERBOOK = False


# --- Helper parsers -------------------------------------------------------
def _parse_float(text: str) -> float | None:
    """Return float value or None if empty/invalid."""
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _parse_duration(text: str) -> int | None:
    """Convert HH:MM:SS string to seconds. Return None if invalid."""
    if not text:
        return None
    m = re.fullmatch(r"(\d{1,2}):(\d{1,2}):(\d{1,2})", text.strip())
    if not m:
        return None
    h, m_, s = map(int, m.groups())
    return h * 3600 + m_ * 60 + s


# Supabase configuration
SUPABASE_URL = "https://obtqpnfcfmybasnzclqf.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9idHFw"
    "bmZjZm15YmFzbnpjbHFmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTExNTYzMDYsImV4cCI6MjA2"
    "NjczMjMwNn0.7XIFyJoBSqV1L-QeMJY14bOfbpGiFqHUTAsqK4e67ao"
)

# Persistent aiohttp sessions keyed by event loop
_supabase_sessions: dict[asyncio.AbstractEventLoop, aiohttp.ClientSession] = {}

async def _get_supabase_session() -> aiohttp.ClientSession:
    """Return a persistent aiohttp session for the running loop."""
    loop = asyncio.get_running_loop()
    sess = _supabase_sessions.get(loop)
    if sess is None or sess.closed:
        sess = aiohttp.ClientSession()
        _supabase_sessions[loop] = sess
    return sess

async def close_supabase_session() -> None:
    """Close the session associated with the running loop if any."""
    loop = asyncio.get_running_loop()
    sess = _supabase_sessions.pop(loop, None)
    if sess and not sess.closed:
        await sess.close()

async def close_all_supabase_sessions() -> None:
    """Close all cached sessions."""
    for sess in list(_supabase_sessions.values()):
        if not sess.closed:
            await sess.close()
    _supabase_sessions.clear()


async def _supabase_post(
    endpoint: str,
    payload: dict | list[dict],
    params: dict | None = None,
    ignore_duplicates: bool = False,
) -> bool:
    """Send a POST request to Supabase REST endpoint."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    if params:
        url += f"?{urlencode(params)}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    prefer = ["return=representation"]
    if ignore_duplicates:
        prefer.append("resolution=ignore-duplicates")
    headers["Prefer"] = ",".join(prefer)

    session = await _get_supabase_session()
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            text = await resp.text()
            if resp.status >= 400:
                body = text.strip()
                print(
                    f"Supabase POST {endpoint} failed: {resp.status} {body}",
                    file=sys.stderr,
                )
                return False
            return True
    except aiohttp.ClientError as e:
        print(f"Supabase POST {endpoint} exception: {e}", file=sys.stderr)
        return False
        

async def _supabase_get(endpoint: str, params: dict) -> list[dict]:
    """Send a GET request to Supabase REST endpoint."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    session = await _get_supabase_session()
    async with session.get(url, headers=headers, params=params) as resp:
        if resp.status >= 400:
            await resp.text()
            return []
        return await resp.json()


async def fetch_closed_logs(
    start: str,
    end: str,
    symbols: list[str] | None = None,
    buy_exchs: list[str] | None = None,
    sell_exchs: list[str] | None = None,
) -> list[dict]:
    """Retrieve closed arbitrage logs filtered by optional symbol and exchanges."""
    params = [
        ("start_dt", f"gte.{start}"),
        ("start_dt", f"lte.{end}"),
    ]
    if symbols:
        params.append(("symbol", f"in.({','.join(symbols)})"))
    if buy_exchs:
        params.append(("buy_exch", f"in.({','.join(buy_exchs)})"))
    if sell_exchs:
        params.append(("sell_exch", f"in.({','.join(sell_exchs)})"))
    return await _supabase_get("closed_arbitrage_logs", params)


async def fetch_db_symbols() -> list[str]:
    """Return unique symbols stored in closed_arbitrage_logs table."""
    recs = await _supabase_get("closed_arbitrage_logs", {"select": "symbol"})
    return sorted({r.get("symbol") for r in recs if r.get("symbol")})


async def fetch_db_buy_exchs() -> list[str]:
    """Return unique buy exchanges stored in closed_arbitrage_logs table."""
    recs = await _supabase_get("closed_arbitrage_logs", {"select": "buy_exch"})
    return sorted({r.get("buy_exch") for r in recs if r.get("buy_exch")})


async def fetch_db_sell_exchs() -> list[str]:
    """Return unique sell exchanges stored in closed_arbitrage_logs table."""
    recs = await _supabase_get("closed_arbitrage_logs", {"select": "sell_exch"})
    return sorted({r.get("sell_exch") for r in recs if r.get("sell_exch")})



# --- Helper: normalize for subscription endpoints ---
def normalize_pair(pair: str) -> str:
    s = pair.upper()
    s = RE_SWAP_SUFFIX.sub("", s)
    s = RE_PERP_SUFFIX.sub("", s)
    return s.replace('-', '')


# --- Symbol normalization & USDT-only filter for UI ---
def normalize_symbol(sym: str) -> str | None:
    s = sym.upper()
    # strip swap/perp suffixes
    s = RE_SWAP_SUFFIX.sub("", s)
    s = RE_PERP_SUFFIX.sub("", s)
    # remove hyphens
    s = s.replace('-', '')
    # strip suffixes after USDT (e.g. "BTCUSDT_UMCBL" -> "BTCUSDT")
    s = RE_AFTER_USDT.sub("", s)
    # must end with USDT
    if not s.endswith("USDT"):
        return None
    # strip leading digits
    s = RE_LEADING_DIGITS.sub("", s)
    # drop if still ends in digits
    if RE_TRAILING_DIGITS.search(s):
        return None
    # keep only alphanumeric
    s = RE_NON_ALNUM.sub("", s)
    return s or None


# --- Flash Delegate for red/green animation ---
class FlashDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._flash_cells: dict[tuple[int,int], tuple[float,bool]] = {}
        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._on_timeout)
        self._timer.setInterval(50)
        self.enabled = True

    def setEnabled(self, enabled: bool):
        """Animasyonları aç/kapat"""
        self.enabled = enabled
        if not enabled:
            self._flash_cells.clear()


    def mark_changed(self, index: QtCore.QModelIndex, positive: bool):
        if not self.enabled:
            return      #animasyonlar kapalıysa hiç işleme alma
        self._flash_cells[(index.row(), index.column())] = (time.time(), positive)
        if not self._timer.isActive():
            self._timer.start()

    def paint(self, painter: QtGui.QPainter, option, index):
        key = (index.row(), index.column())
        if key in self._flash_cells:
            ts, positive = self._flash_cells[key]
            elapsed = time.time() - ts
            DURATION = 0.5   # ← 1.0’den 0.5’e indiriyoruz
            if elapsed < DURATION:
                alpha = int(255 * (1 - elapsed / DURATION))
                color = QtGui.QColor(0,255,0,alpha) if positive else QtGui.QColor(255,0,0,alpha)
                painter.fillRect(option.rect, color)
            else:
                del self._flash_cells[key]
        super().paint(painter, option, index)

    def _on_timeout(self):
        view = self.parent()
        if view and self._flash_cells:
            view.viewport().update()
        elif not self._flash_cells and self._timer.isActive():
            self._timer.stop()


# --- Multi-select filter dropdown ---
class MultiSelectDropdown(QtWidgets.QWidget):
    selectionChanged = QtCore.Signal(set)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFixedSize(25, 20)
        self._items = []
        self._selected_items = set()
        
    def set_items(self, items):
        # 1) Yeni listeyi A→Z sırala
        new_items = sorted(items)
        old_items = set(self._items)
        old_selected = set(self._selected_items)

        # 2) Eğer daha önce hiç öğe yoktu, seçili öğrenci boştu → tümünü seç
        if not old_items:
            self._selected_items = set(new_items)

        # 3) Eğer eski listenin tamamı seçiliydi (Select All yapılmıştı) → tüm yeni listeyi seç
        elif old_selected >= old_items:
            self._selected_items = set(new_items)

        # 4) Aksi halde, önceki seçimi koru (yeniden işaretlenenleri de dahil etme)
        else:
            self._selected_items &= set(new_items)

        # 5) Öğeleri güncelle
        self._items = new_items

    
    def get_selected_items(self):
        return self._selected_items.copy()
    
    def paintEvent(self, event):
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing)
        
        # Draw small triangle
        rect = self.rect()
        center = rect.center()
        
        # Background
        painter.fillRect(rect, QtGui.QColor(240, 240, 240))
        painter.setPen(QtGui.QColor(120, 120, 120))
        painter.drawRect(rect.adjusted(0, 0, -1, -1))
        
        # Triangle
        painter.setPen(QtCore.Qt.NoPen)
        painter.setBrush(QtGui.QColor(80, 80, 80))
        
        triangle = QtGui.QPolygon([
            QtCore.QPoint(center.x() - 4, center.y() - 2),
            QtCore.QPoint(center.x() + 4, center.y() - 2),
            QtCore.QPoint(center.x(), center.y() + 3)
        ])
        painter.drawPolygon(triangle)
    
    def mousePressEvent(self, event):
        if event.button() == QtCore.Qt.LeftButton:
            self._show_dropdown()
    
    def _show_dropdown(self):
        if not self._items:
            return

        # Popup dialog
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowFlags(QtCore.Qt.Popup | QtCore.Qt.FramelessWindowHint)
        dialog.setModal(True)
        dialog.resize(200, 300)

        layout = QtWidgets.QVBoxLayout(dialog)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(2)

        # — Arama Çubuğu —
        search_bar = QtWidgets.QLineEdit()
        search_bar.setPlaceholderText("Ara…")
        search_bar.setFixedHeight(25)
        layout.addWidget(search_bar)
        # --------------------

        # Add Select All / Deselect All buttons
        btn_layout = QtWidgets.QHBoxLayout()
        select_all_btn   = QtWidgets.QPushButton("Select All")
        deselect_all_btn = QtWidgets.QPushButton("Deselect All")
        select_all_btn.setFixedHeight(25)
        deselect_all_btn.setFixedHeight(25)
        btn_layout.addWidget(select_all_btn)
        btn_layout.addWidget(deselect_all_btn)
        layout.addLayout(btn_layout)

        # Create scrollable list
        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)

        list_widget = QtWidgets.QWidget()
        list_layout = QtWidgets.QVBoxLayout(list_widget)
        list_layout.setContentsMargins(0, 0, 0, 0)
        list_layout.setSpacing(1)

        # Add checkboxes for each item
        checkboxes = {}
        for item in self._items:
            checkbox = QtWidgets.QCheckBox(item)
            checkbox.setChecked(item in self._selected_items)
            checkbox.setFixedHeight(20)
            list_layout.addWidget(checkbox)
            checkboxes[item] = checkbox

        scroll.setWidget(list_widget)
        layout.addWidget(scroll)

        # — Arama metnine göre checkbox’ları göster/gizle —
        def filter_items(text):
            txt = text.lower()
            for it, cb in checkboxes.items():
                cb.setVisible(txt in it.lower())

        search_bar.textChanged.connect(filter_items)

        # Select All / Deselect All davranışı
        select_all_btn.clicked.connect(lambda: [cb.setChecked(True)  for cb in checkboxes.values()])
        deselect_all_btn.clicked.connect(lambda: [cb.setChecked(False) for cb in checkboxes.values()])

        # OK/Cancel butonları
        ok_btn     = QtWidgets.QPushButton("OK")
        cancel_btn = QtWidgets.QPushButton("Cancel")
        ok_btn.setFixedHeight(25)
        cancel_btn.setFixedHeight(25)
        btn2 = QtWidgets.QHBoxLayout()
        btn2.addWidget(ok_btn)
        btn2.addWidget(cancel_btn)
        layout.addLayout(btn2)

        # Dialog konumu, sonuç ve sinyaller
        global_pos = self.mapToGlobal(QtCore.QPoint(0, self.height()))
        dialog.move(global_pos)

        result = False
        ok_btn.clicked.connect(lambda: dialog.accept() or setattr(self, "_result", True))
        cancel_btn.clicked.connect(lambda: dialog.reject() or setattr(self, "_result", False))

        dialog.exec()

        if getattr(self, "_result", False):
            old = self._selected_items.copy()
            self._selected_items.clear()
            for it, cb in checkboxes.items():
                if cb.isChecked():
                    self._selected_items.add(it)
            if old != self._selected_items:
                self.selectionChanged.emit(self._selected_items)



# --- Custom header with integrated dropdown ---
class FilterableHeaderView(QtWidgets.QHeaderView):
    def __init__(self, orientation, parent=None):
        super().__init__(orientation, parent)
        self.setStretchLastSection(True)
        self._dropdown = None
        
    def set_dropdown(self, dropdown):
        self._dropdown = dropdown
        if dropdown:
            dropdown.setParent(self)
            self._position_dropdown()
    
    def _position_dropdown(self):
        if not self._dropdown:
            return
            
        # Position dropdown next to Symbol column
        symbol_rect = QtCore.QRect(
            self.sectionPosition(0) + self.sectionSize(0) - 30,
            2,
            25,
            self.height() - 4
        )
        self._dropdown.setGeometry(symbol_rect)
    
    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._position_dropdown()
    
    def paintEvent(self, event):
        super().paintEvent(event)
        self._position_dropdown()


# --- Custom proxy model with symbol filtering ---
class SymbolFilterProxyModel(QtCore.QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._filter_symbols = set()

    def set_symbol_filter(self, symbols):
        self._filter_symbols = symbols
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        if not self._filter_symbols:
            return True
        
        source_model = self.sourceModel()
        if not source_model or source_row >= len(source_model._symbols):
            return False
        
        symbol = source_model._symbols[source_row]
        return symbol in self._filter_symbols
    



class ArbitrajEvent:
    def __init__(self, symbol: str, buy_exch: str, sell_exch: str, rate: float= 0.0, initial_ask: float= 0.0, initial_bid: float= 0.0, initial_buy_fr: float= 0.0, initial_sell_fr: float= 0.0):
        self.symbol    = symbol
        self.buy_exch  = buy_exch
        self.sell_exch = sell_exch
        self.rate      = rate
        self.start_dt  = datetime.now()
        self.end_dt    = None

        self.initial_ask      = initial_ask       # tetiklendiği andaki alım-exch ask fiyatı
        self.initial_bid      = initial_bid       # tetiklendiği andaki satım-exch bid fiyatı
        self.final_ask        = None              # kapanırkenki alım-exch ask fiyatı
        self.final_bid        = None              # kapanırkenki satım-exch bid fiyatı
        self.buy_fr           = initial_buy_fr    # tetiklendiği andaki alım-exch funding rate
        self.sell_fr          = initial_sell_fr   # tetiklendiği andaki satım-exch funding rate
        self.final_rate       = None              # kapanıştaki son arbitraj oranı

    @property
    def duration(self):
        end = self.end_dt or datetime.now()
        delta = end - self.start_dt
        minutes = delta.seconds // 60
        seconds = delta.seconds % 60
        return f"{minutes} dk {seconds} sn" 

class ArbitrajDiffModel(QtCore.QAbstractTableModel):

    symbolsUpdated = QtCore.Signal(list)

    def __init__(self):
        super().__init__()
        self.events: list[ArbitrajEvent] = []
        self._row_map: dict[ArbitrajEvent, int] = {}

    def row_of(self, ev: ArbitrajEvent) -> int:
        """Return the current row index for the given event."""
        return self._row_map.get(ev, -1)

    def rowCount(self, parent=None):
        return len(self.events)

    def columnCount(self, parent=None):
        # eskiden 15 sütun, Chart kolonu ile 16 oldu
        return 16

    def headerData(self, section, orientation, role):
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return [
                "Symbol", "Alım Exch", "Satım Exch", "Oran",
                "Başlangıç Zamanı", "Bitiş Zamanı", "Süre", "Son Oran",
                "İlk Ask", "İlk Bid", "Son Ask", "Son Bid",
                "Alım FR", "Satım FR",
                "Tekrar Sayısı", "Chart"
            ][section]
        return None

    def add_event(self, ev: ArbitrajEvent):
        self.beginInsertRows(QtCore.QModelIndex(), len(self.events), len(self.events))
        self.events.append(ev)
        self._row_map[ev] = len(self.events) - 1
        self.endInsertRows()
        self.symbolsUpdated.emit(sorted({e.symbol for e in self.events}))

    def remove_event(self, row: int):
        # bu metot mutlaka burada olmalı
        self.beginRemoveRows(QtCore.QModelIndex(), row, row)
        ev = self.events.pop(row)
        self._row_map.pop(ev, None)
        self.endRemoveRows()
        for idx in range(row, len(self.events)):
            self._row_map[self.events[idx]] = idx
        # Tekrar Sayısı sütununu güncelle
        if self.rowCount() > 0:
            top = self.index(0, 14)
            bot = self.index(self.rowCount()-1, 14)
            self.dataChanged.emit(top, bot, [QtCore.Qt.DisplayRole])
        self.symbolsUpdated.emit(sorted({e.symbol for e in self.events}))

    def end_event(self, row: int):
        # bu metot da burada olacak
        ev = self.events[row]
        ev.end_dt = datetime.now()
        # tüm hücreleri (0’dan en sonuna) güncelle
        tl = self.index(row, 0)
        br = self.index(row, self.columnCount()-1)
        self.dataChanged.emit(tl, br, [
            QtCore.Qt.DisplayRole,
            QtCore.Qt.BackgroundRole,
            QtCore.Qt.ForegroundRole,
            QtCore.Qt.UserRole
        ])
        # Tekrar Sayısı sütununu da baştan sona güncelle
        if self.rowCount() > 0:
            top = self.index(0, 14)
            bot = self.index(self.rowCount()-1, 14)
            self.dataChanged.emit(top, bot, [QtCore.Qt.DisplayRole])
        self.symbolsUpdated.emit(sorted({e.symbol for e in self.events}))

    def clear_events(self):
        """Reset all events and notify views."""
        self.beginResetModel()
        self.events.clear()
        self._row_map.clear()
        self.endResetModel()
        self.symbolsUpdated.emit([])

    def data(self, index, role=QtCore.Qt.DisplayRole):
        # data metodu mutlaka burada tanımlı olmalı
        ev = self.events[index.row()]
        col = index.column()

        if role == QtCore.Qt.DisplayRole:
            if col == 14:
                if ev.end_dt is None:
                    cnt = sum(1 for e in self.events
                              if e.end_dt is None and e.symbol == ev.symbol)
                else:
                    cnt = sum(1 for e in self.events
                              if e.end_dt is not None and e.symbol == ev.symbol)
                return str(cnt)
            
            if col == 15:
                return "Open Chart"
            
            # önceki sütun verileri:
            return {
                0: ev.symbol,
                1: ev.buy_exch,
                2: ev.sell_exch,
                3: f"{ev.rate:.5f}",
                4: ev.start_dt.strftime("%d/%m/%Y %H:%M:%S"),
                5: ev.end_dt.strftime("%d/%m/%Y %H:%M:%S") if ev.end_dt else "",
                6: ev.duration,
                7: f"{ev.final_rate:.5f}" if ev.final_rate is not None else "",
                8: f"{ev.initial_ask:.8f}",
                9: f"{ev.initial_bid:.8f}",
               10: f"{ev.final_ask:.8f}" if ev.final_ask is not None else "",
               11: f"{ev.final_bid:.8f}" if ev.final_bid is not None else "",
               12: f"{ev.buy_fr:.4f}",
               13: f"{ev.sell_fr:.4f}",
            }.get(col, "")

        if role == QtCore.Qt.BackgroundRole and ev.end_dt:
            return QtGui.QBrush(QtGui.QColor(0, 0, 139))
        if role == QtCore.Qt.ForegroundRole and ev.end_dt:
            return QtGui.QBrush(QtGui.QColor(255, 255, 255))
        if role == QtCore.Qt.UserRole:
            grp = 1 if ev.end_dt else 0
            if col == 3:
                key = ev.rate
            elif col in (7,8,9,10):
                key = float(self.data(index, QtCore.Qt.DisplayRole) or 0)
            elif col in (11,12):
                key = float(self.data(index, QtCore.Qt.DisplayRole).rstrip('%') or 0) / 100
            elif col in (4,5):
                key = ev.end_dt if col == 5 and ev.end_dt else ev.start_dt
            else:
                key = self.data(index, QtCore.Qt.DisplayRole)
            return (grp, key)

        return None



# --- REST helpers for subscription lists ---
async def fetch_bybit_swaps() -> list[str]:
    async with aiohttp.ClientSession() as s:
        r = await s.get(BYBIT_REST_INSTRUMENTS)
        j = await r.json()
    return [
        e["symbol"]
        for e in j.get("result", {}).get("list", [])
        if isinstance(e.get("symbol"), str) and e["symbol"].endswith("USDT")
    ]

async def fetch_bitget_swaps() -> list[str]:
    async with aiohttp.ClientSession() as s:
        r = await s.get(BITGET_REST_CONTRACTS, params={"productType":"USDT-FUTURES"})
        j = await r.json()
    return [
        e["symbol"]
        for e in j.get("data", [])
        if isinstance(e.get("symbol"), str) and e["symbol"].upper().endswith("USDT")
    ]

def next_bitget_funding_ts(now: float | None = None) -> float:
    """Return the next Bitget funding timestamp (UTC)."""
    if now is None:
        now = time.time()
    interval = 8 * 3600  # funding every 8 hours
    return ((int(now) // interval) + 1) * interval

def next_gateio_funding_ts(now: float | None = None) -> float:
    """Return the next Gateio funding timestamp (UTC)."""
    if now is None:
        now = time.time()
    interval = 8 * 3600  # funding every 8 hours
    return ((int(now) // interval) + 1) * interval

async def _bitget_ping_loop(ws, interval: int = 15) -> None:
    """Periodically send Bitget-specific ping messages."""
    try:
        while True:
            await asyncio.sleep(interval)
            await ws.send(json.dumps({"op": "ping"}))
    except asyncio.CancelledError:
        pass


async def fetch_gateio_swaps() -> list[str]:
    async with aiohttp.ClientSession() as s:
        r = await s.get(GATEIO_REST_TICKERS)
        j = await r.json()
    return [
        e["contract"]
        for e in j
        if isinstance(e.get("contract"), str)
    ]

async def fetch_binance_futures() -> list[str]:
    async with aiohttp.ClientSession() as s:
        r = await s.get(BINANCE_REST_EXCHANGE_INFO)
        j = await r.json()
    return [
        e["symbol"]
        for e in j.get("symbols", [])
        if e.get("contractType") == "PERPETUAL" and e.get("quoteAsset") == "USDT"
    ]



# --- Qt Table Model ---
class FundingTableModel(QtCore.QAbstractTableModel):
    symbolsUpdated = QtCore.Signal(list)  # Emit when symbols list changes
    
    def __init__(self):
        super().__init__()
        self._symbols  = []                     # list of normalized symbols
        self._data     = {}                     # symbol -> {exchange: str}
        self._previous = {}                     # (symbol,exchange) -> float (rounded to 4dp)
        # (symbol, exchange) -> next funding timestamp
        self._next_funding_ts: dict[tuple[str, str], float] = {}
        # quick lookup for symbol -> row index
        self._row_map: dict[str, int] = {}
        self.delegate  = None
        self._mod_counter = 0                   # incremented on data updates

        # timer for countdown updates
        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._update_countdowns)
        self._timer.start(1000)

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._symbols)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return len(FUNDING_COLUMNS)

    def headerData(self, section, orientation, role):
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return FUNDING_COLUMNS[section]
        return None

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if role != QtCore.Qt.DisplayRole:
            return None
        r, c = index.row(), index.column()
        sym = self._symbols[r]
        column_name = FUNDING_COLUMNS[c]
        if column_name == "Symbol":
            return sym
        if column_name.endswith("Countdown"):
            exch = column_name.split()[0]
            ts = self._next_funding_ts.get((sym, exch))
            if ts is None:
                return ""
            delta = int(ts - time.time())
            if delta < 0:
                delta = 0
            h, rem = divmod(delta, 3600)
            m, s = divmod(rem, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
        exch = column_name
        return self._data.get(sym, {}).get(exch, "")

    @QtCore.Slot(str, str, float, object)
    def update_rate(self, exchange: str, raw_symbol: str, rate_pct: float, next_ts=None):
        sym = normalize_symbol(raw_symbol)
        if not sym:
            return

        key = (sym, exchange)
        rounded = round(rate_pct, 4)
        prev = self._previous.get(key)

        # only flash if previous exists and value actually changed
        changed = (prev is not None and rounded != prev)
        positive = changed and rounded > prev

        # store rounded for next comparison
        self._previous[key] = rounded

        rate_str = f"{rounded:.4f}"
        modified = False
        # insert new row if needed
        if sym not in self._data:
            self.beginInsertRows(QtCore.QModelIndex(),
                                 len(self._symbols),
                                 len(self._symbols))
            self._symbols.append(sym)
            self._data[sym] = {}
            self._row_map[sym] = len(self._symbols) - 1
            self.endInsertRows()
            
            # Emit signal for new symbol
            self.symbolsUpdated.emit(self._symbols.copy())
            modified = True

        # update value
        if self._data.get(sym, {}).get(exchange) != rate_str:
            modified = True
        self._data[sym][exchange] = rate_str
        row = self._row_map.get(sym, self._symbols.index(sym))
        col = FUNDING_COLUMNS.index(exchange)
        src_idx = self.index(row, col)
        # update the source model cell
        self.dataChanged.emit(src_idx, src_idx, [QtCore.Qt.DisplayRole])

        # store next funding timestamp for supported exchanges and update countdown
        if f"{exchange} Countdown" in FUNDING_COLUMNS:
            if next_ts is not None:
                if self._next_funding_ts.get((sym, exchange)) != next_ts:
                    modified = True
                self._next_funding_ts[(sym, exchange)] = next_ts
                c_col = FUNDING_COLUMNS.index(f"{exchange} Countdown")
                c_idx = self.index(row, c_col)
                self.dataChanged.emit(c_idx, c_idx, [QtCore.Qt.DisplayRole])
            else:
                # next_ts None olduğunda daha önce varsa temizle (opt.)
                if (sym, exchange) in self._next_funding_ts:
                    del self._next_funding_ts[(sym, exchange)]
                    c_col = FUNDING_COLUMNS.index(f"{exchange} Countdown")
                    c_idx = self.index(row, c_col)
                    self.dataChanged.emit(c_idx, c_idx, [QtCore.Qt.DisplayRole])


        # flash via proxy index so empty cells don't flash
        if self.delegate and changed:
            view = self.delegate.parent()
            proxy = view.model()
            view_idx = proxy.mapFromSource(src_idx)
            self.delegate.mark_changed(view_idx, positive)

        if modified:
            self._mod_counter += 1

    def _update_countdowns(self):
        for exch in ("Binance", "OKX", "Bybit", "Bitget", "Gateio"):
            col_name = f"{exch} Countdown"
            if col_name not in FUNDING_COLUMNS:
                continue
            col = FUNDING_COLUMNS.index(col_name)
            for row, sym in enumerate(self._symbols):
                if (sym, exch) in self._next_funding_ts:
                    idx = self.index(row, col)
                    self.dataChanged.emit(idx, idx, [QtCore.Qt.DisplayRole])


class AskBidTableModel(QtCore.QAbstractTableModel):
    symbolsUpdated = QtCore.Signal(list)

    def __init__(self):
        super().__init__()
        self._symbols = []           # sıra ile eklenen semboller
        # { sym: { exch: (bid_price, ask_price, bid_qty, ask_qty) } }
        self._data    = {}
        self._prev    = {}           # { (sym, exch, side): float } , side in {"bid","ask"}
        self.delegate = None
        self._mod_counter = 0        # incremented on data updates
        self._row_map: dict[str, int] = {}

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._symbols)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 1 + 2 * len(AB_EXCHANGES)

    def headerData(self, section, orientation, role):
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            if section == 0:
                return "Symbol"
            exch = AB_EXCHANGES[(section-1)//2]
            side = "Ask" if section%2==1 else "Bid"
            return f"{exch} {side}"
        return None

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if role != QtCore.Qt.DisplayRole:
            return None
        r, c = index.row(), index.column()
        sym = self._symbols[r]
        if c == 0:
            return sym
        exch = AB_EXCHANGES[(c-1)//2]
        entry = self._data.get(sym, {}).get(exch)
        if entry:
            bid, ask = entry[0], entry[1]
        else:
            bid = ask = ""
        val = ask if c%2 == 1 else bid
        return f"{val:.8f}" if isinstance(val, float) else val

    @QtCore.Slot(str, str, float, float, object, object)
    def update_askbid(
        self,
        exchange: str,
        raw_symbol: str,
        bid: float,
        ask: float,
        bid_qty: float | None = None,
        ask_qty: float | None = None,
    ):
        # 1) Symbol normalize
        if exchange not in AB_EXCHANGES:
            return
        sym = normalize_symbol(raw_symbol)
        if not sym:
            return
        
        modified = False
        # 2) Yeni sembol ekle
        if sym not in self._data:
            self.beginInsertRows(QtCore.QModelIndex(),
                                 len(self._symbols),
                                 len(self._symbols))
            self._symbols.append(sym)
            self._data[sym] = {}
            self._row_map[sym] = len(self._symbols) - 1
            self.endInsertRows()
            self.symbolsUpdated.emit(self._symbols.copy())
            modified = True

        # 3) Önceki değerleri al
        key_bid = (sym, exchange, "bid")
        key_ask = (sym, exchange, "ask")
        prev_bid = self._prev.get(key_bid)
        prev_ask = self._prev.get(key_ask)
        prev_entry = self._data.get(sym, {}).get(exchange)

        # 4) Yeni değerleri sakla
        if prev_bid != bid or prev_ask != ask:
            modified = True
        if prev_entry and (prev_entry[2] != bid_qty or prev_entry[3] != ask_qty):
            modified = True
        self._prev[key_bid] = bid
        self._prev[key_ask] = ask
        self._data[sym][exchange] = (bid, ask, bid_qty, ask_qty)

        # 5) Hücreyi güncelle
        row = self._row_map.get(sym, self._symbols.index(sym))
        ei  = AB_EXCHANGES.index(exchange)
        ask_col = 1 + 2*ei
        bid_col = ask_col + 1

        idx_ask = self.index(row, ask_col)
        idx_bid = self.index(row, bid_col)
        self.dataChanged.emit(idx_ask, idx_ask, [QtCore.Qt.DisplayRole])
        self.dataChanged.emit(idx_bid, idx_bid, [QtCore.Qt.DisplayRole])

        # 6) Sadece gerçekten değişen hücreleri flash et
        if self.delegate:
            proxy = self.delegate.parent().model()
            v_ask = proxy.mapFromSource(idx_ask)
            v_bid = proxy.mapFromSource(idx_bid)

            # eğer önceki değer yoksa (ilk kez) ya da değişmemişse flash yok
            if prev_ask is not None and ask != prev_ask:
                self.delegate.mark_changed(v_ask, True)   # ask → yeşil
            if prev_bid is not None and bid != prev_bid:
                self.delegate.mark_changed(v_bid, False)  # bid → kırmızı

        if modified:
            self._mod_counter += 1


class ArbitrageFilterProxyModel(QtCore.QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.threshold = 0.0

    def setThreshold(self, threshold: float):
        self.threshold = threshold
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        ev = self.sourceModel().events[source_row]
        return ev.end_dt is None and ev.rate >= self.threshold
    

    
class OpenArbitrageProxyModel(QtCore.QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.threshold = 0.0
        self._symbols = set()

    def setThreshold(self, threshold: float):
        self.threshold = threshold
        self.invalidateFilter()

    def set_symbol_filter(self, symbols: set[str]):
        self._symbols = set(symbols)
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        ev = self.sourceModel().events[source_row]
        # 1) mutlaka açık ve eşik üstünde olmalı
        if ev.end_dt is not None or ev.rate < self.threshold:
            return False
        # 2) eğer dropdown’dan seçilmiş semboller varsa onlardan biri olmalı
        if self._symbols and ev.symbol not in self._symbols:
            return False
        return True

    
class ClosedArbitrageProxyModel(QtCore.QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._symbols = set()

    def set_symbol_filter(self, symbols: set[str]):
        self._symbols = set(symbols)
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        ev = self.sourceModel().events[source_row]
        # 1) mutlaka kapanmış olmalı
        if ev.end_dt is None:
            return False
        # 2) eğer dropdown’dan seçilmiş semboller varsa onlardan biri olmalı
        if self._symbols and ev.symbol not in self._symbols:
            return False
        return True


    def lessThan(self, left: QtCore.QModelIndex, right: QtCore.QModelIndex):
        ev_l = self.sourceModel().events[left.row()]
        ev_r = self.sourceModel().events[right.row()]

        col = left.column()
        asc = self.sortOrder() == QtCore.Qt.AscendingOrder

        # Bitiş Zamanı sütununa özel: en eski önce
        if col == 5:
            result = ev_l.end_dt < ev_r.end_dt
        else:
            # diğer sütunlarda DisplayRole üzerinden string karşılaştırması
            vl = self.sourceModel().data(left, QtCore.Qt.DisplayRole)
            vr = self.sourceModel().data(right, QtCore.Qt.DisplayRole)
            result = str(vl) < str(vr)

        return result if asc else not result
# --- Funding Rate Diff table model ---
class FundingRateDiffModel(QtCore.QAbstractTableModel):
    """Simple model to display funding rate differences."""

    symbolsUpdated = QtCore.Signal(list)

    def __init__(self):
        super().__init__()
        self._rows: list[list[str]] = []
        self._symbols: list[str] = []
        # (symbol, exchange) -> row index mapping for quick updates
        self._row_map: dict[tuple[str, str], int] = {}

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._rows)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return len(FUNDING_RATE_DIFF_COLUMNS)

    def headerData(self, section, orientation, role):
        if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
            return FUNDING_RATE_DIFF_COLUMNS[section]
        return None

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if role == QtCore.Qt.DisplayRole:
            row = self._rows[index.row()]
            col = index.column()
            if col < len(row):
                return row[col]
        return None

    def update_rows(self, rows: list[list[str]]):
        """Update rows incrementally instead of resetting the model."""
        new_map: dict[tuple[str, str], list[str]] = {
            (r[0], r[1]): r for r in rows
        }

        # Remove rows that no longer exist
        to_remove = [pair for pair in self._row_map if pair not in new_map]
        for pair in to_remove:
            idx = self._row_map.pop(pair)
            self.beginRemoveRows(QtCore.QModelIndex(), idx, idx)
            self._rows.pop(idx)
            self.endRemoveRows()
            for p, i in list(self._row_map.items()):
                if i > idx:
                    self._row_map[p] = i - 1

        # Insert new rows and update existing ones
        for pair, row in new_map.items():
            if pair in self._row_map:
                idx = self._row_map[pair]
                if self._rows[idx] != row:
                    self._rows[idx] = row
                    tl = self.index(idx, 0)
                    br = self.index(idx, len(row) - 1)
                    self.dataChanged.emit(tl, br, [QtCore.Qt.DisplayRole])
            else:
                idx = len(self._rows)
                self.beginInsertRows(QtCore.QModelIndex(), idx, idx)
                self._rows.append(row)
                self._row_map[pair] = idx
                self.endInsertRows()

        new_symbols = [r[0] for r in self._rows]
        if new_symbols != self._symbols:
            self._symbols = new_symbols
            self.symbolsUpdated.emit(self._symbols.copy())
    
# --- Simple chart window for bid/ask history ---


class InteractiveChartView(QtCharts.QChartView):
        """Chart view that supports panning and zooming with the mouse."""

        def __init__(self, chart: QtCharts.QChart, parent=None):
            super().__init__(chart, parent)
            self.setRenderHint(QtGui.QPainter.Antialiasing)
            self.setRubberBand(QtCharts.QChartView.RectangleRubberBand)
            self._last_pos = None

        def mousePressEvent(self, event):
            if event.button() == QtCore.Qt.LeftButton:
                self._last_pos = event.pos()
                self.setCursor(QtCore.Qt.ClosedHandCursor)
            super().mousePressEvent(event)

        def mouseMoveEvent(self, event):
            if self._last_pos is not None:
                delta = event.pos() - self._last_pos
                # Only allow horizontal panning
                self.chart().scroll(-delta.x(), 0)
                self._last_pos = event.pos()
            super().mouseMoveEvent(event)

        def mouseReleaseEvent(self, event):
            if event.button() == QtCore.Qt.LeftButton:
                self._last_pos = None
                self.setCursor(QtCore.Qt.ArrowCursor)
            super().mouseReleaseEvent(event)

        def wheelEvent(self, event):
            factor = 0.9 if event.angleDelta().y() > 0 else 1.1
            ch = self.chart()
            ax_x = ch.axisX()
            if isinstance(ax_x, QtCharts.QDateTimeAxis):
                min_x = ax_x.min().toMSecsSinceEpoch()
                max_x = ax_x.max().toMSecsSinceEpoch()
                span = max_x - min_x
                center = min_x + span / 2
                span *= factor
                ax_x.setRange(
                    QtCore.QDateTime.fromMSecsSinceEpoch(int(center - span / 2)),
                    QtCore.QDateTime.fromMSecsSinceEpoch(int(center + span / 2)),
                )

            super().wheelEvent(event)



class ChartWindow(QtWidgets.QMainWindow):
        """Displays live bid/ask data for a symbol using two exchanges."""

        def __init__(self, symbol: str, ask_exchange: str, bid_exchange: str, dark_mode: bool = True):
            super().__init__()
            self.setWindowFlag(QtCore.Qt.WindowStaysOnTopHint, True)
            self.symbol = symbol
            self.ask_exchange = ask_exchange
            self.bid_exchange = bid_exchange
            self.setWindowTitle(f"{ask_exchange} Ask / {bid_exchange} Bid - {symbol}")
            self.resize(800, 600)


            self.ask_series = QtCharts.QLineSeries()
            self.bid_series = QtCharts.QLineSeries()
            self.ask_series.setName(f"Ask ({ask_exchange})")
            self.bid_series.setName(f"Bid ({bid_exchange})")
            ask_pen = QtGui.QPen(QtGui.QColor(255, 0, 0))  # Parlak kırmızı
            ask_pen.setWidth(3)
            self.ask_series.setPen(ask_pen)

            bid_pen = QtGui.QPen(QtGui.QColor(0, 255, 0))  # Parlak yeşil
            bid_pen.setWidth(3)
            self.bid_series.setPen(bid_pen)


            self.chart = QtCharts.QChart()
            self.chart.addSeries(self.ask_series)
            self.chart.addSeries(self.bid_series)
            self.chart.legend().setVisible(True)
            self.chart.setTitle("Ask Price: - | Bid Price: - | Spread: -")

            self.axis_x = QtCharts.QDateTimeAxis()
            self.axis_x.setFormat("HH:mm:ss")
            self.axis_x.setTitleText("Time")
            self.chart.addAxis(self.axis_x, QtCore.Qt.AlignBottom)
            self.ask_series.attachAxis(self.axis_x)
            self.bid_series.attachAxis(self.axis_x)

            self.axis_y = QtCharts.QValueAxis()
            self.chart.addAxis(self.axis_y, QtCore.Qt.AlignLeft)
            self.ask_series.attachAxis(self.axis_y)
            self.bid_series.attachAxis(self.axis_y)

            # Display only the most recent minute of data by default
            self.window_ms = 60_000

            self.view = InteractiveChartView(self.chart)
            self.setCentralWidget(self.view)

            # Labels to display latest prices
            self.ask_label = QtWidgets.QGraphicsSimpleTextItem(self.chart)
            self.bid_label = QtWidgets.QGraphicsSimpleTextItem(self.chart)


            self._start_ms = None
            self._ask_price = None
            self._bid_price = None

            self.apply_theme(dark_mode)

            # Tema uygulandıktan sonra tekrar kalem renklerini ayarla
            ask_pen = QtGui.QPen(QtGui.QColor(255, 0, 0))  # Parlak kırmızı
            ask_pen.setWidth(3)
            self.ask_series.setPen(ask_pen)

            bid_pen = QtGui.QPen(QtGui.QColor(0, 255, 0))  # Parlak yeşil
            bid_pen.setWidth(3)
            self.bid_series.setPen(bid_pen)

        def apply_theme(self, dark_mode: bool):
            if dark_mode:
                self.chart.setTheme(QtCharts.QChart.ChartThemeDark)
                bg = QtGui.QColor(30, 30, 30)
                fg = QtGui.QColor("white")
            else:
                self.chart.setTheme(QtCharts.QChart.ChartThemeLight)
                bg = QtGui.QColor("white")
                fg = QtGui.QColor("black")

            self.chart.setBackgroundBrush(bg)
            # Label renklerini ayarla
            self.ask_label.setBrush(QtGui.QBrush(QtGui.QColor(255, 0, 0)))
            self.bid_label.setBrush(QtGui.QBrush(QtGui.QColor(0, 255, 0)))

            # Çizgi renklerini sabitle
            ask_pen = QtGui.QPen(QtGui.QColor(255, 0, 0))
            ask_pen.setWidth(3)
            self.ask_series.setPen(ask_pen)

            bid_pen = QtGui.QPen(QtGui.QColor(0, 255, 0))
            bid_pen.setWidth(3)
            self.bid_series.setPen(bid_pen)

        def add_point(self, exchange: str, symbol: str, bid: float, ask: float):
            """Append a bid/ask point if the normalized symbol matches."""
            norm = normalize_symbol(symbol)
            if norm != self.symbol:
                return

            ts = QtCore.QDateTime.currentDateTime()
            x = ts.toMSecsSinceEpoch()
            if self._start_ms is None:
                self._start_ms = x

            ask_updated = False
            bid_updated = False

            if exchange == self.ask_exchange:
                self.ask_series.append(x, ask)
                self.ask_label.setText(f"{ask}")
                p = self.chart.mapToPosition(QtCore.QPointF(x, ask), self.ask_series)
                self.ask_label.setPos(p)
                self._ask_price = ask
                ask_updated = True

            if exchange == self.bid_exchange:
                self.bid_series.append(x, bid)
                self.bid_label.setText(f"{bid}")
                p = self.chart.mapToPosition(QtCore.QPointF(x, bid), self.bid_series)
                self.bid_label.setPos(p)
                self._bid_price = bid
                bid_updated = True

            # If only one series received an update, extend the other series
            # using its last known price so both lines move in sync.
            if ask_updated and not bid_updated and self._bid_price is not None:
                self.bid_series.append(x, self._bid_price)
                p = self.chart.mapToPosition(
                    QtCore.QPointF(x, self._bid_price), self.bid_series
                )
                self.bid_label.setPos(p)

            if bid_updated and not ask_updated and self._ask_price is not None:
                self.ask_series.append(x, self._ask_price)
                p = self.chart.mapToPosition(
                    QtCore.QPointF(x, self._ask_price), self.ask_series
                )
                self.ask_label.setPos(p)

            if not (ask_updated or bid_updated):
                return
            
            if not self.axis_x.min().isValid():
                self.axis_x.setRange(
                    QtCore.QDateTime.fromMSecsSinceEpoch(self._start_ms), ts
                )
            else:
                if x > self.axis_x.max().toMSecsSinceEpoch():
                    self.axis_x.setMax(ts)

            # Maintain a rolling time window on the X axis
            start_ms = x - self.window_ms
            if start_ms < self._start_ms:
                start_ms = self._start_ms

            # Drop points that scrolled out of the visible window
            for series in (self.ask_series, self.bid_series):
                remove_count = 0
                count = series.count()
                while remove_count < count and series.at(remove_count).x() < start_ms:
                    remove_count += 1
                if remove_count:
                    series.removePoints(0, remove_count)

            self.axis_x.setMin(QtCore.QDateTime.fromMSecsSinceEpoch(start_ms))

            # Scale around the most recent price
            price = None
            if self._ask_price is not None and self._bid_price is not None:
                price = (self._ask_price + self._bid_price) / 2
                diff = abs(self._ask_price - self._bid_price)
            elif self._ask_price is not None:
                price = self._ask_price
                diff = 0.0
            elif self._bid_price is not None:
                price = self._bid_price
                diff = 0.0

            if price is not None:
                # Keep a small margin around the price
                margin = max(price * 0.001, diff * 2)
                self.axis_y.setRange(price - margin, price + margin)

            title_ask = f"{self._ask_price}" if self._ask_price is not None else "-"
            title_bid = f"{self._bid_price}" if self._bid_price is not None else "-"
            if self._ask_price is not None and self._bid_price not in (None, 0):
                spread = self._ask_price / self._bid_price - 1
                spread_str = f"{spread:.5f}"
                arb = self._bid_price / self._ask_price - 1 - (FEE_RATE_BUY + FEE_RATE_SELL)
                arb_str = f"{arb:.5f}"
            else:
                spread_str = "-"
                arb_str = "-"
            self.chart.setTitle(
                f"Ask Price: {title_ask} | Bid Price: {title_bid} | Spread: {spread_str} | Arbitrage: {arb_str}"
            )

            self.chart.update()

# --- Orderbook window ----------------------------------------------------
class OrderbookWindow(QtWidgets.QMainWindow):
    """Display order book levels for a single symbol."""

    def __init__(self, symbol: str, exchange: str, dark_mode: bool = True):
        super().__init__()
        self.setWindowFlag(QtCore.Qt.WindowStaysOnTopHint, True)
        self.symbol = symbol
        self.exchange = exchange
        self.setWindowTitle(f"{exchange} Orderbook - {symbol}")
        self.resize(400, 200)

        self.table = QtWidgets.QTableWidget(3, 4)
        self.table.setHorizontalHeaderLabels([
            "Ask Price",
            "Ask Qty (USDT)",
            "Bid Price",
            "Bid Qty (USDT)",
        ])
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.status_bar = QtWidgets.QStatusBar()
        container = QtWidgets.QWidget()
        vbox = QtWidgets.QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.addWidget(self.table)
        vbox.addWidget(self.status_bar)
        self.setCentralWidget(container)

        self.update_status(False)

        self.apply_theme(dark_mode)

    def apply_theme(self, dark_mode: bool):
        bg = "#1e1e1e" if dark_mode else "white"
        fg = "white" if dark_mode else "black"
        self.table.setStyleSheet(
            f"QTableWidget {{ background-color: {bg}; color: {fg}; gridline-color: gray; }}"
        )

    @QtCore.Slot(list, list)
    def update_book(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]]):
        for i in range(3):
            if i < len(asks):
                ap, aq = asks[i]
                ask_val = ap * aq
                item_price = self.table.item(i, 0)
                if item_price is None:
                    item_price = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 0, item_price)
                item_price.setText(f"{ap:.8f}")

                item_qty = self.table.item(i, 1)
                if item_qty is None:
                    item_qty = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 1, item_qty)
                item_qty.setText(f"{ask_val:.4f}")
            else:
                self.table.setItem(i, 0, QtWidgets.QTableWidgetItem(""))
                self.table.setItem(i, 1, QtWidgets.QTableWidgetItem(""))

            if i < len(bids):
                bp, bq = bids[i]
                bid_val = bp * bq
                item_price = self.table.item(i, 2)
                if item_price is None:
                    item_price = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 2, item_price)
                item_price.setText(f"{bp:.8f}")

                item_qty = self.table.item(i, 3)
                if item_qty is None:
                    item_qty = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 3, item_qty)
                item_qty.setText(f"{bid_val:.4f}")
            else:
                self.table.setItem(i, 2, QtWidgets.QTableWidgetItem(""))
                self.table.setItem(i, 3, QtWidgets.QTableWidgetItem(""))

    @QtCore.Slot(bool)
    def update_status(self, connected: bool):
        text = "Connected" if connected else "Disconnected"
        color = "lightgreen" if connected else "darkred"
        self.status_bar.setStyleSheet(
            f"background-color: {color}; color: black; border: 1px solid gray;"
        )
        self.status_bar.showMessage(text)

class CrossOrderbookWindow(QtWidgets.QMainWindow):
    """Display ask side from one exchange and bid side from another."""

    def __init__(self, symbol: str, buy_exchange: str, sell_exchange: str, dark_mode: bool = True):
        super().__init__()
        self.setWindowFlag(QtCore.Qt.WindowStaysOnTopHint, True)
        self.symbol = symbol
        self.buy_exchange = buy_exchange
        self.sell_exchange = sell_exchange
        self.setWindowTitle(f"{buy_exchange} Ask / {sell_exchange} Bid - {symbol}")
        self.resize(400, 200)

        self.table = QtWidgets.QTableWidget(3, 4)
        self.table.setHorizontalHeaderLabels([
            f"{buy_exchange} Ask Price",
            f"{buy_exchange} Ask Qty (USDT)",
            f"{sell_exchange} Bid Price",
            f"{sell_exchange} Bid Qty (USDT)",
        ])
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)

        self.status_bar = QtWidgets.QStatusBar()
        container = QtWidgets.QWidget()
        vbox = QtWidgets.QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.addWidget(self.table)
        vbox.addWidget(self.status_bar)
        self.setCentralWidget(container)

        self._asks: list[tuple[float, float]] = []
        self._bids: list[tuple[float, float]] = []
        self._buy_connected = False
        self._sell_connected = False

        self.apply_theme(dark_mode)
        self.update_status(buy_exchange, False)
        self.update_status(sell_exchange, False)

    def apply_theme(self, dark_mode: bool):
        bg = "#1e1e1e" if dark_mode else "white"
        fg = "white" if dark_mode else "black"
        self.table.setStyleSheet(
            f"QTableWidget {{ background-color: {bg}; color: {fg}; gridline-color: gray; }}"
        )

    @QtCore.Slot(list, list)
    def update_buy(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]]):
        self._asks = asks
        self._refresh()

    @QtCore.Slot(list, list)
    def update_sell(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]]):
        self._bids = bids
        self._refresh()

    def _refresh(self):
        for i in range(3):
            if i < len(self._asks):
                ap, aq = self._asks[i]
                ask_val = ap * aq
                item_price = self.table.item(i, 0)
                if item_price is None:
                    item_price = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 0, item_price)
                item_price.setText(f"{ap:.8f}")

                item_qty = self.table.item(i, 1)
                if item_qty is None:
                    item_qty = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 1, item_qty)
                item_qty.setText(f"{ask_val:.4f}")
            else:
                self.table.setItem(i, 0, QtWidgets.QTableWidgetItem(""))
                self.table.setItem(i, 1, QtWidgets.QTableWidgetItem(""))

            if i < len(self._bids):
                bp, bq = self._bids[i]
                bid_val = bp * bq
                item_price = self.table.item(i, 2)
                if item_price is None:
                    item_price = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 2, item_price)
                item_price.setText(f"{bp:.8f}")

                item_qty = self.table.item(i, 3)
                if item_qty is None:
                    item_qty = QtWidgets.QTableWidgetItem()
                    self.table.setItem(i, 3, item_qty)
                item_qty.setText(f"{bid_val:.4f}")
            else:
                self.table.setItem(i, 2, QtWidgets.QTableWidgetItem(""))
                self.table.setItem(i, 3, QtWidgets.QTableWidgetItem(""))

    @QtCore.Slot(str, bool)
    def update_status(self, exchange: str, connected: bool):
        if exchange == self.buy_exchange:
            self._buy_connected = connected
        elif exchange == self.sell_exchange:
            self._sell_connected = connected

        status = self._buy_connected and self._sell_connected
        text = "Connected" if status else "Disconnected"
        color = "lightgreen" if status else "darkred"
        self.status_bar.setStyleSheet(
            f"background-color: {color}; color: black; border: 1px solid gray;"
        )
        self.status_bar.showMessage(text)

# --- Background worker for DB download ---
class DownloadTask(QtCore.QObject, QtCore.QRunnable):
    """Run Supabase fetch and Excel export in a separate thread."""

    finished = QtCore.Signal(bool, str)

    def __init__(
        self,
        start: str,
        end: str,
        path: str,
        symbols: list[str] | None = None,
        buy_exchs: list[str] | None = None,
        sell_exchs: list[str] | None = None,
    ):
        super().__init__()
        QtCore.QRunnable.__init__(self)
        self.start = start
        self.end = end
        self.path = path
        self.symbols = symbols
        self.buy_exchs = buy_exchs
        self.sell_exchs = sell_exchs
        self.setAutoDelete(False)

    @QtCore.Slot()
    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def job():
            try:
                recs = await fetch_closed_logs(
                    self.start,
                    self.end,
                    self.symbols,
                    self.buy_exchs,
                    self.sell_exchs,
                )
                if not recs:
                    return False, "Veri bulunamadı"
                df = pd.DataFrame(recs)
                await asyncio.to_thread(df.to_excel, self.path, index=False)
                return True, self.path
            except Exception as e:
                return False, str(e)

        ok, msg = loop.run_until_complete(job())
        loop.run_until_complete(close_supabase_session())
        loop.close()
        QtCore.QMetaObject.invokeMethod(
            self,
            "_emit",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(bool, ok),
            QtCore.Q_ARG(str, msg),
        )

    @QtCore.Slot(bool, str)
    def _emit(self, ok: bool, msg: str):
        self.finished.emit(ok, msg)
        self.deleteLater()

# --- Generic DataFrame export worker ---
class ExportTask(QtCore.QObject, QtCore.QRunnable):
    """Run DataFrame export in a separate thread."""

    finished = QtCore.Signal(bool, str)

    def __init__(self, func, path: str):
        super().__init__()
        QtCore.QRunnable.__init__(self)
        self.func = func
        self.path = path
        self.setAutoDelete(False)

    @QtCore.Slot()
    def run(self):
        try:
            self.func()
            ok, msg = True, self.path
        except Exception as e:
            ok, msg = False, str(e)
        QtCore.QMetaObject.invokeMethod(
            self,
            "_emit",
            QtCore.Qt.QueuedConnection,
            QtCore.Q_ARG(bool, ok),
            QtCore.Q_ARG(str, msg),
        )

    @QtCore.Slot(bool, str)
    def _emit(self, ok: bool, msg: str):
        self.finished.emit(ok, msg)
        self.deleteLater()

# --- Arbitrage processing worker ---
class ArbitrageTask(QtCore.QObject, QtCore.QRunnable):
    """Run heavy arbitrage calculations in a separate thread."""

    finished = QtCore.Signal(object)

    def __init__(
        self,
        data: dict,
        funding: dict,
        arb_map: dict,
        ask_fee: float,
        bid_fee: float,
        threshold: float,
        close_threshold: float,
        use_max: bool,
        max_duration: int,
    ):
        super().__init__()
        QtCore.QRunnable.__init__(self)
        self.data = data
        self.funding = funding
        self.arb_map = arb_map
        self.ask_fee = ask_fee
        self.bid_fee = bid_fee
        self.threshold = threshold
        self.close_threshold = close_threshold
        self.use_max = use_max
        self.max_duration = max_duration
        self.setAutoDelete(False)

    @QtCore.Slot()
    def run(self):
        actions = []
        now = datetime.now()

        if self.use_max:
            for key, ev in list(self.arb_map.items()):
                elapsed = (now - ev.start_dt).total_seconds()
                if elapsed > self.max_duration:
                    actions.append(("expire", ev, key))

        for symbol, exch_data in self.data.items():
            for buy_exch, vals_buy in exch_data.items():
                if len(vals_buy) < 2:
                    continue
                ask_price = vals_buy[1]
                if ask_price <= 0:
                    continue
                for sell_exch, vals_sell in exch_data.items():
                    if len(vals_sell) < 1:
                        continue
                    bid_price = vals_sell[0]
                    if sell_exch == buy_exch or bid_price <= 0:
                        continue
                    raw_rate = bid_price / ask_price - 1
                    net_rate = raw_rate - (self.ask_fee + self.bid_fee)

                    if (
                        (buy_exch, symbol) not in self.funding
                        or (sell_exch, symbol) not in self.funding
                    ):
                        continue

                    initial_buy_fr = self.funding[(buy_exch, symbol)]
                    initial_sell_fr = self.funding[(sell_exch, symbol)]

                    if net_rate >= 0.60:
                        continue

                    key = (symbol, buy_exch, sell_exch)

                    if net_rate >= self.threshold:
                        if key not in self.arb_map:
                            ev = ArbitrajEvent(
                                symbol,
                                buy_exch,
                                sell_exch,
                                net_rate,
                                initial_ask=ask_price,
                                initial_bid=bid_price,
                                initial_buy_fr=initial_buy_fr,
                                initial_sell_fr=initial_sell_fr,
                            )
                            actions.append(("add", ev, key))
                    elif net_rate <= self.close_threshold:
                        if key in self.arb_map:
                            ev = self.arb_map[key]
                            actions.append(
                                ("close", ev, key, net_rate, ask_price, bid_price)
                            )

        self._result = actions
        QtCore.QMetaObject.invokeMethod(
            self, "_emit", QtCore.Qt.QueuedConnection
        )

    @QtCore.Slot()
    def _emit(self):
        self.finished.emit(self._result)
        self.deleteLater()

# --- Main Window ---
class MainWindow(QtWidgets.QMainWindow):
    themeChanged = QtCore.Signal(bool)

    def __init__(self):
        self.min_duration = 60
        self.max_duration = 60
        self.use_max_duration = False
        self.ask_fee_rate = 0.0005
        self.bid_fee_rate = 0.0005
        self.dark_mode = True   # Uygulama açıldığında önce dark mod aktif olsun
        super().__init__()
        self.setWindowTitle("Funding & Order Book Dashboard")
        self.resize(900, 600)

        self._arb_map = {}
        # Her exchange’in en son funding‐rate’ini saklayacak dict
        self.current_funding: dict[tuple[str,str], float] = {}
        # Açılan grafik pencerelerini tut
        self.chart_windows: list[ChartWindow] = []
         # Açılan orderbook pencerelerini tut
        self.orderbook_windows: list[OrderbookWindow] = []
        # Orderbook depth data (top 3 levels per exchange)
        self.orderbook_data: dict[str, dict[str, tuple[list[tuple[float, float]], list[tuple[float, float]]]]] = {}
        self._orderbook_counter = 0
        self._orderbook_task_scheduled = False
        # Index price data per symbol/exchange
        self.index_prices: dict[str, dict[str, float]] = {}
        self._index_counter = 0
        # OKX normalized symbol -> instId mapping
        self._okx_symbol_map: dict[str, str] = {}
        # Bybit normalized symbol -> raw symbol mapping
        self._bybit_symbol_map: dict[str, str] = {}
        # Bitget normalized symbol -> raw symbol mapping
        self._bitget_symbol_map: dict[str, str] = {}
        # Gateio normalized symbol -> raw symbol mapping
        self._gateio_symbol_map: dict[str, str] = {}
        # WebSocket tasks started for live feeds
        self._ws_tasks: list[asyncio.Task] = []
        self._really_closing = False

        # Merkezi container ve layout
        container = QtWidgets.QWidget(self)
        self.setCentralWidget(container)
        vlayout = QtWidgets.QVBoxLayout(container)
        vlayout.setContentsMargins(5,5,5,5)
        vlayout.setSpacing(5)

        # Status Bar (Ask/Bid ve Funding‐Rate bağlantı göstergeleri)
        status_layout = QtWidgets.QHBoxLayout()
        status_layout.setSpacing(10)
        self._status_ab = {}
        self._status_fr = {}
        for exch in AB_EXCHANGES:
            lbl_ab = QtWidgets.QLabel(exch)
            lbl_ab.setFixedSize(80, 20)
            lbl_ab.setAlignment(QtCore.Qt.AlignCenter)
            lbl_ab.setStyleSheet("background-color: darkred; color: white; border: 1px solid gray;")
            status_layout.addWidget(lbl_ab)
            self._status_ab[exch] = lbl_ab

            lbl_fr = QtWidgets.QLabel(f"{exch} FR")
            lbl_fr.setFixedSize(80, 20)
            lbl_fr.setAlignment(QtCore.Qt.AlignCenter)
            lbl_fr.setStyleSheet("background-color: darkred; color: white; border: 1px solid gray;")
            status_layout.addWidget(lbl_fr)
            self._status_fr[exch] = lbl_fr

        vlayout.insertLayout(0, status_layout)

        # Üstte butonlar
        # 1) Buton satırı layout’unu oluştur
        btn_layout = QtWidgets.QHBoxLayout()

        # 2) Oluşturduğun her dört butonu da (Funding, Order, Arbitraj, Toggle) ekle
        self.btn_funding      = QtWidgets.QPushButton("Funding Rate Live")
        self.btn_order        = QtWidgets.QPushButton("Ask/Bid")
        self.btn_arb          = QtWidgets.QPushButton("Arbitraj Diff")
        self.btn_fr_diff      = QtWidgets.QPushButton("Funding Rate Diff")
        self.btn_chart        = QtWidgets.QPushButton("Chart Selection")
        self.btn_orderbook_sel = QtWidgets.QPushButton("Orderbook Selection")
        self.btn_db           = QtWidgets.QPushButton("DB Connection")

        # 1) Toggle butonunu oluştur, checkable yap
        self.btn_toggle_theme = QtWidgets.QPushButton("Light Mode")
        self.btn_toggle_theme.setCheckable(True)
        self.btn_toggle_theme.toggled.connect(self.toggle_theme)

        # Senkronizasyon butonu
        self.btn_sync = QtWidgets.QPushButton("Synchronize")
        self.btn_sync.clicked.connect(self.on_synchronize)


        btn_layout.addWidget(self.btn_funding)
        btn_layout.addWidget(self.btn_order)
        btn_layout.addWidget(self.btn_arb)
        btn_layout.addWidget(self.btn_fr_diff)
        btn_layout.addWidget(self.btn_orderbook_sel)
        btn_layout.addWidget(self.btn_chart)
        btn_layout.addWidget(self.btn_db)

        # 3) Sağa itmek için stretch, ardından sync ve toggle butonlarını ekle
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_sync)
        btn_layout.addWidget(self.btn_toggle_theme)
        

        # 4) Layout’u pencerenin ana dikey yerleşimine (vlayout) son olarak ekle
        vlayout.addLayout(btn_layout)

        # 5) Toggle sinyalini metoda bağla
        self.btn_toggle_theme.toggled.connect(self.toggle_theme)


        self.btn_funding.clicked.connect(self.open_funding_tab)
        self.btn_order.clicked.connect(self.open_askbid_tab)
        self.btn_arb.clicked.connect(self.open_arbitrage_tab)
        self.btn_orderbook_sel.clicked.connect(self.open_orderbook_selection_tab)
        self.btn_fr_diff.clicked.connect(self.open_funding_rate_diff_tab)
        self.btn_chart.clicked.connect(self.open_chart_tab)
        self.btn_db.clicked.connect(self.open_db_tab)
        


        # TabWidget
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setDocumentMode(True)
        self.tabs.setTabsClosable(True)
        self.tabs.setMovable(True)
        self.tabs.tabCloseRequested.connect(self._on_tab_closed)
        vlayout.addWidget(self.tabs, stretch=1)

        # Tabloları önceden hazırla ancak başlangıçta sekme açma
        self._setup_funding_table()
        self._setup_askbid_table()
        self.arb_model = ArbitrajDiffModel()

        # Varsayılan arbitraj eşikleri
        self.arb_threshold = 0.003
        self.arb_close_threshold = 0.002


        # Ask/Bid flush timer (~10 Hz)
        self._askbid_data = {}
        self._askbid_timer = QtCore.QTimer(self)
        self._askbid_timer.timeout.connect(self._flush_askbid_data)
        self._askbid_timer.start(100)

        # Arbitrage işlemleri timer (500 ms, initially stopped)
        self._arb_running = False
        self._arb_timer = QtCore.QTimer(self)
        self._arb_timer.timeout.connect(self._start_arbitrage_worker)

        # Funding Rate Diff refresh timer (1s, initially stopped)
        self._fr_diff_params1 = None  # type: dict | None
        self._fr_diff_params2 = None  # type: dict | None
        self._fr_diff_timer = QtCore.QTimer(self)
        self._fr_diff_timer.setInterval(1000)
        self._fr_diff_timer.timeout.connect(self._refresh_fr_diff_models)
        self._fr_diff_last_funding = -1
        self._fr_diff_last_askbid = -1
        self._fr_diff_last_orderbook = -1
        self._fr_diff_last_index = -1

        # Başlangıç teması
        self.toggle_theme(self.btn_toggle_theme.isChecked())

    @QtCore.Slot(int)
    def _on_tab_closed(self, index: int):
        tab_text = self.tabs.tabText(index)
        if tab_text == "Funding Rate Diff":
            self._fr_diff_timer.stop()
            self._fr_diff_params1 = None
            self._fr_diff_params2 = None
        elif tab_text == "Arbitraj Diff":
            self._arb_timer.stop()
        self.tabs.removeTab(index)


    # Status güncelleyiciler
    def _update_status_ab(self, exchange: str, connected: bool):
        lbl = self._status_ab.get(exchange)
        if lbl:
            color = "lightgreen" if connected else "darkred"
            text_color = "black" if connected else "white"
            lbl.setStyleSheet(f"background-color: {color}; color: {text_color}; border: 1px solid gray;")

    def _update_status_fr(self, exchange: str, connected: bool):
        lbl = self._status_fr.get(exchange)
        if lbl:
            color = "lightgreen" if connected else "darkred"
            text_color = "black" if connected else "white"
            lbl.setStyleSheet(f"background-color: {color}; color: {text_color}; border: 1px solid gray;")

    
    def toggle_theme(self, checked: bool):
        """
        checked == True  → Light Mode aktif (buton aşağıda)
        checked == False → Dark Mode aktif
        """
        # 1) dark_mode bayrağını checked’in tersine ayarla
        #    (checked True ise light’a geçildi demektir)
        self.dark_mode = not checked

        # 2) Butonun üzerindeki metni güncelle
        if self.dark_mode:
            self.btn_toggle_theme.setText("Light Mode")
        else:
            self.btn_toggle_theme.setText("Dark Mode")

        

        # 3) Uygulanacak stylesheet’i hazırla

        bg = "#1e1e1e" if self.dark_mode else "white"
        fg = "white" if self.dark_mode else "black"
        grid = "#888" if self.dark_mode else "black"
        style = (
            f"QWidget {{ background-color: {bg}; color: {fg}; }}\n"
            f"QTableView {{ gridline-color: {grid}; border: 1px solid {grid}; }}\n"
            f"QHeaderView::section {{ border: 1px solid {grid}; background-color: {bg}; color: {fg}; }}"
        )

        # 4) Küresel stylesheet'i uygula
        app = QtWidgets.QApplication.instance()
        if app:
            app.setStyleSheet(style)

        for win in list(self.chart_windows):
            win.apply_theme(self.dark_mode)

    # Funding tablosu hazırlayan metod
    def _setup_funding_table(self):
        self.model = FundingTableModel()
        self.funding_proxy = SymbolFilterProxyModel()
        self.funding_proxy.setSourceModel(self.model)
        self.funding_proxy.setDynamicSortFilter(True)

        self.table = QtWidgets.QTableView()
        self.table.setShowGrid(True)
        self.table.setModel(self.funding_proxy)
        self.table.verticalHeader().setVisible(False)

        header = FilterableHeaderView(QtCore.Qt.Horizontal, self.table)
        header.setSectionsClickable(True)
        header.setSortIndicatorShown(True)
        self.symbol_dropdown = MultiSelectDropdown()
        header.set_dropdown(self.symbol_dropdown)
        self.table.setHorizontalHeader(header)

        # sütunları eşit genişlikte paylaş:
        header.setSectionResizeMode(QHeaderView.Stretch)

        self.table.setSortingEnabled(True)
        self.table.sortByColumn(0, QtCore.Qt.AscendingOrder)

        delegate = FlashDelegate(self.table)
        self.table.setItemDelegate(delegate)
        self.model.delegate = delegate

        self.model.symbolsUpdated.connect(self._update_dropdown_items)
        self.symbol_dropdown.selectionChanged.connect(
            self.funding_proxy.set_symbol_filter
        )

        shortcut = QtGui.QShortcut(QtGui.QKeySequence.Copy, self.table)
        shortcut.activated.connect(lambda t=self.table: self._copy_selection(t))

    def _update_dropdown_items(self, symbols: list[str]):
        self.symbol_dropdown.set_items(symbols)

    # Ask/Bid tablosu hazırlayan metod
    def _setup_askbid_table(self):
        from PySide6.QtWidgets import QHeaderView
        from PySide6.QtCore    import Qt

        self.askbid_model = AskBidTableModel()
        self.askbid_proxy = SymbolFilterProxyModel()
        self.askbid_proxy.setSourceModel(self.askbid_model)
        self.askbid_proxy.setDynamicSortFilter(True)

        self.askbid_table = QtWidgets.QTableView()
        self.askbid_table.setShowGrid(True)
        self.askbid_table.setModel(self.askbid_proxy)
        self.askbid_table.verticalHeader().setVisible(False)

        # Sıralamayı aktif et
        self.askbid_table.setSortingEnabled(True)

        # Özel header + dropdown
        header = FilterableHeaderView(QtCore.Qt.Horizontal, self.askbid_table)
        header.setSectionsClickable(True)
        header.setSortIndicatorShown(True)
        self.askbid_dropdown = MultiSelectDropdown()
        header.set_dropdown(self.askbid_dropdown)

        # Sütun genişliklerini eşit paylaştır
        header.setSectionResizeMode(QHeaderView.Stretch)

        # Header’ı tabloya ata
        self.askbid_table.setHorizontalHeader(header)

        # Açılışta Symbol sütununu A→Z (Ascending) sırala ve ok işareti göster
        header.setSortIndicator(0, Qt.AscendingOrder)
        self.askbid_table.sortByColumn(0, Qt.AscendingOrder)



        # sütunları eşit genişlikte paylaş:
        header.setSectionResizeMode(QHeaderView.Stretch)

        delegate = FlashDelegate(self.askbid_table)
        self.askbid_table.setItemDelegate(delegate)
        self.askbid_model.delegate = delegate
        self.askbid_delegate = delegate

        self.askbid_model.symbolsUpdated.connect(self._update_askbid_dropdown)
        self.askbid_dropdown.selectionChanged.connect(self.askbid_proxy.set_symbol_filter)

        # Double-click to open chart
        self.askbid_table.doubleClicked.connect(self._on_generate_chart)

        shortcut = QtGui.QShortcut(QtGui.QKeySequence.Copy, self.askbid_table)
        shortcut.activated.connect(lambda t=self.askbid_table: self._copy_selection(t))

    def _update_askbid_dropdown(self, symbols: list[str]):
        self.askbid_dropdown.set_items(symbols)

    def start_askbid_feeds(self):
        """Launch ask/bid WebSocket tasks if not already running."""
        if getattr(self, "_askbid_task_scheduled", False):
            return
        loop = asyncio.get_event_loop()
        self._ws_tasks.append(loop.create_task(publish_binance_askbid(self._enqueue_askbid, self._update_status_ab)))
        self._ws_tasks.append(loop.create_task(publish_okx_askbid(self._enqueue_askbid, self._update_status_ab)))
        self._ws_tasks.append(loop.create_task(publish_bybit_askbid(self._enqueue_askbid, self._update_status_ab)))
        self._ws_tasks.append(loop.create_task(publish_bitget_askbid(self._enqueue_askbid, self._update_status_ab)))
        self._ws_tasks.append(loop.create_task(publish_gateio_askbid(self._enqueue_askbid, self._update_status_ab)))
        self._askbid_task_scheduled = True

    async def start_orderbook_feeds(self):
        """Launch top-of-book depth feeds for computing 3-level sums."""
        if self._orderbook_task_scheduled:
            return
        loop = asyncio.get_event_loop()
        bin_syms = await fetch_binance_futures()
        self._ws_tasks.append(
            loop.create_task(
                publish_binance_orderbook(
                    bin_syms,
                    lambda s, b, a: self._enqueue_orderbook("Binance", s, b, a),
                    lambda *_: None,
                )
            )
        )

        async with aiohttp.ClientSession() as sess:
            r = await sess.get(OKX_REST_INSTRUMENTS)
            j = await r.json()
        okx_syms = [
            e["instId"]
            for e in j.get("data", [])
            if isinstance(e.get("instId"), str) and e["instId"].endswith("-USDT-SWAP")
        ]
        self._ws_tasks.append(
            loop.create_task(
                publish_okx_orderbook(
                    okx_syms,
                    lambda s, b, a: self._enqueue_orderbook("OKX", s, b, a),
                    lambda *_: None,
                )
            )
        )

        bybit_syms = await fetch_bybit_swaps()
        self._ws_tasks.append(
            loop.create_task(
                publish_bybit_orderbook(
                    bybit_syms,
                    lambda s, b, a: self._enqueue_orderbook("Bybit", s, b, a),
                    lambda *_: None,
                )
            )
        )

        bitget_syms = await fetch_bitget_swaps()
        self._ws_tasks.append(
            loop.create_task(
                publish_bitget_orderbook(
                    bitget_syms,
                    lambda s, b, a: self._enqueue_orderbook("Bitget", s, b, a),
                    lambda *_: None,
                )
            )
        )

        gateio_syms = await fetch_gateio_swaps()
        self._ws_tasks.append(
            loop.create_task(
                publish_gateio_orderbook(
                    gateio_syms,
                    lambda s, b, a: self._enqueue_orderbook("Gateio", s, b, a),
                    lambda *_: None,
                )
            )
        )
        self._orderbook_task_scheduled = True

    # Funding sekmesini aç
    def open_funding_tab(self):
        for idx in range(self.tabs.count()):
            if self.tabs.tabText(idx) == "Funding Rate Live":
                self.tabs.setCurrentIndex(idx)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(0,0,0,0)

        if not hasattr(self, "table"):
            self._setup_funding_table()
        # Export butonunu tutacak yatay "top" layout'u
        top = QtWidgets.QHBoxLayout()
        self.btn_export_funding = QtWidgets.QPushButton("Excel'e Aktar")
        top.addStretch()
        top.addWidget(self.btn_export_funding)
        layout.addLayout(top)

        layout.addWidget(self.table)

        # Export butonu sinyali aynı metoda bağlı kalsın
        self.btn_export_funding.clicked.connect(self.on_export_funding_excel)

        self.tabs.addTab(page, "Funding Rate Live")
        self.tabs.setCurrentWidget(page)

    # Ask/Bid sekmesini aç
    def open_askbid_tab(self):
        for idx in range(self.tabs.count()):
            if self.tabs.tabText(idx) == "Ask/Bid":
                self.tabs.setCurrentIndex(idx)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(0,0,0,0)

        if not hasattr(self, "askbid_table"):
            self._setup_askbid_table()

        top = QtWidgets.QHBoxLayout()
        self.btn_export_askbid = QtWidgets.QPushButton("Excel'e Aktar")
        top.addStretch()
        top.addWidget(self.btn_export_askbid)
        layout.addLayout(top)

        self.btn_export_askbid.clicked.connect(self.on_export_askbid_excel)

        anim_checkbox = QtWidgets.QCheckBox("Hücre Animasyonlarını Aç")
        anim_checkbox.setChecked(True)
        anim_checkbox.toggled.connect(self.askbid_delegate.setEnabled)
        layout.addWidget(anim_checkbox)

        layout.addWidget(self.askbid_table)
        self.tabs.addTab(page, "Ask/Bid")
        self.tabs.setCurrentWidget(page)

        # Ensure feeds are running but don't start multiple times
        self.start_askbid_feeds()

    # Arbitraj sekmesini aç
    def open_arbitrage_tab(self):
        for i in range(self.tabs.count()):
            if self.tabs.tabText(i) == "Arbitraj Diff":
                self.tabs.setCurrentIndex(i)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(5,5,5,5)
        layout.setSpacing(5)

        # Üst bar: eşik + butonlar
        top = QtWidgets.QHBoxLayout()
        self.arb_threshold_input = QtWidgets.QLineEdit("0.003")
        self.arb_threshold_input.setFixedWidth(60)
        self.arb_close_threshold_input = QtWidgets.QLineEdit("0.002")
        self.arb_close_threshold_input.setFixedWidth(60)
        self.btn_arbitrage_calc = QtWidgets.QPushButton("Hesapla")
        self.duration_input = QtWidgets.QLineEdit(str(self.min_duration))
        self.duration_input.setFixedWidth(60)
        self.btn_set_max_duration = QtWidgets.QPushButton("Getir")
        self.max_duration_input = QtWidgets.QLineEdit(str(self.max_duration))
        self.max_duration_input.setFixedWidth(60)
        self.btn_set_duration = QtWidgets.QPushButton("Getir")
        self.ask_fee_input = QtWidgets.QLineEdit(str(self.ask_fee_rate))
        self.ask_fee_input.setFixedWidth(60)
        self.btn_set_ask_fee = QtWidgets.QPushButton("Uygula")
        self.bid_fee_input = QtWidgets.QLineEdit(str(self.bid_fee_rate))
        self.bid_fee_input.setFixedWidth(60)
        self.btn_set_bid_fee = QtWidgets.QPushButton("Uygula")
        # Dropdown yerine switch kullanılacak
        self.duration_mode_switcher = QtWidgets.QPushButton("Min Süre")
        self.duration_mode_switcher.setCheckable(True)
        # Başlangıçta min süre kullanılacağı için checked=False
        self.duration_mode_switcher.setChecked(False)
        self.max_duration_input.setEnabled(False)
        self.btn_set_max_duration.setEnabled(False)
        btn_export_open     = QtWidgets.QPushButton("Açıkları Excel'e Aktar")
        btn_export_closed   = QtWidgets.QPushButton("Kapananları Excel'e Aktar")

        # İşte buraya eklemelisiniz:
        btn_clear_closed    = QtWidgets.QPushButton("Temizle")

        top.addWidget(QtWidgets.QLabel("Açılış Eşik:"));    top.addWidget(self.arb_threshold_input)
        top.addWidget(QtWidgets.QLabel("Kapanış Eşik:"));   top.addWidget(self.arb_close_threshold_input)
        top.addWidget(self.btn_arbitrage_calc)
        top.addWidget(QtWidgets.QLabel("Min Süre (s):"))
        top.addWidget(self.duration_input)
        top.addWidget(self.btn_set_duration)
        top.addWidget(QtWidgets.QLabel("Max Süre (s):"))
        top.addWidget(self.max_duration_input)
        top.addWidget(self.btn_set_max_duration)
        top.addWidget(QtWidgets.QLabel("Ask Fee:")); top.addWidget(self.ask_fee_input); top.addWidget(self.btn_set_ask_fee)
        top.addWidget(QtWidgets.QLabel("Bid Fee:")); top.addWidget(self.bid_fee_input); top.addWidget(self.btn_set_bid_fee)
        top.addWidget(self.duration_mode_switcher)
        top.addWidget(btn_export_open);  top.addWidget(btn_export_closed)
        top.addWidget(btn_clear_closed)
        top.addStretch()
        layout.addLayout(top)

        self.btn_set_duration.clicked.connect(self.on_set_duration)
        self.btn_set_max_duration.clicked.connect(self.on_set_max_duration)
        self.btn_set_ask_fee.clicked.connect(self.on_set_ask_fee)
        self.btn_set_bid_fee.clicked.connect(self.on_set_bid_fee)
        self.duration_mode_switcher.toggled.connect(self.on_switch_duration_mode)
        

        # Ortak model (önceden oluşturulmadıysa)
        if not hasattr(self, "arb_model"):
            self.arb_model = ArbitrajDiffModel()

        # Açık Arbitrajlar Tablosu
        open_box = QtWidgets.QGroupBox("Açık Arbitrajlar")
        v_open   = QtWidgets.QVBoxLayout(open_box)
        self.open_table = QtWidgets.QTableView()
        self.open_table.setShowGrid(True)
        self.open_table.verticalHeader().setVisible(False)
        

        header_o = FilterableHeaderView(QtCore.Qt.Horizontal, self.open_table)
        self.open_dropdown = MultiSelectDropdown()
        header_o.setSectionsClickable(True)
        header_o.setSortIndicatorShown(True)
        header_o.set_dropdown(self.open_dropdown)
        self.open_table.setHorizontalHeader(header_o)

        header_o.setSectionResizeMode(QHeaderView.Stretch)

        self.open_proxy = OpenArbitrageProxyModel()
        self.open_proxy.setDynamicSortFilter(True)
        self.open_proxy.setThreshold(float(self.arb_threshold_input.text()))
        self.open_proxy.setSourceModel(self.arb_model)

        self.open_table.setModel(self.open_proxy)
        self.open_table.setSortingEnabled(True)
        self.open_table.sortByColumn(3, QtCore.Qt.DescendingOrder)
        # Hide Chart column in open arbitrage table
        self.open_table.setColumnHidden(self.arb_model.columnCount() - 1, True)

        shortcut = QtGui.QShortcut(QtGui.QKeySequence.Copy, self.open_table)
        shortcut.activated.connect(lambda t=self.open_table: self._copy_selection(t))

        v_open.addWidget(self.open_table)
        layout.addWidget(open_box)

        # Kapanmış Arbitrajlar Tablosu
        closed_box = QtWidgets.QGroupBox("Kapanmış Arbitrajlar")
        v_closed   = QtWidgets.QVBoxLayout(closed_box)
        self.closed_table = QtWidgets.QTableView()
        self.closed_table.setShowGrid(True)
        self.closed_table.verticalHeader().setVisible(False)

        header_c = FilterableHeaderView(QtCore.Qt.Horizontal, self.closed_table)
        self.closed_dropdown = MultiSelectDropdown()
        header_c.setSectionsClickable(True)
        header_c.setSortIndicatorShown(True)
        header_c.set_dropdown(self.closed_dropdown)
        self.closed_table.setHorizontalHeader(header_c)

        header_c.setSectionResizeMode(QHeaderView.Stretch)

        self.closed_proxy = ClosedArbitrageProxyModel()
        self.closed_proxy.setDynamicSortFilter(True)
        self.closed_proxy.setSourceModel(self.arb_model)

        self.closed_table.setModel(self.closed_proxy)
        self.closed_table.setSortingEnabled(True)
        self.closed_table.sortByColumn(5, QtCore.Qt.DescendingOrder)
        # Allow opening chart via double click on Chart column
        self.closed_table.doubleClicked.connect(self._on_closed_chart)

        shortcut = QtGui.QShortcut(QtGui.QKeySequence.Copy, self.closed_table)
        shortcut.activated.connect(lambda t=self.closed_table: self._copy_selection(t))

        v_closed.addWidget(self.closed_table)
        layout.addWidget(closed_box)

        # Dropdown listelerini güncelle
        self.arb_model.symbolsUpdated.connect(self._update_open_dropdown)
        self.arb_model.symbolsUpdated.connect(self._update_closed_dropdown)
        # Filtre → proxy
        self.open_dropdown.selectionChanged.connect(self.open_proxy.set_symbol_filter)
        self.closed_dropdown.selectionChanged.connect(self.closed_proxy.set_symbol_filter)

        # Sekmeyi ekle ve seç
        self.tabs.addTab(page, "Arbitraj Diff")
        self.tabs.setCurrentWidget(page)

        # Slot’lar
        self.btn_arbitrage_calc.clicked.connect(self.on_arbitrage_calculate)
        btn_export_open.clicked.connect(self.on_export_open_excel)
        btn_export_closed.clicked.connect(self.on_export_closed_excel)

        btn_clear_closed.clicked.connect(self.on_clear_closed)

        # Funding Rate Diff sekmesi
    def open_funding_rate_diff_tab(self):
        for i in range(self.tabs.count()):
            if self.tabs.tabText(i) == "Funding Rate Diff":
                self.tabs.setCurrentIndex(i)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(5, 5, 5, 5)

        # Export button row
        top = QtWidgets.QHBoxLayout()
        self.btn_export_fr_diff = QtWidgets.QPushButton("Excel'e Aktar")
        top.addStretch()
        top.addWidget(self.btn_export_fr_diff)
        layout.addLayout(top)
        
        # First table and controls
        box1 = QtWidgets.QGroupBox("Funding Rate Diff")
        v1 = QtWidgets.QVBoxLayout(box1)

        row1_top = QtWidgets.QHBoxLayout()
        self.max_fr_input1a = QtWidgets.QLineEdit()
        self.max_fr_input1a.setFixedWidth(80)
        row1_top.addWidget(QtWidgets.QLabel("Max Funding Rate:"))
        row1_top.addWidget(self.max_fr_input1a)

        self.min_fr_input1a = QtWidgets.QLineEdit()
        self.min_fr_input1a.setFixedWidth(80)
        row1_top.addWidget(QtWidgets.QLabel("Min Funding Rate:"))
        row1_top.addWidget(self.min_fr_input1a)

        self.btn_generate_fr_diff1a = QtWidgets.QPushButton("Generate")
        row1_top.addWidget(self.btn_generate_fr_diff1a)

        row1_top.addSpacing(20)

        self.max_cd_input1a = QtWidgets.QLineEdit()
        self.max_cd_input1a.setFixedWidth(80)
        self.max_cd_input1a.setInputMask("00:00:00")
        row1_top.addWidget(QtWidgets.QLabel("Max Countdown:"))
        row1_top.addWidget(self.max_cd_input1a)

        self.btn_generate_cd1a = QtWidgets.QPushButton("Generate")
        row1_top.addWidget(self.btn_generate_cd1a)
        row1_top.addStretch()

        row1_bottom = QtWidgets.QHBoxLayout()
        self.max_ask_depth_input1 = QtWidgets.QLineEdit()
        self.max_ask_depth_input1.setFixedWidth(80)
        row1_bottom.addWidget(QtWidgets.QLabel("Max Ask Derinlik (USDT):"))
        row1_bottom.addWidget(self.max_ask_depth_input1)
        self.btn_generate_ask_depth1 = QtWidgets.QPushButton("Generate")
        row1_bottom.addWidget(self.btn_generate_ask_depth1)
        row1_bottom.addSpacing(20)

        self.min_ask_depth_input1 = QtWidgets.QLineEdit()
        self.min_ask_depth_input1.setFixedWidth(80)
        row1_bottom.addWidget(QtWidgets.QLabel("Min Ask Derinlik (USDT):"))
        row1_bottom.addWidget(self.min_ask_depth_input1)
        self.btn_generate_min_ask_depth1 = QtWidgets.QPushButton("Generate")
        row1_bottom.addWidget(self.btn_generate_min_ask_depth1)
        row1_bottom.addSpacing(20)

        self.max_bid_depth_input1 = QtWidgets.QLineEdit()
        self.max_bid_depth_input1.setFixedWidth(80)
        row1_bottom.addWidget(QtWidgets.QLabel("Max Bid Derinlik (USDT):"))
        row1_bottom.addWidget(self.max_bid_depth_input1)
        self.btn_generate_max_bid_depth1 = QtWidgets.QPushButton("Generate")
        row1_bottom.addWidget(self.btn_generate_max_bid_depth1)

        row1_bottom.addSpacing(20)

        self.min_bid_depth_input1 = QtWidgets.QLineEdit()
        self.min_bid_depth_input1.setFixedWidth(80)
        row1_bottom.addWidget(QtWidgets.QLabel("Min Bid Derinlik (USDT):"))
        row1_bottom.addWidget(self.min_bid_depth_input1)
        self.btn_generate_min_bid_depth1 = QtWidgets.QPushButton("Generate")
        row1_bottom.addWidget(self.btn_generate_min_bid_depth1)
        row1_bottom.addStretch()

        v1.addLayout(row1_top)
        v1.addLayout(row1_bottom)

        self.fr_diff_table1 = QtWidgets.QTableView()
        self.fr_diff_table1.setShowGrid(True)
        self.fr_diff_table1.verticalHeader().setVisible(False)
        self.fr_diff_model1 = FundingRateDiffModel()
        self.fr_diff_proxy1 = SymbolFilterProxyModel()
        self.fr_diff_proxy1.setSourceModel(self.fr_diff_model1)
        self.fr_diff_proxy1.setDynamicSortFilter(True)
        self.fr_diff_table1.setModel(self.fr_diff_proxy1)
        header1 = self.fr_diff_table1.horizontalHeader()
        header1.setSectionResizeMode(QHeaderView.Stretch)
        v1.addWidget(self.fr_diff_table1)
        layout.addWidget(box1)

        # Second table and controls
        box2 = QtWidgets.QGroupBox("Funding Rate Diff")
        v2 = QtWidgets.QVBoxLayout(box2)

        row2_top = QtWidgets.QHBoxLayout()
        row2_top.addWidget(QtWidgets.QLabel("Symbols:"))
        self.fr_symbols_dropdown2 = MultiSelectDropdown()
        row2_top.addWidget(self.fr_symbols_dropdown2)
        self.btn_generate_fr_symbols2 = QtWidgets.QPushButton("Generate")
        row2_top.addWidget(self.btn_generate_fr_symbols2)

        row2_top.addSpacing(20)

        self.max_fr_input2a = QtWidgets.QLineEdit()
        self.max_fr_input2a.setFixedWidth(80)
        row2_top.addWidget(QtWidgets.QLabel("Max Funding Rate:"))
        row2_top.addWidget(self.max_fr_input2a)

        self.min_fr_input2a = QtWidgets.QLineEdit()
        self.min_fr_input2a.setFixedWidth(80)
        row2_top.addWidget(QtWidgets.QLabel("Min Funding Rate:"))
        row2_top.addWidget(self.min_fr_input2a)

        self.btn_generate_fr_diff2a = QtWidgets.QPushButton("Generate")
        row2_top.addWidget(self.btn_generate_fr_diff2a)

        row2_top.addSpacing(20)

        self.max_cd_input2a = QtWidgets.QLineEdit()
        self.max_cd_input2a.setFixedWidth(80)
        self.max_cd_input2a.setInputMask("00:00:00")
        row2_top.addWidget(QtWidgets.QLabel("Max Countdown:"))
        row2_top.addWidget(self.max_cd_input2a)

        self.btn_generate_cd2a = QtWidgets.QPushButton("Generate")
        row2_top.addWidget(self.btn_generate_cd2a)
        row2_top.addStretch()

        row2_bottom = QtWidgets.QHBoxLayout()
        self.max_ask_depth_input2 = QtWidgets.QLineEdit()
        self.max_ask_depth_input2.setFixedWidth(80)
        row2_bottom.addWidget(QtWidgets.QLabel("Max Ask Derinlik (USDT):"))
        row2_bottom.addWidget(self.max_ask_depth_input2)
        self.btn_generate_ask_depth2 = QtWidgets.QPushButton("Generate")
        row2_bottom.addWidget(self.btn_generate_ask_depth2)
        row2_bottom.addSpacing(20)

        self.min_ask_depth_input2 = QtWidgets.QLineEdit()
        self.min_ask_depth_input2.setFixedWidth(80)
        row2_bottom.addWidget(QtWidgets.QLabel("Min Ask Derinlik (USDT):"))
        row2_bottom.addWidget(self.min_ask_depth_input2)
        self.btn_generate_min_ask_depth2 = QtWidgets.QPushButton("Generate")
        row2_bottom.addWidget(self.btn_generate_min_ask_depth2)

        row2_bottom.addSpacing(20)

        self.max_bid_depth_input2 = QtWidgets.QLineEdit()
        self.max_bid_depth_input2.setFixedWidth(80)
        row2_bottom.addWidget(QtWidgets.QLabel("Max Bid Derinlik (USDT):"))
        row2_bottom.addWidget(self.max_bid_depth_input2)
        self.btn_generate_max_bid_depth2 = QtWidgets.QPushButton("Generate")
        row2_bottom.addWidget(self.btn_generate_max_bid_depth2)

        row2_bottom.addSpacing(20)

        self.min_bid_depth_input2 = QtWidgets.QLineEdit()
        self.min_bid_depth_input2.setFixedWidth(80)
        row2_bottom.addWidget(QtWidgets.QLabel("Min Bid Derinlik (USDT):"))
        row2_bottom.addWidget(self.min_bid_depth_input2)
        self.btn_generate_min_bid_depth2 = QtWidgets.QPushButton("Generate")
        row2_bottom.addWidget(self.btn_generate_min_bid_depth2)
        row2_bottom.addStretch()

        v2.addLayout(row2_top)
        v2.addLayout(row2_bottom)

        self.fr_diff_table2 = QtWidgets.QTableView()
        self.fr_diff_table2.setShowGrid(True)
        self.fr_diff_table2.verticalHeader().setVisible(False)
        self.fr_diff_model2 = FundingRateDiffModel()
        self.fr_diff_proxy2 = SymbolFilterProxyModel()
        self.fr_diff_proxy2.setSourceModel(self.fr_diff_model2)
        self.fr_diff_proxy2.setDynamicSortFilter(True)
        self.fr_diff_table2.setModel(self.fr_diff_proxy2)
        header2 = self.fr_diff_table2.horizontalHeader()
        header2.setSectionResizeMode(QHeaderView.Stretch)
        v2.addWidget(self.fr_diff_table2)
        self.fr_diff_model2.symbolsUpdated.connect(
            lambda syms: self.fr_symbols_dropdown2.set_items(syms)
        )
        self.fr_symbols_dropdown2.selectionChanged.connect(
            self.fr_diff_proxy2.set_symbol_filter
        )
        layout.addWidget(box2)

        self.tabs.addTab(page, "Funding Rate Diff")
        self.tabs.setCurrentWidget(page)

        self.btn_export_fr_diff.clicked.connect(self.on_export_fr_diff_excel)

        # Connect filters
        self.btn_generate_fr_diff1a.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_cd1a.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_ask_depth1.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_min_ask_depth1.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_max_bid_depth1.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_min_bid_depth1.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_fr_symbols2.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_fr_diff2a.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_cd2a.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_ask_depth2.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_min_ask_depth2.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_max_bid_depth2.clicked.connect(self._update_fr_diff_models)
        self.btn_generate_min_bid_depth2.clicked.connect(self._update_fr_diff_models)


    def open_orderbook_selection_tab(self):
        for i in range(self.tabs.count()):
            if self.tabs.tabText(i) == "Orderbook Selection":
                self.tabs.setCurrentIndex(i)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(5, 5, 5, 5)

        # Binance filter row
        row = QtWidgets.QHBoxLayout()
        row.addWidget(QtWidgets.QLabel("Binance Symbols:"))
        self.orderbook_dropdown = MultiSelectDropdown()
        row.addWidget(self.orderbook_dropdown)
        self.btn_generate_binance_orderbook = QtWidgets.QPushButton("Generate")
        row.addWidget(self.btn_generate_binance_orderbook)

        row.addSpacing(20)

        # OKX filter row
        row.addWidget(QtWidgets.QLabel("OKX Symbols:"))
        self.okx_orderbook_dropdown = MultiSelectDropdown()
        row.addWidget(self.okx_orderbook_dropdown)
        self.btn_generate_okx_orderbook = QtWidgets.QPushButton("Generate")
        row.addWidget(self.btn_generate_okx_orderbook)

        row.addSpacing(20)

        # Bybit filter row
        row.addWidget(QtWidgets.QLabel("Bybit Symbols:"))
        self.bybit_orderbook_dropdown = MultiSelectDropdown()
        row.addWidget(self.bybit_orderbook_dropdown)
        self.btn_generate_bybit_orderbook = QtWidgets.QPushButton("Generate")
        row.addWidget(self.btn_generate_bybit_orderbook)

        row.addSpacing(20)

        # Bitget filter row
        row.addWidget(QtWidgets.QLabel("Bitget Symbols:"))
        self.bitget_orderbook_dropdown = MultiSelectDropdown()
        row.addWidget(self.bitget_orderbook_dropdown)
        self.btn_generate_bitget_orderbook = QtWidgets.QPushButton("Generate")
        row.addWidget(self.btn_generate_bitget_orderbook)

        row.addSpacing(20)

        # Gateio filter row
        row.addWidget(QtWidgets.QLabel("Gateio Symbols:"))
        self.gateio_orderbook_dropdown = MultiSelectDropdown()
        row.addWidget(self.gateio_orderbook_dropdown)
        self.btn_generate_gateio_orderbook = QtWidgets.QPushButton("Generate")
        row.addWidget(self.btn_generate_gateio_orderbook)
        row.addStretch()

        layout.addLayout(row)

        # Cross exchange orderbook row
        cross = QtWidgets.QHBoxLayout()
        cross.addWidget(QtWidgets.QLabel("Buy Exch:"))
        self.ob_buy_combo = QtWidgets.QComboBox()
        self.ob_buy_combo.addItems(AB_EXCHANGES)
        cross.addWidget(self.ob_buy_combo)

        cross.addWidget(QtWidgets.QLabel("Sell Exch:"))
        self.ob_sell_combo = QtWidgets.QComboBox()
        self.ob_sell_combo.addItems(AB_EXCHANGES)
        cross.addWidget(self.ob_sell_combo)

        cross.addWidget(QtWidgets.QLabel("Symbol:"))
        self.ob_symbol_combo = QtWidgets.QComboBox()
        self.ob_symbol_combo.setEditable(True)
        cross.addWidget(self.ob_symbol_combo)

        self.btn_generate_cross_orderbook = QtWidgets.QPushButton("Generate")
        cross.addWidget(self.btn_generate_cross_orderbook)
        cross.addStretch()
        layout.addLayout(cross)
        layout.addStretch()

        self.btn_generate_binance_orderbook.clicked.connect(
            self._on_generate_binance_orderbook
        )
        self.btn_generate_okx_orderbook.clicked.connect(
            self._on_generate_okx_orderbook
        )
        self.btn_generate_bybit_orderbook.clicked.connect(
            self._on_generate_bybit_orderbook
        )
        self.btn_generate_bitget_orderbook.clicked.connect(
            self._on_generate_bitget_orderbook
        )
        self.btn_generate_gateio_orderbook.clicked.connect(
            self._on_generate_gateio_orderbook
        )
        self.btn_generate_cross_orderbook.clicked.connect(
            self._on_generate_cross_orderbook
        )

        def update_cross_symbols():
            buy = self.ob_buy_combo.currentText()
            sell = self.ob_sell_combo.currentText()
            symbols = []
            for sym, data in self.askbid_model._data.items():
                if buy in data and sell in data:
                    symbols.append(sym)
            self.ob_symbol_combo.clear()
            self.ob_symbol_combo.addItems(sorted(symbols))

            comp = QtWidgets.QCompleter(self.ob_symbol_combo.model(), self.ob_symbol_combo)
            comp.setFilterMode(QtCore.Qt.MatchContains)
            comp.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
            self.ob_symbol_combo.setCompleter(comp)

        self.ob_buy_combo.currentIndexChanged.connect(update_cross_symbols)
        self.ob_sell_combo.currentIndexChanged.connect(update_cross_symbols)
        self.askbid_model.symbolsUpdated.connect(lambda _: update_cross_symbols())

        update_cross_symbols()

        self.tabs.addTab(page, "Orderbook Selection")
        self.tabs.setCurrentWidget(page)

        loop = asyncio.get_event_loop()
        loop.create_task(self._refresh_binance_symbols())
        loop.create_task(self._refresh_okx_symbols())
        loop.create_task(self._refresh_bybit_symbols())
        loop.create_task(self._refresh_bitget_symbols())
        loop.create_task(self._refresh_gateio_symbols())

        # Yeni grafik sekmesi
    def open_chart_tab(self):
        for i in range(self.tabs.count()):
            if self.tabs.tabText(i) == "Chart Selection":
                self.tabs.setCurrentIndex(i)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(5, 5, 5, 5)

        sel_layout = QtWidgets.QHBoxLayout()
        sel_layout.addWidget(QtWidgets.QLabel("Ask Borsa:"))
        self.chart_ask_combo = QtWidgets.QComboBox()
        self.chart_ask_combo.addItems(AB_EXCHANGES)
        sel_layout.addWidget(self.chart_ask_combo)

        sel_layout.addWidget(QtWidgets.QLabel("Bid Borsa:"))
        self.chart_bid_combo = QtWidgets.QComboBox()
        self.chart_bid_combo.addItems(AB_EXCHANGES)
        sel_layout.addWidget(self.chart_bid_combo)

        sel_layout.addWidget(QtWidgets.QLabel("Symbol:"))
        self.chart_symbol_combo = QtWidgets.QComboBox()
        self.chart_symbol_combo.setEditable(True)
        sel_layout.addWidget(self.chart_symbol_combo)

        self.btn_generate_chart = QtWidgets.QPushButton("Generate")
        sel_layout.addWidget(self.btn_generate_chart)
        sel_layout.addStretch()
        layout.addLayout(sel_layout)
        layout.addStretch()

        def update_symbols():
            ask_exch = self.chart_ask_combo.currentText()
            bid_exch = self.chart_bid_combo.currentText()
            symbols = []
            for sym, data in self.askbid_model._data.items():
                if ask_exch in data and bid_exch in data:
                    symbols.append(sym)
            self.chart_symbol_combo.clear()
            self.chart_symbol_combo.addItems(sorted(symbols))

            comp = QtWidgets.QCompleter(self.chart_symbol_combo.model(), self.chart_symbol_combo)
            comp.setFilterMode(QtCore.Qt.MatchContains)
            comp.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
            self.chart_symbol_combo.setCompleter(comp)

        self.chart_ask_combo.currentIndexChanged.connect(update_symbols)
        self.chart_bid_combo.currentIndexChanged.connect(update_symbols)
        self.askbid_model.symbolsUpdated.connect(lambda _: update_symbols())

        update_symbols()

        self.btn_generate_chart.clicked.connect(self._on_generate_chart)

        self.tabs.addTab(page, "Chart Selection")
        self.tabs.setCurrentWidget(page)
    
    def open_db_tab(self):
        for i in range(self.tabs.count()):
            if self.tabs.tabText(i) == "DB Connection":
                self.tabs.setCurrentIndex(i)
                asyncio.get_event_loop().create_task(self._refresh_db_symbols())
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(5, 5, 5, 5)

        form = QtWidgets.QHBoxLayout()


        self.start_date_edit = QtWidgets.QDateEdit(QtCore.QDate.currentDate())
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.start_date_edit.setCalendarPopup(True)

        self.start_time_edit = QtWidgets.QTimeEdit(QtCore.QTime.currentTime())
        self.start_time_edit.setDisplayFormat("HH:mm:ss")

        self.end_date_edit = QtWidgets.QDateEdit(QtCore.QDate.currentDate())
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.end_date_edit.setCalendarPopup(True)

        self.end_time_edit = QtWidgets.QTimeEdit(QtCore.QTime.currentTime())
        self.end_time_edit.setDisplayFormat("HH:mm:ss")

        form.addWidget(self.start_date_edit)
        form.addWidget(self.start_time_edit)
        form.addWidget(self.end_date_edit)
        form.addWidget(self.end_time_edit)

        form.addWidget(QtWidgets.QLabel("Symbol:"))
        self.db_symbol_label = QtWidgets.QLabel()
        self.db_symbol_label.setMinimumWidth(100)
        self.db_symbol_label.setFrameShape(QtWidgets.QFrame.StyledPanel)
        form.addWidget(self.db_symbol_label)

        self.db_symbol_dropdown = MultiSelectDropdown()
        self.db_symbol_dropdown.selectionChanged.connect(self._update_db_symbol_display)
        form.addWidget(self.db_symbol_dropdown)

        form.addWidget(QtWidgets.QLabel("Buy Exch:"))
        self.db_buy_label = QtWidgets.QLabel()
        self.db_buy_label.setMinimumWidth(80)
        self.db_buy_label.setFrameShape(QtWidgets.QFrame.StyledPanel)
        form.addWidget(self.db_buy_label)

        self.db_buy_dropdown = MultiSelectDropdown()
        self.db_buy_dropdown.selectionChanged.connect(self._update_db_buy_display)
        form.addWidget(self.db_buy_dropdown)

        form.addWidget(QtWidgets.QLabel("Sell Exch:"))
        self.db_sell_label = QtWidgets.QLabel()
        self.db_sell_label.setMinimumWidth(80)
        self.db_sell_label.setFrameShape(QtWidgets.QFrame.StyledPanel)
        form.addWidget(self.db_sell_label)

        self.db_sell_dropdown = MultiSelectDropdown()
        self.db_sell_dropdown.selectionChanged.connect(self._update_db_sell_display)
        form.addWidget(self.db_sell_dropdown)
        
        self.btn_download_db = QtWidgets.QPushButton("Verileri İndir")
        form.addWidget(self.btn_download_db)
        form.addStretch()
        layout.addLayout(form)
        layout.addStretch()

        self.btn_download_db.clicked.connect(self.on_download_db_data)

        self.tabs.addTab(page, "DB Connection")
        self.tabs.setCurrentWidget(page)

        asyncio.get_event_loop().create_task(self._refresh_db_symbols())



    # Açık dropdown güncelle
    def _update_open_dropdown(self, _):
        open_syms = list({ev.symbol for ev in self.arb_model.events if ev.end_dt is None})
        self.open_dropdown.set_items(open_syms)

    # Kapanan dropdown güncelle
    def _update_closed_dropdown(self, _):
        closed_syms = list({ev.symbol for ev in self.arb_model.events if ev.end_dt is not None})
        self.closed_dropdown.set_items(closed_syms)

        # (İsteğe bağlı) Modeli tamamen resetleyip yeniden çiz
        # self.arb_model.beginResetModel()
        # self.arb_model.endResetModel()

    async def _refresh_db_symbols(self):
        if not (
            hasattr(self, "db_symbol_dropdown")
            and hasattr(self, "db_buy_dropdown")
            and hasattr(self, "db_sell_dropdown")
        ):
            return

        syms = await fetch_db_symbols()
        buys = await fetch_db_buy_exchs()
        sells = await fetch_db_sell_exchs()
        self.db_symbol_dropdown.set_items(syms)
        self.db_buy_dropdown.set_items(buys)
        self.db_sell_dropdown.set_items(sells)
        self._update_db_symbol_display(self.db_symbol_dropdown.get_selected_items())

        self._update_db_buy_display(self.db_buy_dropdown.get_selected_items())
        self._update_db_sell_display(self.db_sell_dropdown.get_selected_items())

    def _update_db_symbol_display(self, selected: set[str]):
        total = len(self.db_symbol_dropdown._items)
        if not selected or len(selected) == total:
            text = "All"
        elif len(selected) == 1:
            text = next(iter(selected))
        else:
            text = f"{len(selected)} selected"
        self.db_symbol_label.setText(text)

    def _update_db_buy_display(self, selected: set[str]):
        total = len(self.db_buy_dropdown._items)
        if not selected or len(selected) == total:
            text = "All"
        elif len(selected) == 1:
            text = next(iter(selected))
        else:
            text = f"{len(selected)} selected"
        self.db_buy_label.setText(text)

    def _update_db_sell_display(self, selected: set[str]):
        total = len(self.db_sell_dropdown._items)
        if not selected or len(selected) == total:
            text = "All"
        elif len(selected) == 1:
            text = next(iter(selected))
        else:
            text = f"{len(selected)} selected"
        self.db_sell_label.setText(text)

    async def _refresh_binance_symbols(self):
        if not hasattr(self, "orderbook_dropdown"):
            return
        syms = await fetch_binance_futures()
        self.orderbook_dropdown.set_items(syms)
    
    async def _refresh_okx_symbols(self):
        if not hasattr(self, "okx_orderbook_dropdown"):
            return
        async with aiohttp.ClientSession() as sess:
            r = await sess.get(OKX_REST_INSTRUMENTS)
            j = await r.json()
        self._okx_symbol_map.clear()
        norm_syms = []
        for e in j.get("data", []):
            inst = e.get("instId")
            if not isinstance(inst, str):
                continue
            norm = normalize_symbol(inst)
            if norm:
                self._okx_symbol_map[norm] = inst
                norm_syms.append(norm)
        self.okx_orderbook_dropdown.set_items(norm_syms)
    
    async def _refresh_bybit_symbols(self):
        if not hasattr(self, "bybit_orderbook_dropdown"):
            return
        syms = await fetch_bybit_swaps()
        self._bybit_symbol_map.clear()
        norm_syms = []
        for s in syms:
            norm = normalize_symbol(s)
            if norm:
                self._bybit_symbol_map[norm] = s
                norm_syms.append(norm)
        self.bybit_orderbook_dropdown.set_items(norm_syms)

    async def _refresh_bitget_symbols(self):
        if not hasattr(self, "bitget_orderbook_dropdown"):
            return
        syms = await fetch_bitget_swaps()
        self._bitget_symbol_map.clear()
        norm_syms = []
        for s in syms:
            norm = normalize_symbol(s)
            if norm:
                self._bitget_symbol_map[norm] = s
                norm_syms.append(norm)
        self.bitget_orderbook_dropdown.set_items(norm_syms)

    async def _refresh_gateio_symbols(self):
        if not hasattr(self, "gateio_orderbook_dropdown"):
            return
        syms = await fetch_gateio_swaps()
        self._gateio_symbol_map.clear()
        norm_syms = []
        for s in syms:
            norm = normalize_symbol(s)
            if norm:
                self._gateio_symbol_map[norm] = s
                norm_syms.append(norm)
        self.gateio_orderbook_dropdown.set_items(norm_syms)

    def _get_raw_symbol(self, exchange: str, norm: str) -> str | None:
        if exchange == "Binance":
            return norm
        if exchange == "OKX":
            return self._okx_symbol_map.get(norm)
        if exchange == "Bybit":
            return self._bybit_symbol_map.get(norm, norm)
        if exchange == "Bitget":
            return self._bitget_symbol_map.get(norm, norm)
        if exchange == "Gateio":
            return self._gateio_symbol_map.get(norm, norm)
        return norm

    def _start_orderbook_feed(self, loop, exchange: str, symbol: str, cb, status_cb=None):
        if not symbol:
            return None
        if status_cb is None:
            status_cb = lambda *_: None
        if exchange == "Binance":
            return loop.create_task(
                publish_binance_orderbook([symbol], lambda _s, b, a: cb(b, a), status_cb)
            )
        if exchange == "OKX":
            return loop.create_task(
                publish_okx_orderbook([symbol], lambda _s, b, a: cb(b, a), status_cb)
            )
        if exchange == "Bybit":
            return loop.create_task(
                publish_bybit_orderbook([symbol], lambda _s, b, a: cb(b, a), status_cb)
            )
        if exchange == "Bitget":
            return loop.create_task(
                publish_bitget_orderbook([symbol], lambda _s, b, a: cb(b, a), status_cb)
            )
        if exchange == "Gateio":
            return loop.create_task(
                publish_gateio_orderbook([symbol], lambda _s, b, a: cb(b, a), status_cb)
            )
        return None

    @QtCore.Slot()
    def _on_generate_binance_orderbook(self):
        if not hasattr(self, "orderbook_dropdown"):
            return
        syms = self.orderbook_dropdown.get_selected_items()
        loop = asyncio.get_event_loop()
        for sym in syms:
            win = OrderbookWindow(sym, "Binance", self.dark_mode)
            self.orderbook_windows.append(win)
            task = loop.create_task(
                publish_binance_orderbook([sym], lambda _s, b, a, w=win: w.update_book(b, a), lambda *_: None)
            )

            def _cleanup(_=None, w=win, t=task):
                if w in self.orderbook_windows:
                    self.orderbook_windows.remove(w)
                t.cancel()

            win.destroyed.connect(_cleanup)
            win.show()
        
    @QtCore.Slot()
    def _on_generate_okx_orderbook(self):
        if not hasattr(self, "okx_orderbook_dropdown"):
            return
        syms = self.okx_orderbook_dropdown.get_selected_items()
        loop = asyncio.get_event_loop()
        for norm in syms:
            inst = self._okx_symbol_map.get(norm)
            if not inst:
                continue
            win = OrderbookWindow(norm, "OKX", self.dark_mode)
            self.orderbook_windows.append(win)
            task = loop.create_task(
                publish_okx_orderbook([inst], lambda _s, b, a, w=win: w.update_book(b, a), lambda *_: None)
            )

            def _cleanup(_=None, w=win, t=task):
                if w in self.orderbook_windows:
                    self.orderbook_windows.remove(w)
                t.cancel()

            win.destroyed.connect(_cleanup)
            win.show()

    @QtCore.Slot()
    def _on_generate_bybit_orderbook(self):
        if not hasattr(self, "bybit_orderbook_dropdown"):
            return
        syms = self.bybit_orderbook_dropdown.get_selected_items()
        loop = asyncio.get_event_loop()
        for norm in syms:
            raw = self._bybit_symbol_map.get(norm, norm)
            win = OrderbookWindow(norm, "Bybit", self.dark_mode)
            self.orderbook_windows.append(win)
            task = loop.create_task(
                publish_bybit_orderbook([raw], lambda _s, b, a, w=win: w.update_book(b, a), lambda *_: None)
            )

            def _cleanup(_=None, w=win, t=task):
                if w in self.orderbook_windows:
                    self.orderbook_windows.remove(w)
                t.cancel()

            win.destroyed.connect(_cleanup)
            win.show()

    @QtCore.Slot()
    def _on_generate_bitget_orderbook(self):
        if not hasattr(self, "bitget_orderbook_dropdown"):
            return
        syms = self.bitget_orderbook_dropdown.get_selected_items()
        loop = asyncio.get_event_loop()
        for norm in syms:
            raw = self._bitget_symbol_map.get(norm, norm)
            win = OrderbookWindow(norm, "Bitget", self.dark_mode)
            self.orderbook_windows.append(win)
            task = loop.create_task(
                publish_bitget_orderbook(
                    [raw],
                    lambda _s, b, a, w=win: w.update_book(b, a),
                    lambda _e, connected, w=win: w.update_status(connected),
                )
            )

            def _cleanup(_=None, w=win, t=task):
                if w in self.orderbook_windows:
                    self.orderbook_windows.remove(w)
                t.cancel()

            win.destroyed.connect(_cleanup)
            win.show()

    @QtCore.Slot()
    def _on_generate_gateio_orderbook(self):
        if not hasattr(self, "gateio_orderbook_dropdown"):
            return
        syms = self.gateio_orderbook_dropdown.get_selected_items()
        loop = asyncio.get_event_loop()
        for norm in syms:
            raw = self._gateio_symbol_map.get(norm, norm)
            win = OrderbookWindow(norm, "Gateio", self.dark_mode)
            self.orderbook_windows.append(win)
            task = loop.create_task(
                publish_gateio_orderbook(
                    [raw],
                    lambda _s, b, a, w=win: w.update_book(b, a),
                    lambda _e, connected, w=win: w.update_status(connected),
                )
            )

            def _cleanup(_=None, w=win, t=task):
                if w in self.orderbook_windows:
                    self.orderbook_windows.remove(w)
                t.cancel()

            win.destroyed.connect(_cleanup)
            win.show()
    @QtCore.Slot()
    def _on_generate_cross_orderbook(self):
        if not hasattr(self, "ob_symbol_combo"):
            return
        symbol = self.ob_symbol_combo.currentText()
        buy_exch = self.ob_buy_combo.currentText()
        sell_exch = self.ob_sell_combo.currentText()
        if not symbol or not buy_exch or not sell_exch:
            return

        buy_raw = self._get_raw_symbol(buy_exch, symbol)
        sell_raw = self._get_raw_symbol(sell_exch, symbol)
        if not buy_raw or not sell_raw:
            return

        win = CrossOrderbookWindow(symbol, buy_exch, sell_exch, self.dark_mode)
        self.orderbook_windows.append(win)
        loop = asyncio.get_event_loop()

        tasks = []
        tasks.append(
            self._start_orderbook_feed(
                loop,
                buy_exch,
                buy_raw,
                lambda b, a, w=win: w.update_buy(b, a),
                lambda e, c, w=win: w.update_status(e, c),
            )
        )
        tasks.append(
            self._start_orderbook_feed(
                loop,
                sell_exch,
                sell_raw,
                lambda b, a, w=win: w.update_sell(b, a),
                lambda e, c, w=win: w.update_status(e, c),
            )
        )

        def _cleanup(_=None, w=win, ts=tasks):
            if w in self.orderbook_windows:
                self.orderbook_windows.remove(w)
            for t in ts:
                if t:
                    t.cancel()

        win.destroyed.connect(_cleanup)
        win.show()


    def _copy_selection(self, table):
        """Copy selected cells of the given table to the clipboard."""
        indexes = table.selectionModel().selectedIndexes()
        if not indexes:
            return
        indexes = sorted(indexes, key=lambda i: (i.row(), i.column()))
        # Collect unique columns in the order they appear in the selection
        columns = []
        seen_cols = set()
        for idx in indexes:
            c = idx.column()
            if c not in seen_cols:
                seen_cols.add(c)
                columns.append(c)
        rows = {}
        for idx in indexes:
            rows.setdefault(idx.row(), []).append(idx)
        

        model = table.model()
        # Header line
        headers = [
            str(model.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole) or "")
            for c in columns
        ]
        lines = ["\t".join(headers)]

        for r in sorted(rows):
            vals = [
                str(model.data(i, QtCore.Qt.DisplayRole) or "")
                for i in sorted(rows[r], key=lambda i: i.column())
            ]
            lines.append("\t".join(vals))

        QtWidgets.QApplication.clipboard().setText("\n".join(lines))


    def on_export_open_excel(self):
        """Açık arbitrajları Excel'e aktar."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"open_arbitrages_{ts}.xlsx"
        self._export_arbitrage(self.open_proxy, name)

    def on_export_closed_excel(self):
        """Kapanmış arbitrajları Excel'e aktar."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"closed_arbitrages_{ts}.xlsx"
        self._export_arbitrage(self.closed_proxy, name)

    def _proxy_to_dataframe(self, proxy) -> pd.DataFrame:
        headers = [
            self.arb_model.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.arb_model.columnCount() - 1)
        ]

        rows = []
        for r in range(proxy.rowCount()):
            rec = {}
            for c in range(proxy.columnCount() - 1):
                idx = proxy.index(r, c)
                rec[headers[c]] = proxy.data(idx, QtCore.Qt.DisplayRole)
            rows.append(rec)

        return pd.DataFrame(rows, columns=headers)


    def _export_arbitrage(self, proxy, default_name):
        # 1) Dosya yolunu sor
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Tabloyu Kaydet",
            default_name,
            "Excel Dosyaları (*.xlsx)"
        )
        if not path:
            return
        
    

        df = self._proxy_to_dataframe(proxy)
        func = lambda: df.to_excel(path, index=False)
        self._start_export(func, path)

    def on_export_askbid_excel(self):
        """Ask/Bid tablosunu Excel'e aktar."""
        ts = datetime.now().strftime("%d%m%Y%H%M%S")
        default_name = f"AskBid {ts}.xlsx"
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Tabloyu Kaydet",
            default_name,
            "Excel Dosyaları (*.xlsx)",
        )
        if not path:
            return
        self._export_askbid(path)

    def _export_askbid(self, filename: str):
        headers = [
            self.askbid_model.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.askbid_model.columnCount())
        ]

        rows = []
        for r in range(self.askbid_proxy.rowCount()):
            rec = {}
            for c in range(self.askbid_proxy.columnCount()):
                idx = self.askbid_proxy.index(r, c)
                rec[headers[c]] = self.askbid_proxy.data(idx, QtCore.Qt.DisplayRole)
            rows.append(rec)

        if not rows:
            QtWidgets.QMessageBox.information(self, "Bilgi", "Tablo boş olduğu için Excel kaydedilmedi.")
            return

        df = pd.DataFrame(rows, columns=headers)
        func = lambda: df.to_excel(filename, index=False)
        self._start_export(func, filename)

    def on_export_funding_excel(self):
        """Funding-rate tablosunu Excel'e aktar."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"funding_rates_{ts}.xlsx"
        self._export_funding(self.funding_proxy, name)

    def _export_funding(self, proxy, default_name):
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Tabloyu Kaydet",
            default_name,
            "Excel Dosyaları (*.xlsx)",
        )
        if not path:
            return

        headers = [
            self.model.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.model.columnCount())
        ]

        rows = []
        for r in range(proxy.rowCount()):
            rec = {}
            for c in range(proxy.columnCount()):
                idx = proxy.index(r, c)
                rec[headers[c]] = proxy.data(idx, QtCore.Qt.DisplayRole)
            rows.append(rec)

        df = pd.DataFrame(rows, columns=headers)
        func = lambda: df.to_excel(path, index=False)
        self._start_export(func, path)

    def on_export_fr_diff_excel(self):
        """Funding rate diff tablolarını Excel'e aktar."""
        ts = datetime.now().strftime('%d%m%Y%H%M%S')
        default_name = f"FundingRateDiff {ts}.xlsx"
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Tabloyu Kaydet",
            default_name,
            "Excel Dosyaları (*.xlsx)",
        )
        if not path:
            return
        self._export_fr_diff(path)

    def _export_fr_diff(self, path: str):
        headers1 = [
            self.fr_diff_model1.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.fr_diff_model1.columnCount())
        ]
        headers2 = [
            self.fr_diff_model2.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.fr_diff_model2.columnCount())
        ]

        def collect(proxy, headers):
            rows = []
            for r in range(proxy.rowCount()):
                rec = {}
                for c in range(proxy.columnCount()):
                    idx = proxy.index(r, c)
                    rec[headers[c]] = proxy.data(idx, QtCore.Qt.DisplayRole)
                rows.append(rec)
            return pd.DataFrame(rows, columns=headers)

        df1 = collect(self.fr_diff_proxy1, headers1)
        df2 = collect(self.fr_diff_proxy2, headers2)

        def save():
            with pd.ExcelWriter(path) as writer:
                df1.to_excel(writer, sheet_name="Üst Tablo", index=False)
                df2.to_excel(writer, sheet_name="Alt Tablo", index=False)

        self._start_export(save, path)

    @QtCore.Slot()
    def on_download_db_data(self):
        start_dt = QtCore.QDateTime(self.start_date_edit.date(), self.start_time_edit.time())
        end_dt = QtCore.QDateTime(self.end_date_edit.date(), self.end_time_edit.time())

        start_str = start_dt.toString("yyyyMMdd_HHmmss")
        end_str = end_dt.toString("yyyyMMdd_HHmmss")
        default_name = f"kapanmisarbitrajlar_db_{start_str}_{end_str}.xlsx"
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Excel Olarak Kaydet",
            default_name,
            "Excel Dosyaları (*.xlsx)"
        )
        if not path:
            return

        start = start_dt.toString("yyyy-MM-ddTHH:mm:ss")
        end = end_dt.toString("yyyy-MM-ddTHH:mm:ss")

        symbols = list(self.db_symbol_dropdown.get_selected_items())
        buys = list(self.db_buy_dropdown.get_selected_items())
        sells = list(self.db_sell_dropdown.get_selected_items())
        worker = DownloadTask(start, end, path, symbols, buys, sells)
        worker.finished.connect(self._on_download_finished)
        QtCore.QThreadPool.globalInstance().start(worker)

    @QtCore.Slot(bool, str)
    def _on_download_finished(self, ok: bool, msg: str):
        if ok:
            QtWidgets.QMessageBox.information(self, "Başarılı", f"Kaydedildi:\n{msg}")
        else:
            QtWidgets.QMessageBox.critical(self, "Hata", msg)

    def _start_export(self, func, path: str):
        worker = ExportTask(func, path)
        worker.finished.connect(self._on_export_finished)
        QtCore.QThreadPool.globalInstance().start(worker)

    @QtCore.Slot(bool, str)
    def _on_export_finished(self, ok: bool, msg: str):
        if ok:
            QtWidgets.QMessageBox.information(self, "Başarılı", f"Kaydedildi:\n{msg}")
        else:
            QtWidgets.QMessageBox.critical(self, "Hata", f"Excel kaydı başarısız:\n{msg}")

    def _start_arb_timer(self):
        """Start arbitrage timer if not already running."""
        if not self._arb_timer.isActive():
            self._arb_timer.start(500)


    @QtCore.Slot()
    def on_set_duration(self):
        """UI’dan girilen saniyeyi alıp self.min_duration’a ata."""
        try:
            self.min_duration = int(self.duration_input.text())
        except ValueError:
            self.min_duration = 60
            self._start_arb_timer()


    @QtCore.Slot()
    def on_set_max_duration(self):
        """UI’dan girilen saniyeyi alıp self.max_duration’a ata."""
        try:
            self.max_duration = int(self.max_duration_input.text())
        except ValueError:
            self.max_duration = 60
            self.max_duration_input.setText(str(self.max_duration))
            self._start_arb_timer()

    @QtCore.Slot()
    def on_set_ask_fee(self):
        """Update ask side fee rate from UI."""
        try:
            self.ask_fee_rate = float(self.ask_fee_input.text())
        except ValueError:
            self.ask_fee_rate = 0.0
            self.ask_fee_input.setText(str(self.ask_fee_rate))
        globals()['FEE_RATE_BUY'] = self.ask_fee_rate
        self._start_arb_timer()

    @QtCore.Slot()
    def on_set_bid_fee(self):
        """Update bid side fee rate from UI."""
        try:
            self.bid_fee_rate = float(self.bid_fee_input.text())
        except ValueError:
            self.bid_fee_rate = 0.0
            self.bid_fee_input.setText(str(self.bid_fee_rate))
        globals()['FEE_RATE_SELL'] = self.bid_fee_rate
        self._start_arb_timer()

    def on_clear_closed(self):
        
        #Kapanan Arbitrajlar tablosunu önce Excel'e kaydeder,
        #sonra sadece 'kapanmış' olan event'leri model'den siler.
        
        # 1) Kapananları Excel'e aktar
        self.on_export_closed_excel()

        # 2) Model'den sadece 'end_dt is not None' olanları sil
        em = self.arb_model  # ArbitrajDiffModel örneği

        for row in reversed(range(len(em.events))):
            ev = em.events[row]
            if ev.end_dt is not None:
                em.remove_event(row)

    @QtCore.Slot()
    def on_synchronize(self):
        asyncio.get_event_loop().create_task(self._perform_synchronization())

    def export_all_arbitrage(self, mode_name: str):
        """Tüm açık ve kapalı arbitrajları tek dosyaya Excel olarak kaydet."""
        timestamp = datetime.now().strftime("%d%m%Y%H%M%S")
        default_name = f"{mode_name} {timestamp}.xlsx"

        headers = [
            self.arb_model.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.arb_model.columnCount())
        ]

        def proxy_to_rows(proxy):
            rows = []
            for r in range(proxy.rowCount()):
                rec = {}
                for c in range(proxy.columnCount()):
                    idx = proxy.index(r, c)
                    rec[headers[c]] = proxy.data(idx, QtCore.Qt.DisplayRole)
                rows.append(rec)
            return rows

        open_rows = proxy_to_rows(self.open_proxy)
        closed_rows = proxy_to_rows(self.closed_proxy)

        # Tablolarda hiç veri yoksa kaydetme ve kullanıcıyı bilgilendir
        if not open_rows and not closed_rows:
            QtWidgets.QMessageBox.information(
                self, "Bilgi", "Tablolar boş olduğu için Excel kaydedilmedi.")
            return
        
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Tabloyu Kaydet",
            default_name,
            "Excel Dosyaları (*.xlsx)",
        )
        if not path:
            return

        def save():
            with pd.ExcelWriter(path) as writer:
                pd.DataFrame(open_rows, columns=headers).to_excel(
                    writer, sheet_name="Açık Arbitrajlar Tablosu", index=False
                )
                pd.DataFrame(closed_rows, columns=headers).to_excel(
                    writer, sheet_name="Kapalı Arbitrajlar Tablosu", index=False
                )

            self._start_export(save, path)

    def on_switch_duration_mode(self, checked: bool):
        """Toggle between min/max duration modes and reset tables."""
        mode_name = "Max" if checked else "Min"

        # Export existing arbitrage tables before clearing
        self.export_all_arbitrage(mode_name)

        # Clear model and internal state
        self.arb_model.clear_events()
        self._arb_map.clear()

        if checked:  # Max mode
            self.duration_mode_switcher.setText("Max Süre")
            self.use_max_duration = True
            self.max_duration_input.setEnabled(True)
            self.btn_set_max_duration.setEnabled(True)
            self.duration_input.setEnabled(False)
            self.btn_set_duration.setEnabled(False)
        else:       # Min mode
            self.duration_mode_switcher.setText("Min Süre")
            self.use_max_duration = False
            self.duration_input.setEnabled(True)
            self.btn_set_duration.setEnabled(True)
            self.max_duration_input.setEnabled(False)
            self.btn_set_max_duration.setEnabled(False)

        self._start_arb_timer()

    def on_arbitrage_calculate(self):
        # 1) Eşikleri çek
        try:
            t_open = float(self.arb_threshold_input.text())
        except ValueError:
            t_open = 0.0
        try:
            t_close = float(self.arb_close_threshold_input.text())
        except ValueError:
            t_close = 0.0

        # 2) Sınıf içinde sakla
        self.arb_threshold       = t_open
        self.arb_close_threshold = t_close

        # 3) Açık-arbitraj tablosu proxy’sine bildir
        self.open_proxy.setThreshold(t_open)
        self.open_proxy.invalidateFilter()

        # 4) Kapanmış-arbitraj tablosunun filtresini de tazeleyelim
        self.closed_proxy.invalidateFilter()

        # Arbitrage hesaplamalarını başlat
        self._start_arb_timer()


    # ─── WebSocket’ten gelen verileri önce dict’e koyan metod ───
    def _enqueue_askbid(self, exchange, symbol, bid, ask, bid_qty=None, ask_qty=None):
        self._askbid_data[(exchange, symbol)] = (bid, ask, bid_qty, ask_qty)
        if not self._askbid_timer.isActive():
            self._askbid_timer.start(100)

    def _enqueue_orderbook(self, exchange: str, symbol: str,
                           bids: list[tuple[float, float]],
                           asks: list[tuple[float, float]]):
        """Store latest orderbook depths for a symbol."""
        norm = normalize_symbol(symbol)
        if not norm:
            return
        if norm not in self.orderbook_data:
            self.orderbook_data[norm] = {}
        self.orderbook_data[norm][exchange] = (bids, asks)
        self._orderbook_counter += 1

    @QtCore.Slot(str, str, float)
    def update_index_price(self, exchange: str, raw_symbol: str, price: float):
        """Update stored index price for a symbol/exchange."""
        norm = normalize_symbol(raw_symbol)
        if not norm:
            return
        if norm not in self.index_prices:
            self.index_prices[norm] = {}
        if self.index_prices[norm].get(exchange) != price:
            self.index_prices[norm][exchange] = price
            self._index_counter += 1

    # ─── ~10 Hz’de dict’i boşaltıp tabloyu güncelleyen metod ───
    def _flush_askbid_data(self):
        if not self._askbid_data:
            self._askbid_timer.stop()
            return

        for (exchange, symbol), (bid, ask, bid_qty, ask_qty) in self._askbid_data.items():
            if symbol and isinstance(symbol, str):
                self.askbid_model.update_askbid(exchange, symbol, bid, ask, bid_qty, ask_qty)
                for win in list(self.chart_windows):
                    win.add_point(exchange, symbol, bid, ask)
        self._askbid_data.clear()
        self._askbid_timer.stop()


    @QtCore.Slot()
    @QtCore.Slot(QtCore.QModelIndex)
    def _on_generate_chart(self, index: QtCore.QModelIndex | None = None):
        """Open chart window either from table click or from button."""

        # If a valid index is provided → same behaviour as before
        if isinstance(index, QtCore.QModelIndex) and index.isValid():
            proxy = self.askbid_table.model()
            src_idx = proxy.mapToSource(index)
            row = src_idx.row()
            col = src_idx.column()
            if col == 0:
                return

            exch = AB_EXCHANGES[(col-1)//2]
            sym = self.askbid_model._symbols[row]

            win = ChartWindow(sym, exch, exch, self.dark_mode)
            self.chart_windows.append(win)
            win.destroyed.connect(lambda _=None, w=win: self.chart_windows.remove(w))
            win.show()
            return

        # Otherwise use combos from chart selection page
        if not hasattr(self, "chart_symbol_combo"):
            return

        symbol = self.chart_symbol_combo.currentText()
        ask_exch = self.chart_ask_combo.currentText()
        bid_exch = self.chart_bid_combo.currentText()

        if not symbol or not ask_exch or not bid_exch:
            return

        win = ChartWindow(symbol, ask_exch, bid_exch, self.dark_mode)
        self.chart_windows.append(win)
        win.destroyed.connect(lambda _=None, w=win: self.chart_windows.remove(w))
        win.show()


    @QtCore.Slot(QtCore.QModelIndex)
    def _on_closed_chart(self, index: QtCore.QModelIndex):
        """Open chart from the closed arbitrage table when clicking Chart column."""
        if not index.isValid():
            return
        if index.column() != self.arb_model.columnCount() - 1:
            return

        proxy = self.closed_table.model()
        src_idx = proxy.mapToSource(index)
        ev = self.arb_model.events[src_idx.row()]

        win = ChartWindow(ev.symbol, ev.sell_exch, ev.buy_exch, self.dark_mode)
        self.chart_windows.append(win)
        win.destroyed.connect(lambda _=None, w=win: self.chart_windows.remove(w))
        win.show()


     
    def _start_arbitrage_worker(self):
        """Trigger background task for arbitrage calculations."""
        if self._arb_running:
            return
        
        data = self.askbid_model._data
        funding = self.current_funding
        worker = ArbitrageTask(
            data,
            funding,
            self._arb_map,
            self.ask_fee_rate,
            self.bid_fee_rate,
            self.arb_threshold,
            self.arb_close_threshold,
            self.use_max_duration,
            self.max_duration,
        )
        worker.finished.connect(self._on_arbitrage_actions)
        self._arb_running = True
        QtCore.QThreadPool.globalInstance().start(worker)

    @QtCore.Slot(object)
    def _on_arbitrage_actions(self, actions: list):
        self._arb_running = False
        now = datetime.now()
        for act in actions:
            kind = act[0]
            if kind == "expire":
                ev, key = act[1], act[2]
                row = self.arb_model.row_of(ev)
                if row >= 0:
                    self.arb_model.remove_event(row)
                self._arb_map.pop(key, None)
            elif kind == "add":
                ev, key = act[1], act[2]
                self.arb_model.add_event(ev)
                self._arb_map[key] = ev
            elif kind == "close":
                ev, key, net_rate, ask_price, bid_price = act[1:]
                self._arb_map.pop(key, None)
                ev.final_rate = net_rate
                ev.final_ask = ask_price
                ev.final_bid = bid_price
                elapsed = (now - ev.start_dt).total_seconds()
                row = self.arb_model.row_of(ev)
                if not self.use_max_duration:
                    if elapsed < self.min_duration:
                        if row >= 0:
                            self.arb_model.remove_event(row)
                    else:
                        if row >= 0:
                            self.arb_model.end_event(row)
                else:
                    if elapsed > self.max_duration:
                        if row >= 0:
                            self.arb_model.remove_event(row)
                    else:
                        if row >= 0:
                            self.arb_model.end_event(row)

    # ------------------------------------------------------------------
    # Funding Rate Diff helpers
    # ------------------------------------------------------------------
    def _compute_fr_diff_rows(
        self,
        *,
        symbols: set[str] | None = None,
        min_fr: float | None = None,
        max_fr: float | None = None,
        max_cd: int | None = None,
        max_ask: float | None = None,
        min_ask: float | None = None,
        max_bid: float | None = None,
        min_bid: float | None = None,
    ) -> list[list[str]]:
        """Collect rows matching the provided thresholds from live data.

        Parameters allow filtering by funding rate, countdown, ask depth and
        bid depth.
        """

        rows: list[list[str]] = []
        now_ts = time.time()

        for sym in self.model._symbols:
            if symbols and sym not in symbols:
                continue
            fdata = self.model._data.get(sym, {})
            adata = self.askbid_model._data.get(sym, {})
            for exch in EXCHANGES:
                rate_str = fdata.get(exch)
                if not rate_str:
                    continue
                try:
                    rate = float(rate_str)
                except ValueError:
                    continue
                if min_fr is not None and rate < min_fr:
                    continue
                if max_fr is not None and rate > max_fr:
                    continue

                ts = self.model._next_funding_ts.get((sym, exch))
                cd_sec = max(0, int(ts - now_ts)) if ts is not None else None
                if max_cd is not None and (cd_sec is None or cd_sec > max_cd):
                    continue

                if cd_sec is None:
                    cd_str = ""
                else:
                    h, rem = divmod(cd_sec, 3600)
                    m, s = divmod(rem, 60)
                    cd_str = f"{h:02d}:{m:02d}:{s:02d}"

                entry = adata.get(exch)
                if entry:
                    bid, ask, bid_qty, ask_qty = (
                        entry[0], entry[1], entry[2], entry[3]
                    )
                else:
                    bid = ask = bid_qty = ask_qty = None

                ob_entry = self.orderbook_data.get(sym, {}).get(exch)
                if ob_entry:
                    ob_bids, ob_asks = ob_entry
                    ask3_val = sum(p * q for p, q in ob_asks[:3])
                    bid3_val = sum(p * q for p, q in ob_bids[:3])
                else:
                    ask3_val = bid3_val = None

                ask_val = None
                if isinstance(ask, float) and isinstance(ask_qty, float):
                    ask_val = ask * ask_qty
                bid_val = None
                if isinstance(bid, float) and isinstance(bid_qty, float):
                    bid_val = bid * bid_qty

                if max_ask is not None and (ask_val is None or ask_val > max_ask):
                    continue
                if min_ask is not None and (ask_val is None or ask_val < min_ask):
                    continue
                if max_bid is not None and (bid_val is None or bid_val > max_bid):
                    continue
                if min_bid is not None and (bid_val is None or bid_val < min_bid):
                    continue

                idx_price = self.index_prices.get(sym, {}).get(exch)

                rows.append([
                    sym,
                    exch,
                    f"{rate:.4f}",
                    cd_str,
                    f"{ask_val:.4f}" if isinstance(ask_val, float) else "",
                    f"{ask3_val:.4f}" if isinstance(ask3_val, float) else "",
                    f"{bid_val:.4f}" if isinstance(bid_val, float) else "",
                    f"{bid3_val:.4f}" if isinstance(bid3_val, float) else "",
                    f"{idx_price:.7f}" if isinstance(idx_price, float) else "",
                ])

        return rows
    
    def _refresh_fr_diff_models(self):
        """Refresh Funding Rate Diff tables using stored parameters."""
        if not hasattr(self, "fr_diff_model1"):
            return
        
        funding_c = getattr(self.model, "_mod_counter", 0)
        askbid_c = getattr(self.askbid_model, "_mod_counter", 0)
        ob_c = self._orderbook_counter
        idx_c = self._index_counter

        if (
            funding_c == self._fr_diff_last_funding
            and askbid_c == self._fr_diff_last_askbid
            and ob_c == self._fr_diff_last_orderbook
            and idx_c == self._fr_diff_last_index
        ):
            return


        if self._fr_diff_params1:
            rows1 = self._compute_fr_diff_rows(**self._fr_diff_params1)
        else:
            rows1 = []
        self.fr_diff_model1.update_rows(rows1)

        if self._fr_diff_params2:
            rows2 = self._compute_fr_diff_rows(**self._fr_diff_params2)
        else:
            rows2 = []
        self.fr_diff_model2.update_rows(rows2)

        self._fr_diff_last_funding = funding_c
        self._fr_diff_last_askbid = askbid_c
        self._fr_diff_last_orderbook = ob_c
        self._fr_diff_last_index = idx_c

    def _reset_fr_diff_counters(self) -> None:
        """Force next refresh to recompute tables."""
        self._fr_diff_last_funding = -1
        self._fr_diff_last_askbid = -1
        self._fr_diff_last_orderbook = -1
        self._fr_diff_last_index = -1

    def _update_fr_diff_models(self):
        """Recompute both FundingRateDiffModel tables from current data."""

        if not hasattr(self, "fr_diff_model1"):
            return

        min1 = _parse_float(self.min_fr_input1a.text())
        max1 = _parse_float(self.max_fr_input1a.text())
        cd1  = _parse_duration(self.max_cd_input1a.text())
        ask1 = _parse_float(self.max_ask_depth_input1.text())
        min_ask1 = _parse_float(self.min_ask_depth_input1.text())
        bid1 = _parse_float(self.max_bid_depth_input1.text())
        min_bid1 = _parse_float(self.min_bid_depth_input1.text())

        if (
            min1 is None
            and max1 is None
            and cd1 is None
            and ask1 is None
            and min_ask1 is None
            and bid1 is None
            and min_bid1 is None
        ):
            self._fr_diff_params1 = None
        else:
            self._fr_diff_params1 = {
                "min_fr": min1,
                "max_fr": max1,
                "max_cd": cd1,
                "max_ask": ask1,
                "min_ask": min_ask1,
                "max_bid": bid1,
                "min_bid": min_bid1,
            }

        symbols2: set[str] | None = None
        if hasattr(self, "fr_symbols_dropdown2"):
            sel = self.fr_symbols_dropdown2.get_selected_items()
            if sel and len(sel) < len(self.fr_symbols_dropdown2._items):
                symbols2 = set(sel)

        min2 = _parse_float(self.min_fr_input2a.text())
        max2 = _parse_float(self.max_fr_input2a.text())
        cd2  = _parse_duration(self.max_cd_input2a.text())
        ask2 = _parse_float(self.max_ask_depth_input2.text())
        min_ask2 = _parse_float(self.min_ask_depth_input2.text())
        bid2 = _parse_float(self.max_bid_depth_input2.text())
        min_bid2 = _parse_float(self.min_bid_depth_input2.text())

        if (
            min2 is None
            and max2 is None
            and cd2 is None
            and ask2 is None
            and min_ask2 is None
            and not symbols2
            and bid2 is None
            and min_bid2 is None
        ):
            self._fr_diff_params2 = None
        else:
            self._fr_diff_params2 = {
                "symbols": symbols2,
                "min_fr": min2,
                "max_fr": max2,
                "max_cd": cd2,
                "max_ask": ask2,
                "min_ask": min_ask2,
                "max_bid": bid2,
                "min_bid": min_bid2,
            }
        
        self._reset_fr_diff_counters()
        self._refresh_fr_diff_models()
        if self._fr_diff_params1 or self._fr_diff_params2:
            self._fr_diff_timer.start()  # her durumda başlat
        else:
            self._fr_diff_timer.stop()

    async def _upload_closed_logs(self) -> bool:
        df = self._proxy_to_dataframe(self.closed_proxy)
        if df.empty:
            return True

        column_mapping = {
            "Symbol": "symbol",
            "Alım Exch": "buy_exch",
            "Satım Exch": "sell_exch",
            "Başlangıç Zamanı": "start_dt",
            "Bitiş Zamanı": "end_dt",
            "Süre": "duration",
            "İlk Ask": "initial_ask",
            "İlk Bid": "initial_bid",
            "Son Ask": "final_ask",
            "Son Bid": "final_bid",
            "Alım FR": "buy_fr",
            "Satım FR": "sell_fr",
            "Oran": "rate",
            "Son Oran": "final_rate",
            "Tekrar Sayısı": "repeat_count",
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # 2) Dict listesi oluştur
        records: list[dict] = []
        for _, row in df.iterrows():
            data = row.to_dict()
            # Numerik alanları dönüştür
            for fld in ("rate","final_rate","initial_ask","initial_bid","final_ask","final_bid","buy_fr","sell_fr"):
                try:    data[fld] = float(data[fld])
                except: data[fld] = None
            try:    data["repeat_count"] = int(data.get("repeat_count",0))
            except: data["repeat_count"] = 0

            # Tarih formatı YYYY-MM-DDTHH:MM:SS
            for col in ("start_dt","end_dt"):
                if col in data and pd.notna(data[col]):
                    ts = pd.to_datetime(data[col], format="%d/%m/%Y %H:%M:%S")
                    data[col] = ts.strftime("%Y-%m-%dT%H:%M:%S")

            records.append(data)

        if not records:
            return True

        # 3) Zaman aralığına göre DB’de varolanları çek
        earliest = min(r["start_dt"] for r in records)
        latest   = max(r["end_dt"]   for r in records if r["end_dt"])
        existing = await fetch_closed_logs(earliest, latest)
        existing_keys = {
            (r["symbol"], r["buy_exch"], r["sell_exch"], r["start_dt"])
            for r in existing
        }

        # 4) Yalnızca yeni kayıtları bırak
        records = [
            r for r in records
            if (r["symbol"], r["buy_exch"], r["sell_exch"], r["start_dt"]) not in existing_keys
        ]
        if not records:
            return True

        # 5) Progress dialog göster
        progress = QtWidgets.QProgressDialog(
            "Kapanmış arbitrajlar veritabanına yükleniyor...",
             None,
             0,
             len(records),
             self,
        )
        progress.setWindowModality(QtCore.Qt.ApplicationModal)
        progress.show()

        # 6) Batch olarak gönder
        batch_size = 500
        sent = 0
        while sent < len(records):
            batch = records[sent:sent+batch_size]
            ok = await _supabase_post("closed_arbitrage_logs", batch)
            if not ok:
                progress.close()
                return False
            sent += len(batch)
            progress.setValue(sent)

        progress.close()
        return True

    async def _perform_synchronization(self):
        ok = await self._upload_closed_logs()

        def notify():
            if ok:
                QtWidgets.QMessageBox.information(
                    self,
                    "Bilgi",
                    "Veri senkronizasyonu başarıyla tamamlandı",
                )
            else:
                QtWidgets.QMessageBox.critical(
                    self,
                    "Hata",
                    "Veri senkronizasyonu başarısız oldu",
                )

        QtCore.QTimer.singleShot(0, notify)

        if ok and hasattr(self, "db_symbol_dropdown"):
            asyncio.get_event_loop().create_task(self._refresh_db_symbols())


    def closeEvent(self, event: QtGui.QCloseEvent):
        self._really_closing = True
        for t in list(self._ws_tasks):
            t.cancel()
        self._ws_tasks.clear()
        super().closeEvent(event)



# --- WebSocket feeders ---
async def publish_binance(cb, status_cb, index_cb=None):
    url = BINANCE_URL
    # Fetch list of USDT perpetual futures to filter websocket updates
    try:
        async with aiohttp.ClientSession() as sess:
            r = await sess.get(BINANCE_REST_EXCHANGE_INFO)
            j = await r.json()
        valid_syms = {
            e["symbol"]
            for e in j.get("symbols", [])
            if e.get("contractType") == "PERPETUAL" and e.get("quoteAsset") == "USDT"
        }
    except Exception as exc:
        valid_syms = set()
        print(f"[Binance] Failed to fetch contract info: {exc}", file=sys.stderr)
    while True:
        try:
            # bağlantı kuruluyor → indicator’u yeşile çek
            async with websockets.connect(url) as ws:
                status_cb("Binance", True)
                print("[Binance] Connected")
                async for raw in ws:
                    m = json.loads(raw)
                    for u in m.get("data", []):
                        if "s" in u and "r" in u:
                            sym = u["s"]
                            if valid_syms and sym not in valid_syms:
                                continue  # ignore non-perpetual or non-USDT pairs
                            ts = u.get("T")
                            if ts is not None:
                                ts = ts / 1000
                            cb("Binance", sym, float(u["r"]) * 100, ts)
                            if index_cb is not None:
                                try:
                                    idx = float(u.get("i"))
                                except (TypeError, ValueError):
                                    idx = None
                                if idx is not None:
                                    index_cb("Binance", u["s"], idx)
        except asyncio.CancelledError:
            break
        except Exception as e:
            # bağlantı koptu → indicator’u mavi yap
            status_cb("Binance", False)
            print(f"[Binance] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


async def publish_okx(cb, status_cb, index_cb=None):
    # 1) REST’ten USDT-SWAP enstrümanları al
    async with aiohttp.ClientSession() as session:
        resp = await session.get(OKX_REST_INSTRUMENTS)
        insts_data = await resp.json()
    insts = [
        e["instId"]
        for e in insts_data.get("data", [])
        if isinstance(e.get("instId"), str) and e["instId"].endswith("-USDT-SWAP")
    ]

    sub = {
        "op": "subscribe",
        "args": [{"channel": "funding-rate", "instId": inst} for inst in insts]
    }
    index_sub = None
    if index_cb:
        index_args = []
        for inst in insts:
            base_pair = re.sub(r"-SWAP$", "", inst)
            index_args.append({"channel": "index-tickers", "instId": base_pair})
        index_sub = {"op": "subscribe", "args": index_args}

    while True:
        try:
            async with websockets.connect(OKX_WS_URL) as ws:
                # bağlantı başarıyla kuruldu
                status_cb("OKX", True)
                print("[OKX] Connected")
                await ws.send(json.dumps(sub))
                if index_sub:
                    await ws.send(json.dumps(index_sub))
                async for raw in ws:
                    m = json.loads(raw)
                    arg = m.get("arg", {})
                    channel = arg.get("channel")
                    for e in m.get("data", []):
                        if channel == "funding-rate":
                            next_ts = None
                            nft = e.get("fundingTime")
                            if nft is not None:
                                try:
                                    next_ts = int(nft) / 1000
                                except (TypeError, ValueError):
                                    next_ts = None
                            cb(
                                "OKX",
                                e["instId"],
                                float(e["fundingRate"]) * 100,
                                next_ts,
                            )
                        elif channel == "index-tickers" and index_cb:
                            price = e.get("idxPx") or e.get("indexPx")
                            sym = e.get("instId")
                            if price is not None and sym:
                                try:
                                    index_cb("OKX", sym, float(price))
                                except (TypeError, ValueError):
                                    pass
        except asyncio.CancelledError:
            break    
        except Exception as ex:
            # bağlantı koptu
            status_cb("OKX", False)
            print(f"[OKX] Error: {ex}, reconnecting in 5s")
            await asyncio.sleep(5)


async def handle_bybit_batch(syms, cb, status_cb, index_cb=None):
    sub = {"op": "subscribe", "args": [f"tickers.{s}" for s in syms]}
    while True:
        try:
            async with websockets.connect(
                BYBIT_WS_URL,
                ping_interval=20, ping_timeout=10,
                open_timeout=BYBIT_OPEN_TIMEOUT, close_timeout=BYBIT_CLOSE_TIMEOUT
            ) as ws:
                # Mark connection up
                status_cb("Bybit", True)
                print(f"[Bybit] Batch {len(syms)} connected")
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    m = json.loads(raw)
                    entries = m.get("data") or []
                    if not isinstance(entries, list):
                        entries = [entries]
                    for d in entries:
                        if "symbol" in d and "fundingRate" in d:
                            next_ts = None
                            nft = d.get("nextFundingTime")
                            if nft is not None:
                                try:
                                    next_ts = int(nft) / 1000
                                except (TypeError, ValueError):
                                    next_ts = None
                            cb("Bybit", d["symbol"], float(d["fundingRate"]) * 100, next_ts)
                            if index_cb and "indexPrice" in d:
                                try:
                                    index_cb("Bybit", d["symbol"], float(d["indexPrice"]))
                                except (TypeError, ValueError):
                                    pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            # Mark connection down
            status_cb("Bybit", False)
            print(f"[Bybit] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


async def publish_bybit(cb, status_cb, index_cb=None):
    # fetch all USDT swaps
    syms = await fetch_bybit_swaps()
    tasks = []
    for batch in [syms[i:i+BYBIT_BATCH_SIZE] for i in range(0, len(syms), BYBIT_BATCH_SIZE)]:
        tasks.append(asyncio.create_task(handle_bybit_batch(batch, cb, status_cb, index_cb)))
        await asyncio.sleep(1)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

async def handle_bitget_batch(syms, cb, status_cb, index_cb=None):
    sub = {"op":"subscribe", "args":[{"instType":"USDT-FUTURES","channel":"ticker","instId":s} for s in syms]}
    while True:
        try:
            async with websockets.connect(BITGET_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                status_cb("Bitget", True)
                print(f"[Bitget] Batch {len(syms)} connected")
                await ws.send(json.dumps(sub))
                ping_task = asyncio.create_task(_bitget_ping_loop(ws))
                try:
                    async for raw in ws:
                        data = json.loads(raw)
                        if data.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        if data.get("action") in ("snapshot", "update"):
                            next_ts = next_bitget_funding_ts()
                            for d in data.get("data", []):
                                if "instId" in d and "fundingRate" in d:
                                    cb(
                                        "Bitget",
                                        d["instId"],
                                        float(d["fundingRate"]) * 100,
                                        next_ts,
                                    )
                                if index_cb and d.get("indexPrice") is not None:
                                    try:
                                        index_cb("Bitget", d["instId"], float(d["indexPrice"]))
                                    except (TypeError, ValueError):
                                        pass
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Bitget", False)
            print(f"[Bitget] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_bitget(cb, status_cb, index_cb=None):
    syms = await fetch_bitget_swaps()
    tasks = []
    for batch in [syms[i:i+50] for i in range(0, len(syms), 50)]:
        tasks.append(asyncio.create_task(handle_bitget_batch(batch, cb, status_cb, index_cb)))
        await asyncio.sleep(1)
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def publish_gateio(cb, status_cb, index_cb=None):
    """
    Fetch Gate.io USDT‐margined futures funding‐rate updates,
    and notify status via status_cb(exchange, connected: bool).
    """
    # 1) get list of USDT contracts
    syms = await fetch_gateio_swaps()
    sub = {
        "time": int(time.time()),
        "channel": "futures.tickers",
        "event": "subscribe",
        "payload": syms
    }

    while True:
        try:
            async with websockets.connect(GATEIO_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                # connection up
                status_cb("Gateio", True)
                print("[Gateio] Connected")
                await ws.send(json.dumps(sub))

                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("event") == "update" and m.get("channel") == "futures.tickers":
                        next_ts = next_gateio_funding_ts()
                        for d in m.get("result", []):
                            if "contract" in d and "funding_rate" in d:
                                cb(
                                    "Gateio",
                                    d["contract"],
                                    float(d["funding_rate"]) * 100,
                                    next_ts,
                                )
                                if index_cb and d.get("index_price") is not None:
                                    try:
                                        index_cb("Gateio", d["contract"], float(d["index_price"]))
                                    except (TypeError, ValueError):
                                        pass
        except asyncio.CancelledError:
            break                                    
        except Exception as e:
            # connection down
            status_cb("Gateio", False)
            print(f"[Gateio] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


async def publish_binance_askbid(cb, status_cb):
    url = "wss://fstream.binance.com/ws/!bookTicker"
    # Fetch valid USDT perpetual symbols once
    valid = set(await fetch_binance_futures())
    while True:
        try:
            async with websockets.connect(url) as ws:
                status_cb("Binance", True)
                print("[Binance AskBid] Connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("e") == "bookTicker" and msg.get("s") in valid:
                        cb(
                            "Binance",
                            msg["s"],
                            float(msg["b"]),
                            float(msg["a"]),
                            float(msg.get("B", 0)),
                            float(msg.get("A", 0)),
                        )
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Binance", False)
            print(f"[Binance AskBid] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

# --- OKX Ask/Bid Feeder ---
async def publish_okx_askbid(cb, status_cb):
    # 1) REST’ten USDT-SWAP enstrümanları al
    async with aiohttp.ClientSession() as sess:
        r = await sess.get(OKX_REST_INSTRUMENTS)
        j = await r.json()
    insts = [
        e["instId"]
        for e in j.get("data", [])
        if isinstance(e.get("instId"), str) and e["instId"].endswith("-USDT-SWAP")
    ]

    # 2) books5 kanalına abone ol
    sub = {
        "op": "subscribe",
        "args": [{"channel": "books5", "instId": inst} for inst in insts]
    }

    while True:
        try:
            async with websockets.connect(OKX_WS_URL) as ws:
                status_cb("OKX", True)
                print("[OKX AskBid] Connected")
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    m = json.loads(raw)
                    arg = m.get("arg", {})
                    if arg.get("channel") == "books5":
                        for entry in m.get("data", []):
                            instId = entry.get("instId")
                            bids = entry.get("bids", [])
                            asks = entry.get("asks", [])
                            if not bids or not asks:
                                continue
                            bid_price = float(bids[0][0])
                            bid_qty = float(bids[0][1])
                            ask_price = float(asks[0][0])
                            ask_qty = float(asks[0][1])
                            cb("OKX", instId, bid_price, ask_price, bid_qty, ask_qty)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("OKX", False)
            print(f"[OKX AskBid] Error: {e}, reconnect in 5s")
            await asyncio.sleep(5)



# --- Bybit Ask/Bid Feeder (v5 Public Linear/USDT Futures) ---
async def publish_bybit_askbid(cb, status_cb):
    # 1) REST’ten USDT swap sembollerini al
    syms = await fetch_bybit_swaps()

    # 2) Geçerli topic string’lerini oluştur: "orderbook.1.SYMBOL"
    topics = [f"orderbook.1.{normalize_pair(s)}" for s in syms]
    sub = {"op": "subscribe", "args": topics}

    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL) as ws:
                status_cb("Bybit", True)
                print("[Bybit AskBid] Connected")
                await ws.send(json.dumps(sub))

                async for raw in ws:
                    m = json.loads(raw)
                    topic = m.get("topic", "")
                    if topic.startswith("orderbook.1."):
                        data = m.get("data", {})
                        sym = data.get("s")
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if not bids or not asks:
                            continue
                        bid_price = float(bids[0][0])
                        bid_qty = float(bids[0][1])
                        ask_price = float(asks[0][0])
                        ask_qty = float(asks[0][1])
                        cb("Bybit", sym, bid_price, ask_price, bid_qty, ask_qty)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Bybit", False)
            print(f"[Bybit AskBid] Error: {e}, reconnect in 5s")
            await asyncio.sleep(5)




# --- Bitget Ask/Bid Feeder (books1, top-of-book) ---
# --- Bitget Ask/Bid feeder ---
async def publish_bitget_askbid(cb, status_cb):
    # 1) grab USDT-FUTURES contract list
    syms = await fetch_bitget_swaps()

    # 2) build the subscribe message
    sub = {
        "op": "subscribe",
        "args": [
            {"instType": "USDT-FUTURES", "channel": "ticker", "instId": s}
            for s in syms
        ]
    }

    while True:
        try:
            async with websockets.connect(BITGET_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                status_cb("Bitget", True)
                print("[Bitget AskBid] Connected")
                await ws.send(json.dumps(sub))

                ping_task = asyncio.create_task(_bitget_ping_loop(ws))
                try:
                    async for raw in ws:
                        m = json.loads(raw)
                        if m.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        if m.get("action") in ("snapshot", "update"):
                            for d in m.get("data", []):
                                inst = d.get("instId")
                                bid  = d.get("bidPr")
                                ask  = d.get("askPr")
                                bid_sz = d.get("bidSz")
                                ask_sz = d.get("askSz")
                                if inst and bid is not None and ask is not None:
                                    cb(
                                        "Bitget",
                                        inst,
                                        float(bid),
                                        float(ask),
                                        float(bid_sz) if bid_sz is not None else None,
                                        float(ask_sz) if ask_sz is not None else None,
                                    )
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Bitget", False)
            print(f"[Bitget AskBid] Error: {e}, reconnect in 5s")
            await asyncio.sleep(5)



# --- GATEIO Ask/Bid feeder ---
async def publish_gateio_askbid(cb, status_cb):
    """
    Subscribe to Gate.io USDT-margined futures best-ask/bid (futures.book_ticker).
    Docs: https://www.gate.io/docs/developers/futures/ws/en/#best-ask-bid-subscription
    """
    # 1) fetch the list of USDT contracts
    syms = await fetch_gateio_swaps()
    sub = {
        "time": int(time.time()),
        "channel": "futures.book_ticker",
        "event": "subscribe",
        "payload": syms
    }

    url = GATEIO_WS_URL
    while True:
        try:
            async with websockets.connect(url) as ws:
                status_cb("Gateio", True)
                print("[Gateio AskBid] Connected")
                await ws.send(json.dumps(sub))

                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("channel") == "futures.book_ticker" and m.get("event") == "update":
                        r = m["result"]
                        cb(
                            "Gateio",
                            r["s"],
                            float(r["b"]),
                            float(r["a"]),
                            float(r.get("B", 0)),
                            float(r.get("A", 0)),
                        )
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Gateio", False)
            print(f"[Gateio AskBid] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_binance_orderbook(symbols: list[str], cb, status_cb):
    streams = '/'.join(f"{s.lower()}@depth5@100ms" for s in symbols)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(url) as ws:
                status_cb("Binance", True)
                print("[Binance Orderbook] Connected")
                async for raw in ws:
                    m = json.loads(raw)
                    data = m.get("data", {})
                    sym = data.get("s")
                    if not sym:
                        continue
                    bids = [(float(p), float(q)) for p, q in data.get("b", [])[:3]]
                    asks = [(float(p), float(q)) for p, q in data.get("a", [])[:3]]
                    cb(sym, bids, asks)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Binance", False)
            print(f"[Binance Orderbook] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_okx_orderbook(symbols: list[str], cb, status_cb):
    sub = {
        "op": "subscribe",
        "args": [{"channel": "books5", "instId": s} for s in symbols],
    }
    while True:
        try:
            async with websockets.connect(OKX_WS_URL) as ws:
                status_cb("OKX", True)
                print("[OKX Orderbook] Connected")
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    m = json.loads(raw)
                    arg = m.get("arg", {})
                    if arg.get("channel") != "books5":
                        continue
                    for entry in m.get("data", []):
                        bids = [
                            (float(level[0]), float(level[1]))
                            for level in entry.get("bids", [])[:3]
                        ]
                        asks = [
                            (float(level[0]), float(level[1]))
                            for level in entry.get("asks", [])[:3]
                        ]
                        inst = entry.get("instId")
                        if inst:
                            cb(inst, bids, asks)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("OKX", False)
            print(f"[OKX Orderbook] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_bybit_orderbook(symbols: list[str], cb, status_cb):
    topics = [f"orderbook.50.{normalize_pair(s)}" for s in symbols]
    sub = {"op": "subscribe", "args": topics}
    while True:
        try:
            async with websockets.connect(BYBIT_WS_URL) as ws:
                status_cb("Bybit", True)
                print("[Bybit Orderbook] Connected")
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    m = json.loads(raw)
                    topic = m.get("topic", "")
                    if not topic.startswith("orderbook.50."):
                        continue
                    data = m.get("data", {})
                    sym = data.get("s")
                    if not sym:
                        continue
                    bids = [
                        (float(p), float(q)) for p, q in data.get("b", [])[:3]
                    ]
                    asks = [
                        (float(p), float(q)) for p, q in data.get("a", [])[:3]
                    ]
                    cb(sym, bids, asks)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Bybit", False)
            print(f"[Bybit Orderbook] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_bitget_orderbook(symbols: list[str], cb, status_cb):
    sub = {
        "op": "subscribe",
        "args": [
            {"instType": "USDT-FUTURES", "channel": "books5", "instId": s}
            for s in symbols
        ],
    }
    while True:
        try:
            async with websockets.connect(BITGET_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                status_cb("Bitget", True)
                print("[Bitget Orderbook] Connected")
                await ws.send(json.dumps(sub))
                ping_task = asyncio.create_task(_bitget_ping_loop(ws))
                try:
                    async for raw in ws:
                        if DEBUG_BITGET_ORDERBOOK:
                            print(f"[Bitget Orderbook Raw] {raw}")
                        m = json.loads(raw)
                        if m.get("op") == "ping":
                            await ws.send(json.dumps({"op": "pong"}))
                            continue
                        if m.get("action") not in ("snapshot", "update"):
                            continue
                        inst = None
                        for entry in m.get("data", []):
                            inst = entry.get("instId") or m.get("arg", {}).get("instId")
                            if not inst:
                                continue
                            bids = [
                                (float(b[0]), float(b[1]))
                                for b in entry.get("bids", [])[:3]
                            ]
                            asks = [
                                (float(a[0]), float(a[1]))
                                for a in entry.get("asks", [])[:3]
                            ]
                            cb(inst, bids, asks)
                finally:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
        except asyncio.CancelledError:
            break
        except InvalidStatusCode as e:
            status_cb("Bitget", False)
            print(f"[Bitget Orderbook] Handshake failed: {e.status_code}")
            if e.headers:
                print(f"[Bitget Orderbook] Response headers: {dict(e.headers)}")
            await asyncio.sleep(5)
        except Exception as e:
            status_cb("Bitget", False)
            print(f"[Bitget Orderbook] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_gateio_orderbook(symbols: list[str], cb, status_cb):
    url = GATEIO_WS_URL
    while True:
        try:
            async with websockets.connect(url) as ws:
                status_cb("Gateio", True)
                print("[Gateio Orderbook] Connected")
                for sym in symbols:
                    sub = {
                        "time": int(time.time()),
                        "channel": "futures.order_book",
                        "event": "subscribe",
                        # Gateio expects all numeric values as strings
                        # [contract, depth, interval]
                        "payload": [sym, str(5), "0"],
                    }
                    await ws.send(json.dumps(sub))
                async for raw in ws:
                    m = json.loads(raw)
                
                    if m.get("channel") == "futures.order_book" and m.get("event") in ("update", "snapshot", "all"):
                        r = m.get("result") or {}
                        sym = r.get("s") or r.get("contract")
                        bids = []
                        for item in r.get("bids", []):
                            if isinstance(item, dict):
                                price = item.get("p")
                                size = item.get("s")
                            else:
                                price, size = item[0], item[1]
                            try:
                                bids.append((float(price), float(size)))
                            except (TypeError, ValueError):
                                continue
                            if len(bids) == 3:
                                break

                        asks = []
                        for item in r.get("asks", []):
                            if isinstance(item, dict):
                                price = item.get("p")
                                size = item.get("s")
                            else:
                                price, size = item[0], item[1]
                            try:
                                asks.append((float(price), float(size)))
                            except (TypeError, ValueError):
                                continue
                            if len(asks) == 3:
                                break
                        if sym:
                            cb(sym, bids, asks)
        except asyncio.CancelledError:
            break
        except Exception as e:
            status_cb("Gateio", False)
            print(f"[Gateio Orderbook] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


# --- Application entrypoint ---
def main():
    app = QtWidgets.QApplication(sys.argv)

    # 🔐 Lisans kontrolü
    ok, msg = run_license_check()
    if not ok:
        QMessageBox.critical(None, "Lisans Hatası", msg)
        sys.exit(1)

    loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(loop)

    # Splash ekranı (görselin yolunu ayarla)
    pixmap_path = resource_path("splash.png")
    splash_pix = QtGui.QPixmap(pixmap_path)
    print("Splash is null?", splash_pix.isNull())  # Kontrol için
    splash = QtWidgets.QSplashScreen(splash_pix, QtCore.Qt.WindowStaysOnTopHint)
    splash.setMask(splash_pix.mask())
    splash.show()
    # Splash ekranını 5 saniye göster
    QtCore.QTimer.singleShot(5000, splash.close)  # 5 saniye sonra kapanır

    # MainWindow 5 saniye sonra gösterilsin
    def show_main():
        window = MainWindow()
        window.showMaximized()

        # → Funding-rate geldiğinde hem modele hem current_funding’e yazacak callback
        def fr_cb(exchange: str, raw_symbol: str, rate_pct: float, next_ts=None):
            # 1) Funding Rate Live tablosunu güncelle
            window.model.update_rate(exchange, raw_symbol, rate_pct, next_ts)

            # 2) normalize edilmiş sembolü al
            norm_sym = normalize_symbol(raw_symbol)
            if norm_sym is None:
                return

            # 3) current_funding sözlüğünü güncelle
            window.current_funding[(exchange, norm_sym)] = rate_pct

            # 4) açık arbitraj fırsatlarını (end_dt is None) tara
            for ev in window.arb_model.events:
                if ev.end_dt is not None:
                    continue
                if ev.symbol != norm_sym:
                    continue

                # hangi sütunu güncelleyeceğimizi belirle
                if exchange == ev.buy_exch:
                    ev.buy_fr = rate_pct
                    col = 12   # “Alım FR” sütun indeksi
                elif exchange == ev.sell_exch:
                    ev.sell_fr = rate_pct
                    col = 13   # “Satım FR” sütun indeksi
                else:
                    continue

                # dataChanged sinyali fırlat
                row = window.arb_model.row_of(ev)
                idx = window.arb_model.index(row, col)
                window.arb_model.dataChanged.emit(idx, idx, [QtCore.Qt.DisplayRole])


        # WebSocket’leri sarılmış callback ile başlat
        window._ws_tasks.append(loop.create_task(publish_binance (fr_cb, window._update_status_fr, window.update_index_price)))
        window._ws_tasks.append(loop.create_task(publish_okx     (fr_cb, window._update_status_fr, window.update_index_price)))
        window._ws_tasks.append(loop.create_task(publish_bybit   (fr_cb, window._update_status_fr, window.update_index_price)))
        window._ws_tasks.append(loop.create_task(publish_bitget  (fr_cb, window._update_status_fr, window.update_index_price)))
        window._ws_tasks.append(loop.create_task(publish_gateio  (fr_cb, window._update_status_fr, window.update_index_price)))

        # Start ask/bid feeds immediately but keep the tab closed
        window.start_askbid_feeds()
        loop.create_task(window.start_orderbook_feeds())

    QtCore.QTimer.singleShot(5000, show_main)

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        loop.run_until_complete(close_all_supabase_sessions())
        loop.stop()
        loop.close()
        QtWidgets.QApplication.quit()
        sys.exit(0)
    

if __name__ == "__main__":
    main()

