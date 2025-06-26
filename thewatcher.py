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
from websockets.exceptions import ConnectionClosedError

from PySide6 import QtWidgets, QtCore, QtGui, QtCharts
from PySide6.QtWidgets import QHeaderView
from PySide6.QtCharts import (QChart, QChartView, QLineSeries, QDateTimeAxis, QValueAxis)
from PySide6.QtWidgets import QGraphicsSimpleTextItem
import qasync
import random
import matplotlib


def resource_path(relative_path):
    """EXE içinden splash.png yolunu çözer"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

# --- Constants & Endpoints ---
EXCHANGES = ["Binance", "OKX", "Bybit", "Bitget", "Gateio"]
# Sadece Binance için Ask/Bid kolonları
AB_EXCHANGES = ["Binance", "OKX", "Bybit", "Bitget", "Gateio"]


BINANCE_URL            = "wss://fstream.binance.com/stream?streams=!markPrice@arr"
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


# --- Helper: normalize for subscription endpoints ---
def normalize_pair(pair: str) -> str:
    s = pair.upper()
    s = re.sub(r'-SWAP$', '', s)
    s = re.sub(r'(-PERP|PERP)$', '', s)
    return s.replace('-', '')


# --- Symbol normalization & USDT-only filter for UI ---
def normalize_symbol(sym: str) -> str | None:
    s = sym.upper()
    # strip swap/perp suffixes
    s = re.sub(r'(-SWAP|SWAP|-PERP|PERP)$', '', s)
    # remove hyphens
    s = s.replace('-', '')
    # must end with USDT
    if not s.endswith("USDT"):
        return None
    # strip leading digits
    s = re.sub(r'^\d+', '', s)
    # drop if still ends in digits
    if re.search(r'\d+$', s):
        return None
    # keep only alphanumeric
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s or None


# --- Flash Delegate for red/green animation ---
class FlashDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._flash_cells: dict[tuple[int,int], tuple[float,bool]] = {}
        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._on_timeout)
        self._timer.start(50)
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
        if view:
            view.viewport().update()


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

    def rowCount(self, parent=None):
        return len(self.events)

    def columnCount(self, parent=None):
        # eskiden 14: şimdi 15 sütun
        return 15

    def headerData(self, section, orientation, role):
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return [
                "Symbol", "Alım Exch", "Satım Exch", "Oran",
                "Başlangıç Zamanı", "Bitiş Zamanı", "Süre", "Son Oran",
                "İlk Ask", "İlk Bid", "Son Ask", "Son Bid",
                "Alım FR", "Satım FR",
                "Tekrar Sayısı"
            ][section]
        return None

    def add_event(self, ev: ArbitrajEvent):
        self.beginInsertRows(QtCore.QModelIndex(), len(self.events), len(self.events))
        self.events.append(ev)
        self.endInsertRows()
        self.symbolsUpdated.emit(sorted({e.symbol for e in self.events}))

    def remove_event(self, row: int):
        # bu metot mutlaka burada olmalı
        self.beginRemoveRows(QtCore.QModelIndex(), row, row)
        self.events.pop(row)
        self.endRemoveRows()
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

async def fetch_gateio_swaps() -> list[str]:
    async with aiohttp.ClientSession() as s:
        r = await s.get(GATEIO_REST_TICKERS)
        j = await r.json()
    return [
        e["contract"]
        for e in j
        if isinstance(e.get("contract"), str)
    ]


# --- Qt Table Model ---
class FundingTableModel(QtCore.QAbstractTableModel):
    symbolsUpdated = QtCore.Signal(list)  # Emit when symbols list changes
    
    def __init__(self):
        super().__init__()
        self._symbols  = []                     # list of normalized symbols
        self._data     = {}                     # symbol -> {exchange: str}
        self._previous = {}                     # (symbol,exchange) -> float (rounded to 4dp)
        self.delegate  = None

    def rowCount(self, parent=QtCore.QModelIndex()):
        return len(self._symbols)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 1 + len(EXCHANGES)

    def headerData(self, section, orientation, role):
        if role == QtCore.Qt.DisplayRole and orientation == QtCore.Qt.Horizontal:
            return "Symbol" if section == 0 else EXCHANGES[section-1]
        return None

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if role != QtCore.Qt.DisplayRole:
            return None
        r, c = index.row(), index.column()
        sym = self._symbols[r]
        if c == 0:
            return sym
        exch = EXCHANGES[c-1]
        return self._data.get(sym, {}).get(exch, "")

    @QtCore.Slot(str, str, float)
    def update_rate(self, exchange: str, raw_symbol: str, rate_pct: float):
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
        # insert new row if needed
        if sym not in self._data:
            self.beginInsertRows(QtCore.QModelIndex(),
                                 len(self._symbols),
                                 len(self._symbols))
            self._symbols.append(sym)
            self._data[sym] = {}
            self.endInsertRows()
            
            # Emit signal for new symbol
            self.symbolsUpdated.emit(self._symbols.copy())

        # update value
        self._data[sym][exchange] = rate_str
        row = self._symbols.index(sym)
        col = EXCHANGES.index(exchange) + 1
        src_idx = self.index(row, col)
        # update the source model cell
        self.dataChanged.emit(src_idx, src_idx, [QtCore.Qt.DisplayRole])

        # flash via proxy index so empty cells don't flash
        if self.delegate and changed:
            view = self.delegate.parent()
            proxy = view.model()
            view_idx = proxy.mapFromSource(src_idx)
            self.delegate.mark_changed(view_idx, positive)


class AskBidTableModel(QtCore.QAbstractTableModel):
    symbolsUpdated = QtCore.Signal(list)

    def __init__(self):
        super().__init__()
        self._symbols = []           # sıra ile eklenen semboller
        self._data    = {}           # { sym: { exch: (bid, ask) } }
        self._prev    = {}           # { (sym, exch, side): float } , side in {"bid","ask"}
        self.delegate = None

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
        bid, ask = self._data.get(sym, {}).get(exch, ("",""))
        val = ask if c%2==1 else bid
        return f"{val:.8f}" if isinstance(val, float) else val

    @QtCore.Slot(str, str, float, float)
    def update_askbid(self, exchange: str, raw_symbol: str, bid: float, ask: float):
        # 1) Symbol normalize
        if exchange not in AB_EXCHANGES:
            return
        sym = normalize_symbol(raw_symbol)
        if not sym:
            return

        # 2) Yeni sembol ekle
        if sym not in self._data:
            self.beginInsertRows(QtCore.QModelIndex(),
                                 len(self._symbols),
                                 len(self._symbols))
            self._symbols.append(sym)
            self._data[sym] = {}
            self.endInsertRows()
            self.symbolsUpdated.emit(self._symbols.copy())

        # 3) Önceki değerleri al
        key_bid = (sym, exchange, "bid")
        key_ask = (sym, exchange, "ask")
        prev_bid = self._prev.get(key_bid)
        prev_ask = self._prev.get(key_ask)

        # 4) Yeni değerleri sakla
        self._prev[key_bid] = bid
        self._prev[key_ask] = ask
        self._data[sym][exchange] = (bid, ask)

        # 5) Hücreyi güncelle
        row = self._symbols.index(sym)
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

        def __init__(self, symbol: str, ask_exchange: str, bid_exchange: str):
            super().__init__()
            self.symbol = symbol
            self.ask_exchange = ask_exchange
            self.bid_exchange = bid_exchange
            self.setWindowTitle(f"{ask_exchange} Ask / {bid_exchange} Bid - {symbol}")
            self.resize(800, 600)


            self.ask_series = QtCharts.QLineSeries()
            self.bid_series = QtCharts.QLineSeries()
            self.ask_series.setName(f"Ask ({ask_exchange})")
            self.bid_series.setName(f"Bid ({bid_exchange})")
            ask_pen = QtGui.QPen(QtGui.QColor("blue").lighter(150))
            ask_pen.setWidth(3)
            self.ask_series.setPen(ask_pen)

            bid_pen = QtGui.QPen(QtGui.QColor("red").lighter(150))
            bid_pen.setWidth(3)
            self.bid_series.setPen(bid_pen)


            self.chart = QtCharts.QChart()
            self.chart.setTheme(QtCharts.QChart.ChartThemeDark)
            self.chart.addSeries(self.ask_series)
            self.chart.addSeries(self.bid_series)
            self.chart.legend().setVisible(True)
            self.chart.setTitle("Ask Price: - | Bid Price: -")

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
            self.chart.setBackgroundBrush(QtGui.QColor(30, 30, 30))
            self.setCentralWidget(self.view)

            # Labels to display latest prices
            self.ask_label = QtWidgets.QGraphicsSimpleTextItem(self.chart)
            self.bid_label = QtWidgets.QGraphicsSimpleTextItem(self.chart)
            self.ask_label.setBrush(QtGui.QBrush(QtGui.QColor("white")))
            self.bid_label.setBrush(QtGui.QBrush(QtGui.QColor("white")))

            self._start_ms = None
            self._ask_price = None
            self._bid_price = None

        def add_point(self, exchange: str, symbol: str, bid: float, ask: float):
            """Append a bid/ask point if the normalized symbol matches."""
            norm = normalize_symbol(symbol)
            if norm != self.symbol:
                return

            ts = QtCore.QDateTime.currentDateTime()
            x = ts.toMSecsSinceEpoch()
            if self._start_ms is None:
                self._start_ms = x

            updated = False

            if exchange == self.ask_exchange:
                self.ask_series.append(x, ask)
                self.ask_label.setText(f"{ask}")
                p = self.chart.mapToPosition(QtCore.QPointF(x, ask), self.ask_series)
                self.ask_label.setPos(p)
                self._ask_price = ask
                updated = True

            if exchange == self.bid_exchange:
                self.bid_series.append(x, bid)
                self.bid_label.setText(f"{bid}")
                p = self.chart.mapToPosition(QtCore.QPointF(x, bid), self.bid_series)
                self.bid_label.setPos(p)
                self._bid_price = bid
                updated = True

            if not updated:
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
            self.chart.setTitle(f"Ask Price: {title_ask} | Bid Price: {title_bid}")

            self.chart.update()

            title_ask = f"{self._ask_price}" if self._ask_price is not None else "-"
            title_bid = f"{self._bid_price}" if self._bid_price is not None else "-"
            self.chart.setTitle(f"Ask Price: {title_ask} | Bid Price: {title_bid}")

# --- Main Window ---
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        self.min_duration = 60
        self.max_duration = 60
        self.use_max_duration = False
        self.dark_mode = True   # Uygulama açıldığında önce dark mod aktif olsun
        super().__init__()
        self.setWindowTitle("Funding & Order Book Dashboard")
        self.resize(900, 600)

        self._arb_map = {}
        # Her exchange’in en son funding‐rate’ini saklayacak dict
        self.current_funding: dict[tuple[str,str], float] = {}
        # Açılan grafik pencerelerini tut
        self.chart_windows: list[ChartWindow] = []

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
        self.btn_order        = QtWidgets.QPushButton("Ask/Bid - Order Book")
        self.btn_arb          = QtWidgets.QPushButton("Arbitraj Diff")
        self.btn_chart        = QtWidgets.QPushButton("Chart Selection")

        # 1) Toggle butonunu oluştur, checkable yap
        self.btn_toggle_theme = QtWidgets.QPushButton("Light Mode")
        self.btn_toggle_theme.setCheckable(True)
        self.btn_toggle_theme.toggled.connect(self.toggle_theme)


        btn_layout.addWidget(self.btn_funding)
        btn_layout.addWidget(self.btn_order)
        btn_layout.addWidget(self.btn_arb)
        btn_layout.addWidget(self.btn_chart)

        # 3) Sağa itmek için stretch, ardından toggle butonunu ekle
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_toggle_theme)
        

        # 4) Layout’u pencerenin ana dikey yerleşimine (vlayout) son olarak ekle
        vlayout.addLayout(btn_layout)

        # 5) Toggle sinyalini metoda bağla
        self.btn_toggle_theme.toggled.connect(self.toggle_theme)


        self.btn_funding.clicked.connect(self.open_funding_tab)
        self.btn_order.clicked.connect(self.open_askbid_tab)
        self.btn_arb.clicked.connect(self.open_arbitrage_tab)
        self.btn_chart.clicked.connect(self.open_chart_tab)
        


        # TabWidget
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.setDocumentMode(True)
        self.tabs.setTabsClosable(True)
        self.tabs.setMovable(True)
        self.tabs.tabCloseRequested.connect(lambda i: self.tabs.removeTab(i))
        vlayout.addWidget(self.tabs, stretch=1)

        # Açılışta sekmeleri hazırla
        self.open_funding_tab()
        self.open_askbid_tab()
        self.open_arbitrage_tab()

        # Arbitraj Hesapla butonu
        self.btn_arbitrage_calc.clicked.connect(self.on_arbitrage_calculate)

        # İlk timer tetiklenmeden önce eşikleri hesapla
        self.on_arbitrage_calculate()

        # Ask/Bid flush timer (~30 Hz)
        self._askbid_data = {}
        self._askbid_timer = QtCore.QTimer(self)
        self._askbid_timer.timeout.connect(self._flush_askbid_data)
        self._askbid_timer.start(33)

        # Arbitrage işlemleri timer (500 ms)
        self._arb_timer = QtCore.QTimer(self)
        self._arb_timer.timeout.connect(self.process_arbitrage)
        self._arb_timer.start(500)


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
        style = (
            "QTableView { background-color: #2b2b2b; color: white; gridline-color: white; }"
            if self.dark_mode
            else
            "QTableView { background-color: white; color: black; gridline-color: black; }"
        )

        # 4) Tüm tabloları aynı anda güncelle
        for tbl in (self.table, self.askbid_table, self.open_table, self.closed_table):
            tbl.setShowGrid(True)
            tbl.setStyleSheet(style)

    # Funding tablosu hazırlayan metod
    def _setup_funding_table(self):
        self.model = FundingTableModel()
        self.funding_proxy = SymbolFilterProxyModel()
        self.funding_proxy.setSourceModel(self.model)
        self.funding_proxy.setDynamicSortFilter(True)

        self.table = QtWidgets.QTableView()
        self.table.setShowGrid(True)
        self.table.setStyleSheet("QTableView { background-color: #2b2b2b; color: white; gridline-color: white; }")
        self.table.setModel(self.funding_proxy)

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

    def _update_dropdown_items(self, symbols: list[str]):
        self.symbol_dropdown.set_items(symbols)

    # Ask/Bid tablosu hazırlayan metod
    def _setup_askbid_table(self):
        from PySide6.QtWidgets import QHeaderView
        from PySide6.QtCore    import Qt

        self.askbid_model = AskBidTableModel()
        askbid_proxy = SymbolFilterProxyModel()
        askbid_proxy.setSourceModel(self.askbid_model)
        askbid_proxy.setDynamicSortFilter(True)

        self.askbid_table = QtWidgets.QTableView()
        self.askbid_table.setShowGrid(True)
        self.askbid_table.setStyleSheet("QTableView { background-color: #2b2b2b; color: white; gridline-color: white; }")
        self.askbid_table.setModel(askbid_proxy)

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
        self.askbid_dropdown.selectionChanged.connect(askbid_proxy.set_symbol_filter)

        # Double-click to open chart
        self.askbid_table.doubleClicked.connect(self._on_generate_chart)

    def _update_askbid_dropdown(self, symbols: list[str]):
        self.askbid_dropdown.set_items(symbols)

    # Funding sekmesini aç
    def open_funding_tab(self):
        for idx in range(self.tabs.count()):
            if self.tabs.tabText(idx) == "Funding Rate Live":
                self.tabs.setCurrentIndex(idx)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(0,0,0,0)

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
            if self.tabs.tabText(idx) == "Ask/Bid - Order Book":
                self.tabs.setCurrentIndex(idx)
                return

        page = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(page)
        layout.setContentsMargins(0,0,0,0)

        self._setup_askbid_table()

        anim_checkbox = QtWidgets.QCheckBox("Hücre Animasyonlarını Aç")
        anim_checkbox.setChecked(True)
        anim_checkbox.toggled.connect(self.askbid_delegate.setEnabled)
        layout.addWidget(anim_checkbox)

        layout.addWidget(self.askbid_table)
        self.tabs.addTab(page, "Ask/Bid - Order Book")
        self.tabs.setCurrentWidget(page)

        if not hasattr(self, "_askbid_task_scheduled"):
            loop = asyncio.get_event_loop()
            loop.create_task(publish_binance_askbid(self._enqueue_askbid, self._update_status_ab))
            loop.create_task(publish_okx_askbid(self._enqueue_askbid, self._update_status_ab))
            loop.create_task(publish_bybit_askbid(self._enqueue_askbid, self._update_status_ab))
            loop.create_task(publish_bitget_askbid(self._enqueue_askbid, self._update_status_ab))
            loop.create_task(publish_gateio_askbid(self._enqueue_askbid, self._update_status_ab))
            self._askbid_task_scheduled = True

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
        btn_set_duration    = QtWidgets.QPushButton("Getir")
        self.max_duration_input = QtWidgets.QLineEdit(str(self.max_duration))
        self.max_duration_input.setFixedWidth(60)
        btn_set_max_duration = QtWidgets.QPushButton("Getir")
        # Dropdown yerine switch kullanılacak
        self.duration_mode_switcher = QtWidgets.QPushButton("Min Süre")
        self.duration_mode_switcher.setCheckable(True)
        # Başlangıçta min süre kullanılacağı için checked=False
        self.duration_mode_switcher.setChecked(False)
        btn_export_open     = QtWidgets.QPushButton("Açıkları Excel'e Aktar")
        btn_export_closed   = QtWidgets.QPushButton("Kapananları Excel'e Aktar")

        # İşte buraya eklemelisiniz:
        btn_clear_closed    = QtWidgets.QPushButton("Temizle")

        top.addWidget(QtWidgets.QLabel("Açılış Eşik:"));    top.addWidget(self.arb_threshold_input)
        top.addWidget(QtWidgets.QLabel("Kapanış Eşik:"));   top.addWidget(self.arb_close_threshold_input)
        top.addWidget(self.btn_arbitrage_calc)
        top.addWidget(QtWidgets.QLabel("Min Süre (s):"))
        top.addWidget(self.duration_input)
        top.addWidget(btn_set_duration)
        top.addWidget(QtWidgets.QLabel("Max Süre (s):"))
        top.addWidget(self.max_duration_input)
        top.addWidget(btn_set_max_duration)
        top.addWidget(self.duration_mode_switcher)
        top.addWidget(btn_export_open);  top.addWidget(btn_export_closed)
        top.addWidget(btn_clear_closed)
        top.addStretch()
        layout.addLayout(top)

        btn_set_duration.clicked.connect(self.on_set_duration)
        btn_set_max_duration.clicked.connect(self.on_set_max_duration)
        self.duration_mode_switcher.toggled.connect(self.on_switch_duration_mode)
        

        # Ortak model
        self.arb_model = ArbitrajDiffModel()

        # Açık Arbitrajlar Tablosu
        open_box = QtWidgets.QGroupBox("Açık Arbitrajlar")
        v_open   = QtWidgets.QVBoxLayout(open_box)
        self.open_table = QtWidgets.QTableView()
        self.open_table.setShowGrid(True)
        self.open_table.setStyleSheet("QTableView { background-color: #2b2b2b; color: white; gridline-color: white; }")
        

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

        v_open.addWidget(self.open_table)
        layout.addWidget(open_box)

        # Kapanmış Arbitrajlar Tablosu
        closed_box = QtWidgets.QGroupBox("Kapanmış Arbitrajlar")
        v_closed   = QtWidgets.QVBoxLayout(closed_box)
        self.closed_table = QtWidgets.QTableView()
        self.closed_table.setShowGrid(True)
        self.closed_table.setStyleSheet("QTableView { background-color: #2b2b2b; color: white; gridline-color: white; }")

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
        
    

        # 2) Başlıkları al
        headers = [
            self.arb_model.headerData(c, QtCore.Qt.Horizontal, QtCore.Qt.DisplayRole)
            for c in range(self.arb_model.columnCount())
        ]

        # 3) Proxy üzerinden satırları oku
        rows = []
        for r in range(proxy.rowCount()):
            rec = {}
            for c in range(proxy.columnCount()):
                idx = proxy.index(r, c)
                rec[headers[c]] = proxy.data(idx, QtCore.Qt.DisplayRole)
            rows.append(rec)

        # 4) DataFrame'e dönüştür ve kaydet
        df = pd.DataFrame(rows, columns=headers)
        try:
            df.to_excel(path, index=False)
            QtWidgets.QMessageBox.information(self, "Başarılı", f"Kaydedildi:\n{path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Hata", f"Excel kaydı başarısız:\n{e}")

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
        try:
            df.to_excel(path, index=False)
            QtWidgets.QMessageBox.information(self, "Başarılı", f"Kaydedildi:\n{path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Hata", f"Excel kaydı başarısız:\n{e}")


    @QtCore.Slot()
    def on_set_duration(self):
        """UI’dan girilen saniyeyi alıp self.min_duration’a ata."""
        try:
            self.min_duration = int(self.duration_input.text())
        except ValueError:
            self.min_duration = 60

    @QtCore.Slot()
    def on_set_max_duration(self):
        """UI’dan girilen saniyeyi alıp self.max_duration’a ata."""
        try:
            self.max_duration = int(self.max_duration_input.text())
        except ValueError:
            self.max_duration = 60
            self.max_duration_input.setText(str(self.max_duration))

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

    def export_all_arbitrage(self, mode_name: str):
        """Tüm açık ve kapalı arbitrajları tek dosyaya Excel olarak kaydet."""
        timestamp = datetime.now().strftime("%d%m%Y%H%M%S")
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        filename = os.path.join(desktop, f"{mode_name} {timestamp}.xlsx")

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

        try:
            with pd.ExcelWriter(filename) as writer:
                pd.DataFrame(open_rows, columns=headers).to_excel(
                    writer, sheet_name="Açık Arbitrajlar Tablosu", index=False
                )
                pd.DataFrame(closed_rows, columns=headers).to_excel(
                    writer, sheet_name="Kapalı Arbitrajlar Tablosu", index=False
                )
            QtWidgets.QMessageBox.information(self, "Başarılı", f"Kaydedildi:\n{filename}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Hata", f"Excel kaydı başarısız:\n{e}")

    def on_switch_duration_mode(self, checked: bool):
        if checked:
            self.duration_mode_switcher.setText("Max Süre")
            self.export_all_arbitrage("Max Süre Excel")
            self.use_max_duration = True
        else:
            self.duration_mode_switcher.setText("Min Süre")
            self.export_all_arbitrage("Min Süre Excel")
            self.use_max_duration = False

        self.arb_model.beginResetModel()
        self.arb_model.events.clear()
        self.arb_model.endResetModel()

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




    # ─── WebSocket’ten gelen verileri önce dict’e koyan metod ───
    def _enqueue_askbid(self, exchange, symbol, bid, ask):
        self._askbid_data[(exchange, symbol)] = (bid, ask)

    # ─── ~30 Hz’de dict’i boşaltıp tabloyu güncelleyen metod ───
    def _flush_askbid_data(self):
        for (exchange, symbol), (bid, ask) in self._askbid_data.items():
            if symbol and isinstance(symbol, str):
                self.askbid_model.update_askbid(exchange, symbol, bid, ask)
                for win in list(self.chart_windows):
                    win.add_point(exchange, symbol, bid, ask)
        self._askbid_data.clear()

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

            win = ChartWindow(sym, exch, exch)
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

        win = ChartWindow(symbol, ask_exch, bid_exch)
        self.chart_windows.append(win)
        win.destroyed.connect(lambda _=None, w=win: self.chart_windows.remove(w))
        win.show()


     
    def process_arbitrage(self):
        fee_rate_buy = 0.0005
        fee_rate_sell = 0.0005
        data = self.askbid_model._data

        for symbol, exch_data in data.items():
            for buy_exch, (_, ask_price) in exch_data.items():
                if ask_price <= 0:
                    continue
                for sell_exch, (bid_price, _) in exch_data.items():
                    if sell_exch == buy_exch or bid_price <= 0:
                        continue

                    raw_rate = bid_price/ask_price - 1
                    total_fee_rate = fee_rate_buy + fee_rate_sell
                    net_rate = raw_rate - total_fee_rate
                    #ask_gross = ask_price * (1 + fee_rate)
                    #net_rate = raw_rate - total_fee_rate
                    buy_key  = (buy_exch, symbol)
                    sell_key = (sell_exch, symbol)

                    if buy_key not in self.current_funding or sell_key not in self.current_funding:
                        continue

                    initial_buy_fr  = self.current_funding[buy_key]
                    initial_sell_fr = self.current_funding[sell_key]


                    if net_rate >= 0.60:
                        continue

                    key = (symbol, buy_exch, sell_exch)


                    # Başlayan fırsat
                    if net_rate >= self.arb_threshold:
                        if key not in self._arb_map:
                            ev = ArbitrajEvent(symbol, buy_exch, sell_exch, net_rate, 
                                               initial_ask= ask_price, 
                                               initial_bid = bid_price, 
                                               initial_buy_fr    = initial_buy_fr, 
                                               initial_sell_fr   = initial_sell_fr,
                            )
                            self.arb_model.add_event(ev)
                            self._arb_map[key] = ev

                    # Biten fırsat
                    elif net_rate <= self.arb_close_threshold:
                        if key in self._arb_map:
                            ev = self._arb_map.pop(key)
                            row = self.arb_model.events.index(ev)

                            # kapanıştaki son arbitraj oranını kaydet
                            ev.final_rate = net_rate

                            # kapanıştaki fiyatları kaydet
                            ev.final_ask = ask_price
                            ev.final_bid = bid_price

                            # Süre kontrolü
                            elapsed = (datetime.now() - ev.start_dt).total_seconds()
                            row = self.arb_model.events.index(ev)
                            if not self.use_max_duration:
                                if elapsed < self.min_duration:
                                    self.arb_model.remove_event(row)
                                else:
                                    self.arb_model.end_event(row)
                            else:
                                if elapsed > self.max_duration:
                                    self.arb_model.remove_event(row)
                                else:
                                    self.arb_model.end_event(row)



# --- WebSocket feeders ---
async def publish_binance(cb, status_cb):
    url = BINANCE_URL
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
                            cb("Binance", u["s"], float(u["r"]) * 100)
        except Exception as e:
            # bağlantı koptu → indicator’u mavi yap
            status_cb("Binance", False)
            print(f"[Binance] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


async def publish_okx(cb, status_cb):
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

    while True:
        try:
            async with websockets.connect(OKX_WS_URL) as ws:
                # bağlantı başarıyla kuruldu
                status_cb("OKX", True)
                print("[OKX] Connected")
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    m = json.loads(raw)
                    for e in m.get("data", []):
                        cb("OKX", e["instId"], float(e["fundingRate"]) * 100)
        except Exception as ex:
            # bağlantı koptu
            status_cb("OKX", False)
            print(f"[OKX] Error: {ex}, reconnecting in 5s")
            await asyncio.sleep(5)


async def handle_bybit_batch(syms, cb, status_cb):
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
                            cb("Bybit", d["symbol"], float(d["fundingRate"]) * 100)
        except Exception as e:
            # Mark connection down
            status_cb("Bybit", False)
            print(f"[Bybit] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


async def publish_bybit(cb, status_cb):
    # fetch all USDT swaps
    syms = await fetch_bybit_swaps()
    # fan out in batches
    for batch in [syms[i:i+BYBIT_BATCH_SIZE] for i in range(0, len(syms), BYBIT_BATCH_SIZE)]:
        asyncio.create_task(handle_bybit_batch(batch, cb, status_cb))
        await asyncio.sleep(1)

async def handle_bitget_batch(syms, cb, status_cb):
    sub = {"op":"subscribe", "args":[{"instType":"USDT-FUTURES","channel":"ticker","instId":s} for s in syms]}
    while True:
        try:
            async with websockets.connect(BITGET_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                status_cb("Bitget", True)
                print(f"[Bitget] Batch {len(syms)} connected")
                await ws.send(json.dumps(sub))
                async for raw in ws:
                    data = json.loads(raw)
                    if data.get("action") in ("snapshot","update"):
                        for d in data.get("data", []):
                            if "instId" in d and "fundingRate" in d:
                                cb("Bitget", d["instId"], float(d["fundingRate"]) * 100)
        except Exception as e:
            status_cb("Bitget", False)
            print(f"[Bitget] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)

async def publish_bitget(cb, status_cb):
    syms = await fetch_bitget_swaps()
    for batch in [syms[i:i+50] for i in range(0, len(syms), 50)]:
        asyncio.create_task(handle_bitget_batch(batch, cb, status_cb))
        await asyncio.sleep(1)


async def publish_gateio(cb, status_cb):
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
                        for d in m.get("result", []):
                            if "contract" in d and "funding_rate" in d:
                                cb("Gateio", d["contract"], float(d["funding_rate"]) * 100)

        except Exception as e:
            # connection down
            status_cb("Gateio", False)
            print(f"[Gateio] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


async def publish_binance_askbid(cb, status_cb):
    url = "wss://fstream.binance.com/ws/!bookTicker"
    while True:
        try:
            async with websockets.connect(url) as ws:
                status_cb("Binance", True)
                print("[Binance AskBid] Connected")
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("e") == "bookTicker":
                        cb("Binance", msg["s"], float(msg["b"]), float(msg["a"]))
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
                            ask_price = float(asks[0][0])
                            cb("OKX", instId, bid_price, ask_price)
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
                        ask_price = float(asks[0][0])
                        cb("Bybit", sym, bid_price, ask_price)
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

                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("action") in ("snapshot", "update"):
                        for d in m.get("data", []):
                            inst = d.get("instId")
                            bid  = d.get("bidPr")
                            ask  = d.get("askPr")
                            if inst and bid is not None and ask is not None:
                                cb("Bitget", inst, float(bid), float(ask))

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
                        cb("Gateio", r["s"], float(r["b"]), float(r["a"]))
        except Exception as e:
            status_cb("Gateio", False)
            print(f"[Gateio AskBid] Error: {e}, reconnecting in 5s")
            await asyncio.sleep(5)


# --- Application entrypoint ---
def main():
    app = QtWidgets.QApplication(sys.argv)
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
        def fr_cb(exchange: str, raw_symbol: str, rate_pct: float):
            # 1) Funding Rate Live tablosunu güncelle
            window.model.update_rate(exchange, raw_symbol, rate_pct)

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
                row = window.arb_model.events.index(ev)
                idx = window.arb_model.index(row, col)
                window.arb_model.dataChanged.emit(idx, idx, [QtCore.Qt.DisplayRole])

        # WebSocket’leri sarılmış callback ile başlat
        loop.create_task(publish_binance (fr_cb, window._update_status_fr))
        loop.create_task(publish_okx     (fr_cb, window._update_status_fr))
        loop.create_task(publish_bybit   (fr_cb, window._update_status_fr))
        loop.create_task(publish_bitget  (fr_cb, window._update_status_fr))
        loop.create_task(publish_gateio  (fr_cb, window._update_status_fr))

    QtCore.QTimer.singleShot(5000, show_main)

    loop.run_forever()

if __name__ == "__main__":
    main()