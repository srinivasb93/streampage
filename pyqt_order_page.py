import sys
import uuid
from time import sleep
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QTabWidget, QLabel, QPushButton, QComboBox, QCheckBox,
                             QTableWidget, QTableWidgetItem, QSpinBox, QDoubleSpinBox,
                             QTimeEdit, QDateEdit, QFormLayout, QGroupBox, QMessageBox,
                             QLineEdit, QGridLayout, QScrollArea)
from PyQt6.QtCore import QThread, pyqtSignal, QTimer, Qt, QDateTime
from PyQt6.QtGui import QFont, QIcon
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import threading
import queue
import schedule
import pyotp
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
logger = logging.getLogger(__name__)

# Load environment variables (e.g., API keys, TOTP secret)
load_dotenv()
UPSTOX_API_KEY = os.getenv("UPSTOX_API_KEY")
UPSTOX_API_SECRET = os.getenv("UPSTOX_API_SECRET")
TOTP_SECRET = os.getenv("TOTP_SECRET")  # Your Upstox TOTP secret
ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

# Assuming these are your existing functions (replace with actual implementations)
from upstox_utils import (get_user_profile_and_funds, get_subscribed_instruments,
                          fetch_live_data, subscribe_to_instrument, is_connected,
                          place_order, get_historical_data, calculate_brokerage,
                          calculate_atr, fetch_instruments, manage_scheduled_orders,
                          update_scheduled_order, init_upstox_api, initialize_websocket, ACCESS_TOKEN)

# Global instruments dictionary and queues
instruments = fetch_instruments()
scheduled_orders_queue = queue.Queue()

apis = init_upstox_api()

class LiveDataThread(QThread):
    data_updated = pyqtSignal(dict)

    def __init__(self, instrument_token):
        super().__init__()
        self.instrument_token = instrument_token
        self.running = True

    def run(self):
        while self.running:
            data = fetch_live_data(instrument_token=self.instrument_token, websocket=True)
            if data:
                self.data_updated.emit(data)
            self.msleep(1000)  # Update every second

    def stop(self):
        self.running = False

class TokenFetchThread(QThread):
    token_fetched = pyqtSignal(str)

    def run(self):
        totp = pyotp.TOTP(TOTP_SECRET)
        totp_code = totp.now()
        # Simulate Upstox token fetch (replace with actual API call)
        # Example: Use Upstox API with API_KEY, API_SECRET, and TOTP
        access_token = "simulated_token"  # Replace with real API logic
        self.token_fetched.emit(access_token)

class OrderManagementWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Stock Order Management")
        self.setGeometry(100, 100, 1400, 900)  # Increased window size for better layout
        self.setWindowIcon(QIcon("icon.png"))

        # Styling
        self.setStyleSheet("""
                    QMainWindow { background-color: #f0f0f0; }
                    QLabel { font-size: 12pt; }
                    QPushButton { 
                        background-color: #4CAF50; 
                        color: white; 
                        padding: 5px; 
                        border-radius: 3px;
                    }
                    QPushButton:hover { background-color: #45a049; }
                    QGroupBox { 
                        font-weight: bold; 
                        border: 1px solid #ddd; 
                        border-radius: 5px; 
                        padding: 10px; 
                        margin-top: 10px;
                    }
                """)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setSpacing(15)
        self.main_layout.setContentsMargins(20, 20, 20, 20)

        # Header Section
        header_layout = QHBoxLayout()
        self.token_button = QPushButton("Fetch Access Token")
        self.token_button.clicked.connect(self.fetch_token)
        self.funds_data = get_user_profile_and_funds(apis['user'])  # Replace with actual API setup
        self.funds_label = QLabel(f"Funds Available: ₹{self.funds_data['data']['equity']['available_margin']:.2f}")
        self.funds_label.setStyleSheet("font-size: 14pt; font-weight: bold; color: #333;")
        header_layout.addWidget(self.funds_label)
        header_layout.addStretch()
        header_layout.addWidget(self.token_button)
        self.main_layout.addLayout(header_layout)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("QTabBar::tab { height: 35px; width: 150px; }")
        self.main_layout.addWidget(self.tabs)

        # Initialize tabs
        self.setup_single_order_tab()
        self.setup_multiple_orders_tab()
        self.setup_scheduled_orders_tab()
        self.setup_auto_orders_tab()
        self.setup_order_book_tab()
        self.setup_positions_tab()

        self.live_threads = {}
        self.start_scheduler()

        # Initialize WebSocket
        initialize_websocket(ACCESS_TOKEN)

    def setup_single_order_tab(self):
        self.single_tab = QWidget()
        self.tabs.addTab(self.single_tab, "Single Order")
        layout = QVBoxLayout(self.single_tab)
        layout.setSpacing(10)

        # Symbol Selection and Live Data
        symbol_group = QGroupBox("Instrument Selection")
        symbol_layout = QGridLayout()
        symbol_layout.addWidget(QLabel("Select Symbol:"), 0, 0)
        self.single_symbol = QComboBox()
        self.single_symbol.addItems(instruments.keys())
        self.single_symbol.currentTextChanged.connect(self.start_live_data)
        symbol_layout.addWidget(self.single_symbol, 0, 1, 1, 2)
        self.single_ltp = QLabel("Last Traded Price: ₹0.00")
        symbol_layout.addWidget(self.single_ltp, 1, 0, 1, 3)
        symbol_group.setLayout(symbol_layout)
        layout.addWidget(symbol_group)

        main_content = QHBoxLayout()

        # Position Sizing Calculator
        calc_group = QGroupBox("Position Sizing Calculator")
        calc_layout = QFormLayout()
        calc_layout.setSpacing(8)

        self.single_capital = QSpinBox()
        self.single_risk = QDoubleSpinBox()
        self.single_product = QComboBox()
        self.single_trans = QComboBox()
        self.single_sl_type = QComboBox()
        self.single_sl_fixed = QDoubleSpinBox()
        self.single_target_fixed = QDoubleSpinBox()
        self.single_sl_pct = QDoubleSpinBox()
        self.single_target_pct = QDoubleSpinBox()
        self.single_atr_period = QSpinBox()
        self.single_sl_atr = QDoubleSpinBox()
        self.single_target_atr = QDoubleSpinBox()

        calc_widgets = [
            ("Total Capital (Rs.):", self.single_capital, (1000, 1000000), 50000),
            ("Risk per Trade (%):", self.single_risk, (0.1, 100.0), 1.0),
            ("Product Type:", self.single_product, ['I', 'D'], None),
            ("Transaction Type:", self.single_trans, ["BUY", "SELL"], None),
            ("Stop Loss Type:", self.single_sl_type, ["Fixed Amount", "Percentage of Entry", "ATR Based"], None),
            ("Stop Loss Value (Rs.):", self.single_sl_fixed, (1.0, 1000.0), 100.0),
            ("Target Value (Rs.):", self.single_target_fixed, (1.0, 1000.0), 250.0),
            ("Stop Loss (%):", self.single_sl_pct, (0.1, 100.0), 1.0),
            ("Target (%):", self.single_target_pct, (0.1, 100.0), 2.5),
            ("ATR Period:", self.single_atr_period, (5, 50), 14),
            ("Stop Loss ATR Multiplier:", self.single_sl_atr, (0.5, 10.0), 2.0),
            ("Target ATR Multiplier:", self.single_target_atr, (0.5, 10.0), 5.0),
        ]

        for label, widget, range_vals, default in calc_widgets:
            if isinstance(widget, QSpinBox) or isinstance(widget, QDoubleSpinBox):
                widget.setRange(*range_vals)
                widget.setValue(default)
            elif isinstance(widget, QComboBox):
                widget.addItems(range_vals)
            calc_layout.addRow(label, widget)

        self.single_trans.setVisible(False)
        self.single_product.currentTextChanged.connect(self.update_trans_visibility)
        self.single_sl_type.currentTextChanged.connect(self.update_sl_inputs)

        calc_button = QPushButton("Calculate")
        calc_button.clicked.connect(self.calculate_position)
        calc_layout.addRow(calc_button)
        self.single_calc_result = QLabel("")
        calc_layout.addRow(self.single_calc_result)
        calc_group.setLayout(calc_layout)
        main_content.addWidget(calc_group)

        # Order Details
        order_group = QGroupBox("Order Details")
        order_layout = QFormLayout()
        order_layout.setSpacing(8)

        self.single_quantity = QSpinBox()
        self.single_order_type = QComboBox()
        self.single_trans_type = QComboBox()
        self.single_product_type = QComboBox()
        self.single_amo = QCheckBox()
        self.single_price = QDoubleSpinBox()
        self.single_trigger = QDoubleSpinBox()
        self.single_stop_loss = QDoubleSpinBox()
        self.single_target = QDoubleSpinBox()

        order_widgets = [
            ("Quantity:", self.single_quantity, (1, 10000), 0),
            ("Order Type:", self.single_order_type, ["MARKET", "LIMIT", "SL", "SL-M"], None),
            ("Transaction Type:", self.single_trans_type, ["BUY", "SELL"], None),
            ("Product Type:", self.single_product_type, ['I', 'D'], None),
            ("AMO Order:", self.single_amo, None, None),
            ("Limit Price:", self.single_price, (0.0, 10000.0), 0.0),
            ("Trigger Price:", self.single_trigger, (0.0, 10000.0), 0.0),
            ("Stop-Loss Price:", self.single_stop_loss, (0.0, 10000.0), 0.0),
            ("Target Price:", self.single_target, (0.0, 10000.0), 0.0),
        ]

        for label, widget, range_vals, default in order_widgets:
            if isinstance(widget, QSpinBox) or isinstance(widget, QDoubleSpinBox):
                widget.setRange(*range_vals)
                if default is not None:
                    widget.setValue(default)
            elif isinstance(widget, QComboBox):
                widget.addItems(range_vals)
            order_layout.addRow(label, widget)

        self.single_other_orders = QComboBox()
        self.single_other_orders.addItems(["None", "Auto-sell if Open > Previous Close", "Schedule Order", "Schedule Short Sell at Open"])
        self.single_other_orders.currentTextChanged.connect(self.update_schedule_visibility)
        order_layout.addRow("Other Orders:", self.single_other_orders)

        self.single_schedule_time = QTimeEdit()
        self.single_schedule_date = QDateEdit()
        order_layout.addRow("Schedule Time:", self.single_schedule_time)
        order_layout.addRow("Schedule Date:", self.single_schedule_date)

        place_button = QPushButton("Place Order")
        place_button.clicked.connect(self.place_order)
        order_layout.addRow(place_button)
        order_group.setLayout(order_layout)
        main_content.addWidget(order_group)

        layout.addLayout(main_content)
        layout.addStretch()

    def setup_multiple_orders_tab(self):
        self.multiple_tab = QWidget()
        self.tabs.addTab(self.multiple_tab, "Multiple Orders")
        layout = QVBoxLayout(self.multiple_tab)

        control_layout = QHBoxLayout()
        control_layout.addWidget(QLabel("Number of Orders:"))
        self.num_orders = QSpinBox()
        self.num_orders.setRange(1, 10)
        self.num_orders.valueChanged.connect(self.update_multi_orders)
        control_layout.addWidget(self.num_orders)
        control_layout.addStretch()
        layout.addLayout(control_layout)

        self.multi_scroll = QScrollArea()
        self.multi_scroll.setWidgetResizable(True)
        self.multi_container = QWidget()
        self.multi_layout = QVBoxLayout(self.multi_container)
        self.multi_scroll.setWidget(self.multi_container)
        layout.addWidget(self.multi_scroll)

        place_multi_button = QPushButton("Place Multiple Orders")
        place_multi_button.clicked.connect(self.place_multiple_orders)
        layout.addWidget(place_multi_button)

        self.multi_widgets = []
        self.update_multi_orders()

    def update_multi_orders(self):
        # Clear existing widgets safely
        while self.multi_layout.count():
            item = self.multi_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        self.multi_widgets.clear()

        try:
            num_orders = self.num_orders.value()
            for i in range(num_orders):
                group = QGroupBox(f"Order {i + 1}")
                row_layout = QHBoxLayout()  # Horizontal layout for each order
                row_layout.setSpacing(10)

                widgets = {
                    "symbol": QComboBox(),
                    "quantity": QSpinBox(),
                    "order_type": QComboBox(),
                    "trans_type": QComboBox(),
                    "product_type": QComboBox(),
                    "amo": QCheckBox("AMO"),
                    "stop_loss": QDoubleSpinBox(),
                    "target": QDoubleSpinBox(),
                    "schedule_short": QCheckBox("Schedule Short"),
                    "schedule_time": QTimeEdit(),
                    "schedule_date": QDateEdit()
                }

                widgets["symbol"].addItems(instruments.keys())
                widgets["quantity"].setRange(1, 10000)
                widgets["order_type"].addItems(["MARKET", "LIMIT", "SL", "SL-M"])
                widgets["trans_type"].addItems(["BUY", "SELL"])
                widgets["product_type"].addItems(['I', 'D'])
                widgets["stop_loss"].setRange(0.0, 10000.0)
                widgets["target"].setRange(0.0, 10000.0)
                widgets["schedule_time"].setVisible(False)
                widgets["schedule_date"].setVisible(False)

                # Add widgets horizontally
                row_layout.addWidget(QLabel("Symbol:"))
                row_layout.addWidget(widgets["symbol"])
                row_layout.addWidget(QLabel("Qty:"))
                row_layout.addWidget(widgets["quantity"])
                row_layout.addWidget(QLabel("Type:"))
                row_layout.addWidget(widgets["order_type"])
                row_layout.addWidget(QLabel("Trans:"))
                row_layout.addWidget(widgets["trans_type"])
                row_layout.addWidget(QLabel("Prod:"))
                row_layout.addWidget(widgets["product_type"])
                row_layout.addWidget(widgets["amo"])
                row_layout.addWidget(QLabel("SL:"))
                row_layout.addWidget(widgets["stop_loss"])
                row_layout.addWidget(QLabel("Target:"))
                row_layout.addWidget(widgets["target"])
                row_layout.addWidget(widgets["schedule_short"])
                row_layout.addWidget(widgets["schedule_time"])
                row_layout.addWidget(widgets["schedule_date"])

                calc_button = QPushButton("Calculate")
                calc_button.clicked.connect(lambda _, idx=i: self.show_calc_dialog(idx))
                row_layout.addWidget(calc_button)  # Calculate button last in row

                widgets["schedule_short"].stateChanged.connect(
                    lambda state, idx=i: self.update_multi_schedule(state, idx))
                group.setLayout(row_layout)
                self.multi_layout.addWidget(group)
                self.multi_widgets.append(widgets)
        except Exception as e:
            logger.error(f"Error updating multiple orders: {e}")
            QMessageBox.critical(self, "Error", f"Failed to update orders: {str(e)}")

    def setup_scheduled_orders_tab(self):
        self.scheduled_tab = QWidget()
        self.tabs.addTab(self.scheduled_tab, "Scheduled Orders")
        layout = QVBoxLayout(self.scheduled_tab)
        self.scheduled_table = QTableWidget()
        self.scheduled_table.setColumnCount(8)
        self.scheduled_table.setHorizontalHeaderLabels(["Order ID", "Symbol", "Quantity", "Type", "Transaction", "Schedule Time", "Stop-Loss", "Actions"])
        self.scheduled_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.scheduled_table)

        self.update_scheduled_table()

        modify_button = QPushButton("Modify Selected")
        modify_button.clicked.connect(self.modify_scheduled_order)
        cancel_button = QPushButton("Cancel Selected")
        cancel_button.clicked.connect(self.cancel_scheduled_order)
        buttons_layout = QHBoxLayout()
        buttons_layout.addWidget(modify_button)
        buttons_layout.addWidget(cancel_button)
        layout.addLayout(buttons_layout)

    def setup_auto_orders_tab(self):
        self.auto_tab = QWidget()
        self.tabs.addTab(self.auto_tab, "Auto Orders")
        layout = QVBoxLayout(self.auto_tab)

        self.auto_symbol = QComboBox()
        self.auto_symbol.addItems(instruments.keys())
        layout.addWidget(QLabel("Select Symbol:"))
        layout.addWidget(self.auto_symbol)

        self.auto_risk = QDoubleSpinBox()
        self.auto_risk.setRange(0.1, 100.0)
        self.auto_risk.setValue(1.0)
        layout.addWidget(QLabel("Risk per Trade (%):"))
        layout.addWidget(self.auto_risk)

        self.auto_sl_type = QComboBox()
        self.auto_sl_type.addItems(["Fixed Amount", "Percentage of Entry", "ATR Based"])
        self.auto_sl_type.currentTextChanged.connect(self.update_auto_sl_inputs)
        layout.addWidget(QLabel("Stop Loss Type:"))
        layout.addWidget(self.auto_sl_type)

        self.auto_sl_fixed = QDoubleSpinBox()
        self.auto_sl_fixed.setRange(1.0, 1000.0)
        self.auto_sl_fixed.setValue(100.0)
        layout.addWidget(QLabel("Stop Loss Value (Rs.):"))
        layout.addWidget(self.auto_sl_fixed)

        self.auto_target_fixed = QDoubleSpinBox()
        self.auto_target_fixed.setRange(1.0, 1000.0)
        self.auto_target_fixed.setValue(250.0)
        layout.addWidget(QLabel("Target Value (Rs.):"))
        layout.addWidget(self.auto_target_fixed)

        self.auto_sl_pct = QDoubleSpinBox()
        self.auto_sl_pct.setRange(0.1, 100.0)
        self.auto_sl_pct.setValue(1.0)
        self.auto_sl_pct.setVisible(False)
        layout.addWidget(QLabel("Stop Loss (%):"))
        layout.addWidget(self.auto_sl_pct)

        self.auto_target_pct = QDoubleSpinBox()
        self.auto_target_pct.setRange(0.1, 100.0)
        self.auto_target_pct.setValue(2.5)
        self.auto_target_pct.setVisible(False)
        layout.addWidget(QLabel("Target (%):"))
        layout.addWidget(self.auto_target_pct)

        self.auto_atr_period = QSpinBox()
        self.auto_atr_period.setRange(5, 50)
        self.auto_atr_period.setValue(14)
        self.auto_atr_period.setVisible(False)
        layout.addWidget(QLabel("ATR Period:"))
        layout.addWidget(self.auto_atr_period)

        self.auto_sl_atr = QDoubleSpinBox()
        self.auto_sl_atr.setRange(0.5, 10.0)
        self.auto_sl_atr.setValue(2.0)
        self.auto_sl_atr.setVisible(False)
        layout.addWidget(QLabel("Stop Loss ATR Multiplier:"))
        layout.addWidget(self.auto_sl_atr)

        self.auto_target_atr = QDoubleSpinBox()
        self.auto_target_atr.setRange(0.5, 10.0)
        self.auto_target_atr.setValue(5.0)
        self.auto_target_atr.setVisible(False)
        layout.addWidget(QLabel("Target ATR Multiplier:"))
        layout.addWidget(self.auto_target_atr)

        add_auto_button = QPushButton("Add Auto Short Sell Order")
        add_auto_button.clicked.connect(self.add_auto_order)
        layout.addWidget(add_auto_button)

        self.auto_orders = []

    def start_live_data(self):
        symbol = self.single_symbol.currentText()
        instrument_token = instruments.get(symbol)

        # Stop all existing threads to prevent multiple feeds
        for token, thread in self.live_threads.items():
            thread.stop()
            thread.wait()
        self.live_threads.clear()

        # Ensure WebSocket is connected
        if not is_connected():
            initialize_websocket(ACCESS_TOKEN)
            sleep(1)  # Wait for connection

        if instrument_token not in get_subscribed_instruments():
            success = subscribe_to_instrument(instrument_token)
            if not success:
                logger.warning(f"Failed to subscribe to {instrument_token}")

        thread = LiveDataThread(instrument_token)
        thread.data_updated.connect(self.update_ltp)  # Simplified connection
        thread.start()
        self.live_threads[instrument_token] = thread

    def update_ltp(self, data):
        # Update LTP only for the currently selected symbol
        current_symbol = self.single_symbol.currentText()
        current_token = instruments.get(current_symbol)
        ltp = data.get('ltp', 0) if current_token in self.live_threads else 0
        self.single_ltp.setText(f"Last Traded Price: ₹{ltp:.2f}")

    def update_trans_visibility(self):
        self.single_trans.setVisible(self.single_product.currentText() == 'I')

    def update_sl_inputs(self):
        sl_type = self.single_sl_type.currentText()
        self.single_sl_fixed.setVisible(sl_type == "Fixed Amount")
        self.single_target_fixed.setVisible(sl_type == "Fixed Amount")
        self.single_sl_pct.setVisible(sl_type == "Percentage of Entry")
        self.single_target_pct.setVisible(sl_type == "Percentage of Entry")
        self.single_atr_period.setVisible(sl_type == "ATR Based")
        self.single_sl_atr.setVisible(sl_type == "ATR Based")
        self.single_target_atr.setVisible(sl_type == "ATR Based")

    def update_auto_sl_inputs(self):
        sl_type = self.auto_sl_type.currentText()
        self.auto_sl_fixed.setVisible(sl_type == "Fixed Amount")
        self.auto_target_fixed.setVisible(sl_type == "Fixed Amount")
        self.auto_sl_pct.setVisible(sl_type == "Percentage of Entry")
        self.auto_target_pct.setVisible(sl_type == "Percentage of Entry")
        self.auto_atr_period.setVisible(sl_type == "ATR Based")
        self.auto_sl_atr.setVisible(sl_type == "ATR Based")
        self.auto_target_atr.setVisible(sl_type == "ATR Based")

    def update_schedule_visibility(self):
        visible = self.single_other_orders.currentText() in ["Schedule Order", "Schedule Short Sell at Open"]
        self.single_schedule_time.setVisible(visible)
        self.single_schedule_date.setVisible(visible)

    def update_multi_schedule(self, state, index):
        widgets = self.multi_widgets[index]
        widgets["schedule_time"].setVisible(state == Qt.CheckState.Checked.value)
        widgets["schedule_date"].setVisible(state == Qt.CheckState.Checked.value)

    def calculate_position(self):
        capital = self.single_capital.value()
        risk_per_trade = self.single_risk.value()
        product_type = self.single_product.currentText()
        transaction_type = self.single_trans.currentText() if product_type == 'I' and self.single_trans.isVisible() else "BUY"
        sl_type = self.single_sl_type.currentText()
        instrument_token = instruments[self.single_symbol.currentText()]
        live_data = fetch_live_data(apis["market_data"], instrument_token)
        entry_price = live_data.get('ltp', 0) if live_data else 0

        if entry_price == 0:
            self.single_calc_result.setText("Error: No live price available")
            return

        risk_amount = capital * (risk_per_trade / 100)
        if sl_type == "Fixed Amount":
            stop_loss_diff = self.single_sl_fixed.value()
            target_diff = self.single_target_fixed.value()
            if transaction_type == "BUY":
                stop_loss_price = entry_price - stop_loss_diff
                target_price = entry_price + target_diff
            else:
                stop_loss_price = entry_price + stop_loss_diff
                target_price = entry_price - target_diff
            quantity = int(risk_amount / stop_loss_diff)
        elif sl_type == "Percentage of Entry":
            stop_loss_diff = entry_price * (self.single_sl_pct.value() / 100)
            target_diff = entry_price * (self.single_target_pct.value() / 100)
            if transaction_type == "BUY":
                stop_loss_price = entry_price - stop_loss_diff
                target_price = entry_price + target_diff
            else:
                stop_loss_price = entry_price + stop_loss_diff
                target_price = entry_price - target_diff
            quantity = int(risk_amount / stop_loss_diff)
        else:  # ATR Based
            atr_period = self.single_atr_period.value()
            stop_loss_atr = self.single_sl_atr.value()
            target_atr = self.single_target_atr.value()
            hist_data = get_historical_data(apis["history"], instrument_token)
            atr = calculate_atr(pd.DataFrame(hist_data), atr_period).iloc[-1] if hist_data else 0
            stop_loss_diff = atr * stop_loss_atr
            target_diff = atr * target_atr
            if transaction_type == "BUY":
                stop_loss_price = entry_price - stop_loss_diff
                target_price = entry_price + target_diff
            else:
                stop_loss_price = entry_price + stop_loss_diff
                target_price = entry_price - target_diff
            quantity = int(risk_amount / stop_loss_diff) if atr > 0 else 0

        quantity = max(1, quantity)
        trade_value = entry_price * quantity
        entry_brokerage = calculate_brokerage(apis["charges"], instrument_token, quantity, entry_price, transaction_type, product_type)
        if product_type == 'I':
            exit_brokerage = calculate_brokerage(apis["charges"], instrument_token, quantity,
                                                target_price if transaction_type == "BUY" else stop_loss_price,
                                                "SELL" if transaction_type == "BUY" else "BUY", product_type)
            total_brokerage = entry_brokerage + exit_brokerage
            entry_charges = trade_value * 0.001
            exit_charges = (target_price if transaction_type == "BUY" else stop_loss_price) * quantity * 0.001
            total_charges = entry_charges + exit_charges
            total_cost = trade_value + total_brokerage + total_charges
            charges_text = f"Brokerage: ₹{total_brokerage:.2f} (Entry: {entry_brokerage:.2f}, Exit: {exit_brokerage:.2f})\nCharges: ₹{total_charges:.2f}"
        else:
            total_brokerage = entry_brokerage
            total_charges = trade_value * 0.001
            total_cost = trade_value + total_brokerage + total_charges
            charges_text = f"Brokerage: ₹{total_brokerage:.2f}\nCharges: ₹{total_charges:.2f}"

        self.single_calc_result.setText(f"Quantity: {quantity}\nStop Loss: ₹{stop_loss_price:.2f}\nTarget: ₹{target_price:.2f}\nTotal Cost: ₹{total_cost:.2f}\n{charges_text}")
        # Sync Order Details with Calculator Results
        self.single_quantity.setValue(quantity)
        self.single_stop_loss.setValue(stop_loss_price)
        self.single_target.setValue(target_price)
        self.single_trans_type.setCurrentText(transaction_type)
        self.single_product_type.setCurrentText(product_type)
        self.single_order_type.setCurrentText("MARKET")  # Default to MARKET, adjust as needed
        self.single_price.setValue(entry_price if self.single_order_type.currentText() == "LIMIT" else 0.0)
        self.single_trigger.setValue(0.0)  # Reset trigger unless SL/SL-M

    def show_calc_dialog(self, index):
        dialog = QWidget()
        dialog.setWindowTitle(f"Position Sizing Calculator - Order {index + 1}")
        layout = QFormLayout()

        capital = QSpinBox()
        capital.setRange(1000, 1000000)
        capital.setValue(50000)
        layout.addRow("Total Capital (Rs.):", capital)

        risk = QDoubleSpinBox()
        risk.setRange(0.1, 100.0)
        risk.setValue(1.0)
        layout.addRow("Risk per Trade (%):", risk)

        product = QComboBox()
        product.addItems(['I', 'D'])
        layout.addRow("Product Type:", product)

        trans = QComboBox()
        trans.addItems(["BUY", "SELL"])
        trans.setVisible(False)
        layout.addRow("Transaction Type:", trans)
        product.currentTextChanged.connect(lambda text: trans.setVisible(text == 'I'))

        sl_type = QComboBox()
        sl_type.addItems(["Fixed Amount", "Percentage of Entry", "ATR Based"])
        layout.addRow("Stop Loss Type:", sl_type)

        sl_fixed = QDoubleSpinBox()
        sl_fixed.setRange(1.0, 1000.0)
        sl_fixed.setValue(100.0)
        layout.addRow("Stop Loss Value (Rs.):", sl_fixed)

        target_fixed = QDoubleSpinBox()
        target_fixed.setRange(1.0, 1000.0)
        target_fixed.setValue(250.0)
        layout.addRow("Target Value (Rs.):", target_fixed)

        sl_pct = QDoubleSpinBox()
        sl_pct.setRange(0.1, 100.0)
        sl_pct.setValue(1.0)
        sl_pct.setVisible(False)
        layout.addRow("Stop Loss (%):", sl_pct)

        target_pct = QDoubleSpinBox()
        target_pct.setRange(0.1, 100.0)
        target_pct.setValue(2.5)
        target_pct.setVisible(False)
        layout.addRow("Target (%):", target_pct)

        atr_period = QSpinBox()
        atr_period.setRange(5, 50)
        atr_period.setValue(14)
        atr_period.setVisible(False)
        layout.addRow("ATR Period:", atr_period)

        sl_atr = QDoubleSpinBox()
        sl_atr.setRange(0.5, 10.0)
        sl_atr.setValue(2.0)
        sl_atr.setVisible(False)
        layout.addRow("Stop Loss ATR Multiplier:", sl_atr)

        target_atr = QDoubleSpinBox()
        target_atr.setRange(0.5, 10.0)
        target_atr.setValue(5.0)
        target_atr.setVisible(False)
        layout.addRow("Target ATR Multiplier:", target_atr)

        sl_type.currentTextChanged.connect(lambda text: [
            sl_fixed.setVisible(text == "Fixed Amount"),
            target_fixed.setVisible(text == "Fixed Amount"),
            sl_pct.setVisible(text == "Percentage of Entry"),
            target_pct.setVisible(text == "Percentage of Entry"),
            atr_period.setVisible(text == "ATR Based"),
            sl_atr.setVisible(text == "ATR Based"),
            target_atr.setVisible(text == "ATR Based")
        ])

        calc_button = QPushButton("Calculate")
        calc_button.clicked.connect(lambda: self.calculate_multi_position(index, capital.value(), risk.value(),
                                                                        product.currentText(), trans.currentText(),
                                                                        sl_type.currentText(), sl_fixed.value(),
                                                                        target_fixed.value(), sl_pct.value(),
                                                                        target_pct.value(), atr_period.value(),
                                                                        sl_atr.value(), target_atr.value(), dialog))
        layout.addRow(calc_button)

        dialog.setLayout(layout)
        dialog.show()

    def calculate_multi_position(self, index, capital, risk_per_trade, product_type, transaction_type, sl_type,
                                sl_fixed, target_fixed, sl_pct, target_pct, atr_period, sl_atr, target_atr, dialog):
        instrument_token = instruments[self.multi_widgets[index]["symbol"].currentText()]
        live_data = fetch_live_data(apis["market_data"], instrument_token)
        entry_price = live_data.get('ltp', 0) if live_data else 0
        transaction_type = transaction_type if product_type == 'I' else "BUY"  # Default to BUY for Delivery

        if entry_price == 0:
            QMessageBox.critical(self, "Error", "No live price available")
            return

        risk_amount = capital * (risk_per_trade / 100)
        if sl_type == "Fixed Amount":
            stop_loss_diff = sl_fixed
            target_diff = target_fixed
            if transaction_type == "BUY":
                stop_loss_price = entry_price - stop_loss_diff
                target_price = entry_price + target_diff
            else:
                stop_loss_price = entry_price + stop_loss_diff
                target_price = entry_price - target_diff
            quantity = int(risk_amount / stop_loss_diff)
        elif sl_type == "Percentage of Entry":
            stop_loss_diff = entry_price * (sl_pct / 100)
            target_diff = entry_price * (target_pct / 100)
            if transaction_type == "BUY":
                stop_loss_price = entry_price - stop_loss_diff
                target_price = entry_price + target_diff
            else:
                stop_loss_price = entry_price + stop_loss_diff
                target_price = entry_price - target_diff
            quantity = int(risk_amount / stop_loss_diff)
        else:  # ATR Based
            hist_data = get_historical_data(apis["history"], instrument_token)
            atr = calculate_atr(pd.DataFrame(hist_data), atr_period).iloc[-1] if hist_data else 0
            stop_loss_diff = atr * sl_atr
            target_diff = atr * target_atr
            if transaction_type == "BUY":
                stop_loss_price = entry_price - stop_loss_diff
                target_price = entry_price + target_diff
            else:
                stop_loss_price = entry_price + stop_loss_diff
                target_price = entry_price - target_diff
            quantity = int(risk_amount / stop_loss_diff) if atr > 0 else 0

        quantity = max(1, quantity)
        widgets = self.multi_widgets[index]
        widgets["quantity"].setValue(quantity)
        widgets["stop_loss"].setValue(stop_loss_price)  # Absolute price
        widgets["target"].setValue(target_price)  # Absolute price
        dialog.close()

    def place_order(self):
        instrument_token = instruments[self.single_symbol.currentText()]
        quantity = self.single_quantity.value()
        order_type = self.single_order_type.currentText()
        transaction_type = self.single_trans_type.currentText()
        product_type = self.single_product_type.currentText()
        amo = self.single_amo.isChecked()
        price = self.single_price.value() if order_type in ["LIMIT", "SL"] else 0
        trigger_price = self.single_trigger.value() if order_type in ["SL", "SL-M"] else 0
        stop_loss = self.single_stop_loss.value()
        target = self.single_target.value()
        other_order = self.single_other_orders.currentText()

        if other_order == "Auto-sell if Open > Previous Close":
            hist_data = get_historical_data(apis["history"], instrument_token)
            if hist_data is not None:
                prev_close = hist_data["close"].iloc[-2]
                current_open = hist_data["close"].iloc[-1]
                if current_open > prev_close:
                    result = place_order(apis["order"], instrument_token, "SELL", quantity, stop_loss=stop_loss, target=target)
                    if result:
                        QMessageBox.information(self, "Success", f"Auto-sell order placed: Order ID {result.data.order_id}")
                else:
                    QMessageBox.information(self, "Info", "Condition not met: Open price not greater than previous close")
        elif other_order in ["Schedule Order", "Schedule Short Sell at Open"]:
            schedule_datetime = QDateTime(self.single_schedule_date.date(), self.single_schedule_time.time()).toPython()
            if schedule_datetime > datetime.now():
                order_data = {
                    "order_id": f"scheduled_{uuid.uuid4()}",
                    "instrument_token": instrument_token,
                    "transaction_type": "SELL" if other_order == "Schedule Short Sell at Open" else transaction_type,
                    "quantity": quantity,
                    "price": price,
                    "order_type": order_type,
                    "trigger_price": trigger_price,
                    "is_amo": amo,
                    "product_type": product_type,
                    "schedule_datetime": schedule_datetime,
                    "validity": "DAY",
                    "stop_loss": stop_loss,
                    "target": target,
                    "strategy": "short_sell_open" if other_order == "Schedule Short Sell at Open" else None
                }
                scheduled_orders_queue.put(order_data)

                def execute_order(order):
                    sleep((order["schedule_datetime"] - datetime.now()).total_seconds())
                    if order["strategy"] == "short_sell_open":
                        hist_data = get_historical_data(apis["history"], order["instrument_token"])
                        if hist_data:
                            prev_close = hist_data["close"].iloc[-2]
                            current_open = fetch_live_data(apis["market_data"], order["instrument_token"]).get('ltp', 0)
                            if current_open > prev_close:
                                result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                                    order["quantity"], order["price"], order["order_type"],
                                                    order["trigger_price"], order["is_amo"], order["product_type"],
                                                    order["validity"], order["stop_loss"], order["target"])
                                if result:
                                    QMessageBox.information(self, "Success", f"Scheduled order executed: Order ID {result.data.order_id}")
                    else:
                        result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                            order["quantity"], order["price"], order["order_type"],
                                            order["trigger_price"], order["is_amo"], order["product_type"],
                                            order["validity"], order["stop_loss"], order["target"])
                        if result:
                            QMessageBox.information(self, "Success", f"Scheduled order executed: Order ID {result.data.order_id}")

                threading.Thread(target=execute_order, args=(order_data,), daemon=True).start()
                self.update_scheduled_table()
                QMessageBox.information(self, "Scheduled", f"Order scheduled for {schedule_datetime}")
            else:
                QMessageBox.critical(self, "Error", "Schedule time must be in the future")
        else:
            result = place_order(apis["order"], instrument_token, transaction_type, quantity, price, order_type,
                                trigger_price, amo, product_type, "DAY", stop_loss, target)
            if result:
                QMessageBox.information(self, "Success", f"Order placed: Order ID {result.data.order_id}")

    def place_multiple_orders(self):
        orders = []
        for i, widgets in enumerate(self.multi_widgets):
            instrument_token = instruments[widgets["symbol"].currentText()]
            order_data = {
                "order_id": f"multi_scheduled_{i}_{uuid.uuid4()}",
                "instrument_token": instrument_token,
                "quantity": widgets["quantity"].value(),
                "product": widgets["product_type"].currentText(),
                "validity": "DAY",
                "price": 0,
                "tag": f"MultiOrder_{i + 1}",
                "order_type": widgets["order_type"].currentText(),
                "transaction_type": "SELL" if widgets["schedule_short"].isChecked() else widgets["trans_type"].currentText(),
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": widgets["amo"].isChecked(),
                "correlation_id": f"order_{i}",
                "slice": True,
                "schedule_datetime": QDateTime(widgets["schedule_date"].date(), widgets["schedule_time"].time()).toPython() if widgets["schedule_short"].isChecked() else None,
                "stop_loss": widgets["stop_loss"].value(),
                "target": widgets["target"].value(),
                "strategy": "short_sell_open" if widgets["schedule_short"].isChecked() else None
            }
            orders.append(order_data)

        for order in orders:
            if order["schedule_datetime"] and order["schedule_datetime"] > datetime.now():
                scheduled_orders_queue.put(order)

                def execute_multi_order(order):
                    sleep((order["schedule_datetime"] - datetime.now()).total_seconds())
                    if order["strategy"] == "short_sell_open":
                        hist_data = get_historical_data(apis["history"], order["instrument_token"])
                        if hist_data:
                            prev_close = hist_data["close"].iloc[-2]
                            current_open = fetch_live_data(apis["market_data"], order["instrument_token"]).get('ltp', 0)
                            if current_open > prev_close:
                                result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                                    order["quantity"], order["price"], order["order_type"],
                                                    order["trigger_price"], order["is_amo"], order["product"],
                                                    order["validity"], order["stop_loss"], order["target"])
                                if result:
                                    QMessageBox.information(self, "Success", f"Scheduled multi-order executed: Order ID {result.data.order_id}")
                    else:
                        result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                            order["quantity"], order["price"], order["order_type"],
                                            order["trigger_price"], order["is_amo"], order["product"],
                                            order["validity"], order["stop_loss"], order["target"])
                        if result:
                            QMessageBox.information(self, "Success", f"Scheduled multi-order executed: Order ID {result.data.order_id}")

                threading.Thread(target=execute_multi_order, args=(order,), daemon=True).start()
                self.update_scheduled_table()
            else:
                result = place_order(apis["order"], order["instrument_token"], order["transaction_type"],
                                    order["quantity"], order["price"], order["order_type"],
                                    order["trigger_price"], order["is_amo"], order["product"],
                                    order["validity"], order["stop_loss"], order["target"])
                if result:
                    QMessageBox.information(self, "Success", f"Order {order['tag']} placed: Order ID {result.data.order_id}")

    def update_scheduled_table(self):
        scheduled_orders = manage_scheduled_orders()
        self.scheduled_table.setRowCount(len(scheduled_orders))
        for i, order in enumerate(scheduled_orders):
            self.scheduled_table.setItem(i, 0, QTableWidgetItem(order["order_id"]))
            self.scheduled_table.setItem(i, 1, QTableWidgetItem(next((k for k, v in instruments.items() if v == order["instrument_token"]), "")))
            self.scheduled_table.setItem(i, 2, QTableWidgetItem(str(order["quantity"])))
            self.scheduled_table.setItem(i, 3, QTableWidgetItem(order["order_type"]))
            self.scheduled_table.setItem(i, 4, QTableWidgetItem(order["transaction_type"]))
            self.scheduled_table.setItem(i, 5, QTableWidgetItem(order["schedule_datetime"].strftime("%Y-%m-%d %H:%M:%S")))
            self.scheduled_table.setItem(i, 6, QTableWidgetItem(f"{order['stop_loss']:.2f}"))
            btn = QPushButton("Cancel")
            btn.clicked.connect(lambda _, oid=order["order_id"]: self.cancel_scheduled_order(oid))
            self.scheduled_table.setCellWidget(i, 7, btn)

    def modify_scheduled_order(self):
        selected = self.scheduled_table.currentRow()
        if selected >= 0:
            order_id = self.scheduled_table.item(selected, 0).text()
            # Implement modify dialog if needed
            QMessageBox.information(self, "Info", "Modify functionality to be implemented")

    def cancel_scheduled_order(self, order_id):
        scheduled_orders = [o for o in manage_scheduled_orders() if o["order_id"] != order_id]
        while not scheduled_orders_queue.empty():
            scheduled_orders_queue.get()
        for o in scheduled_orders:
            scheduled_orders_queue.put(o)
        self.update_scheduled_table()
        QMessageBox.information(self, "Success", "Scheduled order cancelled")

    def add_auto_order(self):
        auto_order = {
            "instrument_token": instruments[self.auto_symbol.currentText()],
            "transaction_type": "SELL",
            "order_type": "MARKET",
            "product_type": "D",
            "is_amo": False,
            "risk_per_trade": self.auto_risk.value(),
            "stop_loss_type": self.auto_sl_type.currentText(),
            "stop_loss_value": self.auto_sl_fixed.value() if self.auto_sl_type.currentText() == "Fixed Amount" else
                             (self.auto_sl_pct.value() if self.auto_sl_type.currentText() == "Percentage of Entry" else self.auto_sl_atr.value()),
            "target_value": self.auto_target_fixed.value() if self.auto_sl_type.currentText() == "Fixed Amount" else
                           (self.auto_target_pct.value() if self.auto_sl_type.currentText() == "Percentage of Entry" else self.auto_target_atr.value()),
            "atr_period": self.auto_atr_period.value() if self.auto_sl_type.currentText() == "ATR Based" else 14
        }
        self.auto_orders.append(auto_order)
        QMessageBox.information(self, "Success", f"Auto order added for {self.auto_symbol.currentText()}")

    def fetch_token(self):
        self.token_thread = TokenFetchThread()
        self.token_thread.token_fetched.connect(self.on_token_fetched)
        self.token_thread.finished.connect(self.token_thread.deleteLater)  # Prevent crash
        self.token_thread.start()

    def on_token_fetched(self, token):
        # Update your API setup with the new token (e.g., apis["user"]["access_token"] = token)
        QMessageBox.information(self, "Success", f"Access Token Fetched: {token}")
        self.funds_data = get_user_profile_and_funds({"user": "api_user", "access_token": token})  # Update funds
        self.funds_label.setText(f"Funds Available: ₹{self.funds_data['data']['equity']['available_margin']:.2f}")

    def setup_order_book_tab(self):
        self.order_book_tab = QWidget()
        self.tabs.addTab(self.order_book_tab, "Order Book")
        layout = QVBoxLayout(self.order_book_tab)

        # Filter Section
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Filter by Status:"))
        self.order_status_filter = QComboBox()
        self.order_status_filter.addItems(["All", "OPEN", "COMPLETE", "CANCELLED"])
        self.order_status_filter.currentTextChanged.connect(self.update_order_book_table)
        filter_layout.addWidget(self.order_status_filter)
        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Order Book Table
        self.order_book_table = QTableWidget()
        self.order_book_table.setColumnCount(9)
        self.order_book_table.setHorizontalHeaderLabels([
            "Order ID", "Symbol", "Quantity", "Order Type", "Transaction", "Status", "Price", "Trigger Price", "Actions"
        ])
        self.order_book_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.order_book_table)

        # Buttons
        buttons_layout = QHBoxLayout()
        cancel_all_button = QPushButton("Cancel All Pending Orders")
        cancel_all_button.clicked.connect(self.cancel_all_pending_orders)
        buttons_layout.addWidget(cancel_all_button)
        buttons_layout.addStretch()
        layout.addLayout(buttons_layout)

        self.update_order_book_table()

    def update_order_book_table(self):
        orders = apis["order"].get_order_book(api_version='v2').data  # Fetch order book data
        status_filter = self.order_status_filter.currentText()
        if status_filter != "All":
            orders = [order for order in orders if order.get("status", "").upper() == status_filter]

        self.order_book_table.setRowCount(len(orders))
        for i, order in enumerate(orders):
            self.order_book_table.setItem(i, 0, QTableWidgetItem(order.order_id))
            symbol = next((k for k, v in instruments.items() if v == order.instrument_token))
            self.order_book_table.setItem(i, 1, QTableWidgetItem(symbol))
            self.order_book_table.setItem(i, 2, QTableWidgetItem(str(order.quantity)))
            self.order_book_table.setItem(i, 3, QTableWidgetItem(order.order_type))
            self.order_book_table.setItem(i, 4, QTableWidgetItem(order.transaction_type))
            self.order_book_table.setItem(i, 5, QTableWidgetItem(order.status))
            self.order_book_table.setItem(i, 6, QTableWidgetItem(f"{order.price:.2f}"))
            self.order_book_table.setItem(i, 7, QTableWidgetItem(f"{order.trigger_price:.2f}"))

            if order.get("status", "").upper() == "OPEN":
                actions_widget = QWidget()
                actions_layout = QHBoxLayout(actions_widget)
                actions_layout.setContentsMargins(0, 0, 0, 0)
                modify_btn = QPushButton("Modify")
                modify_btn.clicked.connect(lambda _, oid=order["order_id"]: self.modify_order(oid))
                cancel_btn = QPushButton("Cancel")
                cancel_btn.clicked.connect(lambda _, oid=order["order_id"]: self.cancel_order(oid))
                actions_layout.addWidget(modify_btn)
                actions_layout.addWidget(cancel_btn)
                self.order_book_table.setCellWidget(i, 8, actions_widget)
            else:
                self.order_book_table.setItem(i, 8, QTableWidgetItem("N/A"))

    def modify_order(self, order_id):
        order = next((o for o in apis["order"].get_order_book('v2').data if o["order_id"] == order_id), None)
        if not order:
            QMessageBox.critical(self, "Error", "Order not found")
            return

        dialog = QWidget()
        dialog.setWindowTitle(f"Modify Order {order_id}")
        layout = QFormLayout()

        quantity = QSpinBox()
        quantity.setRange(1, 10000)
        quantity.setValue(order.get("quantity", 0))
        layout.addRow("Quantity:", quantity)

        price = QDoubleSpinBox()
        price.setRange(0.0, 10000.0)
        price.setValue(order.get("price", 0))
        layout.addRow("Price:", price)

        trigger_price = QDoubleSpinBox()
        trigger_price.setRange(0.0, 10000.0)
        trigger_price.setValue(order.get("trigger_price", 0))
        layout.addRow("Trigger Price:", trigger_price)

        save_btn = QPushButton("Save")
        save_btn.clicked.connect(
            lambda: self.save_modified_order(order_id, quantity.value(), price.value(), trigger_price.value(), dialog))
        layout.addRow(save_btn)

        dialog.setLayout(layout)
        dialog.show()

    def save_modified_order(self, order_id, quantity, price, trigger_price, dialog):
        result = apis["order"].modify_order(order_id, quantity=quantity, price=price, trigger_price=trigger_price)
        if result:
            QMessageBox.information(self, "Success", f"Order {order_id} modified")
            self.update_order_book_table()
        else:
            QMessageBox.critical(self, "Error", "Failed to modify order")
        dialog.close()

    def cancel_order(self, order_id):
        result = apis["order"].cancel_order(order_id, api_version='v2')
        if result:
            QMessageBox.information(self, "Success", f"Order {order_id} cancelled")
            self.update_order_book_table()
        else:
            QMessageBox.critical(self, "Error", "Failed to cancel order")

    def cancel_all_pending_orders(self):
        result = apis["order"].cancel_multi_order()
        if result:
            QMessageBox.information(self, "Success", "All pending orders cancelled")
            self.update_order_book_table()
        else:
            QMessageBox.critical(self, "Error", "Failed to cancel all orders")

    def setup_positions_tab(self):
        self.positions_tab = QWidget()
        self.tabs.addTab(self.positions_tab, "Positions")
        layout = QVBoxLayout(self.positions_tab)

        self.positions_table = QTableWidget()
        self.positions_table.setColumnCount(6)
        self.positions_table.setHorizontalHeaderLabels(["Symbol", "Quantity", "Avg Price", "LTP", "P&L", "Actions"])
        self.positions_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.positions_table)

        self.update_positions_table()

    def update_positions_table(self):
        positions = apis["portfolio"].get_positions(api_version="v2").data  # Fetch position data
        self.positions_table.setRowCount(len(positions))
        for i, pos in enumerate(positions):
            symbol = next((k for k, v in instruments.items() if v == pos.get("instrument_token")), "")
            self.positions_table.setItem(i, 0, QTableWidgetItem(symbol))
            quantity = pos.get("quantity", 0)
            self.positions_table.setItem(i, 1, QTableWidgetItem(str(quantity)))
            avg_price = pos.get("avg_price", 0)
            self.positions_table.setItem(i, 2, QTableWidgetItem(f"{avg_price:.2f}"))
            ltp = fetch_live_data(apis["market_data"], pos.get("instrument_token")).get("ltp", 0)
            self.positions_table.setItem(i, 3, QTableWidgetItem(f"{ltp:.2f}"))
            pnl = (ltp - avg_price) * quantity if quantity > 0 else (avg_price - ltp) * -quantity
            self.positions_table.setItem(i, 4, QTableWidgetItem(f"{pnl:.2f}"))

            if quantity != 0:  # Open position
                square_off_btn = QPushButton("Square Off")
                square_off_btn.clicked.connect(
                    lambda _, token=pos["instrument_token"], qty=quantity: self.square_off_position(token, qty))
                self.positions_table.setCellWidget(i, 5, square_off_btn)
            else:
                self.positions_table.setItem(i, 5, QTableWidgetItem("N/A"))

    def square_off_position(self, instrument_token, quantity):
        transaction_type = "SELL" if quantity > 0 else "BUY"
        result = apis["order"].place_order(instrument_token, transaction_type, abs(quantity))
        if result:
            QMessageBox.information(self, "Success", f"Position squared off")
            self.update_positions_table()
        else:
            QMessageBox.critical(self, "Error", "Failed to square off position")

    def start_scheduler(self):
        def place_auto_orders():
            capital = self.funds_data["data"]["equity"]["available_margin"]
            for order in self.auto_orders:
                hist_data = get_historical_data(apis["history"], order["instrument_token"])
                live_data = fetch_live_data(apis["market_data"], order["instrument_token"])
                if hist_data and live_data:
                    prev_close = hist_data["close"].iloc[-2]
                    current_open = live_data.get('ltp', 0)
                    if current_open > prev_close:
                        risk_amount = capital * (order["risk_per_trade"] / 100)
                        entry_price = current_open
                        if order["stop_loss_type"] == "Fixed Amount":
                            stop_loss_diff = order["stop_loss_value"]
                            target_diff = order["target_value"]
                            stop_loss = entry_price + stop_loss_diff  # SELL, so SL is above entry
                            target = entry_price - target_diff  # SELL, so target is below entry
                            quantity = int(risk_amount / stop_loss_diff)
                        elif order["stop_loss_type"] == "Percentage of Entry":
                            stop_loss_diff = entry_price * (order["stop_loss_value"] / 100)
                            target_diff = entry_price * (order["target_value"] / 100)
                            stop_loss = entry_price + stop_loss_diff  # SELL
                            target = entry_price - target_diff  # SELL
                            quantity = int(risk_amount / stop_loss_diff)
                        else:  # ATR Based
                            df = pd.DataFrame(hist_data)
                            df['ATR'] = calculate_atr(df, order["atr_period"])
                            atr = df['ATR'].iloc[-1] if not df['ATR'].empty else 0
                            stop_loss_diff = atr * order["stop_loss_value"]
                            target_diff = atr * order["target_value"]
                            stop_loss = entry_price + stop_loss_diff  # SELL
                            target = entry_price - target_diff  # SELL
                            quantity = int(risk_amount / stop_loss_diff) if atr > 0 else 1

                        quantity = max(1, quantity)
                        result = place_order(apis["order"], order["instrument_token"], "SELL", quantity, 0, "MARKET", 0,
                                            order["is_amo"], order["product_type"], stop_loss, target)
                        if result:
                            QMessageBox.information(self, "Success", f"Auto Short Sell Placed: Order ID {result.data.order_id}")

        schedule.every().day.at("09:15").do(place_auto_orders)
        threading.Thread(target=lambda: [schedule.run_pending(), sleep(1)] * 10000, daemon=True).start()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # Add application-wide font
    font = QFont("Arial", 10)
    app.setFont(font)
    window = OrderManagementWindow()
    window.show()
    sys.exit(app.exec())