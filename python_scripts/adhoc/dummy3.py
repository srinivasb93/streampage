import datetime
import sys
import traceback

def exception_hook(exc_type, exc_value, exc_traceback):
    print("An unhandled exception occurred:")
    print("Type:", exc_type.__name__)
    print("Value:", str(exc_value))
    print("Traceback:")
    traceback.print_tb(exc_traceback)
    input("Press Enter to exit...")  # Keep console window open

sys.excepthook = exception_hook


try:
    from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QWidget, QAction, QListWidget,
                                 QComboBox, QLineEdit, QPushButton, QColorDialog, QFormLayout, QDateEdit, QProgressBar,
                                 QHBoxLayout, QLabel, QTableWidget, QTableWidgetItem, QSlider, QStatusBar, QDialog)
    from lightweight_charts.widgets import QtChart
    from PyQt5.QtCore import Qt, QTimer, QDate, pyqtSignal
    from PyQt5.QtGui import QColor, QFont, QIcon
    import pandas as pd
    from common_utils import read_write_sql_data as rd
    import yfinance as yf
    import traceback
    import numpy as np
    from scipy.signal import argrelextrema
    import pandas_ta as ta


    CLOSE = '×'
    FULLSCREEN = '□'


    class StockChartApp(QMainWindow):
        update_progress = pyqtSignal(int)
        def __init__(self):
            super().__init__()

            self.setWindowTitle("Advanced Stock Chart App")
            self.setGeometry(100, 100, 1200, 800)

            # Initialize settings
            self.data_source = 'SQL'
            self.symbol = 'TATAMOTORS'
            self.timeframe = 'Daily'
            self.indicator = None
            self.indicator_period = 20
            self.indicator_color = '#0000FF'
            self.drawings = []
            self.sub_charts = 1
            self.support_resistance_lines = []  # Store references to support/resistance lines
            self.hide_portfolio = True

            # Initialize replay variables
            self.replay_data = None
            self.indicators_data = None
            self.patterns_data = None
            self.replay_index = 0
            self.replay_timer = QTimer(self)
            self.replay_speed = 1000  # Speed of replay in milliseconds (1 second)
            self.replay_start_date = datetime.date.today()
            self.replay_date_updated = False
            self.is_paused = False
            self.reset_replay = False

            # Create the start_date_edit attribute
            self.start_date_edit = QDateEdit(self)
            self.start_date_edit.setCalendarPopup(True)
            self.start_date_edit.setDate(QDate.currentDate())  # Set default to current date
            self.start_date_edit.dateChanged.connect(self.update_replay_start_date)

            # Central widget setup
            self.central_widget = QWidget(self)
            self.setCentralWidget(self.central_widget)

            # Create a vertical layout for the central widget
            self.main_layout = QVBoxLayout(self.central_widget)

            # Create a widget to hold the charts
            self.charts_widget = QWidget(self.central_widget)
            self.charts_layout = QVBoxLayout(self.charts_widget)
            self.main_layout.addWidget(self.charts_widget)

            # # Create the chart
            # self.chart = QtChart(self.central_widget, toolbox=True)

            # Create the main chart
            self.chart = QtChart(self.charts_widget, toolbox=True, scale_candles_only=True)
            self.charts_layout.addWidget(self.chart.get_webview(), 80)

            # # Add chart to layout
            # self.layout.addWidget(self.chart.get_webview())
            self.indicators = []
            self.patterns = []
            self.rsi_chart = None  # Store reference to RSI chart
            self.macd_chart = None
            self.main_chart_h = 1
            self.main_chart_w = 1
            self.stock_data = []
            self.max_data_date = datetime.date.today()

            # Create the menu bar
            self.create_menubar()

            # Add progress bar
            self.progress_bar = QProgressBar(self)
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(0)
            self.progress_bar.setTextVisible(False)

            # Add current date label
            self.current_date_label = QLabel("Current Date: ", self)
            self.current_date_label.setAlignment(Qt.AlignCenter)
            self.current_date_label.setFont(QFont("Arial", 10, QFont.Bold))

            # Set up the status bar (bottom bar) for replay controls
            self.create_statusbar()

            # Set up replay timer
            self.replay_timer.timeout.connect(self.update_replay)

            # Setup chart with default stock
            self.setup_chart(apply_patterns=True)

            # Connect update_progress signal
            self.update_progress.connect(self.progress_bar.setValue)

        def create_menubar(self):
            """Create the menu bar with various options."""
            menubar = self.menuBar()

            # Data Source Selection
            data_source_menu = QComboBox(self)
            data_source_menu.addItems(['SQL', 'Yahoo'])
            data_source_menu.currentTextChanged.connect(self.on_data_source_change)

            # Stock Selection
            self.stock_selection_menu = QComboBox(self)
            self.update_stock_selection_menu()  # Load SQL stocks by default
            self.stock_selection_menu.currentTextChanged.connect(self.on_stock_change)

            # Timeframe Selection
            timeframe_menu = QComboBox(self)
            timeframe_menu.addItems(['Daily', 'Weekly', 'Monthly'])
            timeframe_menu.currentTextChanged.connect(self.on_timeframe_change)

            # Add Indicator Action
            apply_indicator_action = QAction('Apply Indicator', self)
            apply_indicator_action.triggered.connect(self.open_indicator_dialog)

            # Modify the Clear Indicators Action
            clear_indicator_action = QAction('Clear Indicators', self)
            clear_indicator_action.triggered.connect(self.clear_indicators_and_patterns)

            # Apply Patterns (Support/Resistance, Trendlines)
            apply_patterns_action = QAction('Apply Patterns', self)
            apply_patterns_action.triggered.connect(self.apply_patterns)

            # Show Portfolio Action
            show_portfolio_action = QAction('Show Portfolio', self)
            show_portfolio_action.triggered.connect(self.show_hide_portfolio)

            # Sub-chart Creation
            sub_chart_menu = QComboBox(self)
            sub_chart_menu.addItems([str(i) for i in range(1, 5)])
            sub_chart_menu.currentTextChanged.connect(self.on_sub_chart_change)

            # Adding widgets to the menu bar
            data_source_widget = QWidget()
            data_source_layout = QHBoxLayout()
            data_source_layout.addWidget(QLabel('Data Source:'))
            data_source_layout.addWidget(data_source_menu)
            data_source_layout.addWidget(QLabel('Stock:'))
            data_source_layout.addWidget(self.stock_selection_menu)
            data_source_layout.addWidget(QLabel('Timeframe:'))
            data_source_layout.addWidget(timeframe_menu)
            data_source_layout.setContentsMargins(0, 0, 0, 0)
            data_source_widget.setLayout(data_source_layout)

            # Add the widget to the menu bar
            menubar.setCornerWidget(data_source_widget, Qt.TopLeftCorner)

            # Add indicator and clear buttons as actions
            menubar.addAction(apply_indicator_action)
            menubar.addAction(clear_indicator_action)
            menubar.addAction(apply_patterns_action)
            menubar.addAction(show_portfolio_action)

            # Add sub-chart creation menu
            sub_chart_widget = QWidget()
            sub_chart_layout = QHBoxLayout()
            sub_chart_layout.addWidget(QLabel('Sub-charts:'))
            sub_chart_layout.addWidget(sub_chart_menu)
            sub_chart_layout.setContentsMargins(0, 0, 0, 0)
            sub_chart_widget.setLayout(sub_chart_layout)

            menubar.setCornerWidget(sub_chart_widget, Qt.TopRightCorner)

        def create_statusbar(self):
            """Create the status bar (bottom bar) for replay controls."""
            statusbar = QStatusBar(self)

            # Add Replay Actions
            self.start_button = QPushButton('Start Replay')
            self.start_button.clicked.connect(self.start_replay)
            self.start_button.setIcon(QIcon('icons/play.png'))

            self.pause_resume_button = QPushButton('Pause Replay')
            self.pause_resume_button.clicked.connect(self.pause_resume_replay)
            self.pause_resume_button.setIcon(QIcon('icons/pause-play.png'))

            self.stop_button = QPushButton('Stop Replay')
            self.stop_button.clicked.connect(self.stop_replay)
            self.stop_button.setIcon(QIcon('icons/stop.png'))

            # Add Start Date Selection
            start_date_label = QLabel('Start Date:')
            # We're now using the self.start_date_edit created in __init__

            # Replay Speed Slider
            replay_speed_slider = QSlider(Qt.Horizontal, self)
            replay_speed_slider.setRange(100, 5000)  # Range from 100 ms to 5000 ms
            replay_speed_slider.setValue(self.replay_speed)
            replay_speed_slider.setToolTip('Replay Speed (ms)')
            replay_speed_slider.valueChanged.connect(self.change_replay_speed)

            # Add widgets to the status bar
            statusbar.addWidget(start_date_label)
            statusbar.addWidget(self.start_date_edit)
            statusbar.addWidget(self.start_button)
            statusbar.addWidget(self.pause_resume_button)
            statusbar.addWidget(self.stop_button)
            statusbar.addWidget(QLabel('Replay Speed:'))
            statusbar.addWidget(replay_speed_slider)

            # Add progress bar to status bar
            statusbar.addPermanentWidget(self.progress_bar, 1)

            # Add current date label to status bar
            statusbar.addPermanentWidget(self.current_date_label)

            # Add replay speed label
            self.speed_label = QLabel("1x", self)
            statusbar.addPermanentWidget(self.speed_label)

            self.setStatusBar(statusbar)

        def on_data_source_change(self, data_source):
            self.data_source = data_source
            if self.data_source == 'SQL':
                self.update_stock_selection_menu()
            else:
                self.stock_selection_menu.setEditable(True)
            self.setup_chart()

        def on_stock_change(self, stock):
            self.symbol = stock
            self.setup_chart()
            self.clear_indicators_and_patterns()

        def on_timeframe_change(self, timeframe):
            self.timeframe = timeframe
            self.setup_chart(keep_drawings=True)
            self.clear_indicators_and_patterns(keep_patterns=True)

        def on_sub_chart_change(self, count):
            self.sub_charts = int(count)

        def update_stock_selection_menu(self):
            self.stock_selection_menu.clear()
            if self.data_source == 'SQL':
                stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
                stock_list = stock_list_df['SYMBOL'].values.tolist()
                self.stock_selection_menu.addItems(stock_list)
            else:
                self.stock_selection_menu.setEditable(True)

        def setup_chart(self,
                        apply_indicators=False,
                        apply_patterns=False,
                        custom_data=None,
                        hist_data=None,
                        in_replay=False,
                        keep_drawings=False):
            if in_replay:
                self.stock_data = custom_data
            else:
                if self.data_source == 'SQL':
                    postfix = ''
                    if self.timeframe == 'Weekly':
                        postfix = '_W'
                    elif self.timeframe == 'Monthly':
                        postfix = '_M'
                    self.stock_data = rd.get_table_data(selected_table=self.symbol + postfix, sort=True, sort_by='Date')
                else:
                    interval = '1d'
                    if self.timeframe == 'Weekly':
                        interval = '1wk'
                    elif self.timeframe == 'Monthly':
                        interval = '1mo'
                    self.stock_data = yf.download(self.symbol + '.NS', interval=interval)

                self.max_data_date = max(self.stock_data['Date'].dt.date)

            if isinstance(self.stock_data, pd.DataFrame) and not self.stock_data.empty:
                # Filter data up to the selected start date
                if not in_replay:
                    if self.replay_start_date and self.replay_date_updated:
                        if self.replay_start_date < self.max_data_date:
                            self.replay_data = self.stock_data.rename(
                                {'Date': 'time',
                                 'Open': 'open',
                                 'High': 'high',
                                 'Low': 'low',
                                 'Close': 'close',
                                 'Volume': 'volume'}, axis=1)
                            self.replay_index = 0
                            start_date = self.replay_start_date.toPyDate()
                            self.stock_data = self.stock_data[self.stock_data['Date'].dt.date <= start_date]

                    candlestick_data = pd.DataFrame({
                        'time': pd.to_datetime(
                            self.stock_data.index if self.data_source == 'Yahoo' else self.stock_data['Date']),
                        'open': self.stock_data['Open'],
                        'high': self.stock_data['High'],
                        'low': self.stock_data['Low'],
                        'close': self.stock_data['Close'],
                        'volume': self.stock_data['Volume']
                    })

                # Set up main chart
                if not in_replay:
                    self.chart.set(candlestick_data if not in_replay else self.stock_data, keep_drawings=keep_drawings)
                    self.chart.watermark(self.symbol)
                    self.chart.legend(True, font_size=24, color_based_on_candle=True)
                else:
                    self.chart.update(hist_data)

                if not in_replay:
                    min_date = candlestick_data['time'].min().date()
                    max_date = candlestick_data['time'].max().date()
                else:
                    min_date = self.stock_data['time'].min().date()
                    max_date = self.stock_data['time'].max().date()
                # Update the date range for the QDateEdit
                if self.replay_start_date and self.replay_date_updated:
                    self.start_date_edit.setDateRange(QDate(min_date), QDate(max_date))
                    self.current_date_label.setText(f"Current Date: {QDate(max_date).toPyDate()}")

                if apply_indicators:
                    if self.indicators:
                        self.apply_indicator(direct_call=False, in_replay=in_replay)

                if apply_patterns and self.chart.lines():
                    if self.data_source == 'SQL':
                        stock_data = rd.get_table_data(selected_table=self.symbol + '_M', sort=True, sort_by='Date')
                    else:
                        stock_data = yf.download(self.symbol)

                    start_date = self.replay_start_date.toPyDate()
                    self.stock_data = stock_data[stock_data['Date'].dt.date <= start_date]

                    self.apply_patterns(direct_call=False, in_replay=in_replay)

                print(f"Chart setup complete. Date range: {min_date} to {max_date}")
            else:
                print("Error: Stock data is not in the expected format.")

        def start_replay(self):
            try:
                if self.replay_data is not None and not self.replay_timer.isActive():
                    print("Starting replay...")
                    print(f"Replay data shape: {self.replay_data.shape}")
                    print(f"Selected start date: {self.replay_start_date}")

                    if self.replay_start_date:
                        # Convert QDate to datetime.date
                        py_date = self.replay_start_date.toPyDate()
                        print(f"Converted start date: {py_date}")

                        # Find the index of the first data point on or after the selected start date
                        start_index = self.replay_data[self.replay_data['time'].dt.date >= py_date].index.min()
                        print(f"Calculated start index: {start_index}")

                        if pd.notna(start_index):
                            self.replay_index = start_index
                            print(f"Setting replay index to: {self.replay_index}")
                        else:
                            print("Selected start date is after all available data. Starting from the beginning.")
                            self.replay_index = 0
                    else:
                        print("No start date selected. Starting from the beginning.")
                        self.replay_index = 0

                    self.replay_timer.start(self.replay_speed)
                    print("Replay timer started.")
                    self.progress_bar.setValue(0)
                    # Update replay speed every frame
                    self.replay_timer.timeout.connect(self.update_replay_speed)

                    # Update button states
                    self.start_button.setEnabled(False)
                    self.pause_resume_button.setText('Pause Replay')
                    self.pause_resume_button.setIcon(QIcon('path_to_pause_icon.png'))
                    self.pause_resume_button.setEnabled(True)
                    self.stop_button.setEnabled(True)
                    self.is_paused = False
                else:
                    print("Unable to start replay. Check if data is loaded and timer is not active.")
            except Exception as e:
                print(f"Error in start_replay: {str(e)}")
                print(traceback.format_exc())

        def update_replay_start_date(self, date):
            """Update the replay start date when the user selects a new date."""
            self.replay_start_date = date
            self.replay_date_updated = True
            print(f"Updated replay start date to: {self.replay_start_date.toString('yyyy-MM-dd')}")
            self.setup_chart(apply_indicators=True, apply_patterns=True)

        # def update_replay(self):
        #     try:
        #         if self.replay_index < len(self.replay_data):
        #             current_data = self.replay_data.iloc[:self.replay_index + 1]
        #             indicator_data = self.calculate_indicators(current_data)
        #             self.chart.set(current_data)
        #
        #             # Update indicators
        #             for indicator in self.indicators:
        #                 if indicator['type'] == 'EMA':
        #                     for line in self.chart.lines():
        #                         if line.name == f"EMA_{indicator['period']}":
        #                             line.set(indicator_data[['time', f"{indicator['type']}_{indicator['period']}"]])
        #                 elif indicator['type'] == 'Lin_Reg':
        #                     for line in self.chart.lines():
        #                         if line.name == f"Lin_Reg_{indicator['period']}":
        #                             line.set(indicator_data[['time', f"{indicator['type']}_{indicator['period']}"]])
        #                 elif indicator['type'] == 'RSI' and self.rsi_chart:
        #                     self.rsi_chart.set(indicator_data[['time', f"RSI_{indicator['period']}"]])
        #                 elif indicator['type'] == 'MACD' and self.macd_chart:
        #                     macd_line = self.macd_chart.create_line('MACD', color='red')
        #                     signal_line = self.macd_chart.create_line('Signal', color='green')
        #                     histogram = self.macd_chart.create_histogram('Histogram', color='cyan')
        #                     macd_data = indicator_data[['time', 'MACD', 'Signal', 'Histogram']]
        #                     macd_line.set(macd_data[['time', 'MACD']])
        #                     signal_line.set(macd_data[['time', 'Signal']])
        #                     histogram.set(macd_data[['time', 'Histogram']])
        #
        #             # Update progress bar
        #             progress = int((self.replay_index / len(self.replay_data)) * 100)
        #             self.update_progress.emit(progress)
        #
        #             # Update current date label
        #             current_date = self.replay_data.iloc[self.replay_index]['time'].strftime('%Y-%m-%d')
        #             self.current_date_label.setText(f"Current Date: {current_date}")
        #
        #             self.replay_index += 1
        #         else:
        #             self.stop_replay()
        #     except Exception as e:
        #         print(f"Error in update_replay: {str(e)}")
        #         self.stop_replay()

        def update_replay(self):
            try:
                if self.replay_index < len(self.replay_data):
                    current_data = self.replay_data.iloc[:self.replay_index + 1]
                    hist_data = self.replay_data.iloc[self.replay_index + 1:]
                    self.setup_chart(apply_indicators=True,
                                     apply_patterns=True,
                                     custom_data=current_data,
                                     hist_data=hist_data.iloc[0],
                                     in_replay=True)

                    # Update progress bar
                    progress = int((self.replay_index / len(self.replay_data)) * 100)
                    self.update_progress.emit(progress)

                    # Update current date label
                    current_date = self.replay_data.iloc[self.replay_index]['time'].strftime('%Y-%m-%d')
                    self.current_date_label.setText(f"Current Date: {current_date}")

                    self.replay_index += 1
                else:
                    self.stop_replay()
            except Exception as e:
                print(f"Error in update_replay: {str(e)}")
                self.stop_replay()

        def change_replay_speed(self, value):
            self.replay_speed = value
            if self.replay_timer.isActive():
                self.replay_timer.setInterval(self.replay_speed)

            # Update speed label
            speed_multiplier = 1000 / value
            self.speed_label.setText(f"{speed_multiplier:.1f}x")

        def update_replay_speed(self):
            # Gradually increase replay speed
            current_speed = self.replay_timer.interval()
            if current_speed > 100:  # Minimum interval of 100ms
                new_speed = max(current_speed - 5, 100)
                self.replay_timer.setInterval(new_speed)
                speed_multiplier = 1000 / new_speed
                self.speed_label.setText(f"{speed_multiplier:.1f}x")

        def keyPressEvent(self, event):
            if event.key() == Qt.Key_Space:
                if self.replay_timer.isActive() or self.is_paused:
                    self.pause_resume_replay()
                else:
                    self.start_replay()
            elif event.key() == Qt.Key_Left:
                self.replay_index = max(0, self.replay_index - 10)
                self.update_replay()
            elif event.key() == Qt.Key_Right:
                self.replay_index = min(len(self.replay_data) - 1, self.replay_index + 10)
                self.update_replay()

        def stop_replay(self):
            if self.replay_timer.isActive():
                self.replay_timer.stop()
            print("Replay stopped.")
            self.replay_index = 0
            self.progress_bar.setValue(0)
            self.setup_chart()
            self.clear_indicators_and_patterns()

            # Reset button states
            self.start_button.setEnabled(True)
            self.pause_resume_button.setText('Pause Replay')
            self.pause_resume_button.setIcon(QIcon('path_to_pause_icon.png'))
            self.pause_resume_button.setEnabled(False)
            self.stop_button.setEnabled(False)
            self.is_paused = False
            self.update_replay_start_date(QDate.currentDate())
            self.start_date_edit.setDate(QDate.currentDate())
            self.reset_replay = False

        def pause_resume_replay(self):
            if self.replay_timer.isActive():
                self.replay_timer.stop()
                self.pause_resume_button.setText('Resume Replay')
                self.pause_resume_button.setIcon(QIcon('path_to_resume_icon.png'))
                self.is_paused = True
            else:
                self.replay_timer.start(self.replay_speed)
                self.pause_resume_button.setText('Pause Replay')
                self.pause_resume_button.setIcon(QIcon('path_to_pause_icon.png'))
                self.is_paused = False

        def open_indicator_dialog(self):
            """Open a dialog box for selecting the indicator type, period, and color."""
            dialog = QDialog(self)
            dialog.setWindowTitle('Add Indicator')
            dialog.setModal(True)

            # Form layout
            form_layout = QFormLayout()

            # Dropdown for Indicator Type
            self.indicator_dropdown = QComboBox()
            self.indicator_dropdown.addItems(['EMA', 'Lin_Reg', 'RSI', 'MACD'])
            form_layout.addRow('Indicator Type:', self.indicator_dropdown)

            # Dropdown for Period
            self.period_dropdown = QComboBox()
            self.period_dropdown.addItems([str(p) for p in [6, 10, 14, 20, 50, 100, 200]])
            form_layout.addRow('Period:', self.period_dropdown)

            # Button for Color Picker
            self.color_picker_btn = QPushButton('Select Color')
            self.color_picker_btn.clicked.connect(self.open_color_picker)
            self.selected_color = QColor(0, 0, 255)  # Default to blue
            form_layout.addRow('Indicator Color:', self.color_picker_btn)

            # Submit and Cancel buttons
            button_layout = QHBoxLayout()
            submit_btn = QPushButton('Submit')
            submit_btn.clicked.connect(lambda: self.apply_indicator(dialog))
            cancel_btn = QPushButton('Cancel')
            cancel_btn.clicked.connect(dialog.close)
            button_layout.addWidget(submit_btn)
            button_layout.addWidget(cancel_btn)

            form_layout.addRow(button_layout)

            dialog.setLayout(form_layout)
            dialog.exec_()

        def open_color_picker(self):
            """Open color picker dialog to select indicator color."""
            color = QColorDialog.getColor(self.selected_color, self, "Select Indicator Color")
            if color.isValid():
                self.selected_color = color
                self.color_picker_btn.setStyleSheet(f'background-color: {color.name()};')

        def calculate_indicators(self, data):
            indicator_data = data.copy()
            for indicator in self.indicators:
                if indicator['type'] == 'EMA':
                    indicator_data[f'EMA_{indicator["period"]}'] = indicator_data['close'].ewm(
                        span=indicator['period']).mean()
                elif indicator['type'] == 'Lin_Reg':
                    indicator_data[f'Lin_Reg_{indicator["period"]}'] = ta.linreg(
                        indicator_data['close'], length=indicator['period'])
                elif indicator['type'] == 'RSI':
                    indicator_data[f'RSI_{indicator["period"]}'] = ta.rsi(
                        indicator_data['close'], timeperiod=indicator['period'])
                elif indicator['type'] == 'MACD':
                    macd = ta.macd(indicator_data['close'])
                    indicator_data['MACD'] = macd['MACD_12_26_9']
                    indicator_data['Signal'] = macd['MACDs_12_26_9']
                    indicator_data['Histogram'] = macd['MACDh_12_26_9']

            return indicator_data

        def apply_indicator(self, dialog=None, direct_call=True, in_replay=False):

            if direct_call:
                indicator = {
                    'type': self.indicator_dropdown.currentText(),
                    'period': int(self.period_dropdown.currentText()),
                    'color': self.selected_color.name()
                }
                self.indicators.append(indicator)

            if isinstance(self.stock_data, pd.DataFrame) and not self.stock_data.empty:
                if not in_replay:
                    indicator_data = pd.DataFrame({
                        'time': pd.to_datetime(self.stock_data['Date']),  # Index should be used as time
                        'close': self.stock_data['Close']
                    })
                else:
                    indicator_data = self.stock_data[['time', 'close']].copy()
                stock_data = self.calculate_indicators(indicator_data)

                if not direct_call:
                    for indicator in self.indicators:
                        if indicator['type'] in ['EMA', 'Lin_Reg']:
                            indicator_line = self.chart.create_line(
                                name=f"{indicator['type']}_{indicator['period']}",
                                color=indicator['color'],
                                width=2,
                                price_label=True
                            )
                            indicator_line.set(stock_data[['time', f"{indicator['type']}_{indicator['period']}"]].dropna())
                        elif indicator['type'] == 'RSI':
                            self.setup_rsi_chart(stock_data)
                        elif indicator['type'] == 'MACD':
                            self.setup_macd_chart(stock_data)

                        self.adjust_chart_layout()
                else:
                    if indicator['type'] in ['EMA', 'Lin_Reg']:
                        indicator_line = self.chart.create_line(
                            name=f"{indicator['type']}_{indicator['period']}",
                            color=indicator['color'],
                            width=2,
                            price_label=True
                        )
                        indicator_line.set(stock_data[['time', f"{indicator['type']}_{indicator['period']}"]].dropna())
                    elif indicator['type'] == 'RSI':
                        self.setup_rsi_chart(stock_data)
                    elif indicator['type'] == 'MACD':
                        self.setup_macd_chart(stock_data)

                    self.adjust_chart_layout()

                    if dialog:
                        dialog.accept()
            else:
                print("Error: Could not apply the indicator. Stock data not available.")

        def setup_rsi_chart(self, stock_data):
            if not self.rsi_chart:
                self.rsi_chart = self.chart.create_subchart(position='bottom',
                                                            width=1, height=0.2,
                                                            sync=True,
                                                            toolbox=True)
                self.rsi_chart.legend(True, font_size=18, text='RSI', color_based_on_candle=True)
                self.rsi_chart.horizontal_line(70, color='red', width=1, style='dashed')
                self.rsi_chart.horizontal_line(30, color='green', width=1, style='solid')
                self.rsi_chart.fit()

                # Add close button
                self.add_close_button(self.rsi_chart, 'rsi')

            rsi_line = self.rsi_chart.create_line(
                name=f"RSI_{self.indicators[-1]['period']}",
                color=self.indicators[-1]['color'],
                width=2,
                price_label=True
            )
            rsi_data = stock_data[['time', f"RSI_{self.indicators[-1]['period']}"]]
            rsi_line.set(rsi_data)

        def setup_macd_chart(self, stock_data):
            if not self.macd_chart:
                self.macd_chart = self.chart.create_subchart(position='bottom', width=1, height=0.2, sync=True,
                                                             toolbox=True)
                self.macd_chart.legend(True, font_size=18, text='MACD', color_based_on_candle=True)

                # Add close button
                self.add_close_button(self.macd_chart, 'macd')

            macd_line = self.macd_chart.create_line(name='MACD', color='blue', width=2, price_label=True)
            signal_line = self.macd_chart.create_line(name='Signal', color='red', width=2, price_label=True)
            histogram = self.macd_chart.create_histogram(name='Histogram', color='green')

            macd_data = stock_data[['time', 'MACD', 'Signal', 'Histogram']]
            macd_line.set(macd_data[['time', 'MACD']])
            signal_line.set(macd_data[['time', 'Signal']])
            histogram.set(macd_data[['time', 'Histogram']])

        def add_close_button(self, chart, chart_type):
            chart.topbar.button('max', FULLSCREEN, False, align='right', func=lambda: self.on_max(chart, chart_type))

        def on_max(self, target_chart, chart_type):
            button = target_chart.topbar['max']
            if button.value == CLOSE:
                self.adjust_chart_layout()
                button.set(FULLSCREEN)
            else:
                self.maximize_chart(target_chart, chart_type)
                button.set(CLOSE)

        def maximize_chart(self, target_chart, chart_type):
            if chart_type == 'main':
                self.main_chart_h = 1
                self.main_chart_w = 1
                if self.rsi_chart:
                    self.rsi_chart.resize(0, 0)
                if self.macd_chart:
                    self.macd_chart.resize(0, 0)
            elif chart_type == 'rsi':
                self.main_chart_h = 0
                self.rsi_chart.resize(1, 1)
                if self.macd_chart:
                    self.macd_chart.resize(0, 0)
            elif chart_type == 'macd':
                self.main_chart_h = 0
                self.macd_chart.resize(1, 1)
                if self.rsi_chart:
                    self.rsi_chart.resize(0, 0)

            self.chart.resize(self.main_chart_w, self.main_chart_h)

        def adjust_chart_layout(self):
            num_indicators = sum(1 for chart in [self.rsi_chart, self.macd_chart] if chart is not None)

            if num_indicators == 0:
                self.main_chart_h = 1
            elif num_indicators == 1:
                self.main_chart_h = 0.8
            else:
                self.main_chart_h = 0.6

            self.chart.resize(self.main_chart_w, self.main_chart_h)

            indicator_height = (1 - self.main_chart_h) / num_indicators if num_indicators > 0 else 0

            if self.rsi_chart:
                self.rsi_chart.resize(1, indicator_height)
            if self.macd_chart:
                self.macd_chart.resize(1, indicator_height)

        def close_chart(self, chart_type):
            if chart_type == 'rsi' and self.rsi_chart:
                self.rsi_chart.resize(0, 0)
                self.rsi_chart = None
                self.indicators = [ind for ind in self.indicators if ind['type'] != 'RSI']
            elif chart_type == 'macd' and self.macd_chart:
                self.macd_chart.resize(0, 0)
                self.macd_chart = None
                self.indicators = [ind for ind in self.indicators if ind['type'] != 'MACD']

            self.adjust_chart_layout()

        def clear_support_resistance_lines(self):
            for line in self.support_resistance_lines:
                line.delete()
            self.support_resistance_lines = []

        def clear_indicators_and_patterns(self, keep_patterns=False, keep_indicators=False):
            """Clear all indicators and patterns from the chart."""
            if not keep_patterns:
                print("deleteing patterns")
                self.clear_support_resistance_lines()
                self.chart.clear_markers()

            if not keep_indicators:
                print("deleting indicators")
                self.indicators.clear()
                for line in self.chart.lines():
                    line.delete()

                if self.rsi_chart:
                    # self.charts_layout.removeWidget(self.rsi_chart.get_webview())
                    # self.rsi_chart.get_webview().deleteLater()
                    self.rsi_chart.resize(0, 0)
                    self.chart.resize(1, 1)
                    self.rsi_chart = None
                if self.macd_chart:
                    # self.charts_layout.removeWidget(self.rsi_chart.get_webview())
                    # self.rsi_chart.get_webview().deleteLater()
                    self.macd_chart.resize(0, 0)
                    self.chart.resize(1, 1)
                    self.macd_chart = None
            print("All indicators and patterns cleared.")

        def apply_patterns(self, direct_call=True, in_replay=False):
            """Apply chart patterns such as Support/Resistance lines."""
            if self.data_source == 'SQL':
                stock_data = rd.get_table_data(selected_table=self.symbol + '_M', sort=True, sort_by='Date')
            else:
                stock_data = yf.download(self.symbol)

            stock_data = stock_data[stock_data['Date'].dt.date <= self.replay_start_date]
            # Clear existing support/resistance lines
            self.clear_support_resistance_lines()

            # Calculate support and resistance levels
            support, resistance, sup_dates, res_dates = self.calculate_support_resistance(stock_data)
            print('Fetched support and resistance levels')
            print(f'support - {support}')
            print(f'Resistance - {resistance}')

            # Plot support lines
            for level, sup_date in zip(support, sup_dates):
                line = self.chart.horizontal_line(level, color='green')
                self.chart.marker(time=sup_date, text='S', color='green', shape='arrow_up')
                self.support_resistance_lines.append(line)

            # Plot resistance lines
            for level, res_date in zip(resistance, res_dates):
                line = self.chart.horizontal_line(level, color='red')
                self.chart.marker(time=res_date, text='R', color='red', shape='arrow_down')
                self.support_resistance_lines.append(line)

            print("Support and resistance lines added to the chart.")

        def calculate_support_resistance(self, data, window=6):
            """Calculate support and resistance levels."""
            # low = data['Low'].values
            # high = data['High'].values

            low_lr = data['Low_LR_6_M'].values
            high_lr = data['High_LR_6_M'].values
            # lr_data = data['LR_6_M'].values

            # Find local minima and maxima
            # low_idx = argrelextrema(low, np.less, order=window)[0]
            # high_idx = argrelextrema(high, np.greater, order=window)[0]

            low_lr_idx = argrelextrema(low_lr, np.less, order=window)[0]
            high_lr_idx = argrelextrema(high_lr, np.greater, order=window)[0]

            # support = low[low_idx]
            # resistance = high[high_idx]

            support_lr = low_lr[low_lr_idx]
            resistance_lr = high_lr[high_lr_idx]

            # Remove levels that are too close to each other
            # support = self.remove_close_levels(list(support) + list(support_lr))
            # resistance = self.remove_close_levels(list(resistance) + list(resistance_lr))

            support = self.remove_close_levels(support_lr)
            resistance = self.remove_close_levels(resistance_lr)

            sup_dates = data['Date'][data['Low_LR_6_M'].isin(support)].values.tolist()
            res_dates = data['Date'][data['High_LR_6_M'].isin(resistance)].values.tolist()

            return support, resistance, sup_dates, res_dates

        def remove_close_levels(self, levels, threshold=0.025):
            """Remove levels that are within threshold% of each other, keeping one representative level."""
            levels = sorted(levels)
            result = []

            for level in levels:
                if not result or all(abs(level - r) / r > threshold for r in result):
                    result.append(level)

            return result

        def show_hide_portfolio(self):
            """Display the portfolio stored in SQL."""
            dialog = QDialog(self)
            dialog.setWindowTitle('My Portfolio')
            dialog.setModal(True)

            portfolio_data = rd.get_table_data(selected_database='analytics', selected_table='OVERALL_SUMMARY_ACCOUNT_WISE')
            table = QTableWidget(dialog)

            # Set up the table structure based on portfolio data
            if not portfolio_data.empty:
                table.setRowCount(len(portfolio_data))
                table.setColumnCount(len(portfolio_data.columns))
                table.setHorizontalHeaderLabels(portfolio_data.columns)

                for i, row in portfolio_data.iterrows():
                    for j, col in enumerate(portfolio_data.columns):
                        table.setItem(i, j, QTableWidgetItem(str(row[col])))

            layout = QHBoxLayout()
            layout.addWidget(table)
            dialog.setLayout(layout)
            dialog.exec_()


    if __name__ == "__main__":
        app = QApplication(sys.argv)
        window = StockChartApp()
        window.show()
        sys.exit(app.exec_())
except Exception as e:
    print("An error occurred while launching the application:")
    print(str(e))
    print("Traceback:")
    print(traceback.format_exc())
    input("Press Enter to exit...")  # This will keep the console window open
