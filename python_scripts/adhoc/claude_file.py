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

        # Initialize replay variables
        self.replay_data = None
        self.replay_index = 0
        self.replay_timer = QTimer(self)
        self.replay_speed = 1000  # Speed of replay in milliseconds (1 second)
        self.replay_start_date = None
        self.is_paused = False

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
        self.chart = QtChart(self.charts_widget, toolbox=True)
        self.charts_layout.addWidget(self.chart.get_webview(), 80)  # 75% of the height

        # # Add chart to layout
        # self.layout.addWidget(self.chart.get_webview())

        self.rsi_chart = None  # Store reference to RSI chart

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
        self.setup_chart()

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
        show_portfolio_action.triggered.connect(self.show_portfolio)

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
        self.clear_indicators_and_patterns(keep_patterns=False, keep_indicators=False)

    def on_timeframe_change(self, timeframe):
        self.timeframe = timeframe
        self.setup_chart()
        self.clear_indicators_and_patterns(keep_indicators=False)

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

    def setup_chart(self):
        if self.data_source == 'SQL':
            postfix = ''
            if self.timeframe == 'Weekly':
                postfix = '_W'
            elif self.timeframe == 'Monthly':
                postfix = '_M'
            stock_data = rd.get_table_data(selected_table=self.symbol + postfix, sort=True, sort_by='Date')
        else:
            interval = '1d'
            if self.timeframe == 'Weekly':
                interval = '1wk'
            elif self.timeframe == 'Monthly':
                interval = '1mo'
            stock_data = yf.download(self.symbol + '.NS', interval=interval)

        if isinstance(stock_data, pd.DataFrame) and not stock_data.empty:
            candlestick_data = pd.DataFrame({
                'time': pd.to_datetime(stock_data.index if self.data_source == 'Yahoo' else stock_data['Date']),
                'open': stock_data['Open'],
                'high': stock_data['High'],
                'low': stock_data['Low'],
                'close': stock_data['Close'],
                'volume': stock_data['Volume']
            })

            # Set up main chart
            self.chart.set(candlestick_data)
            self.chart.watermark(self.symbol)
            self.chart.legend(True, font_size=24, color_based_on_candle=True)

            self.replay_data = candlestick_data
            self.replay_index = 0

            # Update the date range for the QDateEdit
            min_date = candlestick_data['time'].min().date()
            max_date = candlestick_data['time'].max().date()
            self.start_date_edit.setDateRange(QDate(min_date), QDate(max_date))
            self.current_date_label.setText(f"Current Date: {QDate(max_date).toPyDate()}")
            self.start_date_edit.setDate(QDate.currentDate())
            print(f"Chart setup complete. Date range: {min_date} to {max_date}")
        else:
            print("Error: Stock data is not in the expected format.")

    # def update_replay_start_date(self, date):
    #     """Update the replay start date when the user selects a new date."""
    #     self.replay_start_date = date.toPyDate()

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
        print(f"Updated replay start date to: {self.replay_start_date.toString('yyyy-MM-dd')}")

    def update_replay(self):
        try:
            if self.replay_index < len(self.replay_data):
                current_data = self.replay_data.iloc[:self.replay_index + 1]
                self.chart.set(current_data)

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

        # Reset button states
        self.start_button.setEnabled(True)
        self.pause_resume_button.setText('Pause Replay')
        self.pause_resume_button.setIcon(QIcon('path_to_pause_icon.png'))
        self.pause_resume_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.is_paused = False

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
        self.indicator_dropdown.addItems(['EMA', 'Lin_Reg', 'RSI'])
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

    def apply_indicator(self, dialog):
        """Apply the selected indicator to the chart."""
        self.indicator = self.indicator_dropdown.currentText()
        self.indicator_period = int(self.period_dropdown.currentText())
        self.indicator_color = self.selected_color.name()

        # Fetch stock data and apply the indicator
        if self.data_source == 'SQL':
            stock_data = rd.get_table_data(selected_table=self.symbol, sort=True, sort_by='Date')
        else:
            stock_data = yf.download(self.symbol)

        if isinstance(stock_data, pd.DataFrame) and not stock_data.empty:
            indicator_data = pd.DataFrame({
                'time': pd.to_datetime(stock_data['Date']),  # Index should be used as time
                'close': stock_data['Close']
            })

            # Calculate the indicator values based on the type and period
            if self.indicator == 'EMA':
                indicator_data[f'{self.indicator} {self.indicator_period}'] = stock_data['Close'].ewm(
                    span=self.indicator_period).mean()

                # Create a line on the main chart for EMA
                indicator_line = self.chart.create_line(
                    name=f'{self.indicator} {self.indicator_period}',
                    color=self.indicator_color,
                    width=2,
                    price_label=True
                )
                indicator_line.set(indicator_data[['time', f'{self.indicator} {self.indicator_period}']].dropna())

            elif self.indicator == 'Lin_Reg':
                indicator_data[f'{self.indicator} {self.indicator_period}'] = stock_data['Close'].rolling(
                    window=self.indicator_period).mean()

                # Create a line on the main chart for Linear Regression
                indicator_line = self.chart.create_line(
                    name=f'{self.indicator} {self.indicator_period}',
                    color=self.indicator_color,
                    width=2,
                    price_label=True
                )
                indicator_line.set(indicator_data[['time', f'{self.indicator} {self.indicator_period}']].dropna())

            elif self.indicator == 'RSI':
                indicator_data[f'{self.indicator} {self.indicator_period}'] = ta.rsi(stock_data['Close'],
                                                                                     timeperiod=self.indicator_period)

                # Create a separate chart for RSI
                if self.rsi_chart:
                    self.charts_layout.removeWidget(self.rsi_chart.get_webview())
                    self.rsi_chart.get_webview().deleteLater()

                self.rsi_chart = QtChart(self.charts_widget, toolbox=True)
                self.charts_layout.addWidget(self.rsi_chart.get_webview(), 20)  # 25% of the height
                self.rsi_chart.legend(True, font_size=18, text=self.indicator)

                rsi_line = self.rsi_chart.create_line(
                    name=f'{self.indicator} {self.indicator_period}',
                    color=self.indicator_color,
                    width=2,
                    price_label=True
                )

                rsi_line.set(indicator_data[['time', f'{self.indicator} {self.indicator_period}']].dropna())

                # Add overbought and oversold lines to RSI chart
                self.rsi_chart.horizontal_line(70, color='red')
                self.rsi_chart.horizontal_line(30, color='green')

            dialog.accept()  # Close the dialog after applying the indicator
        else:
            print("Error: Could not apply the indicator. Stock data not available.")

    # def clear_indicators(self):
    #     """Clear all indicators from the chart."""
    #     for line in self.chart.lines():
    #         line.delete()
    #     print("All indicators cleared.")

    def clear_support_resistance_lines(self):
        for line in self.support_resistance_lines:
            line.delete()
        self.support_resistance_lines = []

    def clear_indicators_and_patterns(self, keep_patterns=True, keep_indicators=True):
        """Clear all indicators and patterns from the chart."""
        if not keep_patterns:
            self.clear_support_resistance_lines()

        if not keep_indicators:
            for line in self.chart.lines():
                line.delete()

            if self.rsi_chart:
                self.charts_layout.removeWidget(self.rsi_chart.get_webview())
                self.rsi_chart.get_webview().deleteLater()
                self.rsi_chart = None
        print("All indicators and patterns cleared.")

    def apply_patterns(self):
        """Apply chart patterns such as Support/Resistance lines."""
        if self.data_source == 'SQL':
            stock_data = rd.get_table_data(selected_table=self.symbol + '_M', sort=True, sort_by='Date')
        else:
            stock_data = yf.download(self.symbol)
        print('Fetched stock data')
        if isinstance(stock_data, pd.DataFrame) and not stock_data.empty:
            # Clear existing support/resistance lines
            self.clear_support_resistance_lines()

            # Calculate support and resistance levels
            support, resistance = self.calculate_support_resistance(stock_data)
            print('Fetched support and resistance levels')
            print(f'support - {support}')
            print(f'Resistance - {resistance}')

            # Plot support lines
            for level in support:
                line = self.chart.horizontal_line(level, color='green')
                self.support_resistance_lines.append(line)

            # Plot resistance lines
            for level in resistance:
                line = self.chart.horizontal_line(level, color='red')
                self.support_resistance_lines.append(line)

            print("Support and resistance lines added to the chart.")
        else:
            print("Error: Could not apply patterns. Stock data not available.")

    def calculate_support_resistance(self, data, window=10):
        """Calculate support and resistance levels."""
        low = data['Low'].values
        high = data['High'].values

        # Find local minima and maxima
        low_idx = argrelextrema(low, np.less, order=window)[0]
        high_idx = argrelextrema(high, np.greater, order=window)[0]

        support = low[low_idx]
        resistance = high[high_idx]

        # Remove levels that are too close to each other
        support = self.remove_close_levels(support)
        resistance = self.remove_close_levels(resistance)

        return support, resistance

    def remove_close_levels(self, levels, threshold=0.02):
        """Remove levels that are within threshold% of each other."""
        levels = sorted(levels)
        result = [levels[0]]
        for level in levels[1:]:
            if (level - result[-1]) / result[-1] > threshold:
                result.append(level)
        return result

    def show_portfolio(self):
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

        layout = QVBoxLayout()
        layout.addWidget(table)
        dialog.setLayout(layout)
        dialog.exec_()


if __name__ == "__main__":
    app = QApplication([])
    window = StockChartApp()
    window.show()
    app.exec_()
