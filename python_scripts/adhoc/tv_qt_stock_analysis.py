from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QWidget, QAction,
                             QComboBox, QLineEdit, QPushButton, QColorDialog, QFormLayout,
                             QHBoxLayout, QLabel, QTableWidget, QTableWidgetItem, QSlider, QStatusBar, QDialog)
from lightweight_charts.widgets import QtChart
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QFont
import pandas as pd
from common_utils import read_write_sql_data as rd
import yfinance as yf


class StockChartApp(QMainWindow):
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

        # Initialize replay variables
        self.replay_data = None
        self.replay_index = 0
        self.replay_timer = QTimer(self)
        self.replay_speed = 1000  # Speed of replay in milliseconds (1 second)

        # Central widget setup
        self.central_widget = QWidget(self)
        self.setCentralWidget(self.central_widget)
        self.layout = QVBoxLayout(self.central_widget)

        # Create the chart
        self.chart = QtChart(self.central_widget, toolbox=True)

        # Setup chart with default stock
        self.setup_chart()

        # Add chart to layout
        self.layout.addWidget(self.chart.get_webview())

        # Create the menu bar
        self.create_menubar()

        # Set up the status bar (bottom bar) for replay controls
        self.create_statusbar()

        # Set up replay timer
        self.replay_timer.timeout.connect(self.update_replay)

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

        # Clear Indicators Action
        clear_indicator_action = QAction('Clear Indicators', self)
        clear_indicator_action.triggered.connect(self.clear_indicators)

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
        start_button = QPushButton('Start Replay')
        start_button.clicked.connect(self.start_replay)
        pause_button = QPushButton('Pause Replay')
        pause_button.clicked.connect(self.pause_replay)
        stop_button = QPushButton('Stop Replay')
        stop_button.clicked.connect(self.stop_replay)

        # Replay Speed Slider
        replay_speed_slider = QSlider(Qt.Horizontal, self)
        replay_speed_slider.setRange(100, 5000)  # Range from 100 ms to 5000 ms
        replay_speed_slider.setValue(self.replay_speed)
        replay_speed_slider.setToolTip('Replay Speed (ms)')
        replay_speed_slider.valueChanged.connect(self.change_replay_speed)

        # Add widgets to the status bar
        statusbar.addWidget(start_button)
        statusbar.addWidget(pause_button)
        statusbar.addWidget(stop_button)
        statusbar.addWidget(QLabel('Replay Speed:'))
        statusbar.addWidget(replay_speed_slider)

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

    def on_timeframe_change(self, timeframe):
        self.timeframe = timeframe
        self.setup_chart()

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
                'time': pd.to_datetime(stock_data['Date']),
                'open': stock_data['Open'],
                'high': stock_data['High'],
                'low': stock_data['Low'],
                'close': stock_data['Close'],
                'volume': stock_data['Volume']
            })

            self.chart.set(candlestick_data)
            self.chart.watermark(self.symbol)
            self.chart.legend(True)

            self.replay_data = candlestick_data
            self.replay_index = 0
        else:
            print("Error: Stock data is not in the expected format.")

    def start_replay(self):
        if self.replay_data is not None and not self.replay_timer.isActive():
            self.replay_timer.start(self.replay_speed)

    def pause_replay(self):
        if self.replay_timer.isActive():
            self.replay_timer.stop()

    def stop_replay(self):
        if self.replay_timer.isActive():
            self.replay_timer.stop()
        self.replay_index = 0
        self.setup_chart()

    def update_replay(self):
        if self.replay_index < len(self.replay_data):
            current_data = self.replay_data.iloc[:self.replay_index + 1]
            self.chart.set(current_data)
            self.replay_index += 1
        else:
            self.stop_replay()

    def change_replay_speed(self, value):
        self.replay_speed = value
        if self.replay_timer.isActive():
            self.replay_timer.setInterval(self.replay_speed)

    def open_indicator_dialog(self):
        """Open a dialog box for selecting the indicator type, period, and color."""
        dialog = QDialog(self)
        dialog.setWindowTitle('Add Indicator')
        dialog.setModal(True)

        # Form layout
        form_layout = QFormLayout()

        # Dropdown for Indicator Type
        self.indicator_dropdown = QComboBox()
        self.indicator_dropdown.addItems(['EMA', 'Lin_Reg'])
        form_layout.addRow('Indicator Type:', self.indicator_dropdown)

        # Dropdown for Period
        self.period_dropdown = QComboBox()
        self.period_dropdown.addItems([str(p) for p in [6, 10, 20, 50, 100, 200]])
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

            elif self.indicator == 'Lin_Reg':
                indicator_data[f'{self.indicator} {self.indicator_period}'] = stock_data['Close'].rolling(
                    window=self.indicator_period).mean()

            # Create a line on the chart for the indicator
            indicator_line = self.chart.create_line(
                name=f'{self.indicator} {self.indicator_period}',
                color=self.indicator_color,
                width=2,
                price_label=True
            )
            indicator_line.set(indicator_data[['time', f'{self.indicator} {self.indicator_period}']].dropna())

            dialog.accept()  # Close the dialog after applying the indicator
        else:
            print("Error: Could not apply the indicator. Stock data not available.")

    def clear_indicators(self):
        """Clear all indicators from the chart."""
        for line in self.chart.lines():
            line.delete()
        print("All indicators cleared.")

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
