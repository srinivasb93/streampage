import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import urllib

# Replace 'your_database_url' with the connection string to your SQL Server database
# Example: 'mssql+pyodbc://username:password@your_server/your_database'
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                     "DATABASE=NSEDATA;"
                                     "Trusted_Connection=yes")
db_url = "mssql+pyodbc:///?odbc_connect={}".format(params)
engine = create_engine(db_url)

# Define the app
app = dash.Dash(__name__)

# Layout of the app
app.layout = html.Div([
    html.H1("Stock Analysis Dashboard"),

    # Input for stock symbol
    html.Label("Enter Stock Symbol:"),
    dcc.Input(id='stock-input', type='text', value='TATAMOTORS'),

    # Range slider for date selection
    html.Label("Select Date Range:"),
    dcc.RangeSlider(
        id='date-slider',
        marks={i: str(i) for i in range(2010, 2023)},
        min=2010,
        max=2022,
        step=1,
        value=[2018, 2022]
    ),

    # Stock price line chart
    dcc.Graph(id='stock-price-chart'),

    # Stock volume bar chart
    dcc.Graph(id='stock-volume-chart')
])


# Callback to update charts based on user input
@app.callback(
    [Output('stock-price-chart', 'figure'),
     Output('stock-volume-chart', 'figure')],
    [Input('stock-input', 'value'),
     Input('date-slider', 'value')]
)
def update_charts(stock_symbol, date_range):
    # Query stock data from SQL Server database
    query = f"SELECT Date, [Close], Volume FROM stock_data WHERE Symbol = '{stock_symbol}' " \
            f"AND Date >= '{date_range[0]}-01-01' AND Date <= '{date_range[1]}-12-31'"
    stock_data = pd.read_sql(query, engine, parse_dates=['Date'], index_col='Date')

    # Create stock price line chart
    price_chart = {
        'data': [
            {'x': stock_data.index, 'y': stock_data['Close'], 'type': 'line', 'name': stock_symbol}
        ],
        'layout': {
            'title': f'{stock_symbol} Stock Price',
            'xaxis': {'title': 'Date'},
            'yaxis': {'title': 'Stock Price (USD)'}
        }
    }

    # Create stock volume bar chart
    volume_chart = {
        'data': [
            {'x': stock_data.index, 'y': stock_data['Volume'], 'type': 'bar', 'name': 'Volume'}
        ],
        'layout': {
            'title': f'{stock_symbol} Stock Volume',
            'xaxis': {'title': 'Date'},
            'yaxis': {'title': 'Volume'}
        }
    }

    return price_chart, volume_chart


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
