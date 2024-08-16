import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
from sqlalchemy import create_engine
import urllib
from dash import dash_table
import subprocess

# Replace 'your_database_url' with the connection string to your SQL Server database
# Example: 'mssql+pyodbc://username:password@your_server/your_database'
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=IN01-9MCXZH3\SQLEXPRESS;"
                                     "DATABASE=NSEDATA;"
                                     "Trusted_Connection=yes")
db_url = "mssql+pyodbc:///?odbc_connect={}".format(params)
engine = create_engine(db_url)

# Define the app
app = dash.Dash(__name__, external_stylesheets=['styles.css'])


# Layout of the app

app.layout = html.Div([
    html.Header(children=[
        html.H1(children="Data Analysis Dashboard",
                style={'font-family': 'Arial', 'color': '#DFC4FC', 'text-align': 'center',
                       'font-size': '26px'})
    ],
        style={'background-color': 'darkcyan'}),

    # Button to invoke Python script
    html.Button("Daily Data Load",
                id='invoke-script-button',
                style={'position': 'absolute', 'top': 60, 'right': 10}),

    # Output for displaying the result of the script invocation
    html.Div(id='script-result-output'),

    # Input for X-days High Close
    html.Label("Enter X-days for High Close:"),
    dcc.Input(id='days-input', type='number', value=5),

    # 52-week high close value widget
    html.Div(id='high-close-widget', style={'margin-top': '20px', 'font-size': '20px', 'color': 'green'}),

    # Input for stock symbol
    html.Label("Enter Stock Symbol:"),
    dcc.Input(id='stock-input', type='text', value='TATAMOTORS'),

    # Range slider for date selection
    html.Label("Select Date Range:"),
    dcc.RangeSlider(
        id='date-slider',
        marks={i: str(i) for i in range(2010, 2024)},
        min=2010,
        max=2024,
        step=1,
        value=[2018, 2024]
    ),

    # Organize graphs into a responsive grid layout
    html.Div(className='container', children=[
        html.Div(className='row', children=[
            html.Div(className='col-md-6', children=[
                dcc.Graph(id='stock-price-chart')
            ]),
            html.Div(className='col-md-6', children=[
                dcc.Graph(id='stock-volume-chart')
            ])
        ])
    ]),

    # Stock data table
    dash_table.DataTable(
        id='stock-data-table',
        columns=[
            {'name': 'Date', 'id': 'Date'},
            {'name': 'Close', 'id': 'Close'},
            {'name': 'Volume', 'id': 'Volume'},
            {'name': 'Percentage_Change', 'id': 'Percentage_Change'},
        ],
        # style_table={'height': '300px', 'overflowY': 'auto', 'overflowX': 'auto'},
        # style_table={'overflowY': 'auto', 'overflowX': 'auto'},
        style_table={},
        fixed_rows={'headers': True, 'data': 0},  # Fixed headers
        style_data_conditional=[
            {
                'if': {'column_id': 'Percentage_Change'},
                'backgroundColor': '#3D9970',  # green for positive values
                'color': 'white',
            },
            {
                'if': {'column_id': 'Percentage_Change', 'filter_query': '{Percentage_Change} < 0'},
                'backgroundColor': '#FF4136',  # red for negative values
                'color': 'white',
            },
        ]
    )
])


# Callback to update charts based on user input
@app.callback(
    [Output('stock-price-chart', 'figure'),
     Output('stock-volume-chart', 'figure'),
     Output('stock-data-table', 'data'),
     Output('high-close-widget', 'children')],
    [Input('stock-input', 'value'),
     Input('date-slider', 'value'),
     Input('days-input', 'value'),
     Input('stock-price-chart', 'relayoutData')],
    prevent_initial_call=True
)
def update_charts_and_table(stock_symbol, date_range, x_days, relayout_data):
    # Query stock data from SQL Server database
    query = f"SELECT Date, [Close], Volume FROM dbo.{stock_symbol} WHERE " \
            f"Date >= '{date_range[0]}-01-01' AND Date <= '{date_range[1]}-12-31' ORDER BY Date"
    stock_data = pd.read_sql(query, engine, parse_dates=['Date'])

    stock_data["Percentage_Change"] = round(stock_data['Close'].pct_change(periods=1)*100, 2)
    # Calculate X-days high close value
    stock_data[f'{x_days}-Days High Close'] = stock_data['Close'].rolling(window=x_days).max()

    # Create stock price line chart
    price_chart = {
        'data': [
            {'x': stock_data['Date'], 'y': stock_data['Close'], 'type': 'line', 'name': stock_symbol,
             'hoverinfo': 'text+y',  # Include complete date information on hover
             'text': stock_data['Date'].dt.strftime('%b %d, %Y')  # Format date as desired
             }
        ],
        'layout': {
            'title': f'{stock_symbol} Stock Price',
            'xaxis': {'title': 'Date'},
            'yaxis': {'title': 'Stock Price (USD)'},
            'xaxis_type': 'date',
            'uirevision': 'price-chart',  # Assign a revision name for the price chart
        }
    }

    # Create stock volume bar chart
    volume_chart = {
        'data': [
            {'x': stock_data['Date'], 'y': stock_data['Volume'], 'type': 'bar', 'name': 'Volume',
             'hoverinfo': 'text+y',  # Include complete date information on hover
             'text': stock_data['Date'].dt.strftime('%b %d, %Y')  # Format date as desired
             }
        ],
        'layout': {
            'title': f'{stock_symbol} Stock Volume',
            'xaxis': {'title': 'Date'},
            'yaxis': {'title': 'Volume'},
            'xaxis_type': 'date',
            'uirevision': 'volume-chart',  # Assign a revision name for the volume chart
        }
    }

    # Synchronize x-axis ranges between charts
    if relayout_data and 'xaxis.range[0]' in relayout_data and 'xaxis.range[1]' in relayout_data:
        xaxis_range = [relayout_data['xaxis.range[0]'], relayout_data['xaxis.range[1]']]
        price_chart['layout']['xaxis']['range'] = xaxis_range
        volume_chart['layout']['xaxis']['range'] = xaxis_range

    # Prepare data for the DataTable
    table_data = stock_data.to_dict('records')

    # Get the X-days high close value
    x_days_high_close = stock_data[f'{x_days}-Days High Close'].iloc[-1]
    x_days_high_date = stock_data['Date'][stock_data['Close'] == x_days_high_close].values
    x_days_high_date = pd.to_datetime(x_days_high_date[0], format="%d-%m-%Y")

    return price_chart, volume_chart, table_data,\
           f'{x_days}-Days High Close: Rs.{x_days_high_close:.2f}, Date: {x_days_high_date}'


# Callback to handle button click and invoke Python script
@app.callback(
    Output('script-result-output', 'children'),
    [Input('invoke-script-button', 'n_clicks')],
    prevent_initial_call=True
)
def invoke_python_script(n_clicks):
    if n_clicks:
        try:
            # Replace 'your_script.py' with the path to your Python script
            script_path = '../archive/daily_data_load_archive.py'

            # Invoke the Python script using subprocess
            process = subprocess.Popen(['python', script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, error = process.communicate()

            if error:
                output_text = f"Error: {error.decode()}"
            else:
                output_text = output.decode()
        except Exception as e:
            output_text = f"Error: {str(e)}"

        return html.Pre(output_text)


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
