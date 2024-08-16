import pandas as pd
from lightweight_charts import Chart
from common_utils import read_write_sql_data as rd


def get_bar_data(symbol, timeframe):
    if timeframe == 'Weekly':
        symbol = symbol + '_W'
    if timeframe == 'Monthly':
        symbol = symbol + '_M'

    query = f"select * from NSEDATA.dbo.{symbol} order by Date"
    return rd.get_table_data(query=query)


def add_ema_line(chart, df, period=10):
    ema_line = chart.create_line(name=f'EMA {period}', color='#ffeb3b', width=1, price_label=True)
    ema = pd.DataFrame(columns=['time', f'EMA {period}'])
    ema.time = df.Date
    ema[f'EMA {period}'] = df.Close.ewm(span=period).mean()
    ema_line.set(ema.dropna())


def on_search(chart, searched_string):  # Called when the user searches.
    new_data = get_bar_data(searched_string, chart.topbar['timeframe'].value)
    # print(new_data)
    if new_data.empty:
        return
    chart.topbar['symbol'].set(searched_string)
    chart.set(new_data)
    add_ema_line(chart, new_data, period=20)


def on_timeframe_selection(chart):  # Called when the user changes the timeframe.
    new_data = get_bar_data(chart.topbar['symbol'].value, chart.topbar['timeframe'].value)
    if new_data.empty:
        return
    chart.set(new_data)
    add_ema_line(chart, new_data, period=20)


def on_horizontal_line_move(chart, line):
    print(f'Horizontal line moved to: {line.price}')


if __name__ == '__main__':
    chart = Chart(toolbox=True)
    chart.legend(True)

    chart.events.search += on_search

    chart.topbar.textbox('symbol', 'SBIN')
    chart.topbar.switcher('timeframe', ('Daily', 'Weekly', 'Monthly'), default='Daily',
                          func=on_timeframe_selection)
    chart.topbar.menu(name='Indicators', options=['EMA', 'Reg'])

    df = get_bar_data('SBIN', 'Daily')
    chart.set(df)

    # chart.horizontal_line(200, func=on_horizontal_line_move)

    chart.show(block=True)
