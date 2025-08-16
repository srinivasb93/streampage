import pandas as pd
from lightweight_charts import Chart
from common_utils import read_write_sql_data as rd


def get_bar_data(chart, mode='selection', fetch_data=False, need_drawings=False):
    symbol = chart.topbar['symbol'].value if mode == 'selection' else mode
    timeframe = chart.topbar['timeframe'].value
    if timeframe == 'Weekly':
        symbol = symbol + '_W'
    if timeframe == 'Monthly':
        symbol = symbol + '_M'

    query = f"select * from nsedata.public.{symbol} order by Date"
    stock_data = rd.get_table_data(query=query)
    if fetch_data:
        return stock_data
    else:
        chart.set(stock_data, keep_drawings=need_drawings)
        chart.watermark(symbol)


def update_lines_data(chart, data):
    for line in chart.lines():
        line.update()


def add_indicator_line(chart):
    stock_data = get_bar_data(chart, fetch_data=True)
    indicator = chart.topbar['indicators'].value
    period = int(chart.topbar['period'].value)
    ind_color = chart.topbar['ind_color'].value

    # ind_line = chart.create_line(name=f'{indicator} {period}', color='#ffeb3b', width=1, price_label=True)
    ind_line = chart.create_line(name=f'{indicator} {period}', color=ind_color, width=2, price_label=True)

    ind_df = pd.DataFrame(columns=['time', f'{indicator} {period}'])
    ind_df.time = stock_data.Date
    if indicator == 'EMA':
        ind_df[f'{indicator} {period}'] = stock_data.Close.ewm(span=period).mean()
    elif indicator == 'Lin_Reg':
        ind_df[f'{indicator} {period}'] = stock_data.Close.ewm(span=period).mean()

    ind_line.set(ind_df.dropna())


def clear_indicators(chart):
    for line in chart.lines():
        line.delete()


def on_search(chart, searched_string):  # Called when the user searches.
    get_bar_data(chart, mode=searched_string)
    # add_ema_line(chart, new_data, period=20)


def do_nothing(chart):
    pass


def on_timeframe_selection(chart):  # Called when the user changes the timeframe.
    get_bar_data(chart, need_drawings=True)
    clear_indicators(chart)


def on_horizontal_line_move(chart, line):
    print(f'Horizontal line moved to: {line.price}')


if __name__ == '__main__':

    chart = Chart(toolbox=True)
    chart.legend(True)

    chart.events.search += on_search

    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
    stock_list = stock_list_df['SYMBOL'].values.tolist()
    # chart.topbar.textbox('symbol', 'SBIN')
    chart.topbar.menu('symbol', options=stock_list, default='TATAMOTORS', func=get_bar_data)
    chart.topbar.switcher('timeframe', ('Daily', 'Weekly', 'Monthly'), default='Daily',
                          func=on_timeframe_selection)
    chart.topbar.menu(name='indicators', options=('EMA', 'Lin_Reg'), default='EMA', func=do_nothing)
    chart.topbar.menu(name='period', options=(20, 6, 10, 20, 50, 200), func=do_nothing)
    chart.topbar.menu(name='ind_color', options=('red', 'blue', 'yellow', 'white', 'green', 'cyan'), func=do_nothing)
    chart.topbar.button(name='apply_indicator', button_text='Apply Indicator', func=add_indicator_line)
    chart.topbar.button(name='clear_indicator', button_text='Clear Indicator(s)', func=clear_indicators)

    df = get_bar_data(chart, fetch_data=True)
    chart.set(df, keep_drawings=True)
    chart.watermark(chart.topbar['symbol'].value)

    chart.show(block=True)
