import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timedelta
import json


def main():
    st.set_page_config(layout="wide")
    st.title("Advanced Stock Data Replay with TradingView Chart")

    # Sidebar for user inputs
    st.sidebar.header("Settings")
    symbol = st.sidebar.text_input("Stock Symbol", value="AAPL")
    start_date = st.sidebar.date_input("Start Date", value=datetime.now() - timedelta(days=30))

    # Indicator selection
    indicators = st.sidebar.multiselect("Select Indicators",
                                        ['EMA', 'Linear Regression', 'RSI', 'MACD', 'Bollinger Bands'])
    indicator_settings = {}
    for indicator in indicators:
        if indicator == 'EMA':
            indicator_settings[indicator] = st.sidebar.number_input(f"{indicator} Period", min_value=1, value=14,
                                                                    key=indicator)
        elif indicator == 'Linear Regression':
            indicator_settings[indicator] = st.sidebar.number_input(f"{indicator} Period", min_value=1, value=14,
                                                                    key=indicator)
        elif indicator == 'RSI':
            indicator_settings[indicator] = st.sidebar.number_input(f"{indicator} Period", min_value=1, value=14,
                                                                    key=indicator)
        elif indicator == 'MACD':
            fast_period = st.sidebar.number_input("MACD Fast Period", min_value=1, value=12, key="macd_fast")
            slow_period = st.sidebar.number_input("MACD Slow Period", min_value=1, value=26, key="macd_slow")
            signal_period = st.sidebar.number_input("MACD Signal Period", min_value=1, value=9, key="macd_signal")
            indicator_settings[indicator] = (fast_period, slow_period, signal_period)
        elif indicator == 'Bollinger Bands':
            period = st.sidebar.number_input("Bollinger Bands Period", min_value=1, value=20, key="bb_period")
            dev = st.sidebar.number_input("Bollinger Bands Standard Deviations", min_value=0.1, value=2.0, key="bb_dev")
            indicator_settings[indicator] = (period, dev)

    # Convert indicator settings to JSON for JavaScript
    indicator_settings_json = json.dumps(indicator_settings)

    # TradingView Chart with custom indicators and replay controls
    chart_height = 700
    tradingview_chart = f"""
    <div class="tradingview-widget-container" style="height:{chart_height}px">
      <div id="tradingview_chart"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
      <script type="text/javascript">
      var tvWidget;
      var isReplaying = false;
      var replayInterval;
      var currentBar = 0;

      new TradingView.widget({{
        "width": "100%",
        "height": {chart_height},
        "symbol": "{symbol}",
        "interval": "D",
        "timezone": "Etc/UTC",
        "theme": "light",
        "style": "1",
        "locale": "en",
        "toolbar_bg": "#f1f3f6",
        "enable_publishing": false,
        "allow_symbol_change": true,
        "container_id": "tradingview_chart",
        "library_path": "https://s3.tradingview.com/",
        "disabled_features": ["use_localstorage_for_settings"],
        "client_id": "0",
        "custom_indicators_getter": function(PineJS) {{
            return Promise.resolve([
                {{
                    name: "Custom Indicators",
                    metainfo: {{
                        _metainfoVersion: 51,
                        id: "Custom Indicators@tv-basicstudies-1",
                        name: "Custom Indicators",
                        description: "Custom Indicators",
                        shortDescription: "Custom Indicators",
                        is_price_study: true,
                        isCustomIndicator: true,
                        defaults: {{
                            precision: 4,
                            palettes: {{
                                palette_0: {{ colors: ["#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#FF00FF"] }},
                            }},
                        }},
                        plots: [
                            {{ id: "plot_0", type: "line" }},
                            {{ id: "plot_1", type: "line" }},
                            {{ id: "plot_2", type: "line" }},
                            {{ id: "plot_3", type: "line" }},
                            {{ id: "plot_4", type: "line" }}
                        ],
                        styles: {{
                            plot_0: {{ title: "EMA", color: "#FF0000" }},
                            plot_1: {{ title: "Linear Regression", color: "#00FF00" }},
                            plot_2: {{ title: "RSI", color: "#0000FF" }},
                            plot_3: {{ title: "MACD", color: "#FFFF00" }},
                            plot_4: {{ title: "Bollinger Bands", color: "#FF00FF" }}
                        }},
                    }},
                    constructor: function() {{
                        this.f_ema = function(src, len) {{
                            var alpha = 2 / (len + 1);
                            var sum = 0;
                            for (var i = 0; i < len; i++) {{
                                sum += src[i] * Math.pow(1 - alpha, i);
                            }}
                            return sum;
                        }};

                        this.f_linreg = function(src, len) {{
                            var sum_x = 0, sum_y = 0, sum_xy = 0, sum_xx = 0;
                            for (var i = 0; i < len; i++) {{
                                sum_x += i;
                                sum_y += src[i];
                                sum_xy += i * src[i];
                                sum_xx += i * i;
                            }}
                            var m = (len * sum_xy - sum_x * sum_y) / (len * sum_xx - sum_x * sum_x);
                            var b = (sum_y - m * sum_x) / len;
                            return m * (len - 1) + b;
                        }};

                        this.f_rsi = function(src, len) {{
                            var up = 0, down = 0;
                            for (var i = 1; i < len; i++) {{
                                var diff = src[i] - src[i-1];
                                if (diff > 0) up += diff;
                                else down -= diff;
                            }}
                            var rs = up / down;
                            return 100 - (100 / (1 + rs));
                        }};

                        this.f_macd = function(src, fast, slow, signal) {{
                            var fastMA = this.f_ema(src, fast);
                            var slowMA = this.f_ema(src, slow);
                            var macd = fastMA - slowMA;
                            var signalLine = this.f_ema([macd], signal);
                            return macd - signalLine;
                        }};

                        this.f_bb = function(src, len, mult) {{
                            var ma = this.f_ema(src, len);
                            var std = Math.sqrt(src.reduce((sum, x) => sum + Math.pow(x - ma, 2), 0) / len);
                            return [ma + mult * std, ma, ma - mult * std];
                        }};

                        this.main = function(context, input) {{
                            this.context = context;
                            this.input = input;

                            var indicators = {indicator_settings_json};
                            var close = input(0);
                            var src = close;

                            var results = [0, 0, 0, 0, 0];

                            if (indicators.hasOwnProperty('EMA')) {{
                                results[0] = this.f_ema(src, indicators['EMA']);
                            }}
                            if (indicators.hasOwnProperty('Linear Regression')) {{
                                results[1] = this.f_linreg(src, indicators['Linear Regression']);
                            }}
                            if (indicators.hasOwnProperty('RSI')) {{
                                results[2] = this.f_rsi(src, indicators['RSI']);
                            }}
                            if (indicators.hasOwnProperty('MACD')) {{
                                results[3] = this.f_macd(src, indicators['MACD'][0], indicators['MACD'][1], indicators['MACD'][2]);
                            }}
                            if (indicators.hasOwnProperty('Bollinger Bands')) {{
                                var bb = this.f_bb(src, indicators['Bollinger Bands'][0], indicators['Bollinger Bands'][1]);
                                results[4] = bb[1];  // Middle band
                            }}

                            return results;
                        }};
                    }}
                }}
            ]);
        }},
        onChartReady: function() {{
            tvWidget = window.tvWidget;
        }}
      }});

      function startReplay() {{
        if (!isReplaying) {{
            isReplaying = true;
            currentBar = 0;
            replayInterval = setInterval(function() {{
                if (tvWidget && tvWidget.chart()) {{
                    tvWidget.chart().executeActionById("timeScaleReset");
                    tvWidget.chart().setVisibleRange({{
                        from: tvWidget.chart().getAllSources()[0].bars().minIndex() + currentBar,
                        to: tvWidget.chart().getAllSources()[0].bars().minIndex() + currentBar + 30
                    }});
                    currentBar++;
                }}
            }}, 1000);
        }}
      }}

      function pauseResumeReplay() {{
        if (isReplaying) {{
            clearInterval(replayInterval);
            isReplaying = false;
        }} else {{
            startReplay();
        }}
      }}

      function stopReplay() {{
        if (isReplaying) {{
            clearInterval(replayInterval);
            isReplaying = false;
            currentBar = 0;
            if (tvWidget && tvWidget.chart()) {{
                tvWidget.chart().executeActionById("timeScaleReset");
            }}
        }}
      }}
      </script>
    </div>
    """

    components.html(tradingview_chart, height=chart_height)

    # Replay controls
    col1, col2, col3 = st.columns(3)
    start_button = col1.button("Start Replay",
                               on_click=lambda: st.write('<script>startReplay();</script>', unsafe_allow_html=True))
    pause_resume_button = col2.button("Pause/Resume", on_click=lambda: st.write('<script>pauseResumeReplay();</script>',
                                                                                unsafe_allow_html=True))
    stop_button = col3.button("Stop Replay",
                              on_click=lambda: st.write('<script>stopReplay();</script>', unsafe_allow_html=True))


if __name__ == "__main__":
    main()