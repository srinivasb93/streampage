import openpyxl
import streamlit as st
import os
import smtplib
from email.message import EmailMessage
import datetime as dt
import pandas as pd
import threading
import schedule
import logging
from common_utils.logging_utils import configure_logging
import time
from contextlib import nullcontext
from common_utils import upstox_utils
from common_utils.utils import fetch_indicies_sectors_list
from common_utils import read_write_sql_data as rd
from sqlalchemy import text
from python_scripts.stocks_data_load import load_agg_data as ag
from python_scripts.mf_data_load import mf_hist_data_load as mf_hist_load
from python_scripts.stocks_data_load.utilities import update_portfolio
from python_scripts.get_market_data.market_data import load_index_and_stocks_data
from python_scripts.analysis.EOD_analysis import EODAnalysis
# from python_scripts.analysis.EOD_analysis_grok import StockAnalyzer, ReportGenerator

configure_logging()
logger = logging.getLogger(__name__)

st.set_page_config(layout="wide")

try:
    from streamlit.runtime.scriptrunner import get_script_run_ctx
except Exception:
    def get_script_run_ctx():
        return None

def bulk_update_stock_daily_upstox(instrument_keys):
    """Perform a bulk Upstox update combining historical and intraday data."""
    try:
        success_count = 0
        fail_count = 0
        failed_stocks = []
        logger.debug(f"Bulk updating Upstox data for {instrument_keys}")
        data = upstox_utils.get_market_quote(instrument_keys, mode='ohlc')
        logger.debug(f"OHLC Data fetched on date: {dt.datetime.now()}: {data}")
        for symbol, row_data in data.items():
            df = pd.DataFrame([row_data])
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.normalize()
            # check if the timestamp is already present in the table. If present ignore the row and continue. 
            if not df.empty:
                last_date_query = text(f'SELECT MAX("timestamp") FROM public."{symbol}"')
                engine = rd.get_engine('nsedata')
                with engine.connect() as conn:
                    last_date_result = conn.execute(last_date_query).scalar_one_or_none()
                if last_date_result is not None:
                    df = df[df['timestamp'] > last_date_result]
                    if df.empty:
                        logger.info(f"No new data to update for {symbol}")
                        continue

            msg = rd.load_sql_data(
                data_to_load=df,
                table_name=symbol,
                load_type='append',
                database='nsedata',
                schema='public'
            )
            if 'success' in msg.lower():
                logger.info(f"Successfully updated Upstox data for {symbol}")
                success_count += 1
            else:
                logger.error(f"Error updating Upstox data for {symbol}: {msg}")
                fail_count += 1
                failed_stocks.append({'symbol': symbol, 'error': msg})
        if success_count == 0 and fail_count == 0:
            return "No new data to update for any stock", 0, 0, []
        return f"Successfully updated Upstox data for {success_count} symbols, Failed: {fail_count}", success_count, fail_count, failed_stocks
    except Exception as exc:
        logger.exception('Error updating Upstox data for %s', instrument_keys)
        return f"Error fetching Upstox data : {str(exc)}", 0, 0, []

def update_stock_daily_upstox(symbol, instrument_key):
    """Perform an incremental Upstox update combining historical and intraday data."""
    try:
        last_date_query = text(f'SELECT MAX("timestamp") FROM public."{symbol}"')
        engine = rd.get_engine('nsedata')
        with engine.connect() as conn:
            last_date_result = conn.execute(last_date_query).scalar_one_or_none()

        if last_date_result is None:
            return f"Table '{symbol}' is empty. Please run a full historical load first."

        start_date_obj = last_date_result.date() + dt.timedelta(days=1)
        today = dt.date.today()
        # check if today is a weekend (Saturday or Sunday) or a holiday
        if today.weekday() >= 5 or today in get_trading_holidays():
            end_date_obj = today - dt.timedelta(days=1 if today.weekday() == 5 else 2) 
        else:
            end_date_obj = today

        if start_date_obj > end_date_obj:
            return f"Data for '{symbol}' is already up to date."

        frames = []
        if start_date_obj < end_date_obj:
            hist_df = upstox_utils.get_historical_data(
                instrument_token=instrument_key,
                interval='days',
                unit='1',
                start_date=start_date_obj.strftime('%Y-%m-%d'),
                end_date=end_date_obj.strftime('%Y-%m-%d')
            )

            if isinstance(hist_df, pd.DataFrame) and not hist_df.empty:
                hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp']).dt.tz_localize(None)
                hist_df = hist_df[hist_df['timestamp'].dt.date >= start_date_obj]
                frames.append(hist_df)

        # Get intraday data only if start_date_obj is same as end_date_obj
        if start_date_obj == end_date_obj:
            intraday_df = upstox_utils.get_intraday_candle_data(instrument_key)

            if isinstance(intraday_df, pd.DataFrame) and not intraday_df.empty:
                intraday_df['timestamp'] = pd.to_datetime(intraday_df['timestamp']).dt.tz_localize(None)
                frames.append(intraday_df)
     
        if not frames:
            return f"No new data to update for '{symbol}'"

        combined_df = pd.concat(frames, ignore_index=True)
        combined_df.dropna(subset=['timestamp'], inplace=True)
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp']).dt.tz_localize(None)
        combined_df = combined_df[combined_df['timestamp'].dt.date >= start_date_obj]
        combined_df.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        combined_df.sort_values('timestamp', inplace=True)

        if combined_df.empty:
            return f"No new data to update for '{symbol}'"

        return rd.load_sql_data(
            data_to_load=combined_df,
            table_name=symbol,
            load_type='append',
            database='nsedata',
            schema='public'
        )

    except Exception as exc:
        logger.exception('Error updating Upstox data for %s', symbol)
        return f"Error fetching Upstox data for {symbol}: {str(exc)}"


def has_streamlit_context():
    try:
        return get_script_run_ctx() is not None
    except Exception:
        return False


def safe_spinner(message):
    if has_streamlit_context():
        return st.spinner(message)
    return nullcontext()


def display_toaster(status, msg, custom_icon=':material/info_i:', use_default_icon=True):
    colour = 'green' if status == 'Success' else 'red'
    if use_default_icon:
        icon_to_use = ":material/check:" if status == 'Success' else ":material/error:"
    else:
        icon_to_use = custom_icon

    if not has_streamlit_context():
        log_fn = logger.info if status == 'Success' else logger.warning
        if 'fail' in msg.lower() or status == 'Failure':
            log_fn = logger.error
        log_fn('%s', msg)
        return

    st.toast(f':{colour if status != "Skipped" else "blue"}' + f'[{msg}]', icon=icon_to_use)
    if status == 'Success':
        st.success(msg, icon=icon_to_use)
    elif 'fail' in msg.lower():
        st.error(msg, icon=icon_to_use)
    else:
        st.info(msg)


def run_batch_daily_stock_update(data_source='NSE', bulk_update_via_upstox=False):
    """Run the batch daily stock update without UI dependencies."""
    summary = {"success": 0, "failed": [], "data_source": data_source}
    try:
        stocks_df = rd.get_table_data("nsedata", "STOCKS_IN_DB")
    except Exception as exc:
        msg = f"Failed to fetch stocks for batch update: {exc}"
        logger.exception(msg)
        summary["error"] = msg
        return summary

    if stocks_df.empty:
        msg = "No stocks found in 'STOCKS_IN_DB'. Skipping batch update."
        logger.warning(msg)
        summary["message"] = msg
        return summary

    instruments_df = None
    if data_source == 'Upstox':
        try:
            instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
            if bulk_update_via_upstox:
                logger.info("Bulk updating Upstox data for all stocks")
                stocks_to_update = rd.get_table_data("nsedata", "STOCKS_IN_DB")
                instruments_list = stocks_to_update['instrument_token'].tolist()
                instrument_keys = ','.join(instruments_list)
                result, success_count, fail_count, failed_stocks = bulk_update_stock_daily_upstox(instrument_keys)
                summary['success'] += success_count
                summary['failed'].extend(failed_stocks)
                summary['message'] = f"Bulk Upstox data update complete. Success: {summary['success']}, Failed: {len(summary['failed'])}"
                logger.info(summary['message'])
                if summary['failed']:
                    logger.warning("Failed Upstox updates: %s", summary['failed'])
                return summary

        except Exception as exc:
            msg = f"Failed to fetch instruments for Upstox batch update: {exc}"
            logger.exception(msg)
            summary["error"] = msg
            return summary

    for _, row in stocks_df.iterrows():
        symbol = row.get('SYMBOL')

        if not symbol:
            continue
        try:
            if data_source == 'NSE':
                result = rd.update_stock_daily(symbol)
            else:
                instrument_match = instruments_df[instruments_df['trading_symbol'] == symbol]
                if instrument_match.empty:
                    raise ValueError(f"Instrument token not found for {symbol}")
                instrument_key = instrument_match['instrument_token'].iloc[0]
                result = update_stock_daily_upstox(symbol, instrument_key)

            result_text = str(result).lower()
            if 'success' in result_text or 'up to date' in result_text:
                summary['success'] += 1
            else:
                summary['failed'].append({'symbol': symbol, 'error': str(result)})
        except Exception as exc:
            logger.exception("Error updating stock %s", symbol)
            summary['failed'].append({'symbol': symbol, 'error': str(exc)})

    summary['message'] = f"Batch stock update complete. Success: {summary['success']}, Failed: {len(summary['failed'])}"
    logger.info(summary['message'])
    if summary['failed']:
        logger.warning("Failed stock updates: %s", summary['failed'])
    return summary


def run_batch_daily_index_sector_update():
    """Run the batch daily index and sector update without UI dependencies."""
    summary = {"success": 0, "failed": []}
    try:
        indices = fetch_indicies_sectors_list(required='indices') or []
        sectors = fetch_indicies_sectors_list(required='sectors') or []
    except Exception as exc:
        msg = f"Failed to fetch indices/sectors for batch update: {exc}"
        logger.exception(msg)
        summary["error"] = msg
        return summary

    seen = set()
    all_symbols = []
    for symbol in indices + sectors:
        if symbol and symbol not in seen:
            seen.add(symbol)
            all_symbols.append(symbol)

    if not all_symbols:
        msg = "No indices or sectors available for batch update."
        logger.warning(msg)
        summary["message"] = msg
        return summary

    for symbol in all_symbols:
        try:
            result = rd.update_index_sector_daily(symbol)
            result_text = str(result).lower()
            if 'success' in result_text or 'up to date' in result_text:
                summary['success'] += 1
            else:
                summary['failed'].append({'symbol': symbol, 'error': str(result)})
        except Exception as exc:
            logger.exception("Error updating index/sector %s", symbol)
            summary['failed'].append({'symbol': symbol, 'error': str(exc)})

    summary['message'] = f"Batch index/sector update complete. Success: {summary['success']}, Failed: {len(summary['failed'])}"
    logger.info(summary['message'])
    if summary['failed']:
        logger.warning("Failed index/sector updates: %s", summary['failed'])
    return summary


# Function to simulate data loading or updating portfolio
def perform_data_load(data_type='Equity', load_freq='Daily', **kwargs):
    with safe_spinner(f"Performing {data_type} {load_freq} data load/update..."):
        load_status, load_msg = _perform_data_load_core(data_type=data_type, load_freq=load_freq, **kwargs)

    if has_streamlit_context():
        display_toaster(status=load_status, msg=load_msg)
    else:
        log_fn = logger.info if load_status == 'Success' else logger.warning
        if load_status == 'Failure':
            log_fn = logger.error
        log_fn('%s', load_msg)
    return load_msg


def _perform_data_load_core(data_type='Equity', load_freq='Daily', **kwargs):
    if data_type == 'Equity' and load_freq == 'Agg_Data':
        load_status = ag.stocks_agg_data_load()
    elif data_type == 'Equity' and load_freq == 'EOD_Analysis':
        stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB')
        stock_list = stock_list_df['SYMBOL'].values.tolist()
        deduped = []
        for symbol in stock_list:
            if symbol not in deduped:
                deduped.append(symbol)
        logger.info('EOD Analysis task received %s symbols (deduped from %s rows).', len(deduped), len(stock_list))
        stocks_indices_sectors = deduped
        eod_analysis = EODAnalysis(stocks_list=stocks_indices_sectors,
                                   adhoc_date=kwargs.get('date', dt.date.today()),
                                   analysis_days=kwargs.get('analysis_days', 365))

        load_status = eod_analysis.run_analysis()
        print(load_status)
        eod_analysis.print_summary_data_analysis()
    elif data_type == 'MF' and load_freq == 'Historical':
        load_status = mf_hist_load.extract_and_load_latest_mf_hist_data()
    elif data_type in ['Index_data_load', "Index_Stocks_data_load", "Stocks_Ref_data_load", "FnO_snapshot_load",
                       "NSE_Events_load", "ETF_data_load", "Bhavcopy_data_load", "Index_pe_pb_div_load"]:
        if data_type == 'Bhavcopy_data_load':
            # Date should be the previous business day if present time is before 18:00 and today's date if after 18:00
            # After applying the above condition, if the date is a weekend, then the date should be the previous business day
            if dt.datetime.now().hour < 18:
                bhav_date = dt.date.today() - dt.timedelta(days=1)
            else:
                bhav_date = dt.date.today()
            if bhav_date.weekday() >= 5:
                bhav_date = bhav_date - dt.timedelta(days=1 if bhav_date.weekday() == 5 else 2)

            override_date = kwargs.get('date')
            if override_date:
                if isinstance(override_date, str):
                    try:
                        bhav_date = dt.datetime.strptime(override_date, '%d-%m-%Y').date()
                    except ValueError:
                        bhav_date = dt.datetime.strptime(override_date, '%Y-%m-%d').date()
                elif isinstance(override_date, dt.date):
                    bhav_date = override_date

            bhav_date_str = bhav_date.strftime('%d-%m-%Y')
        else:
            override_date = kwargs.get('date')
            if isinstance(override_date, str):
                try:
                    bhav_date_str = dt.datetime.strptime(override_date, '%d-%m-%Y').strftime('%d-%m-%Y')
                except ValueError:
                    bhav_date_str = dt.datetime.strptime(override_date, '%Y-%m-%d').strftime('%d-%m-%Y')
            elif isinstance(override_date, dt.date):
                bhav_date_str = override_date.strftime('%d-%m-%Y')
            else:
                bhav_date_str = (override_date or dt.date.today()).strftime('%d-%m-%Y')
        load_status = load_index_and_stocks_data(data_type, date=bhav_date_str)
    else:
        load_status = 'Skipped'

    load_msg = f'{data_type} {load_freq} data load is {load_status}'
    status_lower = str(load_status).lower()
    if 'success' in status_lower:
        status_label = 'Success'
    elif 'fail' in status_lower:
        status_label = 'Failure'
    else:
        status_label = 'Info'
    return status_label, load_msg


@st.cache_data
def fetch_stocks_data(data_type='Daily', equity_type='Stocks', bhav_copy=False,
                      fetch_count=False, fetch_date=dt.date.today()):
    if equity_type == 'Events':
        return rd.get_table_data(selected_table='NSE_EVENTS')

    if bhav_copy:
        table = 'BHAVCOPY' if equity_type == 'Stocks' else 'NSE_INDICES_DATA'
        return rd.get_table_data(selected_table=table, selected_database='nsedata')

    stock_list_df = rd.get_table_data(selected_table='STOCKS_IN_DB', selected_database='nsedata')
    stocks_list = stock_list_df['SYMBOL'].values.tolist()
    if not stocks_list:
        return pd.DataFrame()

    stock_suffix = {'Weekly': '_W', 'Monthly': '_M', 'Yearly': '_Y'}.get(data_type, '')

    # This query uses a window function to get the latest row for each stock table efficiently.
    union_queries = []
    for stock_name in stocks_list:

        table_name_with_suffix = f'"{stock_name}{stock_suffix}"'
        count_clause = f"""(SELECT count(*) FROM public.{table_name_with_suffix.replace("-", "_")} WHERE timestamp >= '{fetch_date}') as "Row_Count",""" if fetch_count else ""

        union_queries.append(
            f"""
                (SELECT 
                    '{stock_name}' as "Symbol",
                    {count_clause}
                    * FROM public.{table_name_with_suffix.replace("-", "_")}
                ORDER BY timestamp DESC 
                LIMIT 1)
                """
        )

    full_query = " UNION ALL ".join(union_queries)
    return rd.get_table_data(query=full_query, selected_database='nsedata')


HOLIDAY_CACHE = {'dates': set(), 'fetched_on': None}
ACTIVE_TASKS = set()
ACTIVE_TASKS_LOCK = threading.Lock()


def ensure_load_control_table():
    """Create LOAD_DATA_CONTROL table on demand."""
    engine = rd.get_engine('nsedata')
    create_sql = text("""
        CREATE TABLE IF NOT EXISTS public."LOAD_DATA_CONTROL" (
            id BIGSERIAL PRIMARY KEY,
            batch_id TEXT NOT NULL,
            task_name TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            completed_at TIMESTAMP WITHOUT TIME ZONE,
            message TEXT
        );
    """)
    with engine.begin() as conn:
        conn.execute(create_sql)


def get_trading_holidays(force_refresh=False):
    """Fetch trading holidays from NSE_HOLIDAYS table (type='trading')."""
    global HOLIDAY_CACHE
    today = dt.date.today()
    if (not HOLIDAY_CACHE['dates']) or force_refresh or HOLIDAY_CACHE['fetched_on'] != today:
        try:
            query = """
                SELECT trading_date
                FROM public."NSE_HOLIDAYS"
                WHERE type = 'trading'
            """
            df = rd.get_table_data(selected_database='nsedata', query=query)
            if df.empty:
                HOLIDAY_CACHE['dates'] = set()
            else:
                dates = set()
                for value in df['trading_date']:
                    if value is None:
                        continue
                    if hasattr(value, 'date'):
                        dates.add(value.date())
                    else:
                        try:
                            dates.add(dt.datetime.strptime(str(value), '%Y-%m-%d').date())
                        except ValueError:
                            logger.warning('Unrecognized holiday date format: %s', value)
                HOLIDAY_CACHE['dates'] = dates
            HOLIDAY_CACHE['fetched_on'] = today
        except Exception as exc:
            logger.exception('Failed to load trading holidays: %s', exc)
            if force_refresh:
                raise
    return HOLIDAY_CACHE['dates']


def get_daily_batch_id(current_date=None):
    current_date = current_date or dt.date.today()
    return current_date.strftime('SCHEDULED_%Y%m%d')


def is_business_day(check_date=None):
    check_date = check_date or dt.date.today()
    if check_date.weekday() >= 5:
        return False
    holidays = get_trading_holidays()
    return check_date not in holidays


def log_task_status(task_name, status, started_at, completed_at, message, batch_id):
    ensure_load_control_table()
    engine = rd.get_engine('nsedata')
    insert_sql = text("""
        INSERT INTO public."LOAD_DATA_CONTROL"
            (batch_id, task_name, status, started_at, completed_at, message)
        VALUES
            (:batch_id, :task_name, :status, :started_at, :completed_at, :message);
    """)
    params = {
        'batch_id': batch_id,
        'task_name': task_name,
        'status': status,
        'started_at': started_at,
        'completed_at': completed_at,
        'message': message
    }
    try:
        with engine.begin() as conn:
            conn.execute(insert_sql, params)
    except Exception as exc:
        logger.exception('Failed to log status for %s: %s', task_name, exc)


def normalise_task_result(result):
    if isinstance(result, dict):
        message = result.get('message') or str(result)
        failed_entries = result.get('failed') or []
        error = result.get('error')
        is_success = not failed_entries and not error
        return message, is_success
    message = str(result) if result is not None else ''
    lowered = message.lower()
    success_keywords = ['success', 'up to date', 'completed']
    is_success = any(keyword in lowered for keyword in success_keywords) if message else True
    return message, is_success


def run_scheduled_task(task_name, callable_obj, batch_id=None):
    now = dt.datetime.now()
    batch_id = batch_id or get_daily_batch_id(now.date())

    with ACTIVE_TASKS_LOCK:
        if task_name in ACTIVE_TASKS:
            skip_msg = f'Skipped scheduled run because {task_name} is already in progress.'
            logger.info('%s - %s', task_name, skip_msg)
            return {'message': skip_msg, 'status': 'Skipped', 'batch_id': batch_id}
        ACTIVE_TASKS.add(task_name)

    try:
        check_date = now.date()
        holidays = get_trading_holidays()
        reason = None
        if check_date.weekday() >= 5:
            reason = 'weekend'
        elif check_date in holidays:
            reason = 'trading holiday'

        if reason:
            skip_msg = f'Skipped scheduled run because today is a {reason}.'
            log_task_status(task_name, 'Skipped', now, now, skip_msg, batch_id)
            logger.info('%s - %s', task_name, skip_msg)
            return {'message': skip_msg, 'status': 'Skipped', 'batch_id': batch_id}

        status = 'Success'
        message = ''
        try:
            logger.info('Starting scheduled task %s', task_name)
            logger.debug('Invoking callable for %s', task_name)
            result = callable_obj()
            logger.debug('Callable for %s returned %s', task_name, result)
            message, is_success = normalise_task_result(result)
            status = 'Success' if is_success else 'Failure'
        except Exception as exc:
            status = 'Failure'
            message = str(exc)
            logger.exception('Scheduled task %s failed', task_name)
        finished = dt.datetime.now()
        log_task_status(task_name, status, now, finished, message, batch_id)
        logger.info('Completed scheduled task %s with status %s', task_name, status)
        return {'message': message, 'status': status, 'batch_id': batch_id}
    finally:
        with ACTIVE_TASKS_LOCK:
            ACTIVE_TASKS.discard(task_name)


INDEX_DATA_LOAD_TASKS = [
    'Index_data_load',
    'Index_Stocks_data_load',
    'Stocks_Ref_data_load',
    'FnO_snapshot_load',
    'NSE_Events_load',
    'ETF_data_load',
    'Bhavcopy_data_load'
]


def run_all_index_data_load_tasks():
    summary = {'success': 0, 'failed': []}
    for data_type in INDEX_DATA_LOAD_TASKS:
        try:
            result = perform_data_load(data_type=data_type, load_freq='Daily')
            message, is_success = normalise_task_result(result)
            if is_success:
                summary['success'] += 1
            else:
                summary['failed'].append({'task': data_type, 'error': message})
        except Exception as exc:
            logger.exception('Error running %s', data_type)
            summary['failed'].append({'task': data_type, 'error': str(exc)})
    summary['message'] = f"Index data loads complete. Success: {summary['success']}, Failed: {len(summary['failed'])}"
    if summary['failed']:
        logger.warning('Index data load failures: %s', summary['failed'])
    return summary


SCHEDULE_TASK_DEFINITIONS = [
    {
        'name': 'Daily Stock Update',
        'default_time': '16:00',
        'callable': lambda: run_batch_daily_stock_update(data_source='Upstox', bulk_update_via_upstox=True)
    },
    {
        'name': 'Daily Index & Sector Update',
        'default_time': '16:10',
        'callable': run_batch_daily_index_sector_update
    },
    {
        'name': 'Aggregate Equity Data Load',
        'default_time': '16:20',
        'callable': lambda: perform_data_load(data_type='Equity', load_freq='Agg_Data')
    },
    {
        'name': 'EOD Analysis',
        'default_time': '16:30',
        'callable': lambda: perform_data_load(
            data_type='Equity',
            load_freq='EOD_Analysis',
            analysis_date=dt.date.today(),
            analysis_days=365
        )
    },
    {
        'name': 'Index Data Loads',
        'default_time': '16:40',
        'callable': run_all_index_data_load_tasks
    },
    {
        'name': 'MF Daily Data Load',
        'default_time': '16:50',
        'callable': lambda: perform_data_load(data_type='MF', load_freq='Daily')
    }
]


def get_default_schedule_config():
    return {item['name']: item['default_time'] for item in SCHEDULE_TASK_DEFINITIONS}


def normalise_time_value(value, fallback):
    if isinstance(value, dt.time):
        return value.strftime('%H:%M')
    if isinstance(value, dt.datetime):
        return value.strftime('%H:%M')
    if isinstance(value, str):
        try:
            dt.datetime.strptime(value, '%H:%M')
            return value
        except ValueError:
            pass
    logger.warning("Invalid schedule time '%s'. Falling back to %s", value, fallback)
    return fallback


def get_schedule_config():
    defaults = get_default_schedule_config()
    stored = st.session_state.get('schedule_config')
    if not stored:
        st.session_state['schedule_config'] = defaults.copy()
        return st.session_state['schedule_config']

    normalised = {}
    for name, default_time in defaults.items():
        normalised[name] = normalise_time_value(stored.get(name, default_time), default_time)
    st.session_state['schedule_config'] = normalised
    return normalised



def get_task_by_name(task_name):
    for task in SCHEDULE_TASK_DEFINITIONS:
        if task['name'] == task_name:
            return task
    raise KeyError(f"Unknown task name: {task_name}")


def is_final_task(task_name):
    return task_name == SCHEDULE_TASK_DEFINITIONS[-1]['name']


def ensure_schedule_state():
    if 'scheduled_jobs' not in st.session_state:
        st.session_state['scheduled_jobs'] = {}
    if 'schedule_config' not in st.session_state:
        st.session_state['schedule_config'] = get_default_schedule_config()


def cancel_task_job(task_name):
    ensure_schedule_state()
    job_info = st.session_state['scheduled_jobs'].pop(task_name, None)
    if job_info and job_info.get('job'):
        try:
            schedule.cancel_job(job_info['job'])
        except schedule.ScheduleValueError:
            pass


def cancel_all_scheduled_jobs():
    ensure_schedule_state()
    for job_info in list(st.session_state['scheduled_jobs'].values()):
        job = job_info.get('job')
        if job:
            try:
                schedule.cancel_job(job)
            except schedule.ScheduleValueError:
                pass
    st.session_state['scheduled_jobs'].clear()


def schedule_task_job(task_name, time_override=None):
    ensure_schedule_state()
    task = get_task_by_name(task_name)
    defaults = get_default_schedule_config()
    config = get_schedule_config()

    time_str = normalise_time_value(time_override or config.get(task_name) or defaults[task_name], defaults[task_name])

    cancel_task_job(task_name)

    def job():
        result = run_scheduled_task(task_name, task['callable'])
        if is_final_task(task_name) and result.get('status') != 'Skipped':
            send_schedule_summary_email(result['batch_id'])
        return result

    scheduled_job = schedule.every().day.at(time_str).do(job)
    st.session_state['schedule_config'][task_name] = time_str
    st.session_state['scheduled_jobs'][task_name] = {
        'job': scheduled_job,
        'time': time_str
    }
    return time_str




def get_upcoming_scheduled_tasks(limit=5):
    ensure_schedule_state()
    upcoming = []
    for name, info in st.session_state['scheduled_jobs'].items():
        job = info.get('job')
        next_run = getattr(job, 'next_run', None) if job else None
        if next_run is not None:
            upcoming.append((next_run, name))
    upcoming.sort(key=lambda item: item[0])
    return upcoming[:limit]


def get_scheduled_task_times():
    ensure_schedule_state()
    return {name: info.get('time') for name, info in st.session_state['scheduled_jobs'].items()}


def get_task_status(task_name):
    ensure_schedule_state()
    return 'Scheduled' if task_name in st.session_state['scheduled_jobs'] else 'Paused'


def fetch_batch_results(batch_id):
    engine = rd.get_engine('nsedata')
    query = text("""
        SELECT task_name, status, started_at, completed_at, COALESCE(message, '') AS message
        FROM public."LOAD_DATA_CONTROL"
        WHERE batch_id = :batch_id
        ORDER BY started_at;
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={'batch_id': batch_id})


def fetch_task_history(start_date=None, end_date=None, limit=200):
    """Retrieve task execution history from LOAD_DATA_CONTROL."""
    ensure_load_control_table()
    engine = rd.get_engine('nsedata')

    start_date = start_date or (dt.date.today() - dt.timedelta(days=7))
    end_date = end_date or dt.date.today()
    start_dt = dt.datetime.combine(start_date, dt.time.min)
    end_dt = dt.datetime.combine(end_date, dt.time.max)
    limit = max(1, int(limit))

    base_query = (
        """
        SELECT batch_id,
               task_name,
               status,
               started_at,
               completed_at,
               message
        FROM public."LOAD_DATA_CONTROL"
        WHERE started_at BETWEEN :start_dt AND :end_dt
        ORDER BY started_at DESC
        LIMIT :limit
        """
    )

    with engine.connect() as conn:
        return pd.read_sql(
            text(base_query),
            conn,
            params={'start_dt': start_dt, 'end_dt': end_dt, 'limit': limit}
        )

def send_schedule_summary_email(batch_id):
    """Send an email summary for the scheduled batch.

    Environment variables used:
        SMTP_SENDER: From address used for the email.
        SMTP_USER: SMTP login username (defaults to SMTP_SENDER).
        SMTP_PASSWORD: SMTP login password.
        SMTP_HOST: SMTP host (defaults to 'smtp.gmail.com').
        SMTP_PORT: SMTP port (defaults to '587').
        SMTP_USE_TLS: 'true' or 'false' to control STARTTLS (defaults to true).
        SCHEDULE_EMAIL_RECIPIENTS: Comma-separated list of recipients.
    """
    recipients = os.getenv('SCHEDULE_EMAIL_RECIPIENTS')
    sender = os.getenv('SMTP_SENDER')
    username = os.getenv('SMTP_USER') or sender
    host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    port = os.getenv('SMTP_PORT', '587')
    password = os.getenv('SMTP_PASSWORD')
    use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'

    if not (recipients and sender):
        logger.warning('Email summary skipped: SMTP_SENDER and SCHEDULE_EMAIL_RECIPIENTS are required.')
        return

    try:
        batch_results = fetch_batch_results(batch_id)
    except Exception as exc:
        logger.exception('Unable to fetch batch results for email summary: %s', exc)
        return

    if batch_results.empty:
        logger.warning('No batch results found for batch %s. Skipping email.', batch_id)
        return

    subject = f"Scheduled Data Load Summary - {batch_id}"
    lines = ['Scheduled tasks summary', f"Batch: {batch_id}", '']
    for _, row in batch_results.iterrows():
        started = row['started_at']
        completed = row['completed_at']
        message = row['message'] or ''
        lines.append(f"- {row['task_name']}: {row['status']} (start: {started}, end: {completed})")
        if message:
            lines.append(f"  details: {message}")
    body = '\n'.join(lines)

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ','.join([addr.strip() for addr in recipients.split(',') if addr.strip()])
    msg.set_content(body)

    try:
        port_int = int(port)
    except ValueError:
        logger.error('Invalid SMTP_PORT value: %s', port)
        return

    try:
        with smtplib.SMTP(host, port_int, timeout=30) as smtp:
            if use_tls:
                smtp.starttls()
            if username and password:
                smtp.login(username, password)
            smtp.send_message(msg)
        logger.info('Summary email sent for batch %s', batch_id)
    except Exception as exc:
        logger.exception('Failed to send summary email: %s', exc)

def load_historical_stock_data_in_chunks(stock_symbol, instrument_key):
    """
    NEW: Fetches historical data for a stock in two 9-year chunks and loads it to the DB.
    """
    try:
        with st.spinner(f"Initiating historical data load for {stock_symbol}..."):
            start_date_chunk1 = dt.date(2007, 1, 1)
            end_date_chunk1 = start_date_chunk1 + dt.timedelta(days=9 * 365)  # Approx. 9 years

            start_date_chunk2 = end_date_chunk1 + dt.timedelta(days=1)
            end_date_chunk2 = dt.date.today()

            st.write(f"Fetching Chunk 1: {start_date_chunk1} to {end_date_chunk1}")

            # Fetch and load the first chunk
            data_chunk1 = upstox_utils.get_historical_data(
                instrument_token=instrument_key,
                interval="days",
                unit="1",
                start_date=start_date_chunk1,
                end_date=end_date_chunk1
            )

            if not data_chunk1.empty:
                data_chunk1['timestamp'] = data_chunk1['timestamp'].dt.tz_localize(None)
                load_msg1 = rd.load_sql_data(
                    data_to_load=data_chunk1,
                    table_name=stock_symbol.replace(' ', '_'),
                    load_type='replace',  # Replace table with the first chunk
                    database='nsedata',
                    schema='public'
                )
                display_toaster('Success' if 'success' in load_msg1 else 'Failure', f"Chunk 1: {load_msg1}")
                chunk1_loaded = True
            else:
                display_toaster('Skipped', f"Chunk 1: No data received for {stock_symbol} (likely listed after {end_date_chunk1}).")
                chunk1_loaded = False

            st.write(f"Fetching Chunk 2: {start_date_chunk2} to {end_date_chunk2}")
            if not chunk1_loaded:
                st.info(f"Chunk 1 had no data, so Chunk 2 will be used to create the initial table for {stock_symbol}")

            # Fetch and load the second chunk
            data_chunk2 = upstox_utils.get_historical_data(
                instrument_token=instrument_key,
                interval='days',
                unit="1",
                start_date=start_date_chunk2,
                end_date=end_date_chunk2
            )

            if not data_chunk2.empty:
                data_chunk2['timestamp'] = data_chunk2['timestamp'].dt.tz_localize(None)
                # Use 'replace' if chunk 1 wasn't loaded, 'append' if it was
                load_type = 'replace' if not chunk1_loaded else 'append'
                load_msg2 = rd.load_sql_data(
                    data_to_load=data_chunk2,
                    table_name=stock_symbol.replace(' ', '_'),
                    load_type=load_type,
                    database='nsedata',
                    schema='public'
                )
                display_toaster('Success' if 'success' in load_msg2 else 'Failure', f"Chunk 2: {load_msg2}")
                
                # Register the stock after successfully loading data (either chunk 1 or chunk 2)
                if not chunk1_loaded:
                    rd.add_stock_to_registry(stock_symbol, instrument_key, database='nsedata')

            else:
                display_toaster('Skipped', f"Chunk 2: No data received for {stock_symbol}.")
                # If neither chunk has data, this might be an invalid stock or API issue
                if not chunk1_loaded:
                    display_toaster('Failure', f"No data available for {stock_symbol} in either time period.")
                    return 'Failure'

        return 'Success'
    except Exception as e:
        st.error(f"An error occurred during historical data load for {stock_symbol}: {e}")
        return 'Failure'


# Scheduler function
def run_scheduled_tasks():
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


def schedule_data_loads(schedule_config=None):
    """Configure scheduled jobs between 16:00-17:00 on business days."""
    ensure_schedule_state()
    cancel_all_scheduled_jobs()

    defaults = get_default_schedule_config()
    config_source = schedule_config or st.session_state.get('schedule_config') or defaults
    normalised_config = {}
    for task in SCHEDULE_TASK_DEFINITIONS:
        name = task['name']
        default_time = defaults[name]
        normalised_config[name] = normalise_time_value(config_source.get(name, default_time), default_time)

    st.session_state['schedule_config'] = normalised_config

    for task in SCHEDULE_TASK_DEFINITIONS:
        schedule_task_job(task['name'], normalised_config[task['name']])


# Main Streamlit app
def dataload():
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduled_tasks, daemon=True)
    scheduler_thread.start()

    load_or_view = st.sidebar.radio("Choose option", options=['Load', 'View', 'Schedule'], horizontal=True)

    if load_or_view == 'Schedule':
        st.subheader('Schedule Data Loads')
        
        # Enhanced scheduler interface
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### Configure Schedule Times")
            schedule_config = get_schedule_config()

            sch_cols = st.columns([1, 1])
            with sch_cols[0]:
                if st.button("Reset Times to Default", width='content'):
                    st.session_state['schedule_config'] = get_default_schedule_config()
                    schedule_config = get_schedule_config()
                    st.toast('Schedule times reset to defaults.', icon=':material/history:')

                for idx, task in enumerate(SCHEDULE_TASK_DEFINITIONS):
                    current_time = schedule_config.get(task['name'], task['default_time'])
                    try:
                        time_value = dt.datetime.strptime(current_time, '%H:%M').time()
                    except ValueError:
                        time_value = dt.datetime.strptime(task['default_time'], '%H:%M').time()
                    new_time = st.time_input(
                        task['name'],
                        value=time_value,
                        step=dt.timedelta(minutes=2),
                        key=f'schedule_time_{idx}'
                    )
                    schedule_config[task['name']] = new_time.strftime('%H:%M')

                st.session_state['schedule_config'] = schedule_config

            with sch_cols[1]:
                st.markdown("#### Current Schedule")
                for task in SCHEDULE_TASK_DEFINITIONS:
                    status = get_task_status(task['name'])
                    st.write(f"**{task['name']}**: {schedule_config[task['name']]} ({status})")
        
        with col2:
            st.markdown("### Scheduler Controls")
            management_task = st.selectbox(
                "Select Task to Manage",
                options=[task['name'] for task in SCHEDULE_TASK_DEFINITIONS],
                index=0,
                key='schedule_management_task'
            )
            current_status = get_task_status(management_task)
            configured_time = get_schedule_config().get(management_task, get_default_schedule_config()[management_task])
            st.caption(f"Status: **{current_status}** | Time: {configured_time}")

            manage_cols = st.columns(2)
            with manage_cols[0]:
                if st.button("Schedule/Resume Selected Task", width='content', key='btn_schedule_selected'):
                    scheduled_time = schedule_task_job(management_task, get_schedule_config().get(management_task))
                    st.success(f"{management_task} scheduled at {scheduled_time}.")
            with manage_cols[1]:
                if st.button("Pause Selected Task", width='content', key='btn_pause_selected'):
                    cancel_task_job(management_task)
                    st.info(f"{management_task} paused.")

            if st.button("Refresh Trading Holidays", width='content', key='btn_refresh_holidays'):
                try:
                    holidays = get_trading_holidays(force_refresh=True)
                    st.success(f"Holiday calendar refreshed ({len(holidays)} dates).")
                except Exception as exc:
                    st.error(f"Failed to refresh holidays: {exc}")

            st.divider()
            if st.button("Schedule All Tasks", width='content', key='btn_schedule_all'):
                schedule_data_loads(st.session_state.get('schedule_config'))
                st.success("All tasks have been scheduled with the updated timetable!")

            if st.button("Clear All Schedules", width='content', key='btn_clear_all'):
                schedule.clear()
                cancel_all_scheduled_jobs()
                st.warning("All schedules have been cleared!")

        st.markdown("---")

        st.markdown("### Scheduled Task History")
        history_cols = st.columns([1, 1, 1])
        with history_cols[0]:
            history_start = st.date_input("Start Date", value=dt.date.today() - dt.timedelta(days=7), key='history_start')
        with history_cols[1]:
            history_end = st.date_input("End Date", value=dt.date.today(), key='history_end')
        with history_cols[2]:
            history_limit = st.number_input("Max Rows", min_value=10, max_value=1000, value=200, step=10, key='history_limit')

        history_error = None
        if history_end < history_start:
            history_error = "End date cannot be earlier than start date."

        history_button_cols = st.columns([0.2, 0.8])
        with history_button_cols[0]:
            load_history = st.button("Load History", width='content', key='btn_load_history')

        if history_error:
            st.error(history_error)
        elif load_history:
            with st.spinner("Fetching task history..."):
                history_df = fetch_task_history(history_start, history_end, history_limit)
            if history_df.empty:
                st.info("No task executions found for the selected period.")
            else:
                hist_cols = st.columns([.85, .15])
                with hist_cols[0]:
                    st.dataframe(history_df, width='content', hide_index=True)
                with hist_cols[1]:
                    status_counts = history_df['status'].value_counts()
                    st.write("**Status summary:**")
                    st.write(status_counts.to_frame(name='count'))

        st.markdown("---")
        
        # Enhanced manual execution section
        st.markdown("### Manual Task Execution")
        
        # Task selection
        task_options = {
            "Equity Daily Data Load": ("Equity", "Daily"),
            "EOD Analysis": ("Equity", "EOD_Analysis"),
            "Portfolio Update": ("Portfolio", "Update"),
            "MF Data Load": ("MF", "Daily"),
            "Index Data Load": ("Index_data_load", "Daily"),
            "Bhavcopy Data Load": ("Bhavcopy", "Daily"),
            "All Tasks": ("All", "All")
        }
        
        selected_task = st.selectbox(
            "Select Task to Execute",
            options=list(task_options.keys()),
            index=0,
            help="Choose which task to run manually"
        )
        
        # Additional options for specific tasks
        if selected_task == "EOD Analysis":
            analysis_date = st.date_input("Analysis Date", value=dt.date.today(), max_value=dt.date.today())
            analysis_days = st.number_input("Lookback Period (days)", min_value=5, max_value=5000, value=365)
        
        if selected_task == "Bhavcopy Data Load":
            bhavcopy_date = st.date_input("Bhavcopy Date", value=dt.date.today(), max_value=dt.date.today())
        
        # Execution button
        if st.button(f"Execute {selected_task}", width='content'):
            if selected_task == "All Tasks":
                # Execute all tasks in sequence
                with st.status("Executing all scheduled tasks...", expanded=True) as status:
                    task_results = {}
                    
                    # Task 1: Equity Daily Data Load
                    st.write("📊 Executing Equity Daily Data Load...")
                    try:
                        result1 = perform_data_load(data_type='Equity', load_freq='Daily')
                        task_results["Equity Daily Data Load"] = "Success" if "success" in str(result1) else "Failed"
                        st.write(f"✅ Equity Daily Data Load: {task_results['Equity Daily Data Load']}")
                    except Exception as e:
                        task_results["Equity Daily Data Load"] = f"Error: {str(e)}"
                        st.write(f"❌ Equity Daily Data Load: {task_results['Equity Daily Data Load']}")
                    
                    # Task 2: EOD Analysis
                    st.write("📈 Executing EOD Analysis...")
                    try:
                        result2 = perform_data_load(data_type='Equity', load_freq='EOD_Analysis', 
                                                  analysis_date=dt.date.today(), analysis_days=365)
                        task_results["EOD Analysis"] = "Success" if "success" in str(result2) else "Failed"
                        st.write(f"✅ EOD Analysis: {task_results['EOD Analysis']}")
                    except Exception as e:
                        task_results["EOD Analysis"] = f"Error: {str(e)}"
                        st.write(f"❌ EOD Analysis: {task_results['EOD Analysis']}")
                    
                    # Task 3: Portfolio Update
                    st.write("💼 Executing Portfolio Update...")
                    try:
                        result3 = update_portfolio.update_overall_portfolio_summary(
                            fetch_type='load_and_fetch',
                            for_date=dt.date.today(),
                            mf_snap_reload=False,
                            bhavcopy_reload=False
                        )
                        task_results["Portfolio Update"] = "Success" if "success" in str(result3) else "Failed"
                        st.write(f"✅ Portfolio Update: {task_results['Portfolio Update']}")
                    except Exception as e:
                        task_results["Portfolio Update"] = f"Error: {str(e)}"
                        st.write(f"❌ Portfolio Update: {task_results['Portfolio Update']}")
                    
                    # Task 4: MF Data Load
                    st.write("📈 Executing MF Data Load...")
                    try:
                        result4 = perform_data_load(data_type='MF', load_freq='Daily')
                        task_results["MF Data Load"] = "Success" if "success" in str(result4) else "Failed"
                        st.write(f"✅ MF Data Load: {task_results['MF Data Load']}")
                    except Exception as e:
                        task_results["MF Data Load"] = f"Error: {str(e)}"
                        st.write(f"❌ MF Data Load: {task_results['MF Data Load']}")
                    
                    # Task 5: Index Data Load
                    st.write("📊 Executing Index Data Load...")
                    try:
                        result5 = perform_data_load(data_type='Index_data_load', load_freq='Daily')
                        task_results["Index Data Load"] = "Success" if "success" in str(result5) else "Failed"
                        st.write(f"✅ Index Data Load: {task_results['Index Data Load']}")
                    except Exception as e:
                        task_results["Index Data Load"] = f"Error: {str(e)}"
                        st.write(f"❌ Index Data Load: {task_results['Index Data Load']}")
                    
                    # Summary
                    status.update(label="All tasks completed!", state="complete")
                    
                    # Display summary
                    st.markdown("### 📋 Execution Summary")
                    summary_df = pd.DataFrame(list(task_results.items()), columns=['Task', 'Status'])
                    st.dataframe(summary_df, width='content')
                    
                    success_count = sum(1 for status in task_results.values() if status == "Success")
                    total_count = len(task_results)
                    st.metric("Success Rate", f"{success_count}/{total_count}", f"{success_count/total_count*100:.1f}%")
                    
            else:
                # Execute single task
                task_type, task_freq = task_options[selected_task]
                
                with st.status(f"Executing {selected_task}...", expanded=True) as status:
                    try:
                        if selected_task == "EOD Analysis":
                            result = perform_data_load(data_type=task_type, load_freq=task_freq, 
                                                     analysis_date=analysis_date, analysis_days=analysis_days)
                        elif selected_task == "Bhavcopy Data Load":
                            from python_scripts.stocks_data_load.bhav_copy_extract import load_bhavcopy_data
                            result = load_bhavcopy_data(bhavcopy_date)
                        elif selected_task == "Portfolio Update":
                            result = update_portfolio.update_overall_portfolio_summary(
                                fetch_type='load_and_fetch',
                                for_date=dt.date.today(),
                                mf_snap_reload=False,
                                bhavcopy_reload=False
                            )
                        else:
                            result = perform_data_load(data_type=task_type, load_freq=task_freq)
                        
                        status.update(label=f"{selected_task} completed!", state="complete")
                        
                        if "success" in str(result).lower():
                            st.success(f"✅ {selected_task} completed successfully!")
                        else:
                            st.warning(f"⚠️ {selected_task} completed with issues: {result}")
                            
                    except Exception as e:
                        status.update(label=f"{selected_task} failed!", state="error")
                        st.error(f"❌ {selected_task} failed: {str(e)}")
        
        # Schedule management
        st.markdown("---")
        st.markdown("### Schedule Management")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("View Next Scheduled Tasks", width='content'):
                upcoming = get_upcoming_scheduled_tasks()
                if upcoming:
                    st.write("**Next scheduled tasks:**")
                    for next_run, task_name in upcoming:
                        time_str = next_run.strftime('%Y-%m-%d %H:%M:%S') if hasattr(next_run, 'strftime') else str(next_run)
                        st.write(f"- {task_name} at {time_str}")
                else:
                    st.info("No tasks are currently scheduled.")
        
        with col2:
            if st.button("Pause Scheduler", width='content'):
                # This would require implementing a pause mechanism
                st.warning("Scheduler pause functionality needs to be implemented.")
        
        with col3:
            if st.button("Resume Scheduler", width='content'):
                # This would require implementing a resume mechanism
                st.info("Scheduler resume functionality needs to be implemented.")

    elif load_or_view == 'View':
        stock_or_events = st.sidebar.selectbox('Choose data to view',
                                                    ['Stocks', 'Indices', 'Events'])

        if stock_or_events == 'Stocks':
            data_type = st.sidebar.radio("Choose data type",
                                         options=['Daily', 'Weekly', 'Monthly', 'Yearly'],
                                         horizontal=True)
            stocks_data_radio = st.sidebar.radio("Stocks/Bhavcopy Data", options=["Stocks", 'Bhavcopy'], horizontal=True)
            if stocks_data_radio:
                st.subheader("View data in SQL database")
                st.write(f'Latest :rainbow[{data_type} snapshot] of {stocks_data_radio} data stored in SQL database')

                if stocks_data_radio == 'Stocks':
                    fetch_date = st.sidebar.date_input("Choose date", value="today", max_value=dt.date.today())
                    st.dataframe(fetch_stocks_data(data_type, fetch_count=True, fetch_date=fetch_date), hide_index=True)
                elif stocks_data_radio == 'Bhavcopy':
                    stocks_index_radio = st.sidebar.radio("Stocks/Index Bhav", options=['Stocks', "Index"],
                                                          horizontal=True)
                    st.dataframe(fetch_stocks_data(equity_type=stocks_index_radio, bhav_copy=True).dropna(),
                                 hide_index=True)
        elif stock_or_events == 'Events':
            st.dataframe(fetch_stocks_data(equity_type=stock_or_events, fetch_count=True), hide_index=True)

    elif load_or_view == 'Load':
        st.subheader('Equity and Mutual Fund Data Loader')

        load_tabs = st.tabs(["Ad-hoc & Portfolio Loaders", "Stock History", "Index/Sector History", "File Upload"])

        with load_tabs[0]:
            st.markdown("##### Ad-hoc Data Loaders")

            eq_col, mf_col, index_col = st.columns([1, 1, 1], vertical_alignment='top', gap="medium")

            # Select box for Equity data load
            equity_load_type = eq_col.selectbox('Select Equity Data Load Type',
                                                    ['Agg_Data', 'EOD_Analysis'])

            if equity_load_type == 'EOD_Analysis':
                date = eq_col.date_input("Select date for analysis", max_value=dt.date.today(), format='YYYY-MM-DD')
                analysis_days = eq_col.number_input("Lookback period for Analysis", min_value=5, max_value=5000, value=365)

            if eq_col.button('Load Equity Data'):
                with st.spinner(f"Equity {equity_load_type} data load in progress.."):
                    perform_data_load(data_type='Equity', load_freq=equity_load_type,
                                      analysis_date=date if equity_load_type == 'EOD_Analysis' else None,
                                      analysis_period=analysis_days if equity_load_type == 'EOD_Analysis' else None)

            # Select box for Mutual Fund data load
            mf_load_type = mf_col.selectbox('Select Mutual Fund Data Load Type',
                                               ['Daily', 'Agg_Data', 'Historical'])
            if mf_col.button('MF Data Load'):
                with st.spinner(f"MF {mf_load_type} data load in progress.."):
                    perform_data_load(data_type='MF', load_freq=mf_load_type)

            with index_col:
                index_load_type = st.selectbox("Choose Index data load type",
                                               options=['Index_data_load',
                                                        "Index_Stocks_data_load",
                                                        "Stocks_Ref_data_load",
                                                        "FnO_snapshot_load",
                                                        "NSE_Events_load",
                                                        "ETF_data_load",
                                                        "Bhavcopy_data_load",
                                                        "Index_pe_pb_div_load"],)
                
                # Add date selection for Bhavcopy
                if index_load_type == 'Bhavcopy_data_load':
                    bhavcopy_date = st.date_input("Select Date for Bhavcopy", 
                                                value=dt.date.today(), 
                                                max_value=dt.date.today(),
                                                help="Select the date for which to load Bhavcopy data")
                
                if st.button('Load Index Data'):
                    with st.spinner(f"{index_load_type} in progress.."):
                        perform_data_load(data_type=index_load_type)
            
            st.markdown('---')
            row_cols = st.columns(2)
            with row_cols[0]:
                st.markdown('### Update Portfolio in the SQL database..')
                # Button to update portfolio in SQL Server
                if st.button('Update Portfolio'):
                    # Replace with actual SQL update logic
                    current_date = dt.date.today()
                    with st.spinner("Updating portfolio in SQL Server..."):
                        date_day = current_date.strftime("%A")
                        if date_day == 'Sunday':
                            for_date = current_date - dt.timedelta(days=2)
                        elif date_day == 'Saturday':
                            for_date = current_date - dt.timedelta(days=1)
                        elif date_day == 'Monday':
                            if dt.datetime.now().hour < 19:
                                for_date = current_date - dt.timedelta(days=3)
                            else:
                                for_date = current_date
                        else:
                            if dt.datetime.now().hour < 19:
                                for_date = current_date - dt.timedelta(days=1)
                            else:
                                for_date = current_date
                        pf_load_msg = update_portfolio.update_overall_portfolio_summary(fetch_type='load_and_fetch',
                                                                                        for_date=for_date,
                                                                                        mf_snap_reload=False,
                                                                                        bhavcopy_reload=False)
                        load_status = 'Success' if 'success' in pf_load_msg else "Failure"
                        display_toaster(status=load_status, msg=pf_load_msg)
            with row_cols[1]:
                # Check if instrument_token for each symbol in STOCKS_IN_DB is matching with instrument_token for corresponding trading_symbol in instruments table in trading_db
                # Read data from STOCKS_IN_DB and instruments table in trading_db. do inner join on trading_symbol and SYMBOL and check if instrument_token is matching. No for loop required.
                stocks_to_update = rd.get_table_data("nsedata", "STOCKS_IN_DB")
                instruments_df = rd.get_table_data("trading_db", "instruments")

                if st.button('Check instrument_token matching'):
                    with st.spinner("Checking instrument_token matching..."):
                        if not stocks_to_update.empty and not instruments_df.empty:
                            joined_df = pd.merge(stocks_to_update, instruments_df, left_on='SYMBOL', right_on='trading_symbol', how='inner')
                            joined_df = joined_df[joined_df['instrument_token_x'] != joined_df['instrument_token_y']]
                            joined_df = joined_df[['trading_symbol', 'instrument_token_x', 'instrument_token_y']]
                            joined_df.rename(columns={'instrument_token_x': 'instrument_token_in_stocks_in_db', 'instrument_token_y': 'instrument_token_in_instruments_table'}, inplace=True)
                            if joined_df.empty:
                                st.success("No mismatching instrument_token found for any stock")
                            else:
                                st.dataframe(joined_df, width='content')
                                st.warning("Mismatching instrument_token found for above stocks")
                                symbols_to_update = joined_df['trading_symbol'].tolist()
                                # Search for mismatched instrument_token in STOCKS_IN_DB and update the instrument_token
                                for index, row in joined_df.iterrows():
                                    symbol = row['trading_symbol']
                                    instrument_token = row['instrument_token_in_instruments_table']
                                    # Find the row in STOCKS_IN_DB where SYMBOL is matching with trading_symbol
                                    stock_to_update = stocks_to_update[stocks_to_update['SYMBOL'] == symbol]
                                    if not stock_to_update.empty:
                                        stocks_to_update.loc[stock_to_update.index, 'instrument_token'] = instrument_token
                                        stocks_to_update.loc[stock_to_update.index, 'last_updated'] = dt.datetime.now()
                                        logger.info(f"Instrument_token updated successfully for {symbol}")
                                    else:
                                        st.warning(f"No stock found in STOCKS_IN_DB for {symbol}")
                                rd.load_sql_data(data_to_load=stocks_to_update, table_name='STOCKS_IN_DB', load_type='replace', database='nsedata', schema='public')
                                st.success(f"Instrument_token updated successfully for {symbols_to_update}!")
                        else:
                            st.error("No stocks found in STOCKS_IN_DB or instruments table in trading_db")

            st.markdown('---')
            st.markdown('### Check and remove duplicate data from the tables referred in STOCKS_IN_DB')
            if st.button('Check and remove duplicate data'):
                with st.spinner("Checking and removing duplicate data..."):
                    stocks_to_update = rd.get_table_data("nsedata", "STOCKS_IN_DB")
                    duplicate_count = 0
                    fail_count = 0
                    failed_stocks = []
                    if not stocks_to_update.empty:
                        for index, row in stocks_to_update.iterrows():
                            symbol = row['SYMBOL']
                            table_data = rd.get_table_data("nsedata", f"{symbol}")
                            if not table_data.empty:
                                # check for duplicates first and go for remove and reload only if duplicates are found
                                if table_data.duplicated(subset='timestamp').any():
                                    table_data.drop_duplicates(subset='timestamp', keep='last', inplace=True)
                                    table_data.sort_values('timestamp', inplace=True)
                                    rd.load_sql_data(data_to_load=table_data, table_name=symbol, load_type='replace', database='nsedata', schema='public')
                                    logger.info(f"Duplicate data removed for {symbol}...")
                                    duplicate_count += 1
                                else:
                                    logger.info(f"No duplicate data found for {symbol}...")
                            else:
                                fail_count += 1
                                failed_stocks.append({'symbol': symbol, 'error': "No data found for the stock"})
                                st.write(f"No data found for {symbol}...")
                    st.success("Duplicate data removed successfully!")
                    st.write(f"Duplicate data removed for {duplicate_count} stocks, Failed: {fail_count}")
                    if failed_stocks:
                        st.dataframe(failed_stocks, width='content')
                    else:
                        st.success("No duplicate data found for any stock")
        with load_tabs[1]:
            st.markdown("##### Historical Stock Data Loader")
            data_src_cols = st.columns(5)
            data_source = data_src_cols[0].selectbox("Select Data Source",
                                                       options=['NSE', 'Upstox'],
                                                       index=0,
                                                       help="Choose the source for historical data.")
            # --- NEW: Batch daily update for stocks ---
            st.markdown("---")

            load_cols = st.columns([.3, .4, .4], gap="medium")

            with load_cols[0]:
                st.subheader("Batch Daily Update")
                
                # Add data source selection for batch update
                batch_data_source = st.selectbox("Data Source for Batch Update", 
                                                options=['NSE', 'Upstox'], 
                                                index=0, 
                                                key="batch_data_source",
                                                help="Choose the source for daily batch updates")

                bulk_update_via_upstox = st.checkbox("Bulk Update via Upstox", value=False, help="Use this option only for latest one day data update alone")
                
                if st.button("Update All Stocks Daily", width='content'):
                    stocks_to_update = rd.get_table_data("nsedata", "STOCKS_IN_DB")
                    if not stocks_to_update.empty:
                        success_count = 0
                        fail_count = 0
                        failed_stocks = []
                        
                        with st.status("Performing batch daily update for stocks...", expanded=True) as status:
                            if bulk_update_via_upstox:
                                instruments_list = stocks_to_update['instrument_token'].tolist()
                                instrument_keys = ','.join(instruments_list)
                                result, success_count, fail_count, failed_stocks = bulk_update_stock_daily_upstox(instrument_keys)
                            else:
                                for index, row in stocks_to_update.iterrows():
                                    symbol = row['SYMBOL']
                                    st.write(f"Updating {symbol}...")
                                    
                                    if batch_data_source == 'NSE':
                                        result = rd.update_stock_daily(symbol)
                                    else:  # Upstox
                                        # For Upstox, we need to get the instrument key
                                        if "instruments" not in st.session_state:
                                            instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
                                            st.session_state.instruments = instruments_df
                                        
                                        try:
                                            instrument_key = st.session_state.instruments[
                                                st.session_state.instruments['trading_symbol'] == symbol]['instrument_token'].iloc[0]
                                            result = update_stock_daily_upstox(symbol, instrument_key)
                                        except Exception as e:
                                            result = f"Error getting instrument key for {symbol}: {str(e)}"

                                    if "success" in result or "up to date" in result:
                                        success_count += 1
                                    else:
                                        fail_count += 1
                                        failed_stocks.append({'symbol': symbol, 'error': result})
                                
                            display_toaster('Success' if 'success' in result.lower() or 'up to date' in result.lower() else 'Failure',
                                            result)
                                
                            status.update(label=f"Stock update complete! Success: {success_count}, Failed: {fail_count}",
                                        state="complete")
                            
                            # Store failed stocks in session state for retry
                            if failed_stocks:
                                st.session_state.failed_stocks = failed_stocks
                                st.warning(f"{fail_count} stocks failed to update. Use the retry button below to retry failed stocks.")
                            else:
                                st.session_state.failed_stocks = []

                    if st.session_state.failed_stocks:
                        st.subheader("Retry Failed Updates")
                        st.write(f"Found {len(st.session_state.failed_stocks)} failed stocks:")
                        
                        # Display failed stocks
                        failed_df = pd.DataFrame(st.session_state.failed_stocks)
                        st.dataframe(failed_df, width='content')
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Retry All Failed", width='content'):
                                retry_success = 0
                                retry_failed = []
                                
                                with st.status("Retrying failed stocks...", expanded=True) as retry_status:
                                    for failed_stock in st.session_state.failed_stocks:
                                        symbol = failed_stock['symbol']
                                        st.write(f"Retrying {symbol}...")
                                        
                                        if batch_data_source == 'NSE':
                                            result = rd.update_stock_daily(symbol)
                                        else:  # Upstox
                                            try:
                                                instrument_key = st.session_state.instruments[
                                                    st.session_state.instruments['trading_symbol'] == symbol]['instrument_token'].iloc[0]
                                                result = update_stock_daily_upstox(symbol, instrument_key)
                                            except Exception as e:
                                                result = f"Error getting instrument key for {symbol}: {str(e)}"
                                        
                                        if "success" in result or "up to date" in result:
                                            retry_success += 1
                                        else:
                                            retry_failed.append({'symbol': symbol, 'error': result})
                                        
                                        display_toaster('Success' if 'success' in result or 'up to date' in result else 'Failure',
                                                        result)
                                    
                                    retry_status.update(label=f"Retry complete! Success: {retry_success}, Still Failed: {len(retry_failed)}",
                                                    state="complete")
                                    
                                    # Update failed stocks list
                                    st.session_state.failed_stocks = retry_failed
                                    if not retry_failed:
                                        st.success("All failed stocks have been successfully updated!")
                        
                        with col2:
                            if st.button("Clear Failed List", width='content'):
                                st.session_state.failed_stocks = []
                                st.rerun()
            with load_cols[1]:
                st.subheader("Batch Historical Load by Index/Sector")

                all_stocks_df = rd.get_table_data(selected_database='nsedata', selected_table='ALL_STOCKS')

                if "instruments" not in st.session_state:
                    instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
                    st.session_state.instruments = instruments_df

                if not all_stocks_df.empty:
                    # Get unique indices and sectors for the dropdown
                    index_sector_list = sorted(all_stocks_df['STK_INDEX_SYMBOL'].unique().tolist())

                    selected_group = st.selectbox(
                        "Select an Index or Sector to load all its constituent stocks",
                        options=index_sector_list + ['STOCKS_IN_DB'],
                        index=None,
                        placeholder="Choose a group..."
                    )

                    start_date = st.date_input("Select Start Date for Historical Load",
                                               value=dt.date(2007, 1, 1),
                                               min_value=dt.date(2000, 1, 1),
                                               max_value=dt.date.today()
                                               )

                    if selected_group and st.button(f"Load History for All Stocks in {selected_group}",
                                                    width='content'):
                        # Filter stocks for the selected group
                        if selected_group == 'STOCKS_IN_DB':
                            stocks_in_db = rd.get_table_data(selected_table='STOCKS_IN_DB', selected_database='nsedata')["SYMBOL"].values.tolist()
                            stocks_to_load = all_stocks_df[all_stocks_df['SYMBOL'].isin(stocks_in_db)]
                        else:
                            stocks_to_load = all_stocks_df[all_stocks_df['STK_INDEX_SYMBOL'] == selected_group]

                        with st.status(f"Loading history for {len(stocks_to_load)} stocks in {selected_group}...",
                                       expanded=True) as status:
                            for index, row in stocks_to_load.iterrows():
                                symbol = row['SYMBOL']

                                instrument_key = st.session_state.instruments[
                                    st.session_state.instruments['trading_symbol'] == symbol]['instrument_token'].iloc[0]

                                st.write(f"Loading {symbol}...")

                                if data_source == 'NSE':
                                    end_date = dt.date.today()
                                    load_msg = rd.load_stock_history(symbol, start_date, end_date)
                                    if "success" in load_msg:
                                        rd.add_stock_to_registry(symbol, instrument_key, database='nsedata')

                                elif data_source == 'Upstox':
                                    load_msg = load_historical_stock_data_in_chunks(symbol, instrument_key)

                                display_toaster('Success' if 'success' in load_msg else 'Failure', load_msg)
                            status.update(label="Batch historical load complete!", state="complete")
                else:
                    st.warning("Could not retrieve stock list from 'ALL_STOCKS' for batch loading.")

            with load_cols[2]:
                st.subheader("Manual Historical Load")
                # Fetch all stocks to populate the selector
                all_stocks_df = rd.get_table_data(selected_table='ALL_STOCKS', selected_database='nsedata')
                etfs_df = rd.get_table_data(selected_table='ETF_DATA', selected_database='nsedata')
                instruments_df = rd.get_table_data(selected_table='instruments', selected_database='trading_db')
                stocks_in_db = rd.get_table_data(
                    selected_table='STOCKS_IN_DB', selected_database='nsedata')["SYMBOL"].values.tolist()

                if not all_stocks_df.empty:

                    selected_stock = st.selectbox(
                        "Select Stock to Load Historical Data",
                        options=sorted(set(all_stocks_df['SYMBOL'].values.tolist() + etfs_df["Symbol"].values.tolist())),
                        index=None,
                        placeholder="Choose a stock..."
                    )

                    if selected_stock:
                        instrument_key = instruments_df[
                            instruments_df['trading_symbol'] == selected_stock]['instrument_token'].iloc[0]

                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button(f"Load History for {selected_stock}", width='content'):
                            if data_source == 'NSE':
                                start_date = dt.date(2007, 1, 1)
                                end_date = dt.date.today() - dt.timedelta(days=1)
                                load_msg = rd.load_stock_history(selected_stock, start_date, end_date)
                                if "success" in load_msg:
                                    rd.add_stock_to_registry(selected_stock, instrument_key, database='nsedata')
                                display_toaster('Success' if 'success' in load_msg else 'Failure', load_msg)
                            elif data_source == 'Upstox':
                                load_historical_stock_data_in_chunks(selected_stock, instrument_key)

                        if selected_stock in stocks_in_db:
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("##### :rainbow[Stock already exists in the database.]")
                        else:
                            st.markdown("<br>", unsafe_allow_html=True)
                            st.markdown("##### :red[Stock does not exist in the database.]")
                else:
                    st.warning("Could not retrieve the list of stocks. Please ensure 'ALL_STOCKS' table is populated.")

        with load_tabs[2]:

            st.markdown("##### NSE Index and Sector Data Loader")

            indices = fetch_indicies_sectors_list(required='indices')
            sectors = fetch_indicies_sectors_list(required='sectors')
            all_symbols = indices + sectors

            # --- NEW: Button for batch daily update ---
            st.subheader("Batch Daily Update")
            if st.button("Update All Indices & Sectors Daily", width='content'):
                success_count = 0
                fail_count = 0
                with st.status("Performing batch daily update...", expanded=True) as status:
                    for symbol in all_symbols:
                        st.write(f"Updating {symbol}...")
                        result = rd.update_index_sector_daily(symbol)
                        if "success" in result or "up to date" in result:
                            success_count += 1
                        else:
                            fail_count += 1
                        display_toaster('Success' if 'success' in result or 'up to date' in result else 'Failure',
                                        result)

                    status.update(label=f"Batch update complete! Success: {success_count}, Failed: {fail_count}",
                                  state="complete")

            st.markdown("---")
            st.subheader("Manual Load/Update")

            # UI for selection
            load_type = st.radio("Select Load Type", ["Full History", "Daily Update"], horizontal=True)
            data_source = st.selectbox("Select Data Source", ["openchart", "nsepython", "jugaad_data"], index=0, placeholder="Choose a data source...")  

            col1, col2 = st.columns(2, width='stretch') 
            with col1:
                selected_index = st.selectbox("Select Index", ['All Indices'] + indices, index=None, placeholder="Choose an index...")
            with col2:
                selected_sector = st.selectbox("Select Sector", ['All Sectors'] + sectors, index=None, placeholder="Choose a sector...")

            symbol_to_load = selected_index if selected_index else selected_sector

            if symbol_to_load:
                if load_type == "Full History":
                    st.markdown(f"**Mode:** Load complete history for `{symbol_to_load}`.")
                    col_start, col_end, col_btn = st.columns([1, 1, 1])
                    with col_start:
                        start_date = st.date_input("Start Date", dt.date(2007, 1, 1))
                    with col_end:
                        end_date = st.date_input("End Date", dt.date.today())
                    with col_btn:
                        st.markdown("<br>", unsafe_allow_html=True)
                        if st.button(f"Load Full History for {symbol_to_load}", width='content'):
                            with st.spinner(f"Loading full history for {symbol_to_load}..."):
                                if symbol_to_load == 'All Indices':
                                    for index in indices:
                                        result = rd.load_index_sector_history(index, start_date, end_date, data_source=data_source)
                                        display_toaster('Success' if 'success' in result else 'Failure', result)
                                elif symbol_to_load == 'All Sectors':
                                    for sector in sectors:
                                        result = rd.load_index_sector_history(sector, start_date, end_date, data_source=data_source)
                                        display_toaster('Success' if 'success' in result else 'Failure', result)
                                else:
                                    result = rd.load_index_sector_history(symbol_to_load, start_date, end_date, data_source=data_source)
                                    display_toaster('Success' if 'success' in result else 'Failure', result)

                else:  # Daily Update
                    st.markdown(f"**Mode:** Incrementally update daily data for `{symbol_to_load}`.")
                    if st.button(f"Run Daily Update for {symbol_to_load}", width='content'):
                        with st.spinner(f"Updating {symbol_to_load}..."):
                            result = rd.update_index_sector_daily(symbol_to_load)
                            display_toaster('Success' if 'success' in result else 'Info', result)
            else:
                st.warning("Please select an index or a sector to proceed.", width='stretch')
        
        with load_tabs[3]:
            uploaded_file = st.file_uploader("Upload the file", type=['xlsx'])
            if uploaded_file is not None:
                wb = openpyxl.load_workbook(uploaded_file)
                sheet_selector = st.selectbox("Select sheet", options=wb.sheetnames)
                database_list = ["analytics", "nsedata", "mfdata", "STRATEGY"]
                database = st.selectbox("Select database", options=database_list)
                table_list = ["EQUITY_HOLDINGS", "MF_HOLDINGS", "RETURNS_RECEIVED", "OTHERS"]
                table_selected = st.selectbox("Select table", options=table_list)
                if table_selected == 'OTHERS':
                    table_name = st.text_input("Enter table name")
                else:
                    table_name = table_selected

                data = pd.read_excel(uploaded_file, sheet_selector)
                view_col, load_col = st.columns(2, vertical_alignment="center")

                with view_col:
                    view_btn = st.button("View Data")
                with load_col:
                    sub_btn = st.button("Load Data")

                if sub_btn:
                    load_msg = rd.load_sql_data(data_to_load=data, table_name=table_name, database=database)
                    if "success" in load_msg:
                        st.success(load_msg)
                    else:
                        st.error(load_msg)
            else:
                st.write('No file chosen yet for upload')
        try:
            if view_btn:
                st.write(data)
        except Exception:
            pass


if __name__ == '__main__':
    dataload()