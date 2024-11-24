import pandas as pd
from datetime import timedelta,datetime


## Working with data --------------------------------------------------------------------------------------------------


def data_fetching(conn,date, all_fetch=True, condition=None):

    if all_fetch:
        query = f"SELECT * FROM '{date}'"
        df = pd.read_sql_query(query, conn)
        df["date"] = date
        

    else:    
        query = f"SELECT * FROM '{date}' {condition}"
        df = pd.read_sql_query(query, conn)
        df["date"] = date

    return df


def data_resample_clean_func(data,timeframe):

    data["datetime"] = pd.to_datetime(data["date"] + " " + data["time"], format="%d%m%Y %H:%M:%S")
    data["date"] = pd.to_datetime(data["date"], format = "%d%m%Y")
    data.set_index("datetime", inplace=True)
    
    resample_df = data.resample(timeframe).agg({
                    'symbol': 'first',
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'date' : 'first'
                })
    
    
    resample_df = resample_df.dropna(axis=0)
    resample_df["time"] = resample_df.index.time
    resample_df = resample_df.sort_index(ascending = True)

    return resample_df



## Options functions ------------------------------------------------------------------------------

def get_expiry(conn,date,option_type,exp_week):
    opt_exp = None
    date_str = date.strftime('%d%m%Y')
    expiry_list = conn.execute(f'SELECT DISTINCT "expiry" FROM \'{date_str}\' WHERE instrument_type = "{option_type}";').fetchall()
    e_list = [e[0] for e in expiry_list]
    expiry_list = sorted([pd.to_datetime(date_str, format='%d-%m-%Y') for date_str in e_list])
   
    if date==expiry_list[0]:

        expiry_list = expiry_list[1:]

    else:
        pass

    if exp_week == 'cur':
        opt_exp = expiry_list[0]
    elif exp_week == 'next':
        opt_exp = expiry_list[1]
    else:
        print("exp_week is invalid")

    return opt_exp, expiry_list




def get_options_price(data,date_time,ohlcv, resample_number):
    price=None
    date_time = date_time + timedelta(minutes=resample_number)
    try :
        price = data[data.index >= date_time].iloc[0][ohlcv]
    except:
        while price is None:
            date_time = date_time - timedelta(minutes=1)
            try:
                price = data[data.index >= date_time].iloc[0][ohlcv]
            except:
                pass
    return price


def strike_price(conn, date,date_time,expiry_date, option_type,spot_price,strike_perc_val):

    date_str = date.strftime('%d%m%Y')
    condition = f"WHERE expiry = '{expiry_date.strftime('%d-%m-%Y')}' AND instrument_type = '{option_type}'"
    opt_df = data_fetching(conn, date_str, all_fetch=False, condition=condition)
    premium = round(spot_price * (strike_perc_val/100))
    strike_ltp_df = pd.DataFrame(columns = ['strike', 'ltp'])
   
    
    if option_type=="CE" :
        opt_df=  opt_df[opt_df["strike"] > spot_price]
        opt_df["datetime"] = pd.to_datetime(opt_df["date"] + " " + opt_df["time"], format="%d%m%Y %H:%M:%S")
        opt_df.set_index("datetime", inplace=True)
        otm_strikes = list(opt_df["strike"].unique())
        for strike in otm_strikes:
            otm_strikes_df = opt_df[opt_df["strike"]==strike]
            strike_ltp_df.loc[len(strike_ltp_df)] = [strike, get_options_price(otm_strikes_df,date_time,"close", 15)]  

    elif option_type=="PE":
        opt_df = opt_df[opt_df["strike"] < spot_price]
        opt_df["datetime"] = pd.to_datetime(opt_df["date"] + " " + opt_df["time"], format="%d%m%Y %H:%M:%S")
        opt_df.set_index("datetime", inplace=True)
        otm_strikes = opt_df["strike"].unique()
        for strike in otm_strikes:
            otm_strikes_df = opt_df[opt_df["strike"]==strike]
            strike_ltp_df.loc[len(strike_ltp_df)] = [strike, get_options_price(otm_strikes_df,date_time,"close", 15)]     

    else:
        pass

    strike_ltp_df["closest_strike"] = abs(strike_ltp_df["ltp"] - premium)
    required_strike_price = strike_ltp_df.sort_values("closest_strike").iloc[0]["strike"]

    return required_strike_price , otm_strikes
    



## Working with Trade executions -----------------------------------------------------------------------------------------

def trade_info():
    return {
        'trade_status' : None,
        'main_trade_status' :  None,
        'hedge_trade_status' : None,
        'instrument_id' : "BANKNIFTY",
        'entry_date' : None,
        'entry_dayname' : None,
        'dte' : None,
        'entry_signal_time' : None,
        'main_entry_price' : None,
        'hedge_entry_price' : None,
        'entry_reason' : None,
        'market_bias' : None,
        'exit_date' : None,
        'exit_dayname' : None,
        'exit_signal_time' : None,
        'main_exit_price' : None,
        'hedge_exit_price' : None,
        'exit_reason' : None,
        'pnl_raw' : None,
        'pnl_slippage' : None,
        'mtm' : 0,
        'trade_id' : 0,
        'associate_trade_id' : 0,
        'trade_type' : None,
        'intraday_pnl' : 0,
        'overnight_pnl' : 0,
        'max_loss' : 0,
        'max_profit' : 0
        
    }



## Working with Output executions ----------------------------------------------------------------

def duration_days_counts(start_date, end_date,trading_dates):

    return (trading_dates.index(start_date) - trading_dates.index(end_date))

