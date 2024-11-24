import pandas as pd
import sqlite3
import re
import openpyxl
import pandas_ta as ta
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
import math
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime,timedelta
from bt_utils_yash import data_fetching, data_resample_clean_func,get_expiry,strike_price,trade_info,duration_days_counts,get_options_price


spot_db_path = r"W:\asiatic\asiatic_supertrend\SPOT.db"
option_db_path = r"W:\asiatic\asiatic_supertrend\OPT.db"

## Spot data fixing --------------------------------
spot_csv_path = r"W:\asiatic\asiatic_supertrend\daily.csv"
spot_daily_data = pd.read_csv(spot_csv_path,dtype={"date" : str})
spot_daily_data["date"] = pd.to_datetime(spot_daily_data["date"], format = "%d%m%Y")
trading_dates = spot_daily_data["date"].to_list()
# spot_daily_data['date'] = pd.to_datetime(spot_daily_data['date'], format="%d%m%Y")

## spot data fixed ------------------------------------

conn_spot = sqlite3.connect(spot_db_path)
print("spot db connceted")
conn_option = sqlite3.connect(option_db_path)
print("option db connceted")



def one_time_lookback_check(date,lookback_candles_check,spot_daily_data, trading_dates,conn, resample_timeframe):
    date_available = False
    
    while date_available==False:
        if date in trading_dates:
            date_available = True
        else:
            date = date - timedelta(days=1)

    lookback_condition_bool  = True
    market_sentiment = None
    
    starting_index = spot_daily_data[spot_daily_data["date"]==date].index[0]   
    # previous_ending_index = starting_index - (lookback_candles_check+1)
    
    while lookback_condition_bool:

        high = spot_daily_data.iloc[starting_index]["rolling_high"]
        low = spot_daily_data.iloc[starting_index]["rolling_low"]
        index_date = spot_daily_data.iloc[starting_index]["date"]

        ## FIX this ..................

        resample_df = data_fetching(conn,index_date.strftime("%d%m%Y"), True, condition=None)
        resample_df = data_resample_clean_func(resample_df,resample_timeframe)

        close_to_check_high_side = resample_df["close"].max()
        close_to_check_low_side = resample_df["close"].min()
    

        if close_to_check_high_side > high:
            market_sentiment = "Bullish"
            print(f"Bullish sentiment found at {spot_daily_data.iloc[starting_index]['date']}")
            lookback_condition_bool = False
            

        elif close_to_check_low_side < low:
            market_sentiment = "Bearish"
            print(f"Bearish sentiment found at {spot_daily_data.iloc[starting_index]['date']}")
            lookback_condition_bool=False
            

        else:
            market_sentiment = "Indecisive"
            print(f"Indecisive at {spot_daily_data.iloc[starting_index]['date']} looking for previous lookback data ")

        starting_index = starting_index -1
        # previous_ending_index = previous_ending_index -1

    
    return market_sentiment

def supertrend_data_add(date, trading_dates, conn,supertrend_lookback_period,supertrend_multiplier,timeframe_resample):
    count = 0
    supertrend_data_list = []
    supertrend_date = date
    while count < 3:
        # Check if supertrend_date is in trading_dates
        if supertrend_date in trading_dates:
            data_fetching_date = supertrend_date.strftime("%d%m%Y")
            data_df = data_fetching(conn, data_fetching_date, True)  # Assuming data_fetching is defined elsewhere
            supertrend_data_list.append(data_df)
            supertrend_date -= timedelta(days=1)  # Move to the previous day
            count += 1
        else:
            supertrend_date -= timedelta(days=1)  # Move to the previous day if the date is not in trading_dates

    concat_df = pd.concat(supertrend_data_list)
    concat_df = data_resample_clean_func(concat_df,timeframe_resample)
    concat_df["supertrend_value"], concat_df["supertrend_direction"] = concat_df.ta.supertrend( length=supertrend_lookback_period, multiplier=supertrend_multiplier).iloc[:, 0],concat_df.ta.supertrend( length=supertrend_lookback_period, multiplier=supertrend_multiplier).iloc[:, 1]
    concat_df = concat_df.copy()
    concat_df.loc[:, "prev_supertrend_direction"] = concat_df["supertrend_direction"].shift(1)
    spot_resample = concat_df[concat_df["date"]==date]


    return spot_resample  # Return the data list




def trade_execution(start_date,end_date,conn,timeframe_resample, lookback_condition_days,supertrend_length,supertrend_multiplier):
    trades_data = []
    option_all_data = {}
    all_trade_logs = []
    in_trade = False
    trade_number = 0
    trade_type = None
    hedge_trade_open = False
    resample_number = int(''.join(filter(str.isdigit, timeframe_resample)))
    
    spot_daily_data["rolling_high"] = spot_daily_data["high"].rolling(lookback_condition_days).max().shift(1)
    spot_daily_data["rolling_low"] = spot_daily_data["low"].rolling(lookback_condition_days).min().shift(1)
    print(spot_daily_data)

    lookback_sentiment = one_time_lookback_check(start_date - timedelta(days=1),lookback_condition_days,spot_daily_data,trading_dates,conn, timeframe_resample)
    print(lookback_sentiment)


    while start_date <= end_date:

        if start_date in trading_dates:
            
            spot_data_resample_df = supertrend_data_add(start_date, trading_dates, conn,supertrend_length,supertrend_multiplier,timeframe_resample)


            # print(spot_data_resample_df)
            for index, row in spot_data_resample_df[:].iterrows():
           
                print(row.name)

               ## Logging max loss and max profit ---------
                if in_trade==True:
                    print("updating max_profit & max_loss")
                    if row.date in option_all_data.keys():
                        main_option_df = option_all_data[row.date]
                        
                        if main_entry_price - get_options_price(main_option_df,row.name,"low", resample_number) > max_profit:
                            max_profit = main_entry_price - get_options_price(main_option_df,row.name,"low", resample_number)
                        else:
                            pass
                        if main_entry_price - get_options_price(main_option_df,row.name,"high", resample_number)  < max_loss:
                            max_loss = main_entry_price -get_options_price(main_option_df,row.name,"high", resample_number)
                        else:
                            pass
                    else:
                        condition = "WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry}' AND strike = '{main_strike_price}'"
                        main_option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                        main_option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
                        main_option_df.set_index("datetime", inplace=True)
                        main_option_df.sort_index(inplace=True)
                        
                        if main_entry_price - get_options_price(main_option_df,row.name,"low", resample_number) > max_profit:
                            max_profit = main_entry_price - get_options_price(main_option_df,row.name,"low", resample_number)
                        else:
                            pass
                        if main_entry_price - get_options_price(main_option_df,row.name,"high", resample_number)  < max_loss:
                            max_loss = main_entry_price -get_options_price(main_option_df,row.name,"high", resample_number)
                        else:
                            pass

                        option_all_data[row.date] = main_option_df
                #---------------------------

                
                ## Checking market sentiment using daily data at resampling rate ---------------------------------------------

                if row["close"] > spot_daily_data[spot_daily_data["date"] == start_date]["rolling_high"].iloc[0]:
                    lookback_sentiment = "Bullish"
                    
                    # print(f"lookback_sentiment is {lookback_sentiment} at {row.name}")
                elif row["close"] < spot_daily_data[spot_daily_data["date"] == start_date]["rolling_low"].iloc[0]:
                    lookback_sentiment = "Bearish"
                    
                    # print(f"lookback_sentiment is {lookback_sentiment} at {row.name}")

                else:
                    # print(f"lookback_sentiment is {lookback_sentiment} at {row.name}")
                    pass
                
                
                ## Exit conditions ---------------------------------------

                if hedge_trade_open==True and row.date == ( trading_dates[trading_dates.index(trade_info_log["entry_date"])+1] ) and str(row.time)=="09:15:00":
                    print(f"Hedge trade closed on {row.name}")
                    condition = "WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry}' AND strike = '{hedge_strike_price}'"
                    hedge_option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                    hedge_option_df["datetime"] = pd.to_datetime(hedge_option_df["date"] + " " + hedge_option_df["time"], format="%d%m%Y %H:%M:%S")
                    hedge_option_df.set_index("datetime", inplace=True)
                    hedge_option_df.sort_index(inplace=True) 
                    trade_info_log["hedge_exit_price"] =  get_options_price(main_option_df,row.name,"close", resample_number)
                    hedge_trade_open = False


                if in_trade==True and row["supertrend_direction"] == 1 and trade_type=="Call_Sell":
            
                    print(f"Call Sell trade closed on {row.name} by doing Call Buy")
                    in_trade=False
                    trade_type = None
                    main_option_df = option_all_data[row.date]
                    trade_info_log["exit_date"] = row.date
                    trade_info_log["exit_signal_time"] = row.name
                    trade_info_log["main_exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
                    trade_info_log["exit_reason"] = "Supertrend_bullish_signal"
                    trade_info_log["pnl_raw"] = trade_info_log["main_entry_price"] - trade_info_log["main_exit_price"]
                    trade_info_log["max_loss"] = max_loss
                    trade_info_log["max_profit"] = max_profit

                    max_profit = None
                    max_loss = None
                    all_trade_logs.append(trade_info_log)


                if in_trade==True and row["supertrend_direction"] == -1 and trade_type=="Put_Sell":

                    print(f"Put_Sell trade closed on {row.name} by doing Put Buy")
                    in_trade=False
                    trade_type = None
                    main_option_df = option_all_data[row.date]
                    trade_info_log["exit_date"] = row.date
                    trade_info_log["exit_signal_time"] =row.name
                    trade_info_log["main_exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
                    trade_info_log["exit_reason"] = "Supertrend_bearish_signal"
                    trade_info_log["pnl_raw"] = trade_info_log["main_entry_price"] - trade_info_log["main_exit_price"]
                    trade_info_log["max_loss"] = max_loss
                    trade_info_log["max_profit"] = max_profit
                    
                    max_loss = None
                    max_profit=None
                    all_trade_logs.append(trade_info_log)


                if in_trade==True and row.date==latest_expiry:
                    if trade_type=="Put_Sell":
                        print(f"Doing rollover ->  Put_Sell trade closed on {row.name} by doing Put Buy")

                        main_option_df = option_all_data[row.date]
                        trade_info_log["exit_date"] = row.date
                        trade_info_log["exit_signal_time"] =row.name
                        trade_info_log["main_exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
                        trade_info_log["exit_reason"] = "Supertrend_bearish_signal"
                        trade_info_log["pnl_raw"] = trade_info_log["main_entry_price"] - trade_info_log["main_exit_price"]
                        trade_info_log["max_loss"] = max_loss
                        trade_info_log["max_profit"] = max_profit
                        
                        max_loss = None
                        max_profit=None
                        all_trade_logs.append(trade_info_log)

#                       ----------------------------------------------------------

                        trade_number = trade_number +1
                        print(f"After rollover -> Put sell at {row.name}")
                        latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                        main_strike_price , all_strikes = strike_price(conn_option, row.date, row.name,latest_expiry, option_type, row.close, 0.3)
                        all_strikes = sorted(all_strikes)
                        hedge_strike_price = all_strikes[all_strikes.index(main_strike_price) - 4]
                        condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND (strike = '{main_strike_price}' OR strike = '{hedge_strike_price}')"
                        option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                        option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
                        option_df.set_index("datetime", inplace=True)
                        main_option_df = option_df[option_df["strike"]==main_strike_price].sort_index()
                        hedge_option_df = option_df[option_df["strike"]==hedge_strike_price].sort_index()
                        
                        option_all_data[row.date] = main_option_df                
                      
                        main_entry_price = get_options_price(main_option_df,row.name,"close", resample_number)               
                        hedge_entry_price = get_options_price(hedge_option_df,row.name,"close", resample_number)

                        max_profit = get_options_price(main_option_df,row.name,"low", resample_number)
                        max_loss = get_options_price(main_option_df,row.name,"high", resample_number)
                
                        trade_info_log = trade_info()
                        trade_info_log["trade_status"] = "Closed"
                        trade_info_log['main_trade_status'] ="Closed"
                        trade_info_log['hedge_trade_status'] ="Closed"
                        trade_info_log["entry_date"] = row.date
                        trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                        trade_info_log["entry_signal_time"] = row.name
                        trade_info_log["main_entry_price"] = main_entry_price
                        trade_info_log["hedge_entry_price"] = hedge_entry_price
                        trade_info_log["entry_reason"] = "Supertrend bullish & Market Sentiment Bullish"
                        trade_info_log["market_bias"] = lookback_sentiment
                        trade_info_log["trade_id"] = trade_number
                        trade_info_log["associate_trade_id"] = trade_number
                        trade_info_log["trade_type"] = "PUT_SELL"


                    elif trade_type=="Call_Sell":
                        
                        print(f"Doing rollover ->  Call_Sell trade closed on {row.name} by doing Call Buy")

                        main_option_df = option_all_data[row.date]
                        trade_info_log["exit_date"] = row.date
                        trade_info_log["exit_signal_time"] =row.name
                        trade_info_log["main_exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
                        trade_info_log["exit_reason"] = "Supertrend_bullish_signal"
                        trade_info_log["pnl_raw"] = trade_info_log["main_entry_price"] - trade_info_log["main_exit_price"]
                        trade_info_log["max_loss"] = max_loss
                        trade_info_log["max_profit"] = max_profit
                        
                        max_loss = None
                        max_profit=None
                        all_trade_logs.append(trade_info_log)

#                       ----------------------------------------------------------

                        trade_number = trade_number +1
                        print(f"After rollover -> Call sell at {row.name}")
                        latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                        main_strike_price , all_strikes = strike_price(conn_option, row.date, row.name,latest_expiry, option_type, row.close, 0.3)
                        all_strikes = sorted(all_strikes)
                        hedge_strike_price = all_strikes[all_strikes.index(main_strike_price) - 4]
                        condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND (strike = '{main_strike_price}' OR strike = '{hedge_strike_price}')"
                        option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                        option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
                        option_df.set_index("datetime", inplace=True)
                        main_option_df = option_df[option_df["strike"]==main_strike_price].sort_index()
                        hedge_option_df = option_df[option_df["strike"]==hedge_strike_price].sort_index()
                        
                        option_all_data[row.date] = main_option_df                
                      
                        main_entry_price = get_options_price(main_option_df,row.name,"close", resample_number)               
                        hedge_entry_price = get_options_price(hedge_option_df,row.name,"close", resample_number)

                        max_profit = get_options_price(main_option_df,row.name,"low", resample_number)
                        max_loss = get_options_price(main_option_df,row.name,"high", resample_number)
                
                        trade_info_log = trade_info()
                        trade_info_log["trade_status"] = "Closed"
                        trade_info_log['main_trade_status'] ="Closed"
                        trade_info_log['hedge_trade_status'] ="Closed"
                        trade_info_log["entry_date"] = row.date
                        trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                        trade_info_log["entry_signal_time"] = row.name
                        trade_info_log["main_entry_price"] = main_entry_price
                        trade_info_log["hedge_entry_price"] = hedge_entry_price
                        trade_info_log["entry_reason"] = "Supertrend bearish & Market Sentiment bearish"
                        trade_info_log["market_bias"] = lookback_sentiment
                        trade_info_log["trade_id"] = trade_number
                        trade_info_log["associate_trade_id"] = trade_number
                        trade_info_log["trade_type"] = "Call_SELL"

                    


                ## Checking Trade conditions ---------------------------------------

                
                if lookback_sentiment == "Bullish" and row["supertrend_direction"] == 1 and row["prev_supertrend_direction"]==-1  and in_trade==False:
                    
                    option_type = "PE"
                    in_trade = True
                    hedge_trade_open = True
                    trade_type = "Put_Sell"
                    print(f"Put sell at {row.name}")
                    trade_number = trade_number +1
                    

                    latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                    main_strike_price , all_strikes = strike_price(conn_option, row.date, row.name,latest_expiry, option_type, row.close, 0.3)
                    all_strikes = sorted(all_strikes)
                    hedge_strike_price = all_strikes[all_strikes.index(main_strike_price) - 4]
                    condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND (strike = '{main_strike_price}' OR strike = '{hedge_strike_price}')"
                    option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                    option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
                    option_df.set_index("datetime", inplace=True)
                    main_option_df = option_df[option_df["strike"]==main_strike_price].sort_index()
                    hedge_option_df = option_df[option_df["strike"]==hedge_strike_price].sort_index()
                    
                    option_all_data[row.date] = main_option_df
               
                    
                    main_entry_price = get_options_price(main_option_df,row.name,"close", resample_number)               
                    hedge_entry_price = get_options_price(hedge_option_df,row.name,"close", resample_number)

                    max_profit = get_options_price(main_option_df,row.name,"low", resample_number)
                    max_loss = get_options_price(main_option_df,row.name,"high", resample_number)
               

                    trade_number = trade_number +1
                    trade_info_log = trade_info()
                    trade_info_log["trade_status"] = "Closed"
                    trade_info_log['main_trade_status'] ="Closed"
                    trade_info_log['hedge_trade_status'] ="Closed"
                    trade_info_log["entry_date"] = row.date
                    trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                    trade_info_log["entry_signal_time"] = row.name
                    trade_info_log["main_entry_price"] = main_entry_price
                    trade_info_log["hedge_entry_price"] = hedge_entry_price
                    trade_info_log["entry_reason"] = "Supertrend bullish & Market Sentiment Bullish"
                    trade_info_log["market_bias"] = lookback_sentiment
                    trade_info_log["trade_id"] = trade_number
                    trade_info_log["associate_trade_id"] = trade_number
                    trade_info_log["trade_type"] = "PUT_SELL"

           
                if lookback_sentiment == "Bearish" and row["supertrend_direction"] == -1 and  row["prev_supertrend_direction"]== 1  and in_trade==False:
                    

                    option_type = "CE"
                    in_trade = True
                    hedge_trade_open = True
                    trade_type = "Call_Sell"
                    # hedge_type = "Call_Buy"
                    print(f"Call sell at {row.name}")
                    trade_number = trade_number +1
            

                    latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                    main_strike_price , all_strikes = strike_price(conn_option, row.date, row.name,latest_expiry, option_type, row.close, 0.3)
                    all_strikes = sorted(all_strikes)
                    hedge_strike_price = all_strikes[all_strikes.index(main_strike_price) - 4]
                    condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND (strike = '{main_strike_price}' OR strike = '{hedge_strike_price}')"
                    option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                    option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
                    option_df.set_index("datetime", inplace=True)
                    main_option_df = option_df[option_df["strike"]==main_strike_price].sort_index()
                    hedge_option_df = option_df[option_df["strike"]==hedge_strike_price].sort_index()
                    
                    option_all_data[row.date] = main_option_df
               
                    
                    main_entry_price = get_options_price(main_option_df,row.name,"close", resample_number)               
                    hedge_entry_price = get_options_price(hedge_option_df,row.name,"close", resample_number)

                    max_profit = get_options_price(main_option_df,row.name,"low", resample_number)
                    max_loss = get_options_price(main_option_df,row.name,"high", resample_number)
               
                

                    trade_number = trade_number +1
                    trade_info_log = trade_info()
                    trade_info_log["trade_status"] = "Closed"
                    trade_info_log['main_trade_status'] ="Closed"
                    trade_info_log['hedge_trade_status'] ="Closed"
                    trade_info_log["entry_date"] = row.date
                    trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                    trade_info_log["entry_signal_time"] = row.name
                    trade_info_log["main_entry_price"] = main_entry_price
                    trade_info_log["hedge_entry_price"] = hedge_entry_price
                    trade_info_log["entry_reason"] = "Supertrend Bearish & Market Sentiment Bearish"
                    trade_info_log["market_bias"] = lookback_sentiment
                    trade_info_log["trade_id"] = trade_number
                    trade_info_log["associate_trade_id"] = trade_number
                    trade_info_log["trade_type"] = "CALL_SELL"

                else:
                    pass
                

        else:
            print(f"Market Closed on {start_date}")


        start_date = start_date +  timedelta(days=1)
        

    if trade_type is not None:
        if trade_type=="Put_Sell" :
            print(f"Put_Sell trade closed on {row.name} by doing Put Buy")
            in_trade=False
            trade_type = None
            main_option_df = option_all_data[row.date]
            trade_info_log["exit_date"] = row.date
            trade_info_log["exit_signal_time"] = row.name
            trade_info_log["main_exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
            trade_info_log["exit_reason"] = "Supertrend_bearish_signal"
            trade_info_log["pnl_raw"] = trade_info_log["main_entry_price"] - trade_info_log["main_exit_price"]
            trade_info_log["max_loss"] = max_loss
            trade_info_log["max_profit"] = max_profit
            
            max_loss = None
            max_profit=None
            all_trade_logs.append(trade_info_log)

        else:
            print(f"Call Sell trade closed on {row.name} by doing Call Buy")
            in_trade=False
            trade_type = None
            main_option_df = option_all_data[row.date]
            trade_info_log["exit_date"] = row.date
            trade_info_log["exit_signal_time"] = row.name
            trade_info_log["main_exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
            trade_info_log["exit_reason"] = "Supertrend_bullish_signal"
            trade_info_log["pnl_raw"] = trade_info_log["main_entry_price"] - trade_info_log["main_exit_price"]
            trade_info_log["max_loss"] = max_loss
            trade_info_log["max_profit"] = max_profit

            max_profit = None
            max_loss = None
            all_trade_logs.append(trade_info_log)


    trade_df = pd.DataFrame(all_trade_logs)
    return trade_df


# def output_transaction_metric(trades_df):

#     transaction_list = []
#     total_trades = trades_df["trade_number"].unique()
#     for trade in total_trades:
#         pnl = None
#         max_profit = None
#         max_loss = None
#         trade_rows_with_number = trades_df[trades_df["trade_number"]==trade]
#         trade_rows_with_number.sort_values("Time", inplace=True)
#         trade_rows_with_number.reset_index(drop=True, inplace=True)


#         if trade_rows_with_number.iloc[0]["direction"]=="SHORT":
#             pnl = trade_rows_with_number.iloc[0]["Price"] - trade_rows_with_number.iloc[1]["Price"]
#         else:
#             pnl = trade_rows_with_number.iloc[1]["Price"] - trade_rows_with_number.iloc[0]["Price"]


#         total_trade_days = 0
#         start_trading_date = trade_rows_with_number.iloc[0]["date"]
#         while start_trading_date<= trade_rows_with_number.iloc[1]["date"]:

#             if start_trading_date in trading_dates:
#                 total_trade_days = total_trade_days+1
#             else:
#                 pass
#             start_trading_date  = start_trading_date + timedelta(days=1)


#         if trade_rows_with_number.iloc[0]["direction"]=="SHORT":
#             max_profit = trade_rows_with_number.iloc[0]["Price"] - trade_rows_with_number.iloc[1]["min_low"]
#             max_loss = trade_rows_with_number.iloc[0]["Price"] - trade_rows_with_number.iloc[1]["max_high"]
#         else:
#             max_profit = trade_rows_with_number.iloc[1]["max_high"] - trade_rows_with_number.iloc[0]["Price"] 
#             max_loss = trade_rows_with_number.iloc[1]["min_low"] - trade_rows_with_number.iloc[0]["Price"]

    
#         metric = {"symbol": trade_rows_with_number.iloc[0]["symbol"], "entry_bias" :  trade_rows_with_number.iloc[0]["bias"], 
#                   "direction" : trade_rows_with_number.iloc[0]["direction"],
#                 "entry": trade_rows_with_number.iloc[0]["Price"], "entry_time" : trade_rows_with_number.iloc[0]["Time"], 
#                 "entry_day" : trade_rows_with_number.iloc[0]["date"],
#                 "entry_date" : trade_rows_with_number.iloc[0]["date"].strftime("%A"), "entry_reason" : trade_rows_with_number.iloc[0]["reason"],
#                 "exit": trade_rows_with_number.iloc[1]["Price"],"exit_bias" :  trade_rows_with_number.iloc[1]["bias"], 
#                 "exit_time" : trade_rows_with_number.iloc[1]["Time"],"exit_day" : trade_rows_with_number.iloc[1]["date"],
#                 "exit_date" : trade_rows_with_number.iloc[1]["date"].strftime("%A"),
#                 "exit_reason" : trade_rows_with_number.iloc[1]["reason"], "pnl" : pnl, "quantity" : 1, "duration":total_trade_days,
#                 "max_profit" : max_profit, "max_loss" : max_loss}
        
#         transaction_list.append(metric)

#     transaction_df = pd.DataFrame(transaction_list)
#     return transaction_df



# def mtm_calculation(start_date,end_date,transaction_df,spot_daily_data):
    
#     trading_dates = spot_daily_data["date"].to_list()
#     spot_new_daily_data  = spot_daily_data.set_index("date")
#     date_range = pd.date_range(start=start_date, end=end_date)
#     df_mtm = pd.DataFrame(index=date_range, columns=['mtm'])
#     df_mtm["mtm"] = 0
#     for i in range(len(transaction_df)):
#         entry_date = transaction_df.iloc[i]["entry_day"]
#         exit_date = transaction_df.iloc[i]["exit_day"]
#         if entry_date!=exit_date:
#             date_mtm = entry_date

            
#             if transaction_df.iloc[i]["direction"]=="LONG":
#                 buying_price = transaction_df.iloc[i]["entry"]
#                 selling_price = transaction_df.iloc[i]["exit"]
#                 while date_mtm!=(exit_date + timedelta(days=1)):
#                     if date_mtm in trading_dates:
#                         if date_mtm==entry_date:
#                             df_mtm.loc[date_mtm] = df_mtm.loc[date_mtm] + (spot_new_daily_data.loc[date_mtm]["close"] - buying_price)
#                         elif date_mtm < exit_date:
#                             df_mtm.loc[date_mtm] = df_mtm.loc[date_mtm] + (spot_new_daily_data.loc[date_mtm]["close"] - spot_new_daily_data.loc[date_mtm]["open"])
#                         elif date_mtm == exit_date:
#                             df_mtm.loc[date_mtm] = df_mtm.loc[date_mtm] + (selling_price - spot_new_daily_data.loc[date_mtm]["open"])
                
#                     else:
#                         pass
#                     date_mtm = date_mtm + timedelta(days=1)

#             elif transaction_df.iloc[i]["direction"]=="SHORT":
#                 buying_price = transaction_df.iloc[i]["exit"]
#                 selling_price = transaction_df.iloc[i]["entry"]
#                 while date_mtm!=(exit_date + timedelta(days=1)):
#                     if date_mtm in trading_dates:
#                         if date_mtm==entry_date:
#                             df_mtm.loc[date_mtm] = df_mtm.loc[date_mtm] + (selling_price - spot_new_daily_data.loc[date_mtm]["close"])
#                         elif date_mtm < exit_date:
#                             df_mtm.loc[date_mtm] = df_mtm.loc[date_mtm] + (spot_new_daily_data.loc[date_mtm]["open"] - spot_new_daily_data.loc[date_mtm]["close"])
#                         elif date_mtm == exit_date:
#                             df_mtm.loc[date_mtm] = df_mtm.loc[date_mtm] + (spot_new_daily_data.loc[date_mtm]["open"] - buying_price)
#                     else:
#                         pass
                

#                     date_mtm = date_mtm + timedelta(days=1)



#         else:
        
#             df_mtm.loc[entry_date] = df_mtm.loc[entry_date] + transaction_df.iloc[i]["pnl"]

#     df_mtm['mtm'] = df_mtm['mtm'].cumsum()
#     df_mtm['drawdown'] = df_mtm['mtm'] - df_mtm['mtm'].cummax()

#     return df_mtm

# def output_stats(start_date, end_date,transaction_df, spot_daily_data):

#     date_range = pd.date_range(start=start_date, end=end_date)

#     # Create a DataFrame with the date range as the index and a column 'daily_pnl' with all values set to 0
#     df = pd.DataFrame(index=date_range, columns=['pnl'])
#     df['pnl'] = 0

#     ## Daily PNL  ----------------------------------------------------------------
#     daily_pnl = transaction_df.groupby("entry_day").agg({"pnl": "sum"})
#     daily_df_pnl = df.merge(daily_pnl, left_index=True, right_index=True, how='left')
#     daily_df_pnl['pnl'] = daily_df_pnl['pnl_x'].fillna(0) + daily_df_pnl['pnl_y'].fillna(0)
#     daily_df_pnl.drop(columns=['pnl_x', 'pnl_y'], inplace=True)
    
#     ## MTM, DD

#     mtm_df = mtm_calculation(start_date,end_date,transaction_df,spot_daily_data)

#     ## Monthly PNL ---------------------------------------------------------------
#     monthly_pnl = daily_df_pnl.resample("ME").agg({"pnl": "sum"}).reset_index()

#     ## Yearly PNL --------------------------------------------------------
#     yearly_pnl = daily_df_pnl.resample("YE").agg({"pnl": "sum"}).reset_index()

#     ## Save monthly name-wise results ------------------------------------------------
#     monthly_name_pnl_df = pd.DataFrame({
#         'exit_month': monthly_pnl["index"].dt.strftime('%B'),
#         'pnl': monthly_pnl["pnl"].values
#     })
#     monthly_name_pnl_df = monthly_name_pnl_df.groupby('exit_month').sum().reset_index()
#     monthly_name_pnl_df['exit_month'] = pd.Categorical(monthly_name_pnl_df['exit_month'], 
#                                             categories=['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'], 
#                                             ordered=True)
#     monthly_name_pnl_df = monthly_name_pnl_df.sort_values('exit_month').reset_index(drop=True)

#     # day name-wise results
#     day_name_wise_result = transaction_df[["entry_day", "pnl"]]
#     day_name_wise_result['entryDay'] = day_name_wise_result['entry_day'].dt.day_name()
#     day_name_pnl = day_name_wise_result.groupby('entryDay')['pnl'].sum().reset_index()
#     weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
#     day_name_pnl['entryDay'] = pd.Categorical(day_name_pnl['entryDay'], categories=weekday_order, ordered=True)
#     day_name_pnl = day_name_pnl.sort_values('entryDay').reset_index(drop=True)


#     return daily_df_pnl,mtm_df,monthly_pnl,yearly_pnl,monthly_name_pnl_df,day_name_pnl

# def write_dataframe_to_sheet(sheet, df, start_row, start_col):
#     for i, row in df.iterrows():
#         for j, value in enumerate(row):
#             # Writing each value to the specified location
#             sheet.cell(row=start_row + i, column=start_col + j, value=value)



# def write_to_excel(transaction_df, file_path,daily_pnl,mtm_df,monthly_pnl, yearly_pnl , monthly_name_pnl_df, day_name_pnl):
#     sheet1 = "inputs"
#     sheet2 = "transactions"
#     sheet3 = "stats"
#     # workbook = openpyxl.load_workbook(file_path)

#     ## input data write

#     input_df = [{"Parameter" : "INDEX" , "Value":"NIFTY"}, {"Parameter" : "lookback" , "Value":"4"}, {"Parameter" : "supertrend_setting" , "Value":"(14,3)"}]
#     input_df = pd.DataFrame(input_df)

#     ## data write
#     writer = pd.ExcelWriter(file_path, engine='openpyxl')
#     transaction_df.to_excel(writer, sheet_name=sheet2, index=False)  
#     input_df.to_excel(writer, sheet_name=sheet1, index=False) 

#     daily_pnl = daily_pnl.reset_index(drop=False).rename(columns={"index": "date"})
#     daily_pnl.to_excel(writer, sheet_name = sheet3, startrow = 1, startcol = 1, index=False)

#     mtm_df.to_excel(writer, sheet_name = sheet3, startrow = 1, startcol = 3, index=False)

#     monthly_pnl = monthly_pnl.rename(columns={"index": "months"})
#     monthly_pnl.to_excel(writer, sheet_name = sheet3, startrow = 1, startcol = 7, index=False)

#     yearly_pnl = yearly_pnl.rename(columns={"index": "years"})
#     yearly_pnl.to_excel(writer, sheet_name = sheet3, startrow = 1, startcol = 10, index=False)

#     monthly_name_pnl_df = monthly_name_pnl_df.rename(columns={"index": "months_by_name"})
#     monthly_name_pnl_df.to_excel(writer, sheet_name = sheet3, startrow = 10, startcol = 10, index=False)

#     day_name_pnl = day_name_pnl.rename(columns={"index": "days_by_name"})
#     day_name_pnl.to_excel(writer, sheet_name = sheet3, startrow = 25, startcol = 10, index=False)


#     writer.close()
    

#     print(f"Data successfully written to {file_path}")

start_date = "2023-09-10"
end_date = "2023-09-30"
date_list = pd.date_range(start=start_date, end=end_date).tolist()
trades_df = trade_execution(date_list[0],date_list[-1], conn_spot, "15min", 4,14,3)
# transaction_df = output_transaction_metric(trades_df)
# print(transaction_df)

# daily_pnl ,mtm_df, monthly_pnl, yearly_pnl , monthly_name_pnl_df, day_name_pnl= output_stats(start_date,end_date,transaction_df,spot_daily_data)


# write_to_excel(transaction_df, r"W:\asiatic\asiatic_supertrend\results.xlsx",daily_pnl,mtm_df,monthly_pnl, yearly_pnl , monthly_name_pnl_df, day_name_pnl)

    


conn_spot.close()
print("spot db conncetion closed")
# conn_option.close()
# print("option db conncetion closed")