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
from bt_utils_yash import data_fetching, data_resample_clean_func,get_expiry,strike_price,trade_info,duration_days_counts,get_options_price,main_hedge_option_df


spot_db_path = r"C:\Users\Administrator\Desktop\yash\supertrend\NIFTY_SPOT.db"
option_db_path =  r"C:\Users\Administrator\Desktop\yash\supertrend\NIFTY_OPT.db"

## Spot data fixing --------------------------------
spot_csv_path =r"C:\Users\Administrator\Desktop\yash\supertrend\daily_db_nifty.csv"
spot_daily_data = pd.read_csv(spot_csv_path,dtype={"date" : str})
spot_daily_data["date"] = pd.to_datetime(spot_daily_data["date"], format = "%d%m%Y")
trading_dates = spot_daily_data["date"].to_list()
# spot_daily_data['date'] = pd.to_datetime(spot_daily_data['date'], format="%d%m%Y")

## spot data fixed ------------------------------------

conn_spot = sqlite3.connect(spot_db_path)
print("spot db connceted")
conn_option = sqlite3.connect(option_db_path)
print("option db connceted")

results_file_path  = r"C:\Users\Administrator\Desktop\yash\supertrend\results_bt.xlsx"


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
    main_trade_number = 0
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
                
                 
                original_datetime = row.name + timedelta(minutes=resample_number)
                while original_datetime.time()>= pd.to_datetime("15:30:00", format = "%H:%M:%S").time():
                    original_datetime = original_datetime - timedelta(minutes=1)


                print(f'spot_resampled row -> {row.name}')
                print(f'original timestamp -> {original_datetime}')

               ## Logging max loss and max profit ---------
                if in_trade==True:
                    print("In a trade -> updating max_profit & max_loss")
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

                if hedge_trade_open==True and row["supertrend_direction"] == 1 and trade_type=="call_sell":
                 
                    print(f"Hedge trade(call_buy) closed on {original_datetime}")
                    df_option_dict =  main_hedge_option_df(conn_option, row.date,option_type,latest_expiry,"hedge",hedge_strike = hedge_strike)
                    hedge_option_df = df_option_dict["hedge"]
                    hedge_exit_price = hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0]["close"]    
                    hedge_exit_time =  hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0].index
                    hedge_trade_info_log["exit_date"] = row.date
                    hedge_trade_info_log["exit_signal_time"] = original_datetime
                    hedge_trade_info_log["exit_price"] = hedge_exit_price
                    hedge_trade_info_log["exit_date"] = row.date
                    hedge_trade_open = False
                
                elif hedge_trade_open==True and row["supertrend_direction"] == -1 and trade_type=="put_sell":
                 
                    print(f"Hedge trade(put_buy) closed on {original_datetime}")
                    df_option_dict =  main_hedge_option_df(conn_option, row.date,option_type,latest_expiry,"hedge",hedge_strike = hedge_strike)
                    hedge_option_df = df_option_dict["hedge"]
                    hedge_exit_price = hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0]["close"]    
                    hedge_exit_time =  hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0].index
                    hedge_trade_info_log["exit_date"] = row.date
                    hedge_trade_info_log["exit_signal_time"] = original_datetime
                    hedge_trade_info_log["exit_time"] = hedge_exit_time
                    hedge_trade_info_log["exit_price"] = hedge_exit_price
                    hedge_trade_info_log["exit_reason"] = "Hedge trade closing on same time as main trade"
                    hedge_trade_info_log["pnl_raw"] = hedge_trade_info_log["exit_price"] -  hedge_trade_info_log["entry_price"] 
                    hedge_trade_info_log["max_loss"] = hedge_tracking_low - hedge_trade_info_log["entry_price"]
                    # hedge_trade_info_log["max_loss_date"] = hedge_exit_price
                    hedge_trade_info_log["max_profit"] = hedge_tracking_high - hedge_trade_info_log["entry_price"]
                    # hedge_trade_info_log["max_profit_date"] = hedge_exit_price


                    hedge_trade_open = False
                    

                if in_trade==True and row["supertrend_direction"] == 1 and trade_type=="Call_Sell":
            
                    print(f"Call Sell trade closed on {row.name} by doing Call Buy")
                    in_trade=False
                    trade_type = None               

                    main_option_df = option_all_data[row.date]
                    trade_info_log["exit_date"] = row.date
                    trade_info_log["exit_signal_time"] = original_datetime
                    trade_info_log["exit_price"] = get_options_price(main_option_df,row.name,"close", resample_number)
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


                if in_trade==True and row.date==latest_expiry and str(row.time)=="15:15:00":
                    if trade_type=="Put_Sell":
                        print(f"Doing rollover ->  Put_Sell trade closed on {row.name} by doing Put Buy")

                        main_option_df = option_all_data[row.date]
                        trade_info_log["trade_status"] = "Closed"
                        trade_info_log['main_trade_status'] ="Closed"
                        trade_info_log['hedge_trade_status'] ="Closed"
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

                        main_trade_number = main_trade_number +1
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
                        trade_info_log["trade_status"] = "Open"
                        trade_info_log['main_trade_status'] ="Open"
                        trade_info_log['hedge_trade_status'] ="Open"
                        trade_info_log["entry_date"] = row.date
                        trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                        trade_info_log["entry_signal_time"] = row.name
                        trade_info_log["main_entry_price"] = main_entry_price
                        trade_info_log["hedge_entry_price"] = hedge_entry_price
                        trade_info_log["entry_reason"] = "Supertrend bullish & Market Sentiment Bullish"
                        trade_info_log["market_bias"] = lookback_sentiment
                        trade_info_log["trade_id"] = main_trade_number
                        trade_info_log["associate_trade_id"] = trade_number
                        trade_info_log["trade_type"] = "PUT_SELL"


                    elif trade_type=="Call_Sell":
                        
                        print(f"Doing rollover ->  Call_Sell trade closed on {row.name} by doing Call Buy")

                        trade_info_log["trade_status"] = "Closed"
                        trade_info_log['main_trade_status'] ="Closed"
                        trade_info_log['hedge_trade_status'] ="Closed"
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

                        main_trade_number = main_trade_number +1
                        print(f"After rollover -> Call sell at {row.name}")
                        latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                        main_strike_price , all_strikes = strike_price(conn_option, row.date, row.name,latest_expiry, option_type, row.close, 0.3)
                        condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND strike = '{main_strike_price}')"
                        main_option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
                        main_option_df["datetime"] = pd.to_datetime(main_option_df["date"] + " " + main_option_df["time"], format="%d%m%Y %H:%M:%S")
                        main_option_df.set_index("datetime", inplace=True)
                        main_option_df.sort_index(inplace=True)

                        
                        option_all_data[row.date] = main_option_df                
                      
                        main_entry_price = get_options_price(main_option_df,row.name,"close", resample_number)               

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
                        trade_info_log["trade_id"] = main_trade_number
                        trade_info_log["associate_trade_id"] = trade_number
                        trade_info_log["trade_type"] = "Call_SELL"

                    

                ## Checking Trade conditions ---------------------------------------

                
                if lookback_sentiment == "Bullish" and row["supertrend_direction"] == 1 and row["prev_supertrend_direction"]==-1  and in_trade==False:
                    
                    option_type = "PE"
                    in_trade = True
                    hedge_trade_open = True
                    trade_type = "put_sell"
                    print(f"put sell at {row.name}")
                    main_trade_number = main_trade_number +1
                    trade_number = trade_number +1

                    latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                    main_strike , hedge_strike = strike_price(conn_option, row.date, original_datetime,latest_expiry, option_type, row.close, 4 , 0.3)
                    option_df_dict = main_hedge_option_df(conn_option, row.date,option_type,latest_expiry,"both",main_strike,hedge_strike)

                    
                    main_option_df = option_df_dict["main"]
                    hedge_option_df = option_df_dict["hedge"]
                    
                    option_all_data[row.date] = main_option_df

                    # getting_entry_price
                    main_entry_price = main_option_df[main_option_df.index >= original_datetime].iloc[0]["close"] 
                    main_entry_time =  main_option_df[main_option_df.index >= original_datetime].iloc[0].index        
                    hedge_entry_price = hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0]["close"]    
                    hedge_entry_time =  hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0].index

                    
                    max_profit =  main_option_df[main_option_df.index >= original_datetime].iloc[0]["low"]
                    max_loss = main_option_df[main_option_df.index >= original_datetime].iloc[0]["high"]
               
           
                    main_trade_info_log = trade_info_log()
                                 
                    main_trade_info_log["trade_status"] = "Open"
                    main_trade_info_log['instrument_id'] ="NIFTY"
                    main_trade_info_log['main/hedge'] ="main"
                    main_trade_info_log['trade_type'] =trade_type
                    main_trade_info_log["entry_date"] = row.date
                    main_trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                    main_trade_info_log["entry_signal_time"] = original_datetime
                    main_trade_info_log["entry_time"] = main_entry_time
                    main_trade_info_log["entry_price"] = main_entry_price
                    main_trade_info_log["entry_reason"] = "Supertrend Bullish & Market Sentiment Bullish"
                    main_trade_info_log["market_bias"] = lookback_sentiment
                    main_trade_info_log["trade_id"] = main_trade_number
                    main_trade_info_log["associate_trade_id"] = trade_number

                    hedge_trade_info_log = trade_info_log()
                    main_trade_number = main_trade_number +1
                    hedge_trade_info_log["trade_status"] = "Open"
                    hedge_trade_info_log['instrument_id'] ="NIFTY"
                    hedge_trade_info_log['main/hedge'] ="hedge"
                    hedge_trade_info_log['trade_type'] = "put_buy"
                    hedge_trade_info_log["entry_date"] = row.date
                    hedge_trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                    hedge_trade_info_log["entry_signal_time"] = original_datetime
                    hedge_trade_info_log["entry_time"] = hedge_entry_time
                    hedge_trade_info_log["entry_price"] = hedge_entry_price
                    hedge_trade_info_log["entry_reason"] = f"Hedge trade for {trade_type}"
                    hedge_trade_info_log["market_bias"] = lookback_sentiment
                    hedge_trade_info_log["trade_id"] = main_trade_number
                    hedge_trade_info_log["associate_trade_id"] = trade_number





           
                if lookback_sentiment == "Bearish" and row["supertrend_direction"] == -1 and  row["prev_supertrend_direction"]== 1  and in_trade==False:
                    
                    option_type = "CE"
                    in_trade = True
                    hedge_trade_open = True
                    trade_type = "call_sell"
                    print(f"call sell at {row.name}")
                    main_trade_number = main_trade_number +1
                    trade_number = trade_number +1

                    latest_expiry , expiry_list = get_expiry(conn_option,row.date,option_type,"cur")
                    main_strike , hedge_strike = strike_price(conn_option, row.date, original_datetime,latest_expiry, option_type, row.close, 4 , 0.3)
                    option_df_dict = main_hedge_option_df(conn_option, row.date,option_type,latest_expiry,"both",main_strike,hedge_strike)
                    main_option_df = option_df_dict["main"]
                    hedge_option_df = option_df_dict["hedge"]
                    
                    option_all_data[row.date] = main_option_df

                    # getting_entry_price
                    main_entry_price = main_option_df[main_option_df.index >= original_datetime].iloc[0]["close"] 
                    main_entry_time =  main_option_df[main_option_df.index >= original_datetime].iloc[0].index        
                    hedge_entry_price = hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0]["close"]    
                    hedge_entry_time =  hedge_option_df[hedge_option_df.index >= original_datetime].iloc[0].index

                    
                    max_profit =  main_option_df[main_option_df.index >= original_datetime].iloc[0]["low"]
                    max_loss = main_option_df[main_option_df.index >= original_datetime].iloc[0]["high"]
               
           
                    main_trade_info_log = trade_info_log()
                                 
                    main_trade_info_log["trade_status"] = "Open"
                    main_trade_info_log['instrument_id'] ="NIFTY"
                    main_trade_info_log['main/hedge'] ="main"
                    main_trade_info_log['trade_type'] =trade_type
                    main_trade_info_log["entry_date"] = row.date
                    main_trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                    main_trade_info_log["entry_signal_time"] = original_datetime
                    main_trade_info_log["entry_time"] = main_entry_time
                    main_trade_info_log["entry_price"] = main_entry_price
                    main_trade_info_log["entry_reason"] = "Supertrend Bearish & Market Sentiment Bearish"
                    main_trade_info_log["market_bias"] = lookback_sentiment
                    main_trade_info_log["trade_id"] = main_trade_number
                    main_trade_info_log["associate_trade_id"] = trade_number

                    hedge_trade_info_log = trade_info_log()
                    main_trade_number = main_trade_number +1
                    hedge_trade_info_log["trade_status"] = "Open"
                    hedge_trade_info_log['instrument_id'] ="NIFTY"
                    hedge_trade_info_log['main/hedge'] ="hedge"
                    hedge_trade_info_log['trade_type'] = "put_buy"
                    hedge_trade_info_log["entry_date"] = row.date
                    hedge_trade_info_log["dte"] = duration_days_counts(row.date, latest_expiry,trading_dates)
                    hedge_trade_info_log["entry_signal_time"] = original_datetime
                    hedge_trade_info_log["entry_time"] = hedge_entry_time
                    hedge_trade_info_log["entry_price"] = hedge_entry_price
                    hedge_trade_info_log["entry_reason"] = f"Hedge trade for {trade_type}"
                    hedge_trade_info_log["market_bias"] = lookback_sentiment
                    hedge_trade_info_log["trade_id"] = main_trade_number
                    hedge_trade_info_log["associate_trade_id"] = trade_number


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



start_date = "2023-09-01"
end_date = "2023-09-10"
date_list = pd.date_range(start=start_date, end=end_date).tolist()
trades_df = trade_execution(date_list[0],date_list[-1], conn_spot, "15min", 4,14,3)

writer = pd.ExcelWriter(results_file_path, engine='openpyxl')
trades_df.to_excel(writer, sheet_name="transactions", index=False)
writer.close()

# transaction_df = output_transaction_metric(trades_df)
# print(transaction_df)

# daily_pnl ,mtm_df, monthly_pnl, yearly_pnl , monthly_name_pnl_df, day_name_pnl= output_stats(start_date,end_date,transaction_df,spot_daily_data)


# write_to_excel(transaction_df, r"W:\asiatic\asiatic_supertrend\results.xlsx",daily_pnl,mtm_df,monthly_pnl, yearly_pnl , monthly_name_pnl_df, day_name_pnl)

    


conn_spot.close()
print("spot db conncetion closed")
conn_option.close()
print("option db conncetion closed")