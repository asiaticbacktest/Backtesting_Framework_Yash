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
    return data[data.index >= date_time].iloc[0][ohlcv]



def strike_price(conn, date,date_time,expiry_date, option_type,spot_price,hedge_strike_factor,strike_perc_val):

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
        otm_strikes = list(opt_df["strike"].unique())
        for strike in otm_strikes:
            otm_strikes_df = opt_df[opt_df["strike"]==strike]
            strike_ltp_df.loc[len(strike_ltp_df)] = [strike, get_options_price(otm_strikes_df,date_time,"close", 15)]     


    strike_ltp_df["closest_strike"] = abs(strike_ltp_df["ltp"] - premium)
    main_strike_price = strike_ltp_df.sort_values("closest_strike").iloc[0]["strike"]


    if option_type=="PE":
        hedge_strike_price = otm_strikes[otm_strikes.index(main_strike_price) - hedge_strike_factor]
    elif option_type=="CE":
        hedge_strike_price = otm_strikes[otm_strikes.index(main_strike_price) - hedge_strike_factor]


    return main_strike_price , hedge_strike_price
    
def main_hedge_option_df(conn_option, date,option_type,latest_expiry,dfs,main_strike=None,hedge_strike=None):
    df = {"main" : None, "hedge":None}
    if dfs == "both":
        if option_type=="PE":
            condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND (strike = '{main_strike}' OR strike = '{hedge_strike}')"
            option_df = data_fetching(conn_option,date.strftime("%d%m%Y"), False, condition=condition)
            option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
            option_df.set_index("datetime", inplace=True)
            main_option_df = option_df[option_df["strike"]==main_strike].sort_index()
            hedge_option_df = option_df[option_df["strike"]==hedge_strike].sort_index()
            df["main"] = main_option_df
            df["hedge"] = hedge_option_df
        else:
            condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND (strike = '{main_strike}' OR strike = '{hedge_strike}')"
            option_df = data_fetching(conn_option,date.strftime("%d%m%Y"), False, condition=condition)
            option_df["datetime"] = pd.to_datetime(option_df["date"] + " " + option_df["time"], format="%d%m%Y %H:%M:%S")
            option_df.set_index("datetime", inplace=True)
            main_option_df = option_df[option_df["strike"]==main_strike].sort_index()
            hedge_option_df = option_df[option_df["strike"]==hedge_strike].sort_index()
            df["main"] = main_option_df
            df["hedge"] = hedge_option_df

    elif dfs=="main":
        condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND strike = '{main_strike}')"
        main_option_df = data_fetching(conn_option,date.strftime("%d%m%Y"), False, condition=condition)
        option_df["datetime"] = pd.to_datetime(main_option_df["date"] + " " + main_option_df["time"], format="%d%m%Y %H:%M:%S")
        main_option_df = main_option_df.set_index("datetime").sort_index()
        df["main"] = main_option_df

    elif dfs=="hedge":
        if option_type=="PE":
            condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND strike = '{hedge_strike}')"
            hedge_option_df = data_fetching(conn_option,date.strftime("%d%m%Y"), False, condition=condition)
            hedge_option_df["datetime"] = pd.to_datetime(hedge_option_df["date"] + " " + hedge_option_df["time"], format="%d%m%Y %H:%M:%S")
            hedge_option_df.set_index("datetime").sort_index()
            df["hedge"] = hedge_option_df

        elif option_type=="CE":
            condition = f"WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry.strftime('%d-%m-%Y')}' AND strike = '{hedge_strike}')"
            hedge_option_df = data_fetching(conn_option,date.strftime("%d%m%Y"), False, condition=condition)
            hedge_option_df["datetime"] = pd.to_datetime(hedge_option_df["date"] + " " + hedge_option_df["time"], format="%d%m%Y %H:%M:%S")
            hedge_option_df.set_index("datetime").sort_index()
            df["hedge"] = hedge_option_df

    return df


## Working with Trade executions -----------------------------------------------------------------------------------------

def trade_info():
    return{
        'trade_status' : None,
        'instrument_id' : None,
        'main/hedge':None,
        'trade_type' : None,
        'entry_date' : None,
        # 'entry_dayname' : None,
        'dte' : None,
        'entry_signal_time' : None,
        'entry_time' : None,
        'entry_price' : None,
        'entry_reason' : None,
        'market_bias' : None,
        'exit_date' : None,
        # 'exit_dayname' : None,
        'exit_signal_time' : None,
        'exit_time' : None,
        'exit_price' : None,
        'exit_reason' : None,
        'pnl_raw' : None,
        'trade_id' : None,
        'associate_trade_id' : None, 
        'intraday_pnl' : 0,
        'overnight_pnl' : 0,
        'max_loss' : 0,
        'max_loss_date' : None,
        'max_profit' : 0,
        'max_profit_date' : None,
    }




## Working with Output executions ----------------------------------------------------------------

def duration_days_counts(start_date, end_date,trading_dates):

    return (trading_dates.index(end_date) - trading_dates.index(start_date))





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


# if hedge_trade_open==True and row.date == ( trading_dates[trading_dates.index(trade_info_log["entry_date"])+1] ) and str(row.time)=="09:15:00":
#     print(f"Hedge trade closed on {row.name}")
#     condition = "WHERE instrument_type = '{option_type}' AND expiry = '{latest_expiry}' AND strike = '{hedge_strike_price}'"
#     hedge_option_df = data_fetching(conn_option,row.date.strftime("%d%m%Y"), False, condition=condition)
#     hedge_option_df["datetime"] = pd.to_datetime(hedge_option_df["date"] + " " + hedge_option_df["time"], format="%d%m%Y %H:%M:%S")
#     hedge_option_df.set_index("datetime", inplace=True)
#     hedge_option_df.sort_index(inplace=True) 
#     trade_info_log["hedge_exit_price"] =  get_options_price(main_option_df,row.name,"close", resample_number)
#     hedge_trade_open = False