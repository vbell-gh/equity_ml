#!/usr/bin/env python
# coding: utf-8
# updates the holding data to PSQLserver
# this file is connected to cronjob in /cron folder

import pandas as pd
from psql import engine_etf
from datetime import datetime

engine = engine_etf()

holdings = pd.read_excel('https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xlk.xlsx')


def holdings_update(holdings):
    name = str(holdings.iloc[0, 1]).lower()


    def get_date(holdings_df):  # returns the date which is inside the holdings df
        date = holdings_df.iloc[1][1]
        date = date.replace('As of ','')
        date = pd.to_datetime(date)
        return date


    holdings_df_date = get_date(holdings)


    def get_last_db_date():  # gets the last date from the sqlbase
        last_entry = pd.read_sql_query( 'SELECT * FROM xlk ORDER BY date DESC LIMIT 1', engine)
        last_date = last_entry['date'][0]
        return last_date


    last_date = get_last_db_date()


    def upload_df_to_sql(holdings):  # formats and uploads the df to the sql
        holdings = holdings.rename(columns=holdings.iloc[3])
        holdings = holdings[4:]
        holdings = holdings.filter(['Ticker', 'Weight'])
        holdings = holdings.dropna()
        holdings['date'] = holdings_df_date
        holdings['Weight'] = pd.to_numeric(holdings['Weight'], errors='coerce')
        holdings.columns = holdings.columns.str.lower()
        holdings.to_sql(name=name,
                        con=engine,
                        if_exists='append',
                        index=False)
        print('updated')


    if holdings_df_date != last_date:
        upload_df_to_sql(holdings)
    else:
        print('already uploaded')


holdings_update(holdings)


print(datetime.now())
