import requests as r
import pandas as pd
from datetime import date, timedelta, datetime, tzinfo
import math

from psql import engine_etf, engine_equity
from tiingo_key import ApiKey


today = pd.Timestamp.today(tz='UTC').date()
headers = ApiKey.headers

engine_etf  = engine_etf()
engine_eq = engine_equity()

#Class for creating ticker data based on etf, the default freq is 5min data.
class Etf:
   
    def __init__(self, etf_symbol, frequency='5min', ticker = None):
            self.etf_symbol = etf_symbol
            self.frequency = frequency
            self.ticker = ticker

    
    # get all tickers that are in the holding from the db
    def get_holdings(self):
        sql_scrip = ('SELECT DISTINCT ticker FROM {}'.format(self.etf_symbol))
        tickers = pd.read_sql_query(sql_scrip, engine_etf)
        tickers = tickers['ticker']
        return tickers

    # get the data between selected dates start_date and end_date
    def get_hist_data(self, start_date, end_date):
        iex_url = 'https://api.tiingo.com/iex/{}'.format(self.etf_symbol)
        start_date ='/prices?startDate={}'.format(start_date)
        end_date = '&endDate={}'.format(end_date)
        frequency = '&resampleFreq={}'.format(self.frequency)
        force_fill = '&forceFill=true'
        columns = '&columns=open,high,low,close,volume' 
        url = iex_url + start_date + end_date + frequency + force_fill + columns
        requestResponse = r.get(url, headers=headers)
        data = requestResponse.json()
        data = pd.json_normalize(data)
        return data     

    # get the last date when the db for a specific ticker was updated
    # the returned date is UTC!
    def get_last_date(self, table):
        try:
            sql_query = ('SELECT date FROM {} ORDER BY date DESC LIMIT 1'.format(table))
            date = pd.read_sql_query(sql_query,engine_eq)
            date = date['date'][0]
        except:
            date = 0
        return date
    
    
    def get_holding_data(self):
        max_rows = 9900
        # the api returns 9,999 rows max

        fq_daily_items = {'10min':40, '5min':80, '1min': 400,
                     '15min':27,'30min':14, '60min':7,
                      '10hour':1, }
        
        max_days = math.floor(max_rows/fq_daily_items[self.frequency]) 
                #returns the max days to get from 1 request
        
        dates = pd.date_range(start='01-01-2019', 
                          end=today, 
                          freq='{}D'.format(max_days))
                 #returns range with dates  frequency which is the max to get from the API
        
        if self.ticker == None:
            holdings = self.get_holdings()
        else:
            holdings = self.ticker
        #if the etf is specified it goes through all the holdings
        #if ticker is specified uses only the specific ticker


        # The pipeline of the loop is:
        # 1. Go through all symbols in the etf
        # 2. For each symbol check which is the last date.
        # 3.1. If there is no last date from 01-01-2019 untill today
        # 3.2. If there is last date, get all the data as per the date freq above
        # 4. Upload to the sql server 
        
        holdings = holdings.append(self.etf_symbol) #add the etf symbol to the data
        
        for symbol in holdings:
            table = symbol.lower()
            previous_date = self.get_last_date(table)
            df = pd.DataFrame(columns=['date', 'open', 
                                    'high', 'low', 
                                    'close', 'volume'])
            outcome = []
            if previous_date == 0:        
                try:
                    for day in dates:
                        new_df = self.get_hist_data( 
                                    start_date= day,
                                    end_date= day + timedelta(days=max_days-1))
                        df = df.append(new_df, ignore_index=True)
                        outcome.append('Created')
                except Exception as e:
                    print('There was an Error in creating {}: {}'.format(table, e))
            else:
                start_date = previous_date + pd.DateOffset(1)
                dates = pd.date_range(start=start_date.date(), 
                                      end=today, tz=None,
                                      freq='{}D'.format(max_days))
                try:
                    for day in dates:
                        new_df = self.get_hist_data( 
                                    start_date= day,
                                    end_date= day + timedelta(days=max_days-1))
                        df = df.append(new_df, ignore_index=True)
                        outcome.append('Updated')
                except Exception as e:
                    print('There was an Error in updating {}: {}'.format(table, e))            

            df['date'] = pd.to_datetime(df['date'], utc=True)
            df.to_sql(name=table, con=engine_eq, index=False, if_exists='append')
            print('{} - {}'.format(table, outcome[0]))
           

#xlk = Etf(etf_symbol='xlk')
#xlk.get_holding_data()
#run the above when you want to update xlk

#xlk = Etf(etf_symbol='xlk', ticker=['xlk'])
#xlk.get_holding_data()
#run the above to upload the xlk