import chunk
import time
import tqdm
import pandas as pd
from upload_data import load_all
import vaex
import glob
import os
import re

def save_clean_data(dirBase, dirSaveBase, country=''):
    """
    Uploads, cleans and saves data to parquet file
    """
    start = '2007-01-31'
    end = '2007-02-28'
    # partitioning by month from start_date to end_date
    months = pd.date_range(start, end, freq='M')
    # loop through months
    print('Cleaning data monthly from {} to {}'.format(start, end))
    for i in tqdm.tqdm(range(len(months)-1)):
        start_date = months[i] + pd.Timedelta(days=1)
        end_date = months[i+1]
        load_all(start_date=start_date, end_date=end_date, 
                 doSave=True, dirBase=dirBase, dirSaveBase=dirSaveBase, country=country, saveOnly=True)
        

def save_vwap_trade(dirBase = "E:/FinancialData/sp100_2004-8/clean/events/US", dirSaveBase = "E:/FinancialData/sp100_2004-8/clean/vwap/trade", 
                    tick = '1min'):
    """
    Calculates and saves vwap (Volume Weighted Average Price) of the trade price over a specified tick (only available for 1 min for now) to parquet file.
    Requires vaex library.
    """
    #extracting all file names:
    file_pattern = os.path.join(dirBase, '*-to-*-events.parquet')
    file_list = glob.glob(file_pattern)
    #loop through files:
    print('Calculating and saving {}-vwap for all available clean trade files'.format(tick))
    for file in tqdm.tqdm(file_list):
        #loading file
        alldata = vaex.open(file)
        #calculating vwap
        df_trade = alldata[~alldata['trade_price'].isna()] # we keep only the "trade" events.
        
        df_trade['xltime'] = df_trade['xltime'].astype('datetime64')
        df_trade['minute'] = df_trade['xltime'].dt.strftime('%Y-%m-%d %H:%M')
        #group by minute and ticker, then calculate VWAP
        vwap_df = df_trade.groupby(by=['minute', 'ticker'], 
                                   agg={'vwap': vaex.agg.sum(df_trade['trade_price'] * df_trade['trade_volume']) 
                                        / vaex.agg.sum(df_trade['trade_volume'])
                                        })
        #save to parquet file
        vwap_file_name = os.path.join(dirSaveBase, 
                                      re.sub(r'events', 'vwap-trade-{}'.format(tick), os.path.basename(file)))
        vwap_df.export_parquet(vwap_file_name,use_deprecated_int96_timestamps=True)


#defined to compare the speed of vaex and pandas for these computations
def save_vwap_trade_pandas(dirBase="E:/FinancialData/sp100_2004-8/clean/events/US", 
                           dirSaveBase="E:/FinancialData/sp100_2004-8/clean/vwap/trade"):
    """
    Calculates and saves vwap (Volume Weighted Average Price) of the trade price over a 1 minute tick to parquet file using pandas.
    """
    # Extracting all file names
    file_pattern = os.path.join(dirBase, '*-to-*-events.parquet')
    file_list = glob.glob(file_pattern)
    
    # Loop through files
    print('Calculating and saving 1min-vwap for all available clean trade files')
    for file in tqdm.tqdm(file_list):
        # Loading file
        alldata = pd.read_parquet(file)
        
        # Calculating vwap
        # Keep only the trade events
        df_trade = alldata.dropna(subset=['trade_price'])  
        
        # Ensure 'xltime' is a datetime type for grouping
        #put index as a column
        df_trade.reset_index(inplace=True)
        #df_trade['xltime'] = pd.to_datetime(df_trade['xltime'])
        df_trade['minute'] = df_trade['xltime'].dt.floor('T')  # nearest minute
        
        # Group by minute and ticker, then calculate VWAP
        vwap_df = df_trade.groupby(by=['minute', 'ticker']).apply(
            lambda x: pd.Series({
                'vwap': (x['trade_price'] * x['trade_volume']).sum() / x['trade_volume'].sum()
            })
        ).reset_index()
        
        # Save to parquet file
        vwap_file_name = os.path.join(dirSaveBase, 
                                      re.sub('events', 'vwap-trade-1min', os.path.basename(file)))
        
        vwap_df.to_parquet(vwap_file_name, index=False)



def save_vwa_bbo(dirBase = "E:/FinancialData/sp100_2004-8/clean/events/US", dirSaveBase = "E:/FinancialData/sp100_2004-8/clean/vwap/bbo", 
                    tick = '1min'):
    """
    Calculates and saves vwa (Volume Weighted Average) of the bid and ask price over a specified tick to parquet file.
    Requires vaex library.
    """
    #extracting all file names:
    file_pattern = os.path.join(dirBase, '*-to-*-events.parquet')
    file_list = glob.glob(file_pattern)
    #loop through files:
    print('Calculating and saving {}-vwap for all available clean bbo files'.format(tick))
    for file in tqdm.tqdm(file_list):
        #loading file
        alldata = vaex.open(file)
        #calculating vwap
        df = alldata[~alldata['bid-price'].isna() | ~alldata['ask-price'].isna() | ~alldata['ask-volume'].isna() | ~alldata['ask-volume'].isna()] # we keep only the "bbo" events.

        # Calculate price-volume products
        df['bid_price_volume'] = df['bid-price'] * df['bid-volume']
        df['ask_price_volume'] = df['ask-price'] * df['ask-volume']

        # Convert 'xltime' to datetime and resample by minute
        df['xltime'] = df['xltime'].astype('datetime64')
        df['minute'] = df['xltime'].dt.strftime('%Y-%m-%d %H:%M')
        
        vwa_bbo_df = df.groupby(by=['minute', 'ticker'], 
                        agg={'bid_vwa': vaex.agg.sum(df['bid_price_volume']) / vaex.agg.sum(df['bid-volume']),
                             'ask_vwa': vaex.agg.sum(df['ask_price_volume']) / vaex.agg.sum(df['ask-volume'])
                             })
        vwa_bbo_name = os.path.join(dirSaveBase, 
                                      re.sub(r'events', 'vwap-bbo-{}'.format(tick), os.path.basename(file)))
        vwa_bbo_df.export_parquet(vwa_bbo_name,use_deprecated_int96_timestamps=True)

def save_last_bbo(dirBase = "E:/FinancialData/sp100_2004-8/clean/events/US", dirSaveBase = "E:/FinancialData/sp100_2004-8/clean/lastbbo", 
                    tick = '1min'):
    """
    Calculates and saves vwa (Volume Weighted Average) of the bid and ask price over a specified tick to parquet file.
    Requires vaex library.
    """
    #extracting all file names:
    file_pattern = os.path.join(dirBase, '*-to-*-events.parquet')
    file_list = glob.glob(file_pattern)
    #loop through files:
    print('Calculating and saving {}-vwap for all available clean bbo files'.format(tick))
    for file in tqdm.tqdm(file_list):
        #loading file
        alldata = vaex.open(file)
        #calculating vwap
        df = alldata[~alldata['bid-price'].isna()] # we keep only the "bbo" events.

        # Convert 'xltime' to datetime and resample by minute
        df['xltime'] = pd.to_datetime(df['xltime'], utc=True)

        #Convert the timezone from UTC to 'America/New_York'
        df['xltime'] = df['xltime'].dt.tz_convert('America/New_York')
        df['minute'] = df['xltime'].dt.strftime('%Y-%m-%d %H:%M')
        
        vwa_bbo_df = df.groupby(by=['minute', 'ticker'], 
                        agg={'bid_last': vaex.agg.last(df['bid-price']),
                             'ask_last': vaex.agg.last(df['ask-price'])
                             })
        vwa_bbo_name = os.path.join(dirSaveBase, 
                                      re.sub(r'events', 'lastbbo-{}'.format(tick), os.path.basename(file)))
        vwa_bbo_df.export_parquet(vwa_bbo_name,use_deprecated_int96_timestamps=True)
