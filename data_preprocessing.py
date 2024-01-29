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
    start = '2006-12-31'
    end = '2008-12-31'
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
        df_bbo = alldata[~alldata['bid-price'].isna()] # we keep only the "bbo" events.
        
        df_bbo['xltime'] = df_bbo['xltime'].astype('datetime64')
        df_bbo['minute'] = df_bbo['xltime'].dt.strftime('%Y-%m-%d %H:%M')
        #group by minute and ticker, then calculate VWA for ask and bid
        vwa_ask_df = df_bbo.groupby(by=['minute', 'ticker'], 
                                   agg={'vwa-ask': vaex.agg.sum(df_bbo['ask-price'] * df_bbo['ask-volume']) 
                                        / vaex.agg.sum(df_bbo['ask-volume'])
                                        })
        vwa_bid_df = df_bbo.groupby(by=['minute', 'ticker'],
                                      agg={'vwa-bid': vaex.agg.sum(df_bbo['bid-price'] * df_bbo['bid-volume']) 
                                         / vaex.agg.sum(df_bbo['bid-volume'])
                                         })
        #join the two dataframes
        vwa_bbo_df = vwa_ask_df.join(vwa_bid_df, on=['minute', 'ticker'], how='outer') #to be checked
        #save to parquet file
        vwa_bbo_name = os.path.join(dirSaveBase, 
                                      re.sub(r'events', 'vwa-bbo-{}'.format(tick), os.path.basename(file)))
        vwa_bbo_df.export_parquet(vwa_bbo_name,use_deprecated_int96_timestamps=True)