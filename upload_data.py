
from email.utils import parsedate_to_datetime
import pandas as pd
import pytz
import glob
import re
import os
import vaex


#Defining functions to clean timestamp of multiple files
def clean_timestamp(df, timezone = 'America/New_York'):
    """
    This function takes a dataframe with a column named 'xltime' and converts it to a datetime column in the local timezone.
    """
    df['utc_time'] = pd.to_datetime(df['xltime'], unit='D', origin = '1899-12-30', utc=True)
    df.sort_values(by=['utc_time'], inplace=True)
    df['local_time'] = df['utc_time'].dt.tz_convert(pytz.timezone(timezone))
    df.drop('xltime', axis=1, inplace=True)
    return df

def file_clean_timestamp(file_path):
    df = pd.read_parquet(file_path, engine='pyarrow')
    df = clean_timestamp(df)
    return df

def upload_clean_data(data_folder_path, dask_cores = 8):
    """
    Uploads all clean data from a folder into a single pandas dataframe.
    """
    allfiles = glob.glob(data_folder_path + "/*")
    return None

def load_TRTH_trade(filename,
             tz_exchange="America/New_York",
             only_non_special_trades=True,
             only_regular_trading_hours=True,
             open_time="09:30:00",
             close_time="16:00:00",
             merge_sub_trades=True):
    """
    Loads trade files and cleans the timestamp which is set as index.
    """
    try:
        if re.search('(csv|csv\\.gz)$',filename):
            DF = pd.read_csv(filename)
        if re.search(r'arrow$',filename):
            DF = pd.read_arrow(filename)
        if re.search('parquet$',filename):
            DF = pd.read_parquet(filename)

    except Exception as e:
        print("load_TRTH_trade could not load "+filename)
        print(e)
        return None
    
    try:
        DF.shape
    except Exception as e: # DF does not exist
        print("DF does not exist")
        print(e)
        return None

    
    if DF.shape[0]==0:
        return None
    
    if only_non_special_trades:
        DF = DF[DF["trade-stringflag"]=="uncategorized"]

    DF.drop(columns=["trade-rawflag","trade-stringflag"],axis=1,inplace=True)
    
    DF.index = pd.to_datetime(DF["xltime"],unit="d",origin="1899-12-30",utc=True)
    DF.index = DF.index.tz_convert(tz_exchange)  # .P stands for Arca, which is based at New York
    DF.drop(columns="xltime",inplace=True)
    
    if only_regular_trading_hours:
        DF=DF.between_time(open_time,close_time)    # warning: ever heard e.g. about Thanksgivings?
    
    if merge_sub_trades:
           DF=DF.groupby(DF.index).agg(trade_price=pd.NamedAgg(column='trade-price', aggfunc='mean'),
                                       trade_volume=pd.NamedAgg(column='trade-volume', aggfunc='sum'))
    
    return DF

def load_TRTH_bbo(filename,
             tz_exchange="America/New_York",
             only_regular_trading_hours=True,
             merge_sub_trades=True):
    """
    Loads bbo files and cleans the timestamp which is set as index.
    """
    try:
        if re.search(r'(csv|csv\.gz)$',filename):
            DF = pd.read_csv(filename)
        if re.search(r'arrow$',filename):
            DF = pd.read_arrow(filename)
        if re.search(r'parquet$',filename):
            DF = pd.read_parquet(filename) 
    except Exception as e:
        print("load_TRTH_bbo could not load "+filename)
        return None
    
    try:
        DF.shape
    except Exception as e: # DF does not exist
        print("DF does not exist")
        print(e)
        return None

    if DF.shape[0]==0:
        return None
    
        
    DF.index = pd.to_datetime(DF["xltime"],unit="d",origin="1899-12-30",utc=True)
    DF.index = DF.index.tz_convert(tz_exchange)  # .P stands for Arca, which is based at New York
    DF.drop(columns="xltime",inplace=True)
    
    if only_regular_trading_hours:
        DF=DF.between_time("09:30:00","16:00:00")    # ever heard about Thanksgivings?
        
    if merge_sub_trades:
        DF=DF.groupby(DF.index).last()
    

        
    return DF


def load_merge_trade_bbo(ticker,date,
                         country="US",
                         dirBase="data/raw/equities/",
                         suffix="parquet",
                         suffix_save=None,
                         dirSaveBase="data/clean/equities/events",
                         saveOnly=False,
                         doSave=False
                        ):
    """
    Loads trade and bbo files and merges them into a single dataframe.
    """
    
    file_trade=dirBase+"/"+country+"/trade/"+ticker+"/"+str(date.date())+"-"+ticker+"-trade."+suffix
    file_bbo=file_trade.replace("trade","bbo")
    trades=load_TRTH_trade(file_trade)
    bbos  =load_TRTH_bbo(file_bbo)
    try:
        trades.shape + bbos.shape
    except:
        return None
    
    events=trades.join(bbos,how="outer")
    #add day to events
    events['day'] = events.index.date
    #add time to events
    events['time'] = events.index.time
    #add ticker to events
    events['ticker'] = ticker
    
    if doSave:
        dirSave=dirSaveBase+"/"+country+"/events/"+ticker
        if not os.path.isdir(dirSave):
            os.makedirs(dirSave)

        if suffix_save:
            suffix=suffix_save
        
        file_events=dirSave+"/"+str(date.date())+"-"+ticker+"-events"+"."+suffix
       # pdb.set_trace()

        saved=False
        if suffix=="arrow":
            events=vaex.from_pandas(events,copy_index=True)
            events.export_arrow(file_events)
            saved=True
        if suffix=="parquet":
         #   pdb.set_trace()
            events.to_parquet(file_events,use_deprecated_int96_timestamps=True)
            saved=True
            
        if not saved:
            print("suffix "+suffix+" : format not recognized")
            
        if saveOnly:
            return saved
    return events

import glob
def get_all_tickers(dirBase = "data/raw/equities/", country = "US"):
    """
    Returns a list of all the tickers in the given directory
    """
    #find tickers in data folder
    data_folder = dirBase + country + '/trade/*'
    tickers = [f.split('\\')[-1].split('_')[0] for f in glob.glob(data_folder)]
    return tickers

import re
def get_all_dates(ticker, dirBase = "data/raw/equities/", country = "US"):
    """
    Returns a list of all the dates for which there is data for a given ticker
    """
    #find tickers in data folder
    data_folder = dirBase + country + '/trade/' + ticker + '/*'
    date_pattern = r'(\d{4}-\d{2}-\d{2})'

    dates = [pd.to_datetime(re.search(date_pattern, os.path.basename(f)).group(1)) if re.search(date_pattern, os.path.basename(f)) else None for f in glob.glob(data_folder)]

    return dates


def load_all_dates(ticker, start_date = pd.to_datetime('2004-01-01'), end_date = pd.to_datetime('2023-12-31'), country = "US", dirBase="data/raw/equities/", suffix="parquet", suffix_save=None, dirSaveBase="data/clean/equities/events", saveOnly=False, doSave=False):
    """
    Loads all data from start_date to end_date for a given ticker
    
    date: date object with format 'YYYY-MM-DD'
    Retunrs: Pandas dataframe with all the events all the days
    """
    all_events = pd.DataFrame()
    
    all_dates = get_all_dates(ticker, dirBase = dirBase, country = country)
    for date in all_dates:
        events = load_merge_trade_bbo(ticker, date, country = country, dirBase = dirBase, suffix = suffix, suffix_save = suffix_save, dirSaveBase = dirSaveBase, saveOnly = saveOnly, doSave = doSave)
        all_events = pd.concat([all_events, events], axis = 0)
    
    return all_events

def load_all(start_date = pd.to_datetime('2004-01-01'), end_date = pd.to_datetime('2023-12-31'), tickers:list = None, country = "US", dirBase="data/raw/equities/", suffix="parquet", suffix_save=None, dirSaveBase="data/clean/equities/events", saveOnly=False, doSave=False):
    """
    Loads all data from start_date to end_date
    
    If tickers is None, loads all the data available
    If tickers is a list of tickers, loads only those tickers
    
    Returns: Pandas dataframe with all the events for all tickers and the given days
    """
    all_events = pd.DataFrame()
    if tickers is None:
        all_tickers = get_all_tickers(dirBase = dirBase, country = country)
    else:
        all_tickers = tickers
    for ticker in all_tickers:
        events = load_all_dates(ticker = ticker, start_date=start_date, end_date = end_date, country = country, dirBase = dirBase, suffix = suffix, suffix_save = suffix_save, dirSaveBase = dirSaveBase, saveOnly = saveOnly, doSave = doSave)
        all_events = pd.concat([all_events, events], axis = 0)

    return all_events