import csv
import os
import queue
import shutil
import time
from datetime import datetime, timedelta
import dateparser
import pytz
from binance.client import Client
from binance.enums import HistoricalKlinesType
from joblib import Parallel, delayed
from typing import List
import pandas as pd
from collections import deque
from io import StringIO

def call_api(func_to_call, client, clientlist, params, wait=False):
    """
    Calls an API function on a client and handles potential exceptions and retries.

    Args:
        func_to_call: The name of the API function to call.
        client: The client object.
        clientlist: A queue of client objects.
        params: The parameters to pass to the API function.
        wait: A flag indicating whether to wait before retrying. Defaults to False.

    Returns:
        A tuple containing the updated client object and the API response.

    Raises:
        None.
    """
    x = getattr(client, func_to_call)(**params)
    out = []
    while True:
        try:
            z = next(x)
            out.append(z)
        except StopIteration:
            break
        except Exception as e:
            print(e)
            if wait:
                time.sleep(10)

            if out != []:
                try:
                    params["start_str"] = out[-1]["T"]
                except:
                    params["start_str"] = out[-1][0]
            client = clientlist.get()
            client, v = call_api(func_to_call, client, clientlist, params, wait=True)
            out.extend(v)
            break
    x = out

    return client, x


def date_to_milliseconds(date_str):
    """
    Converts a date string to milliseconds since the epoch.

    Args:
        date_str: A string representing a date.

    Returns:
        The number of milliseconds since the epoch corresponding to the input date.

    Raises:
        None.
    """

    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    d = dateparser.parse(date_str)
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)
    return int((d - epoch).total_seconds() * 1000.0)

def getstate(
    outlist, exchangeinfo,client, mcap,tokennames= ["BUSD"],datapath='path'
):
    """
    Gets a list of tokens based on specified criteria from the Binance exchange.

    Args:
        outlist: A list of tokens to exclude from the result.
        exchangeinfo: Information about the exchange.
        client: The Binance client.
        mcap: The market capitalization threshold.
        tokennames: A list of token names to consider. Defaults to ["BUSD"].
        datapath: The path where the data files are located. Defaults to 'path'.

    Returns:
        A list of tokens that meet the specified criteria.

    Raises:
        None.
    """

    sm = client.get_klines(
        symbol="BTCBUSD",
        interval=Client.KLINE_INTERVAL_1DAY,
        limit=100,
    )
    lasttime = sm[-1][0]
    currlist = []
    for ab in exchangeinfo["symbols"]:
        if (
            ab["quoteAsset"] == tokennames[0]
            and ab["symbol"] not in outlist
            and "BEAR" not in ab["symbol"]
            and "BULL" not in ab["symbol"]
            and "UP" not in ab["symbol"]
            and "DOWN" not in ab["symbol"]
        ):
            sm = client.get_klines(
                symbol=ab["symbol"],
                interval=Client.KLINE_INTERVAL_1DAY,
                limit=100,
            )
            if sm[-1][0] != lasttime:
                continue
            sm.pop(-1)
            vol = [float(x[5]) for x in sm]
            v = [float(x[4]) for x in sm]
            vol = sum(vol) / len(vol)
            v = sum(v) / len(v)
            if vol * v > mcap:
                currlist.append(ab["symbol"])
    downloaded = os.listdir(f"{datapath}/raw")
    downloaded = [x.split(".")[0] for x in downloaded]
    currlist = list(set(currlist).difference(downloaded))
    print("number of tokens:", len(currlist))
    return currlist

def pull_from_binance(
    start, end, interval, q, path, token,app=False
):
    """
    Pulls historical kline data from Binance for a specified time period and interval.

    Args:
        start: The start time for the data.
        end: The end time for the data.
        interval: The time interval of the data.
        q: A queue of clients for making API calls.
        path: The path where the data will be saved.
        token: The token symbol.
        app: A flag indicating whether to append to existing data. Defaults to False.

    Returns:
        The kline data as a pandas DataFrame.

    Raises:
        None.
    """
    client = q.get_nowait()
    client, klines = call_api(
        client=client,
        func_to_call="get_historical_klines_generator",
        clientlist=q,
        params=dict(
            symbol=token,
            interval=interval,
            start_str=start,
            end_str=end,
            klines_type=HistoricalKlinesType.SPOT,
        ),
    )
    
    if klines == []:
        return

    headers = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "symbol",
    ]
    klineframe = pd.DataFrame(columns=headers)
    klineframe["date"] = [
        datetime.fromtimestamp(x[0] / 1000.0, tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
        for x in klines
    ]
    klineframe["open"] = [x[1] for x in klines]
    klineframe["high"] = [x[2] for x in klines]
    klineframe["low"] = [x[3] for x in klines]
    klineframe["close"] = [x[4] for x in klines]
    klineframe["volume"] = [x[5] for x in klines]
    klineframe["symbol"] = [token for _ in klines]
    klineframe = klineframe.set_index("date")
    klineframe = klineframe.astype(
        {
            "open": "float64",
            "high": "float64",
            "low": "float64",
            "close": "float64",
            "volume": "float64",
            "symbol": "object",
        }
    )
    if app:
        klineframe.to_csv(f"{path}/{token}.csv",mode='a',header=False)
    else:
        klineframe.to_csv(f"{path}/{token}.csv")
    
    return klineframe

def startdownload(start, end, dir,interval, app=False, clients=[],mcap=None,tokens=["BUSD"],datapath='data'):
    """
    Starts the data download process from Binance for a specified time period with a custom interval.

    Args:
        start: The start time for the data download.
        end: The end time for the data download.
        dir: The directory where the downloaded data will be saved.
        interval: The time interval of the data.
        app: A flag indicating whether to append to existing data. Defaults to False.
        clients: A list of clients.
        mcap: The market capitalization threshold for the data download. Defaults to None.
        tokens: A list of tokens to be included in the data download. Defaults to ["BUSD"].
        datapath: The path where the data files are located. Defaults to 'data'.

    Returns:
        The downloaded data.

    Raises:
        None.
    """
    if not os.path.exists(dir):
        os.mkdir(dir)
    exchg = clients[0].get_exchange_info()
    if app == True:
        currlist = [
            popl.split(".")[0]
            for popl in os.listdir(
                f"{datapath}/raw"
            )

        ]
    else:
        currlist = getstate([], exchg,client=clients[0],mcap=mcap,tokennames=tokens,datapath=datapath)
    fuln = (len(currlist) // len(clients))+1
    q = queue.SimpleQueue()
    for _ in range(fuln * 10000):
        for item in clients:
            q.put(item)

    cs = len(clients) // 4
    if not isinstance(start,dict):
        start = {x:start for x in currlist}
    out = Parallel(n_jobs=cs,backend="threading")(
            delayed(pull_from_binance)(int(start[i]), int(end), interval, q, dir, i,app)
            for i in currlist
        )
    return out

def run(d, clilist,interval,mcap, tokens,starttime="2023-01-01 00:00:00",datapath='data'):
    """
    Runs the data download process from Binance for a specified time period with a custom interval.

    Args:
        d: The end time for the data download.
        clilist: A list of clients.
        interval: The time interval of the data.
        mcap: The market capitalization threshold for the data download.
        tokens: A list of tokens to be included in the data download.
        starttime: The start time for the data download. Defaults to "2023-01-01 00:00:00".
        datapath: The path where the data files are located. Defaults to 'data'.

    Returns:
        None.

    Raises:
        None.
    """


    csv_path = f"{datapath}/raw"
    if os.path.exists(csv_path):
        shutil.rmtree(csv_path)
    os.mkdir(csv_path)
    dmin = d.strftime("%Y-%m-%d %H:%M:%S") 
    strt = date_to_milliseconds(starttime)
    dmin = date_to_milliseconds(dmin)
    startdownload(start=strt, end=dmin, dir=csv_path, clients=clilist,interval=interval,mcap=mcap,tokens=tokens,datapath=datapath)
    time.sleep(10)
    for clin in clilist:
        clin.close_connection()

def appendtoexististing(data,datapath):
    """
    Appends data to existing CSV files in the specified data path.

    Args:
        data: A list of dataframes to be appended.
        datapath: The path where the data files are located.

    Returns:
        None.

    Raises:
        None.
    """
    for df in data:
        if df is not None:
            
            symbol = df['symbol'].head(1).values[0]
            df.to_csv(f"{datapath}/raw/{symbol}.csv", mode='a', index=True, header=False)
            print(f"updated {symbol}")
        else:
            os.remove(f"{datapath}/raw/{symbol}.csv")
            continue

def sortinterval(input):
    """
    Sorts the input interval string and returns the corresponding Binance API constant.

    Args:
        input: The input interval string.

    Returns:
        The corresponding Binance API constant.

    Raises:
        None.
    """
    revintverals ={
    '1m':Client.KLINE_INTERVAL_1MINUTE,
    '3m':Client.KLINE_INTERVAL_3MINUTE,
    '5m':Client.KLINE_INTERVAL_5MINUTE,
    '15m':Client.KLINE_INTERVAL_15MINUTE,
    '30m':Client.KLINE_INTERVAL_30MINUTE,
    '1h':Client.KLINE_INTERVAL_1HOUR,
    '2h':Client.KLINE_INTERVAL_2HOUR,
    '4h':Client.KLINE_INTERVAL_4HOUR,
    '6h':Client.KLINE_INTERVAL_6HOUR,
    '8h':Client.KLINE_INTERVAL_8HOUR,
    '12h':Client.KLINE_INTERVAL_12HOUR,
    '1d':Client.KLINE_INTERVAL_1DAY,
    '3d':Client.KLINE_INTERVAL_3DAY,
    '1w':Client.KLINE_INTERVAL_1WEEK,
    '1M':Client.KLINE_INTERVAL_1MONTH,}
    return revintverals[input]


def append(datapath,endtime,interval,clients):
    """
    Appends new data to existing CSV files in the specified data path.

    Args:
        datapath: The path where the data files are located.
        endtime: The end time of the data to be appended.
        interval: The time interval of the data.
        clients: A list of clients.

    Returns:
        The appended data.

    Raises:
        None.
    """
    csvs = {}
    for fl in os.listdir(f"{datapath}/raw"):
        with open(os.path.join(f"{datapath}/raw",fl), 'r') as f:
            q = deque(f, 1)  
        cv = pd.read_csv(StringIO(''.join(q)), header=None).values[0][0]
        if cv == endtime.strftime("%d %B, %Y, %H:%M:%S"):
            continue
        csvs[fl.split(".")[0]] = cv
    csv_path = f"{datapath}/app"
    if os.path.exists(f"{datapath}/app"):
        shutil.rmtree(f"{datapath}/app")
    os.mkdir(f"{datapath}/app")
    dmin = endtime + timedelta(minutes=60)
    timings = {x:date_to_milliseconds((pd.to_datetime(csvs[x]) + timedelta(minutes=1)).strftime("%d %B, %Y, %H:%M:%S")) for x in csvs}
    dmin = date_to_milliseconds(dmin.strftime("%d %B, %Y, %H:%M:%S"))
    out = startdownload(
        start=timings,
        end=dmin,
        dir=csv_path,
        app=True,
        clients=clients,
        interval=interval,
        datapath=datapath
    )
    time.sleep(1)
    for clin in clients:
        clin.close_connection()
    appendtoexististing(out,datapath)
    return out
    
def download(clients=None,datapath="data",starttime="2023-01-01 00:00:00",endtime="now",freq="1d",mcap="5m",tokens=["BUSD"]):
    """
    Downloads data from Binance for specified clients and saves it to a specified data path.

    Args:
        clients: A list of clients or a path to a CSV file containing client information. Each client should be represented by a tuple of (api_key, api_secret).
        datapath: The path where the downloaded data will be saved. Defaults to "data".
        starttime: The start time for the data download. Defaults to "2023-01-01 00:00:00".
        endtime: The end time for the data download. Defaults to "now".
        freq: The frequency of the data to be downloaded. Defaults to "1d".
        mcap: The market capitalization threshold for the data download. Defaults to "5m".
        tokens: A list of tokens to be included in the data download. Defaults to ["BUSD"].

    Returns:
        The downloaded data, if appending.

    Raises:
        None.
    """
    if not os.path.exists(datapath):
        os.mkdir(datapath)
    if isinstance(clients,str):
        with open(clients,'r') as f:
            reader = csv.reader(f)
            clients = [Client(row[0],row[1]) for row in reader]
    elif isinstance(clients, list):
        clients = [Client(row[0],row[1]) for row in clients]
    else:
        print("Clients are in an unsupported format")
        
    if isinstance(mcap, str):
        try:
            mcap = int(mcap)
        except:
            mcap = mcap.replace("m","000000")
            mcap = mcap.replace("k", "000")
            mcap = int(mcap)
    print("starting data download")
    interval = sortinterval(freq)
    if endtime == "now":
        endtime = datetime.now(pytz.utc)
    if os.path.exists(f"{datapath}/raw/"):
        if len(os.listdir(f"{datapath}/raw/")) != 0:
            print(f"data download and prep time elapsed: {datetime.now(pytz.utc) - endtime}")
            return append(datapath,endtime,interval,clients)
        else:
            run(endtime, clilist=clients,starttime=starttime,interval=interval,mcap=mcap,tokens=tokens,datapath=datapath)
    else:
    
        run(endtime, clilist=clients,starttime=starttime,interval=interval,mcap=mcap,tokens=tokens,datapath=datapath)
    print(f"data download and prep time elapsed: {datetime.now(pytz.utc) - endtime}")
