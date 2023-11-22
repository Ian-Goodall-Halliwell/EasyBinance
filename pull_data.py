import csv
import os
import queue
import shutil
import time
from datetime import datetime, timedelta
from threading import Lock
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


def date_to_milliseconds(date_str: str) -> int:
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    d = dateparser.parse(date_str)
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)
    return int((d - epoch).total_seconds() * 1000.0)

def get_exchange_info(client) -> dict:
    return client.get_exchange_info()


def getstate(
    outlist: List[str], exchangeinfo: dict,client, mcap,tokennames: List[str] = ["BUSD"],
) -> List[str]:
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
    downloaded = os.listdir("data/raw")
    downloaded = [x.split(".")[0] for x in downloaded]
    currlist = list(set(currlist).difference(downloaded))
    print("number of tokens:", len(currlist))
    return currlist

def download1(
    start: str, end: str, interval: str, q, path: str, token,app=False
):

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

def startdownload_1m(start, end, dir,interval, app=False, clients=[],mcap=None,tokens=["BUSD"]):
    if not os.path.exists(dir):
        os.mkdir(dir)
    exchg = get_exchange_info(client=clients[0])
    if app == True:
        currlist = [
            popl.split(".")[0]
            for popl in os.listdir(
                "data/raw"
            )

        ]
    else:
        currlist = getstate([], exchg,client=clients[0],mcap=mcap,tokennames=tokens)
    fuln = (len(currlist) // len(clients))+1
    q = queue.SimpleQueue()
    for _ in range(fuln * 10000):
        for item in clients:
            q.put(item)

    cs = len(clients) // 4
    if not isinstance(start,dict):
        start = {x:start for x in currlist}
    out = Parallel(n_jobs=cs,backend="threading")(
            delayed(download1)(int(start[i]), int(end), interval, q, dir, i,app)
            for i in currlist
        )
    return out

def run1m(d, clilist,interval,mcap, tokens,starttime="2023-01-01 00:00:00",):
    csv_path = "data/raw"
    if os.path.exists(csv_path):
        shutil.rmtree(csv_path)
    os.mkdir(csv_path)
    dmin = d.strftime("%Y-%m-%d %H:%M:%S") 
    strt = date_to_milliseconds(starttime)
    dmin = date_to_milliseconds(dmin)
    out = startdownload_1m(start=strt, end=dmin, dir=csv_path, clients=clilist,interval=interval,mcap=mcap,tokens=tokens)
    time.sleep(30)
    for clin in clilist:
        clin.close_connection()

def append1m(d, clilist,csvs,interval):
    
    return out

def appendtoexististing(data):
    for df in data:
        if df is not None:
            
            symbol = df['symbol'].head(1).values[0]
            df.to_csv(f"data/raw/{symbol}.csv", mode='a', index=True, header=False)
            print(symbol)
        else:
            os.remove(f"data/raw/{symbol}.csv")
            continue

def sortinterval(input):
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
    csvs = {}
    for fl in os.listdir(f"{datapath}/raw/"):
        with open(os.path.join(f"{datapath}/raw/",fl), 'r') as f:
            q = deque(f, 1)  
        cv = pd.read_csv(StringIO(''.join(q)), header=None).values[0][0]
        if cv == endtime.strftime("%d %B, %Y, %H:%M:%S"):
            continue
        csvs[fl.split(".")[0]] = cv
    csv_path = "data/app"
    if os.path.exists("data/app"):
        shutil.rmtree("data/app")
        os.mkdir("data/app")
    else:
        os.mkdir("data/app")

    dmin = endtime + timedelta(minutes=60)
    timings = {x:date_to_milliseconds((pd.to_datetime(csvs[x]) + timedelta(minutes=1)).strftime("%d %B, %Y, %H:%M:%S")) for x in csvs}
    dmin = date_to_milliseconds(dmin.strftime("%d %B, %Y, %H:%M:%S"))
    out = startdownload_1m(
        start=timings,
        end=dmin,
        dir=csv_path,
        app=True,
        clients=clients,
        interval=interval
    )
    time.sleep(1)
    for clin in clients:
        clin.close_connection()
    return out
    
def download(clients=None,datapath="data",starttime="2023-01-01 00:00:00",endtime="now",freq="1d",mcap="5m",tokens=["BUSD"]):
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
    interval = sortinterval(freq)
    if endtime == "now":
        endtime = datetime.now(pytz.utc)
    if os.path.exists(f"{datapath}/raw/"):
        if len(os.listdir(f"{datapath}/raw/")) != 0:
            return append(datapath,endtime,interval,clients)
        else:
            run1m(endtime, clilist=clients,starttime=starttime,interval=interval,mcap=mcap,tokens=tokens)
    else:
    
        run1m(endtime, clilist=clients,starttime=starttime,interval=interval,mcap=mcap,tokens=tokens)
    print(f"data download and prep time elapsed: {datetime.now(pytz.utc) - endtime}")

if __name__ == "__main__":
    
    out = download(clients= "C:/Users/Ian/Desktop/clients.csv",datapath="data",starttime="2023-01-01 00:00:00",endtime="now",freq="5m", mcap="5000k",tokens=["BUSD"])
    
    
