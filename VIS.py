import pandas as pd
from zipfile import ZipFile
import datetime as dt
import requests
from io import BytesIO
import time
import math

pd.options.display.max_columns = 9999
pd.options.display.max_rows = 9999

today = dt.datetime.today()  # + dt.timedelta(-1)
daystring = today.strftime("%Y%m%d")
day = today.strftime("%d")
month = today.strftime("%m")
year = today.year
# https://borsaistanbul.com/data/thb/2021/08/thb202108091.zip
# https://borsaistanbul.com/data/vadeli/viop_20210809.csv
spot_url = f"https://borsaistanbul.com/data/thb/{year}/{month}/thb{daystring}1.zip"
fut_url = f"https://borsaistanbul.com/data/vadeli/viop_{daystring}.csv"

with requests.Session() as s:
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/50.0.2661.102 Safari/537.36'}
    cookie = {}
    spot_file = s.get(spot_url, headers=headers).content
    future_file = s.get(fut_url, headers=headers).content


def unzip(zipped):
    with ZipFile(BytesIO(zipped), 'r') as myzip:
        # Get a list of all archived file names from the zip
        listOfFileNames = myzip.namelist()
        # Iterate over the file names
        for fileName in listOfFileNames:
            # Check filename endswith csv
            if fileName.endswith('.csv'):
                return myzip.open(fileName)
                # x = myzip.open(fileName)
                # Extract a single file from zip
                # myzip.extract(fileName,"zip_folder")


df_spot_raw = pd.read_csv(unzip(spot_file), sep=";", skiprows=1)

df_fut_raw = pd.read_csv(BytesIO(future_file), sep=";", skiprows=1)

df_spot1 = pd.read_csv(unzip(spot_file), sep=";", skiprows=1)[["TRADE DATE", "INSTRUMENT SERIES CODE",
                                                               "INSTRUMENT NAME", "CLOSING PRICE"]]

df_fut1 = pd.read_csv(fut_url, sep=";", skiprows=1)[["TRADE DATE", "INSTRUMENT SERIES", "INSTRUMENT NAME",
                                                     "UNDERLYING", "CLOSING PRICE", "EXPIRATION DATE"]]

exp_dates = df_fut1.iloc[:, -1].unique()
exp_dates.sort()
ttm = []
rf = 0.19

t = 0
for i in exp_dates:
    filtered = df_fut1[(df_fut1["EXPIRATION DATE"] == i) & df_fut1["INSTRUMENT NAME"].str.contains("VIS")]
    merged = pd.merge(df_spot1, filtered, "inner", left_on="INSTRUMENT SERIES CODE", right_on="UNDERLYING")
    if not merged.empty:
        df_spot1 = pd.merge(df_spot1, filtered, "inner", left_on="INSTRUMENT SERIES CODE", right_on="UNDERLYING",
                            suffixes=(f"_t{t}", f"_t{t + 1}"))
        ttm.append((dt.datetime.strptime(i, "%Y-%m-%d") - dt.datetime.today()).days)
        t += 1

df_spot2 = df_spot1[[*df_spot1.columns[:4], "CLOSING PRICE_t1"]].copy()

ttm.sort()
for i in range(len(ttm)):
    df_spot2[f"F_{i + 1}_theo"] = df_spot2[f"CLOSING PRICE_t0"] * math.exp(1) ** (rf * ttm[i] / 365)
    df_spot2[f"F_{i + 1}_pct"] = df_spot2[f"CLOSING PRICE_t{i + 1}"] / df_spot2[f"F_{i + 1}_theo"] - 1
    df_spot2[f"Fut{i + 1}vsSpot_pct"] = df_spot2[f"CLOSING PRICE_t{i + 1}"] / df_spot2[f"CLOSING PRICE_t0"] - 1
    if i + 2 <= len(ttm):
        df_spot2[f"CLOSING PRICE_t{i + 2}"] = df_spot1[f"CLOSING PRICE_t{i + 2}"]

df_spot2.to_excel("Arb.xlsx")
