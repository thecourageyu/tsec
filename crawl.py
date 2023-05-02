# -*- coding: utf-8 -*-
# http://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=20170524&type=ALLBUT0999

import os
import re
import sys
import csv
import time
import string
import logging
import requests
import argparse
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from os import mkdir
from os.path import isdir

# +
class Crawler():
    def __init__(self, prefix="data"):
        ''' Make directory if not exist when initialize '''

        self.prefix = prefix
        for sub_dir in ["tse", "otc"]:
            os.makedirs(os.path.join(self.prefix, sub_dir), exist_ok=True)

    def _clean_row(self, row):
        ''' Clean comma and spaces '''
        for index, content in enumerate(row):
            row[index] = re.sub(",", "", content.strip())
        return row

    def _record(self, stock_id, row):
        ''' Save row to csv file '''
        f = open('{}/{}.csv'.format(self.prefix, stock_id), 'a')
        cw = csv.writer(f, lineterminator='\n')
        cw.writerow(row)
        f.close()

    def _get_tse_data(self, date_tuple):
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'

        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params)

        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
            return

        content = page.json()
        
        # For compatible with original data
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])

        
        
        for data in content['data9']:  # json data key
            print(type(data), len(data))
            sign = '-' if data[9].find('green') > 0 else ''
            row = self._clean_row([
                date_str_mingguo, # 日期
                data[2], # 成交股數
                data[4], # 成交金額
                data[5], # 開盤價
                data[6], # 最高價
                data[7], # 最低價
                data[8], # 收盤價
                sign + data[10], # 漲跌價差
                data[3], # 成交筆數
            ])

            self._record(data[0].strip(), row)
            
    def get_tse_data(self, date_tuple):
        
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX'

        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALL',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params)

        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
            return

        content = page.json()

        tse_data = pd.DataFrame(content["data9"], 
                                columns=["證券代號", 
                                                "證券名稱", 
                                                "成交股數",
                                                "成交筆數",
                                                "成交金額",
                                                "開盤價",
                                                "最高價",
                                                "最低價",
                                                "收盤價",
                                                "漲跌(+/-)",
                                                "漲跌價差",
                                                "最後揭示買價",
                                                "最後揭示買量",
                                                "最後揭示賣價",
                                                "最後揭示賣量",
                                                "本益比"])

        # 跌 => 漲跌價差補上負號
        diff_prices = tse_data["漲跌價差"].values
        for idx, up_down in enumerate(tse_data["漲跌(+/-)"].values):
            if re.search(".*color.green.*", up_down):
                diff_prices[idx] = "-" + diff_prices[idx]

        tse_data.drop(columns=["漲跌價差", "漲跌(+/-)"], inplace=True)
        tse_data["漲跌價差"] = diff_prices
        
        # string to float, (1) "," to ""; (2) "--" to -9999
        for col in tse_data.columns[2:]:
            try:
                tse_data[col] = [re.sub(",", "", s) for s in tse_data[col].values]
                tse_data[col] = [re.sub("--", "-9999", s) for s in tse_data[col].values]
                tse_data.astype({col: "float"})
            except Exception as e:
                print(col, e)
        
        return tse_data

    def _get_otc_data(self, date_tuple):
        """
        reference url
            https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote.php?l=zh-tw&d=110/01/09
        """
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
        print(url)
        page = requests.get(url)

        if not page.ok:
            logging.error("Can not get OTC data at {}".format(date_str))
            return

        result = page.json()
        
        if result['reportDate'] != date_str:
            logging.error("Get error date OTC data at {}".format(date_str))
            return

        for table in [result['mmData'], result['aaData']]:
            for tr in table:
                row = self._clean_row([
                    date_str,
                    tr[8], # 成交股數
                    tr[9], # 成交金額
                    tr[4], # 開盤價
                    tr[5], # 最高價
                    tr[6], # 最低價
                    tr[2], # 收盤價
                    tr[3], # 漲跌價差
                    tr[10] # 成交筆數
                ])
                self._record(tr[0], row)
                
    def get_otc_data(self, date_tuple):
        """
        reference url
            https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote.php?l=zh-tw&d=110/01/09
        """
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
#         print(url)
        page = requests.get(url)

        if not page.ok:
            logging.error("Can not get OTC data at {}".format(date_str))
            return

        result = page.json()
        
        if result['reportDate'] != date_str:
            logging.error("Get error date OTC data at {}".format(date_str))
            return
            
        cols = ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "均價", "成交股數", "成交金額(元)", "成交筆數", "最後買價",
                "最後買量(千股)", "最後賣價", "最後賣量(千股)", "發行股數", "次日參考價", "次日漲停價", "次日跌停價"]
        
        otc_data = pd.DataFrame(result['aaData'], columns=cols)

        for col in cols[2:]:
#             print(col,  otc_data[col])
            otc_data[col] = [re.sub(",", "", s) for s in otc_data[col].values]
            otc_data[col] = [re.sub("---", "-9999", s) for s in otc_data[col].values]
            otc_data[col] = [re.sub("除息", "-9001", s) for s in otc_data[col].values]
            otc_data.astype({col: "float"})
        
        return otc_data

#     def get_data(self, date_tuple):
#         print('Crawling {}'.format(date_tuple))
#         self._get_tse_data(date_tuple)
#         self._get_otc_data(date_tuple)
        
        
    def get_data(self, start_date, end_date, output_dir: str = None):
        start_date = datetime(start_date[0], start_date[1], start_date[2])
        end_date = datetime(end_date[0], end_date[1], end_date[2])
        current = start_date
        while current <= end_date:
            print(">>> {}".format(current))
            try:
                tse_data = self.get_tse_data(current.timetuple()[0:3])
                tse_data.to_csv(os.path.join(self.prefix, "tse", "tse_{}.csv".format(current.strftime("%Y%m%d"))), 
                                index=False, encoding="utf-8-sig")
            except Exception as e:
                print("  get tse data failed!\n  {}\n".format(e))
            try: 
                otc_data = self.get_otc_data(current.timetuple()[0:3])
                otc_data.to_csv(os.path.join(self.prefix, "otc", "otc_{}.csv".format(current.strftime("%Y%m%d"))), 
                                index=False, encoding="utf-8-sig")
            except Exception as e:
                print("  get otc data failed!\n {}\n".format(e))
            current += timedelta(days=1)
# -

datetime(2021, 1, 10)

current.timetuple()


def main():
    # Set logging
    if not os.path.isdir('log'):
        os.makedirs('log')
    logging.basicConfig(filename='log/crawl-error.log',
        level=logging.ERROR,
        format='%(asctime)s\t[%(levelname)s]\t%(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')

    # Get arguments
    parser = argparse.ArgumentParser(description='Crawl data at assigned day')
    parser.add_argument('day', type=int, nargs='*',
        help='assigned day (format: YYYY MM DD), default is today')
    parser.add_argument('-b', '--back', action='store_true',
        help='crawl back from assigned day until 2004/2/11')
    parser.add_argument('-c', '--check', action='store_true',
        help='crawl back 10 days for check data')

    args = parser.parse_args()

    # Day only accept 0 or 3 arguments
    if len(args.day) == 0:
        first_day = datetime.today()
    elif len(args.day) == 3:
        first_day = datetime(args.day[0], args.day[1], args.day[2])
    else:
        parser.error('Date should be assigned with (YYYY MM DD) or none')
        return

    crawler = Crawler()

    # If back flag is on, crawl till 2004/2/11, else crawl one day
    if args.back or args.check:
        # otc first day is 2007/04/20
        # tse first day is 2004/02/11

        last_day = datetime(2004, 2, 11) if args.back else first_day - timedelta(10)
        max_error = 5
        error_times = 0

        while error_times < max_error and first_day >= last_day:
            try:
                crawler.get_data((first_day.year, first_day.month, first_day.day))
                error_times = 0
            except:
                date_str = first_day.strftime('%Y/%m/%d')
                logging.error('Crawl raise error {}'.format(date_str))
                error_times += 1
                continue
            finally:
                first_day -= timedelta(1)
    else:
        crawler.get_data((first_day.year, first_day.month, first_day.day))

# !dir

if __name__ == '__main__':
#     main()

    datetime.today().day

    crawl = Crawler("data")
#     crawl.get_data((2023, 1, 9))
    content = crawl.get_data((2023, 4, 30), (2023, 5, 2), "d")
#     content = crawl.get_otc_data((2023, 4, 22))
#     content = crawl.get_tse_data((2023, 4, 25))
#     content = crawl.get_otc_data((2023, 4, 25))

    content

    cols = ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "均價", "成交股數", "成交金額(元)", "成交筆數", "最後買價",
            "最後買量(千股)", "最後賣價", "最後賣量(千股)", "發行股數", "次日參考價", "次日漲停價", "次日跌停價"]
    otc_data = pd.DataFrame(content['aaData'], columns=cols)
    

    for col in cols[2:]:
        print(col)
        otc_data[col] = [re.sub(",", "", s) for s in otc_data[col].values]
        otc_data[col] = [re.sub("---", "-9999", s) for s in otc_data[col].values]
        otc_data.astype({col: "float"})

    [i for i in content.iloc[0]]

    for k, v in content.items():
#         print(k, v[0:10], type(v))
        print(k, type(v))
        if isinstance(v, list):
            print(v[0:5])
        elif isinstance(v, dict):
            print(v.keys())




