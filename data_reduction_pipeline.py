from web_report_api import webreport_api
from datetime import timedelta, datetime
import pandas as pd

import os
import gzip
import re


class DataReductionPipeline(object):

    def __init__(self, host, port):
        self.wr = webreport_api(host, port)

    def save(self, monitor, query, text_header, text_body):

        path = "/home/mhuertas/Work/DataScienceND/Project_1/datascientistnd_blogpost/raw/" + \
               query['date_ini'].strftime("%Y_%m_%d") + "/" +  monitor['name'] + "/"
        os.makedirs(path)
        f_name = path + 'samples'

        with gzip.open(f_name + '.gz', 'wb') as f:
            f.write((text_header + "\n" + text_body).encode())
            f.close()

    def parse_text(self, text):
        text = text.split('\n')
        text_header = text[0].replace("/", ".")
        text_body = [re.sub('-$', '', line) for line in text[1:]]
        text_body = [re.sub('-[^(\d)]', ',', line) for line in text_body]
        text_body = '\n'.join(text_body)

        return text_header, text_body

    def retrieve(self, monitor, query):

        date_ini = query['date_ini']
        date_end = query['date_end']

        self.wr.add_monitor(monitor['id'])

        for n in range(0, query['days']):
            self.wr.add_date_range(date_ini, date_end)
            self.wr.build()

            text_body = ""
            text_header = ""
            for p in range(0, round(11 * 60 * 60 * (monitor['period'] / 1000000) / 30000)):
                raw_text = self.wr.next()

                text_header, current_text_body = self.parse_text(raw_text)
                text_body = text_body + current_text_body

            self.save(monitor, query, text_header, text_body)

            date_ini = date_ini + timedelta(days=1)
            date_end = date_end + timedelta(days=1)


    def filter(self, f_name, monitor, epsilon):

        to_remove = []

        df = pd.read_csv(f_name + ".gz")
        df.drop('TimeStamp', axis=1, inplace=True)
        df['TimeStampLong'] = pd.to_datetime(df['TimeStampLong'], unit='us')
        pivot = df[monitor][0]
        for idx, row in df.iterrows():

            if idx==0: continue

            if abs(pivot - row[monitor]) < epsilon:
                to_remove.append(idx)
            else:
                pivot = row[monitor]

        df.drop(to_remove, axis=0, inplace=True)
        df.to_csv(f_name + "_reduced" + ".gz", compression="gzip", index=None)


if __name__ == "__main__":
    pipeline = DataReductionPipeline("calp-vwebrepo", "8081")

    monitor = {'id': 3623, 'name': 'MACS/AzimuthAxis/position', 'period': 5000000, 'epsilon': 0.00002}
    query = {'date_ini': datetime(2021, 3, 1, 20, 0, 0), 'date_end': datetime(2021, 3, 2, 7, 0, 0), 'days': 1}
    #pipeline.retrieve(monitor, query)

    pipeline.filter('./raw/2021_03_01/MACS/AzimuthAxis/position/samples',
                    'MACS.AzimuthAxis.position',
                    1)
