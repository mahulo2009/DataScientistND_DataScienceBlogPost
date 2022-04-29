import logging

from Client import Client

from datetime import timedelta, datetime, time
import pandas as pd
import io
import os
import re
from os.path import exists
import glob
import argparse
import json

class DataReductionPipeline(object):

    def __init__(self, query, name):
        self._client = Client()

        self._path = os.path.expanduser("~") + "/.cache/webreport/" + name + "/"
        self._query_data = query

    def make_file_name(self, date_ini, date_end, id,page=None):

        if not os.path.exists(self._path):
            os.makedirs(self._path)

        file_name_monitor = str(id) + \
                            "_" + \
                            date_ini.strftime("%Y_%m_%d_%H_%M_%S") + \
                            "_" + \
                            date_end.strftime("%Y_%m_%d_%H_%M_%S")

        if page is None:
            file_name = self._path + file_name_monitor + ".gz"
        else:
            file_name = self._path + file_name_monitor + "_raw_" + str(page) + ".gz"

        return file_name

    def filter(self,data_frame,monitor_name,epsilon):

        to_remove = []

        pivot = data_frame[monitor_name][0]
        for idx, row in data_frame.iterrows():
            if idx == 0:
                continue
            if abs(pivot - row[monitor_name]) < epsilon:
                to_remove.append(idx)
            else:
                pivot = row[monitor_name]

        data_frame.drop(to_remove, axis=0, inplace=True)

        return data_frame

    def merge_all_samples(self):

        logging.info('Merging all files...')

        logging.info(self._path + "*.gz")
        file_names = glob.glob(self._path + "*.gz")
        file_names = [fn for fn in file_names if not "raw" in fn]

        data_frames = [pd.read_csv(filename) for filename in file_names]

        data_frame = data_frames[0]
        if len(data_frames) >= 2:
            data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
            for idx in range(2, len(data_frames)):
                data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

        data_frame.sort_values(by=['TimeStampLong'], inplace=True)
        data_frame.to_csv(self._path + "merge_samples.gz", compression='infer',index=False)

        return data_frame

    def retrieve_all_samples(self):

        for idx, m in enumerate(self._query_data['monitors']):

            date_ini = datetime.strptime(self._query_data['search']['date_ini'] +
                                         " " +
                                         self._query_data['search']['time_ini'],
                                         '%Y-%m-%d %H:%M:%S')

            date_end = datetime.strptime(self._query_data['search']['date_end'] +
                                         " " +
                                         self._query_data['search']['time_end'],
                                         '%Y-%m-%d %H:%M:%S')

            file_name = self.make_file_name(date_ini,
                                            date_end,
                                            m['name'])

            logging.info('Retrieve all samples of monitor %s safe to file %s ...', m['name'], file_name)

            if not exists(file_name):

                data_frames_days = []
                for n in range(0, (date_end-date_ini).days+1):
                    logging.info('Retrieve samples of monitor %s day %s ...', m['name'], n)

                    cursor = self._client.execute(date_ini, date_end, query, index=idx, index_type="monitors")

                    data_frames_page = []
                    try:
                        for p, r in enumerate(cursor):
                            logging.info('Retrieve samples of monitor %s day %s page %s...', m['name'], n, p)

                            data_frame = pd.read_csv(io.StringIO(r), sep=",")

                            raw_file_name = self.make_file_name(date_ini, date_end, m['name'], p)
                            if not exists(raw_file_name):
                                data_frame.to_csv(raw_file_name, index=False, compression='infer')

                            data_frames_page.append(data_frame)

                    except:
                        print("An exception occurred")

                    data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)
                    data_frame = self.filter(data_frame, m["name"], m["epsilon"])

                    data_frames_days.append(data_frame)

                    date_ini = date_ini + timedelta(days=1)
                    date_end = date_end + timedelta(days=1)

                data_frame = pd.concat(data_frames_days, ignore_index=True, sort=False)
                data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

                data_frame.to_csv(file_name, index=False, compression='infer')

    def retrieve_magnitude(self):

        for idx, m in enumerate(self._query_data['magnitudes']):

            date_ini = datetime.strptime(self._query_data['search']['date_ini'] +
                                         " " +
                                         self._query_data['search']['time_ini'],
                                         '%Y-%m-%d %H:%M:%S')

            date_end = datetime.strptime(self._query_data['search']['date_end'] +
                                         " " +
                                         self._query_data['search']['time_end'],
                                         '%Y-%m-%d %H:%M:%S')

            file_name = self.make_file_name(date_ini,
                                            date_end,
                                            m['name'])

            if not exists(file_name):

                data_frames_days = []
                for n in range(0, (date_end-date_ini).days+1):

                    cursor = self._client.execute(date_ini, date_end, query, index=idx, index_type="magnitudes")
                    data_frames_page = []
                    try:
                        for r in cursor:
                            data_frame = pd.read_csv(io.StringIO(r), sep=",")
                            data_frames_page.append(data_frame)

                    except:
                        print("An exception occurred")

                    data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

                    data_frames_days.append(data_frame)

                    date_ini = date_ini + timedelta(days=1)
                    date_end = date_end + timedelta(days=1)

                data_frame = pd.concat(data_frames_days, ignore_index=True, sort=False)
                data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

                print(file_name)
                data_frame.to_csv(file_name, index=False, compression='infer')


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logging.info('Data Reduction Pipeline Start Up!')

    parser = argparse.ArgumentParser(description='Monitor Manager Web Report Api.')
    parser.add_argument('config', help='query configuration file')
    parser.add_argument('name', help='query session name')
    args = parser.parse_args()

    with open(args.config) as json_file:
        query = json.load(json_file)

    pipeline = DataReductionPipeline(query, args.name)
    pipeline.retrieve_all_samples()
    pipeline.retrieve_magnitude()
    pipeline.merge_all_samples()

