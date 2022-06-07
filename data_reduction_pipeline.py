import logging

from Client import Client, execute

from datetime import timedelta, datetime
import pandas as pd
import io
import os
from os.path import exists
import glob
import argparse
import json


def remove_similar_consecutive_values(data_frame, monitor_name, epsilon):

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


class DataReductionPipeline(object):

    def __init__(self, a_query, name):
        self._client = Client()

        self._path = os.path.expanduser("~") + "/.cache/webreport/" + name + "/"
        self._query_data = a_query

        self._date_ini = datetime.strptime(self._query_data['search']['date_ini'], '%Y-%m-%d')
        self._date_end = datetime.strptime(self._query_data['search']['date_end'], '%Y-%m-%d')

        self._time_ini = datetime.strptime(self._query_data['search']['time_ini'], '%H:%M:%S')
        self._time_end = datetime.strptime(self._query_data['search']['time_end'], '%H:%M:%S')

    def merge_all_samples(self):

        logging.info('Merging all files...')

        search = self._path + "**/*.gz"
        file_names = glob.glob(search, recursive=True)
        file_names = [fn for fn in file_names if "raw" not in fn]

        data_frames = [pd.read_csv(filename) for filename in file_names]

        data_frame = data_frames[0]
        if len(data_frames) >= 2:
            data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
            for idx in range(2, len(data_frames)):
                data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

        data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')
        data_frame.sort_values(by=['TimeStampLong'], inplace=True)
        data_frame.to_csv(self._path + "merge_samples.gz", compression='infer', index=False)

        return data_frame

    def make_raw_file_name(self, date_ini, date_end, a_id, page=None):

        path = self._path + "/" + date_ini.strftime("%Y-%m-%d") + "/" + str(a_id).replace(".", "/") + "/raw/"
        if not os.path.exists(path):
            os.makedirs(path)

        file_name_monitor = str(a_id) + \
                            "." + \
                            date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            date_end.strftime("%Y-%m-%d_%H_%M_%S")
        if page is None:
            file_name_monitor = file_name_monitor + ".gz"
        else:
            file_name_monitor = file_name_monitor + ".raw." + str(page).rjust(4, '0') + ".gz"

        return path + "/" + file_name_monitor

    def make_filter_file_name(self, date_ini, date_end, a_id):
        path = self._path + "/" + date_ini.strftime("%Y-%m-%d") + "/" + str(a_id).replace(".", "/")

        if not os.path.exists(path):
            os.makedirs(path)

        file_name_monitor = str(a_id) + \
                            "." + \
                            date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            date_end.strftime("%Y-%m-%d_%H_%M_%S")
        file_name_monitor = file_name_monitor + ".gz"

        return path + "/" + file_name_monitor

    def func(self, date_ini, date_end, monitor):

        data_frames_page = []
        cursor = execute(date_ini, date_end, monitor)

        try:
            for p, r in enumerate(cursor):
                file_name = self.make_raw_file_name(date_ini, date_end, monitor['name'], p)
                if not exists(file_name):
                    logging.info('Retrieve samples of monitor page %s...', p)
                    data_frame = pd.read_csv(io.StringIO(r.run()), sep=",")
                    data_frame.to_csv(file_name, index=False, compression='infer')
                else:
                    data_frame = pd.read_csv(file_name)
                data_frames_page.append(data_frame)
        except:
            print("An exception occurred")

        data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

        return data_frame

    def retrieve_all_samples(self, func):

        date_range_ini = datetime.combine(self._date_ini, self._time_ini.time())
        # todo be carefull when the time range is in the same day, not plus 1 day.
        date_range_end = datetime.combine(self._date_ini, self._time_end.time()) + timedelta(days=1)

        while date_range_ini < self._date_end:

            for idx, m in enumerate(self._query_data['monitors']):
                logging.info('Retrieve all samples of monitor %s %s from %s to %s...',
                             idx, m['name'], date_range_ini, date_range_end)

                data_frame = func(date_range_ini, date_range_end, m)

                # todo filter for magnitude
                file_name = self.make_filter_file_name(date_range_ini, date_range_end, m['name'])
                if not exists(file_name):
                    if m["type"] == "monitors":
                        data_frame = remove_similar_consecutive_values(data_frame, m["name"], m["epsilon"])
                        data_frame.to_csv(file_name, index=False, compression='infer')
                    else:  # todo coping the same file for simple merge, review this.
                        data_frame.to_csv(file_name, index=False, compression='infer')

            date_range_ini = date_range_ini + timedelta(days=1)
            date_range_end = date_range_end + timedelta(days=1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Data Reduction Pipeline Start Up!')

    parser = argparse.ArgumentParser(description='Monitor Manager Web Report Api.')
    parser.add_argument('config', help='query configuration file')
    parser.add_argument('name', help='query session name')
    args = parser.parse_args()

    with open(args.config) as json_file:
        query = json.load(json_file)

    pipeline = DataReductionPipeline(query, args.name)
    pipeline.retrieve_all_samples(pipeline.func)
    pipeline.merge_all_samples()
