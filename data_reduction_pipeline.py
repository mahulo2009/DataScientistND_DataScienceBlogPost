from web_report_api import webreport_api
from datetime import timedelta, datetime, time
import matplotlib.pyplot as plt
import pandas as pd
import os
import gzip
import re
import io
from os.path import exists

class DataReductionPipeline(object):

    def __init__(self, host, port):
        self.wr = webreport_api(host, port)

    @staticmethod
    def parse_text(text):

        text = text.split('\n')
        text_header = text[0].replace("/", ".")
        text_body = [re.sub('-$', '', line) for line in text[1:]]
        text_body = [re.sub('-[^(\d)]', ',', line) for line in text_body]
        text_body = '\n'.join(text_body)

        return text_header, text_body

    @staticmethod
    def number_pages(date_ini, date_end, period, samples_per_page):

        return round((date_end - date_ini).seconds * (1.0 / (period / 1000000.0)) / samples_per_page)

    @staticmethod
    def filters_samples(data_frame, monitor, epsilon):
        to_remove = []

        pivot = data_frame[monitor][0]
        for idx, row in data_frame.iterrows():
            if idx == 0:
                continue
            if abs(pivot - row[monitor]) < epsilon:
                to_remove.append(idx)
            else:
                pivot = row[monitor]

        data_frame.drop(to_remove, axis=0, inplace=True)

        return data_frame

    @staticmethod
    def clean_samples(data_frame):

        data_frame.drop('TimeStamp', axis=1, inplace=True)
        # df['TimeStampLong'] = pd.to_datetime(df['TimeStampLong'], unit='us')

        return data_frame

    @staticmethod
    def merge_all_samples(data_frames):

        data_frame = data_frames[0]
        if len(data_frames) >= 2:
            data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
            for idx in range(2, len(data_frames)):
                data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

        data_frame.sort_values(by=['TimeStampLong'], inplace=True)
        # todo this line other place
        data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')
        return data_frame

    @staticmethod
    def concatenate_all_samples(data_frames_monitor):

        data_frame = data_frames_monitor[0]
        if len(data_frames_monitor) >= 2:
            data_frame = pd.concat([data_frames_monitor[0], data_frames_monitor[1]], ignore_index=True, sort=True)
            for idx in range(2, len(data_frames_monitor)):
                data_frame = pd.concat([data_frame, data_frames_monitor[idx]], ignore_index=True, sort=True)

        return data_frame

    def retrieve_samples(self, monitor_id, period, date_ini, date_end):

        file_name = "raw/" +  \
                    str(monitor_id) + \
                    "_" + \
                    date_ini.strftime("%Y_%m_%d_%H_%M_%S") + \
                    "_" + date_end.strftime("%Y_%m_%d_%H_%M_%S") + \
                    ".gz"

        if exists(file_name):
            return pd.read_csv(file_name)
        else:

            self.wr.set_monitor([monitor_id])
            self.wr.set_date_range(date_ini, date_end)
            self.wr.build()

            text_body = ""
            text_header = ""
            for p in range(0, self.number_pages(date_ini, date_end, period, 30000.0)):
                raw_text = self.wr.next()

                text_header, current_text_body = self.parse_text(raw_text)
                text_body = text_body + current_text_body

            data_frame = pd.read_csv(io.StringIO(text_header + "\n" + text_body), sep=",")
            data_frame.to_csv(file_name,
                              index=False,
                              compression='infer')

            return data_frame

    def retrieve_all_samples(self, query_data):

        data_frames_merge = []
        for monitor in query_data['monitors']:

            date_ini = query_data['search']['date_ini']
            date_end = query_data['search']['date_end']

            data_frames_monitor = []
            for n in range(0, query_data['search']['days']):

                data_frame = self.retrieve_samples(monitor['id'],
                                                    monitor['period'],
                                                    date_ini,
                                                    date_end)

                data_frames_monitor.append(data_frame)

                date_ini = date_ini + timedelta(days=1)
                date_end = date_end + timedelta(days=1)

            data_frame = self.concatenate_all_samples(data_frames_monitor)
            data_frame = self.clean_samples(data_frame)
            data_frame = self.filters_samples(data_frame, monitor['name'], monitor['epsilon'])
            data_frames_merge.append(data_frame)

        data_frame = self.merge_all_samples(data_frames_merge)

        return data_frame


if __name__ == "__main__":
    query = {
        'search':
            {
                'date_ini': datetime(2021, 3, 1, 20, 0, 0),
                'date_end': datetime(2021, 3, 2, 7, 0, 0),
                'time_ini': time(20, 0, 0),
                'time_end': time(7, 0, 0),
                'days': 31
            },
        'monitors':
            [
                {
                    'id': 3623,
                    'name': 'MACS.AzimuthAxis.position',
                    'period': 200000,
                    'epsilon': 0.5
                },
                {
                    'id': 3625,
                    'name': 'MACS.AzimuthAxis.followingError',
                    'period': 200000,
                    'epsilon': 0.00002
                },
                {
                    'id': 3696,
                    'name': 'MACS.ElevationAxis.position',
                    'period': 200000,
                    'epsilon': 0.5
                }
            ]
    }

    pipeline = DataReductionPipeline("calp-vwebrepo", "8081")
    df = pipeline.retrieve_all_samples(query)
    df.to_csv("samples.gz", compression='infer')

    # {
    #     'id': 12128,
    #     'name': 'MACS/AzimuthAxis/position',
    #     'period': 5000000,
    #     'epsilon': 0.5
    # },
    # {
    #     'id': 8116,
    #     'name': 'MACS/AzimuthAxis/position',
    #     'period': 5000000,
    #     'epsilon': 0.5
    # },
    # {
    #     'id': 8117,
    #     'name': 'MACS/AzimuthAxis/position',
    #     'period': 5000000,
    #     'epsilon': 0.5
    # },
    # {
    #     'id': 8122,
    #     'name': 'MACS/AzimuthAxis/position',
    #     'period': 5000000,
    #     'epsilon': 0.5
    # },
    # {
    #     'id': 8264,
    #     'name': 'MACS/AzimuthAxis/position',
    #     'period': 5000000,
    #     'epsilon': 0.5
    # },
    # {
    #     'id': 8265,
    #     'name': 'MACS/AzimuthAxis/position',
    #     'period': 5000000,
    #     'epsilon': 0.5
    # }

    # pipeline.retrieve(query)

    # df = pipeline.retrieve_samples(3623,
    #                  200000,
    #                  datetime(2021, 3, 1, 20, 0, 0),
    #                  datetime(2021, 3, 1, 21, 0, 0))

    # df = pipeline.clean_samples(df)
    # df = pipeline.filters_samples(df, 'MACS.AzimuthAxis.position', 0.5)

    # print(df)

    # monitor = {'id': 3623, 'name': 'MACS/AzimuthAxis/position', 'period': 5000000, 'epsilon': 0.00002}
    # query = {'date_ini': datetime(2021, 3, 1, 20, 0, 0), 'date_end': datetime(2021, 3, 2, 7, 0, 0), 'days': 1}
    # pipeline.retrieve(monitor, query)

    # pipeline.filter('./raw/2021_03_01/MACS/AzimuthAxis/position/samples',
    #                'MACS.AzimuthAxis.position',
    #                1)

    # path = "/home/mhuertas/Work/DataScienceND/Project_1/datascientistnd_blogpost/raw/2021_03_01/MACS/AzimuthAxis/"
    # df_position = pd.read_csv(path+"position/samples_reduced.gz")
    # print(df_position.head())
    # df_followingError = pd.read_csv(path+"followingError/samples_reduced.gz")
    # print(df_followingError.head())

    # df = df_position.merge(df_followingError,how='outer', on='TimeStampLong')
    # print(df.tail())

    # def retrieve(self, query):
    #
    #     date_ini = query['search']['date_ini']
    #     date_end = query['search']['date_end']
    #
    #     self.wr.add_monitor(query['monitors'][0]['id'])
    #
    #     for n in range(0, query['search']['days']):
    #         self.wr.add_date_range(date_ini, date_end)
    #         self.wr.build()
    #
    #         text_body = ""
    #         text_header = ""
    #         for p in range(0, round(11 * 60 * 60 * (query['monitors'][0]['period'] / 1000000) / 30000)):
    #             raw_text = self.wr.next()
    #
    #             text_header, current_text_body = self.parse_text(raw_text)
    #             text_body = text_body + current_text_body
    #
    #         # self.save(monitor, query, text_header, text_body)
    #
    #         date_ini = date_ini + timedelta(days=1)
    #         date_end = date_end + timedelta(days=1)
    #
    # def filter(self, f_name, monitor, epsilon):
    #
    #     to_remove = []
    #
    #     df = pd.read_csv(f_name + ".gz")
    #     df.drop('TimeStamp', axis=1, inplace=True)
    #     df['TimeStampLong'] = pd.to_datetime(df['TimeStampLong'], unit='us')
    #     pivot = df[monitor][0]
    #     for idx, row in df.iterrows():
    #
    #         if idx == 0: continue
    #
    #         if abs(pivot - row[monitor]) < epsilon:
    #             to_remove.append(idx)
    #         else:
    #             pivot = row[monitor]
    #
    #     df.drop(to_remove, axis=0, inplace=True)
    #     df.to_csv(f_name + "_reduced" + ".gz", compression="gzip", index=None)
    #
    # def save(self, monitor, query, text_header, text_body):
    #
    #     path = "/home/mhuertas/Work/DataScienceND/Project_1/datascientistnd_blogpost/raw/" + \
    #            query['date_ini'].strftime("%Y_%m_%d") + "/" + monitor['name'] + "/"
    #     os.makedirs(path)
    #     f_name = path + 'samples'
    #
    #     with gzip.open(f_name + '.gz', 'wb') as f:
    #         f.write((text_header + "\n" + text_body).encode())
    #         f.close()
