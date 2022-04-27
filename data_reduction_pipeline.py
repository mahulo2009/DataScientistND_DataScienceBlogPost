import logging

from web_report_api import webreport_api
from datetime import timedelta, datetime, time
import pandas as pd
import re
import io
import math
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

        return math.ceil((date_end - date_ini).seconds * (1.0 / (period / 1000000.0)) / samples_per_page)

    @staticmethod
    def filters_samples(data_frame, date_ini, date_end, monitor_id, monitor_name,period,epsilon):

        file_name = "filtered/" +  \
                    str(monitor_id) + \
                    "_" + \
                    date_ini.strftime("%Y_%m_%d_%H_%M_%S") + \
                    "_" + date_end.strftime("%Y_%m_%d_%H_%M_%S") + \
                    ".gz"

        if exists(file_name):
            logging.info('File %s already exits.', file_name)

            return pd.read_csv(file_name)
        else:

            logging.info('File %s not exits, filtering', file_name)

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

            logging.info('Removing %s entries from samples', str(len(to_remove)))

            data_frame.to_csv(file_name,
                              index=False,
                              compression='infer')

            logging.info('File %s write to file.', file_name)

            return data_frame

    @staticmethod
    def clean_samples(data_frame):

        data_frame.drop('TimeStamp', axis=1, inplace=True)
        # df['TimeStampLong'] = pd.to_datetime(df['TimeStampLong'], unit='us')

        return data_frame

    @staticmethod
    def merge_all_samples(data_frames):

        logging.info('Merging all files...')

        data_frame = data_frames[0]
        if len(data_frames) >= 2:
            data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
            for idx in range(2, len(data_frames)):
                data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

        data_frame.sort_values(by=['TimeStampLong'], inplace=True)
        # todo this line other place
        data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

        logging.info('Merging all files completed!')

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

            logging.info('File %s already exits.', file_name)

            return pd.read_csv(file_name)
        else:

            logging.info('File %s not exits, querying the server', file_name)

            self.wr.set_monitor([monitor_id])
            self.wr.set_date_range(date_ini, date_end)
            self.wr.build()

            text_body = ""
            text_header = ""
            for p in range(0, self.number_pages(date_ini, date_end, period, 30000.0)):

                logging.info('File %s not exits, querying the server, page %s.', file_name, str(p))

                raw_text = self.wr.next()

                text_header, current_text_body = self.parse_text(raw_text)
                text_body = text_body + current_text_body

            data_frame = pd.read_csv(io.StringIO(text_header + "\n" + text_body), sep=",")
            data_frame.to_csv(file_name,
                              index=False,
                              compression='infer')

            logging.info('File %s write to file.',file_name)

            return data_frame

    def retrieve_all_samples(self, query_data):

        data_frames_merge = []
        for monitor in query_data['monitors']:

            date_ini = query_data['search']['date_ini']
            date_end = query_data['search']['date_end']

            logging.info('Processing monitor %s date ini %s date end %s', monitor['name'], date_ini, date_end)

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

            data_frame = self.filters_samples(data_frame,
                                              date_ini,
                                              date_end,
                                              monitor['id'] ,
                                              monitor['name'],
                                              monitor['period'],
                                              monitor['epsilon'])

            data_frames_merge.append(data_frame)

        for magnitude in query_data['magnitudes']:

            date_ini = query_data['search']['date_ini']
            date_end = query_data['search']['date_end']

            data_frames_magnitude = []
            for n in range(0, query_data['search']['days']):

                logging.info('Processing monitor %s date ini %s date end %s', magnitude['name'], date_ini, date_end)

                data_frame = self.retrieve_magnitude(magnitude['id'],
                                                    date_ini,
                                                    date_end)

                data_frames_magnitude.append(data_frame)

                date_ini = date_ini + timedelta(days=1)
                date_end = date_end + timedelta(days=1)

            data_frame = self.concatenate_all_samples(data_frames_magnitude)
            data_frame = self.clean_samples(data_frame)

            data_frames_merge.append(data_frame)

        data_frame = self.merge_all_samples(data_frames_merge)


        return data_frame

    def retrieve_magnitude(self, magnitude_id, date_ini, date_end):

        file_name = "raw/magnitude_" +  \
                    str(magnitude_id) + \
                    "_" + \
                    date_ini.strftime("%Y_%m_%d_%H_%M_%S") + \
                    "_" + date_end.strftime("%Y_%m_%d_%H_%M_%S") + \
                    ".gz"

        if exists(file_name):

            logging.info('File %s already exits.', file_name)

            return pd.read_csv(file_name)
        else:

            logging.info('File %s not exits, querying the server', file_name)

            self.wr.set_magnitud([magnitude_id])
            self.wr.set_date_range(date_ini, date_end)
            self.wr.build()

            text_body = ""
            text_header = ""

            raw_text = self.wr.query()

            text_header, current_text_body = self.parse_text(raw_text)
            text_body = text_body + current_text_body

            data_frame = pd.read_csv(io.StringIO(text_header + "\n" + text_body), sep=",")
            data_frame.to_csv(file_name,
                              index=False,
                              compression='infer')

            logging.info('File %s write to file.',file_name)

            return data_frame

    def process(self,data_frame):

        data_frame.fillna(method='ffill',inplace=True)
        data_frame = data_frame.dropna()

        return data_frame

if __name__ == "__main__":

    logging.basicConfig( level=logging.INFO)
    logging.info('Data Reduction Pipeline Start Up!')

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
                },
                {
                    'id': 12128,
                    'name': 'ECS.UpperShutter.actualPosition',
                    'period': 1000000,
                    'epsilon': 0.5
                },
                {
                    'id': 667,
                    'name': 'ECS.DomeRotation.actualPosition',
                    'period': 1000000,
                    'epsilon': 0.5
                },
                {
                    'id': 8264,
                    'name': 'EMCS.WeatherStation.meanWindSpeed',
                    'period': 1000000,
                    'epsilon': 1.0
                },
                {
                    'id': 8265,
                    'name': 'EMCS.WeatherStation.windDirection',
                    'period': 1000000,
                    'epsilon': 1.0
                },
                {
                    'id': 8116,
                    'name': 'OE.ObservingEngine.slowGuideErrorA',
                    'period': 1000000,
                    'epsilon': 0.01
                },
                {
                    'id': 8117,
                    'name': 'OE.ObservingEngine.slowGuideErrorB',
                    'period': 1000000,
                    'epsilon': 0.01
                }
            ],

        'magnitudes':
            [
                {
                    'id': 4238,
                    'name': 'OE.ObservingEngine.currentObservingState',
                }
            ]

    }

    pipeline = DataReductionPipeline("calp-vwebrepo", "8081")
    #data_frame = pipeline.retrieve_all_samples(query)
    #data_frame.to_csv("samples.gz", compression='infer')


    data_frame = pd.read_csv("samples.gz", compression='infer')
    data_frame = pipeline.process(data_frame)
    data_frame.to_csv("final_samples.gz", compression='infer')

