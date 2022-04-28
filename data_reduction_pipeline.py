import logging

from Client import Client

from datetime import timedelta, datetime, time
import pandas as pd
import io
from os.path import exists
import glob

query = {
    'search':
        {
            'date_ini': datetime(2021, 3, 1, 22, 0, 0),
            'date_end': datetime(2021, 3, 2, 6, 0, 0),
            'time_ini': time(20, 0, 0),
            'time_end': time(7, 0, 0),
            'days': 1
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
                'epsilon': 0.5
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
                'epsilon': 4.8e-07
            },
            {
                'id': 8117,
                'name': 'OE.ObservingEngine.slowGuideErrorB',
                'period': 1000000,
                'epsilon': 4.8e-07
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


class DataReductionPipeline(object):

    def __init__(self, path):
        self._client = Client()
        self._path = path


    def make_file_name(self, date_ini, date_end, id):

        logging.info('Check file with id = %s exits', id)

        file_name = "filtered/" + \
                    self._path + \
                    str(id) + \
                    "_" + \
                    date_ini.strftime("%Y_%m_%d_%H_%M_%S") + \
                    "_" + \
                    date_end.strftime("%Y_%m_%d_%H_%M_%S") + \
                    ".gz"

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

        logging.info("AAA %s","filtered/"+self._path + '*.gz')
        file_names = glob.glob("filtered/"+self._path + '*.gz')


        data_frames = [pd.read_csv(filename) for filename in file_names]

        data_frame = data_frames[0]
        if len(data_frames) >= 2:
            data_frame = pd.merge(data_frames[0], data_frames[1], how='outer')
            for idx in range(2, len(data_frames)):
                data_frame = pd.merge(data_frame, data_frames[idx], how='outer')

        data_frame.sort_values(by=['TimeStampLong'], inplace=True)

        return data_frame


    def retrieve_all_samples(self, query_data):

        for idx, m in enumerate(query_data['monitors']):

            date_ini = query_data['search']['date_ini']
            date_end = query_data['search']['date_end']

            file_name = self.make_file_name(date_ini,
                                            date_end + timedelta(days=query_data['search']['days']),
                                            m['id'])

            logging.info('retrieve_all_samples %s ...',file_name)

            if not exists(file_name):

                data_frames_days = []
                for n in range(0, query_data['search']['days']):

                    cursor = self._client.execute(date_ini, date_end, query, index=idx, index_type="monitors")
                    data_frames_page = []
                    for r in cursor:
                        data_frame = pd.read_csv(io.StringIO(r), sep=",")
                        data_frames_page.append(data_frame)

                    data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)
                    data_frame = self.filter(data_frame, m["name"], m["epsilon"])

                    data_frames_days.append(data_frame)

                    date_ini = date_ini + timedelta(days=1)
                    date_end = date_end + timedelta(days=1)

                data_frame = pd.concat(data_frames_days, ignore_index=True, sort=False)
                data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

                file_name = "filtered/" + \
                            str(m["id"]) + \
                            "_" + \
                            query_data['search']['date_ini'].strftime("%Y_%m_%d_%H_%M_%S") + \
                            "_" + \
                            date_end.strftime("%Y_%m_%d_%H_%M_%S") + \
                            ".gz"


                data_frame.to_csv(file_name, index=False, compression='infer')

    def retrieve_magnitude(self, query_data):

        for idx, m in enumerate(query_data['magnitudes']):

            date_ini = query_data['search']['date_ini']
            date_end = query_data['search']['date_end']

            file_name = self.make_file_name(date_ini,
                                            date_end + timedelta(days=query_data['search']['days']),
                                            m['id'])

            if not exists(file_name):

                data_frames_days = []
                for n in range(0, query_data['search']['days']):

                    cursor = self._client.execute(date_ini, date_end, query, index=idx, index_type="magnitudes")
                    data_frames_page = []
                    for r in cursor:
                        data_frame = pd.read_csv(io.StringIO(r), sep=",")
                        data_frames_page.append(data_frame)

                    data_frame = pd.concat(data_frames_page, ignore_index=True, sort=False)

                    data_frames_days.append(data_frame)

                    date_ini = date_ini + timedelta(days=1)
                    date_end = date_end + timedelta(days=1)

                data_frame = pd.concat(data_frames_days, ignore_index=True, sort=False)
                data_frame['TimeStampLong'] = pd.to_datetime(data_frame['TimeStampLong'], unit='us')

                data_frame.to_csv(file_name, index=False, compression='infer')


if __name__ == "__main__":

    logging.basicConfig( level=logging.INFO)
    logging.info('Data Reduction Pipeline Start Up!')

    pipeline = DataReductionPipeline('test_1/')
    pipeline.retrieve_all_samples(query)
    pipeline.retrieve_magnitude(query)
    data_frame = pipeline.merge_all_samples()
    data_frame.to_csv("sample_test.csv", index=False)


