from datetime import datetime, time

import requests
import re
import math

from numpy import finfo

_DEFAULT_BASE_URL = "http://calp-vwebrepo:8081/WebReport/rest/webreport"
_DEFAULT_SAMPLES_PER_PAGE = 30000

_QUERY_MONITOR_TOKEN = "idmonitor"
_QUERY_MAGNITUDE_TOKEN = "idmagnitud"
_QUERY_PAGE_START_TOKEN = "iDisplayStart"
_QUERY_PAGE_LENGTH_TOKEN = "iDisplayLength"

query = {
    'search':
        {
            'date_ini': datetime(2021, 3, 1, 20, 0, 0),
            'date_end': datetime(2021, 3, 1, 20, 0, 30),
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


def _parse_raw_test(text):
    text = text.split('\n')

    text_header = ','.join(text[0].replace("/", ".").split(",")[1:])
    #todo extract this info to metadata monitor
    text_header = re.sub("\(.*\)", "", text_header)


    text_body = [','.join(line.split(",")[1:]) for line in text[1:]]
    text_body = '\n'.join(text_body)

    return text_header, text_body


class Cursor(object):

    def __init__(self, url_base, pages):

        self.url_base = url_base
        self._current = 0
        self._pages = pages

    def __iter__(self):
        return self

    def __next__(self):
        if self._current >= self._pages:
            raise StopIteration

        final_uri = self.url_base + "/download" + \
                    "&" + _QUERY_PAGE_START_TOKEN + "=" + str(self._current) + \
                    "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        self._current += 1

        r = requests.get(final_uri)
        if r.status_code == 200:
            header, body = _parse_raw_test(r.text)
        else:
            # todo check this
            raise requests.exceptions.HTTPError

        return header + '\n' + body


class Client(object):

    def __init__(self, base_url=_DEFAULT_BASE_URL):
        self.base_url = base_url

    def _parse_search(self, date_ini, date_end):

        search_uri_part = ""
        search_uri_part = search_uri_part + date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + date_end.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + "/0?"

        return search_uri_part

    def _parse_monitor(self, monitor):

        return _QUERY_MONITOR_TOKEN + "=" + str(monitor["id"])

    def _parse_magnitude(self, magnitude):

        return _QUERY_MAGNITUDE_TOKEN + "=" + str(magnitude["id"])

    def _parse_monitors(self, monitors):

        query_monitor_uri_part = ""

        for m in monitors:
            query_monitor_uri_part = query_monitor_uri_part + self._parse_monitor(m) + "&"

        query_monitor_uri_part = query_monitor_uri_part[:-1]

        return query_monitor_uri_part

    def _parse_magnitudes(self, magnitudes):

        query_magnitude_uri_part = ""

        for m in magnitudes:
            query_magnitude_uri_part = query_magnitude_uri_part + self._parse_magnitude(m) + "&"

        query_magnitude_uri_part = query_magnitude_uri_part[:-1]

        return query_magnitude_uri_part

    def _number_pages(self, date_ini, date_end, period):

        return math.ceil((date_end - date_ini).seconds *
                         (1.0 / (period / 1000000.0)) / _DEFAULT_SAMPLES_PER_PAGE)

    def execute(self, date_ini, date_end, query, index=None, index_type=None):

        if index is not None:
            if index_type is None:
                # todo raise error
                pass
            else:
                url_base = _DEFAULT_BASE_URL + \
                           self._parse_search(date_ini, date_end)

                if index_type == "monitors":
                    url_base = url_base  + self._parse_monitor(query[index_type][index])
                elif index_type == "magnitudes":
                    url_base = url_base  + self._parse_magnitude(query[index_type][index])

        else:
            url_base = _DEFAULT_BASE_URL + \
                       self._parse_search(date_ini, date_end) + \
                       self._parse_monitors(query['monitors']) + \
                       self._parse_magnitudes(query['magnitudes'])


        # todo the maximum period
        pages = self._number_pages(date_ini,
                                   date_end,
                                   query['monitors'][0]['period'])

        cursor = Cursor(url_base, pages)

        return cursor

    def get(self, component, monitor):

        final_uri = self.base_url + \
                    "/components/" + component + \
                    "/monitors/" + monitor

        print(final_uri)


if __name__ == "__main__":


    client = Client()
    client.get("MACS/AzimuthAxis","position")
    #cursor = client.execute(query['search']['date_ini'],
    #                        query['search']['date_end'],
    #                        query, index=0, index_type="monitors")
    #for r in cursor:
    #    print(r)

    # print(client._parse_search(query['search']))
    # print(client._parse_monitors(query['monitors']))
    # print(client._parse_magnitudes(query['magnitudes']))

    # client.execute(query,index=0,index_type="monitors")
