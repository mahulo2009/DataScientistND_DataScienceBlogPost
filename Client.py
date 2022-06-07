from datetime import datetime, time

import logging
import requests
import re
import math

_DEFAULT_BASE_URL = "http://calp-vwebrepo:8081/WebReport/rest/webreport"
_DEFAULT_SAMPLES_PER_PAGE = 30000

_QUERY_MONITOR_TOKEN = "idmonitor"
_QUERY_MAGNITUDE_TOKEN = "idmagnitud"
_QUERY_PAGE_START_TOKEN = "iDisplayStart"
_QUERY_PAGE_LENGTH_TOKEN = "iDisplayLength"


def _parse_raw_test(text):
    text = text.split('\n')

    text_header = ','.join(text[0].replace("/", ".").split(",")[1:])
    # todo extract this info to metadata monitor
    text_header = re.sub(r"\(.*\)", "", text_header)

    text_body = [','.join(line.split(",")[1:]) for line in text[1:]]
    text_body = '\n'.join(text_body)

    return text_header, text_body


class Executor(object):

    def __init__(self, uri):
        self._uri = uri

    def run(self):
        r = requests.get(self._uri)
        if r.status_code == 200:
            header, body = _parse_raw_test(r.text)
        else:
            # todo check this
            raise requests.exceptions.HTTPError

        return header + '\n' + body


class Cursor(object):

    def __init__(self, url_base, pages, initial_page=0):

        self.url_base = url_base
        self._current = initial_page
        self._pages = pages

    def __iter__(self):
        return self

    def __next__(self):
        if self._current >= self._pages:
            raise StopIteration

        final_uri = _DEFAULT_BASE_URL + "/download" + self.url_base + \
                    "&" + _QUERY_PAGE_START_TOKEN + "=" + str(self._current) + \
                    "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        self._uri = final_uri
        logging.debug('Url %s ', final_uri)

        self._current += 1

        return Executor(final_uri)

    def run(self):
        response = requests.get(self._uri)
        if response.status_code == 200:
            header, body = _parse_raw_test(response.text)
        else:
            # todo check this
            raise requests.exceptions.HTTPError

        return self._current, header + '\n' + body


def _parse_search(query_date_ini, query_date_end):

    search_uri_part = ""
    search_uri_part = search_uri_part + query_date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
    search_uri_part = search_uri_part + query_date_end.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
    search_uri_part = search_uri_part + "/0?"

    return search_uri_part


def _parse_single_monitor(a_query):
    query_parsed = ""
    if a_query["type"] == "monitors":
        query_parsed = _QUERY_MONITOR_TOKEN
    elif a_query["type"] == "magnitudes":
        query_parsed = _QUERY_MAGNITUDE_TOKEN

    query_parsed = query_parsed + "=" + str(a_query["id"])

    return query_parsed


def _parse_monitors(a_query):
    query_monitor_uri = ""

    if isinstance(a_query, (list, tuple)):
        for q in a_query:
            query_monitor_uri = query_monitor_uri + _parse_single_monitor(q) + "&"
        query_monitor_uri = query_monitor_uri[:-1]
    else:
        query_monitor_uri = _parse_single_monitor(a_query)

    return query_monitor_uri


def _number_pages(query_date_ini, query_date_end, period):
    return math.ceil((query_date_end - query_date_ini).total_seconds() *
                     (1.0 / (period / 1000000.0)) / _DEFAULT_SAMPLES_PER_PAGE)


def execute(query_date_ini, query_date_end, a_query, initial_page=0):
    logging.debug('Execute query from %s to %s ', query_date_ini, query_date_end)

    url_base = _parse_search(query_date_ini, query_date_end)

    url_base = url_base + _parse_monitors(a_query)

    # todo how to calculate page when there are multiple monitors or when magnitude
    pages = _number_pages(query_date_ini,
                          query_date_end,
                          a_query["period"])

    logging.debug('Number of pages %s', pages)

    a_cursor = Cursor(url_base, pages, initial_page)

    return a_cursor


class Client(object):

    def __init__(self, base_url=_DEFAULT_BASE_URL):
        self.base_url = base_url

    def get(self, component, monitor):

        final_uri = self.base_url + \
                    "/components/" + component + \
                    "/monitors/" + monitor

        print(final_uri)

#todo move this to unit testing

query = {

    "search":
        {
            "date_ini": "2021-03-01",
            "date_end": "2021-03-02",
            "time_ini": "00:00:00",
            "time_end": "00:00:00"
        },

    "monitors":
        [
            {
                "id": 3623,
                "name": "MACS.AzimuthAxis.position",
                "period": 200000,
                "epsilon": 0.5,
                "type": "monitors"
            }
        ]
}

if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    date_ini = datetime.strptime(query['search']['date_ini'] +
                                 " " +
                                 query['search']['time_ini'],
                                 '%Y-%m-%d %H:%M:%S')

    date_end = datetime.strptime(query['search']['date_end'] +
                                 " " +
                                 query['search']['time_end'],
                                 '%Y-%m-%d %H:%M:%S')

    client = Client()
    cursor = execute(date_ini,
                            date_end,
                            query['monitors'][0])
    for r in cursor:
        print(r.run())

    # client.get("MACS/AzimuthAxis","position")

    # cursor = client.execute(query['search']['date_ini'],
    #                        query['search']['date_end'],
    #                        query, index=0, index_type="monitors")
    # for r in cursor:
    #    print(r)

    # print(client._parse_search(query['search']))
    # print(client._parse_monitors(query['monitors']))
    # print(client._parse_magnitudes(query['magnitudes']))

    # client.execute(query,index=0,index_type="monitors")
