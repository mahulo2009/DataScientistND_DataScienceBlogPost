from datetime import datetime

import requests

class webreport_api(object):

    def __init__(self,host,port):
        self.uri_base = "http://"+host+":"+port+"/WebReport/rest/webreport/download"
        self.uri = None
        self.date_ini = None
        self.date_fin = None
        self.monitors = []
        self.monitors_array = []
        self.magnitudes = []
        self.page = 0

    def set_monitor(self,monitor_ids):
        self.monitors = monitor_ids

    def set_date_range(self,date_ini,date_fin):
        self.date_ini=date_ini
        self.date_fin=date_fin


    def add_date_range(self,date_ini,date_fin):
        self.date_ini=date_ini
        self.date_fin=date_fin


    def add_monitor(self,monitor_id):
        self.monitors.append(monitor_id)

    def add_monitor_array(self,monitor_id):
        self.monitors_array.append(monitor_id)

    def add_magnitud(self,magnitud_id):
        self.magnitudes.append(magnitud_id)

    def build(self):
        self.page = 0
        self.uri = self.uri_base 
        self.uri = self.uri + self.date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        self.uri = self.uri + self.date_fin.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        self.uri = self.uri + "/0?"

        if self.monitors:
            for id in self.monitors:
                self.uri = self.uri  + "idmonitor=" + str( id ) + "&"

        if self.magnitudes:
            for id in self.magnitudes:
                self.uri = self.uri  + "idmagnitud=" + str( id ) + "&"

        if self.monitors_array:
            for id in self.monitors_array:
                self.uri = self.uri  + "idmonitor=" + str( id ) + "%5B%5B-1%5D%5D&"

        if self.monitors or self.magnitudes:                        
            self.uri = self.uri[:-1]

        print(self.uri)

    def query(self):
        x = requests.get(self.uri)
        return x.text

    def next(self):
        final_uri = self.uri + "&iDisplayStart=" + str(self.page * 30000) + "&iDisplayLength=" + str(30000)
        self.page = self.page + 1

        print (final_uri)
        x = requests.get(final_uri)

        return x.text

