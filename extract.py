from web_report_api import webreport_api
from datetime import datetime, timedelta
import re
import matplotlib.pyplot as plt

import pandas as pd
import os
import gzip

def extract(monitors, monitors_array, magnitudes, date_ini, date_end, delta_time, delta_date, ndays, path):
    wr = webreport_api("calp-vwebrepo", "8081")

    for m in monitors:
        wr.add_monitor(m)

    for m in monitors_array:
        wr.add_monitor_array(m)

    for m in magnitudes:
        wr.add_magnitud(m)

    for n in range(0, ndays):

        d1 = date_ini
        d2 = date_end

        text_body = ""
        text_header = ""
        while d1 < d2:
            wr.add_date_range(d1, d1 + timedelta(hours=1))
            wr.build()

            result_splited = wr.query().split('\n')

            text_header = result_splited[0].replace("/", ".")

            current_text_body = [re.sub('-$', '', line) for line in result_splited[1:]]
            current_text_body = [re.sub('-[^(\d)]', ',', line) for line in current_text_body]

            text_body = text_body + '\n'.join(current_text_body)

            d1 = d1 + delta_time

        f = open(path + 'samples_from_' +
                 date_ini.strftime("%Y_%m_%d_%H_%M_%S") + "_to_" +
                 date_end.strftime("%Y_%m_%d_%H_%M_%S") + ".csv", 'w')

        f.write(text_header + "\n" + text_body)
        f.close()

        date_ini = date_ini + delta_date
        date_end = date_end + delta_date

        print("-----")


def test1():
    date_ini = datetime(2021, 3, 1, 20, 0, 0)
    date_end = datetime(2021, 3, 1, 21, 0, 0)
    wr = webreport_api("calp-vwebrepo", "8081")
    wr.add_monitor(3625)
    wr.add_date_range(date_ini, date_end)
    wr.build()

    text = wr.query().split('\n')
    text_header = text[0].replace("/", ".")

    text_body = [re.sub('-$', '', line) for line in text[1:]]
    text_body = [re.sub('-[^(\d)]', ',', line) for line in text_body]

    text = text_header + '\n' + '\n'.join(text_body)
    f = open('samples_from_' +
             date_ini.strftime("%Y_%m_%d_%H_%M_%S") + "_to_" +
             date_end.strftime("%Y_%m_%d_%H_%M_%S") + ".csv", 'w')
    f.write(text)
    f.close()


def test2():
    df = pd.read_csv('samples_from_2021_03_01_20_00_00_to_2021_03_01_21_00_00.csv')

    pivot = df['MACS.AzimuthAxis.followingError'][0]
    for idx, row in df.iterrows():
        if abs(pivot - row['MACS.AzimuthAxis.followingError']) < 0.00002:
            df.drop(idx, inplace=True)
            pivot = row['MACS.AzimuthAxis.followingError']

            # print(row['MACS.AzimuthAxis.followingError'] )

    df.plot(x='TimeStampLong', y='MACS.AzimuthAxis.followingError')
    plt.show()


def test3():
    df = pd.read_csv('samples_from_2021_03_01_20_00_00_to_2021_03_01_21_00_00.csv')

    pivot = df['MACS.AzimuthAxis.followingError'][0]
    for idx, row in df.iterrows():
        if abs(pivot - row['MACS.AzimuthAxis.followingError']) < 0.00002:
            df.drop(idx, inplace=True)
            pivot = row['MACS.AzimuthAxis.followingError']

            # print(row['MACS.AzimuthAxis.followingError'] )

    df.plot(x='TimeStampLong', y='MACS.AzimuthAxis.followingError')
    plt.show()


def test4():
    df = pd.read_csv('samples_from_2021_03_01_20_00_00_to_2021_03_02_07_00_00.csv')
    df.drop('TimeStamp', axis=1, inplace=True)
    df['TimeStampLong'] = pd.to_datetime(df['TimeStampLong'], unit='us')
    pivot = df['MACS.AzimuthAxis.followingError'][0]
    for idx, row in df.iterrows():
        if abs(pivot - row['MACS.AzimuthAxis.followingError']) < 0.00002:
            df.drop(idx, inplace=True)
            pivot = row['MACS.AzimuthAxis.followingError']

    df.to_csv('samples_from_2021_03_01_20_00_00_to_2021_03_02_07_00_00.csv')


def test5():
    date_ini = datetime(2021, 3, 1, 20, 0, 0)
    date_end = datetime(2021, 3, 2, 7, 0, 0)
    wr = webreport_api("calp-vwebrepo", "8081")
    wr.add_date_range(date_ini, date_end)
    wr.add_monitor(3625)
    wr.build()

    text_body = ""
    text_header = ""
    for p in range(0, round(11 * 60 * 60 * 5 / 30000)):
        raw_text = wr.next()
        raw_text = raw_text.split('\n')
        text_header = raw_text[0].replace("/", ".")
        current_text_body = [re.sub('-$', '', line) for line in raw_text[1:]]
        current_text_body = [re.sub('-[^(\d)]', ',', line) for line in current_text_body]

        text_body = text_body + '\n'.join(current_text_body)

    fname = 'samples_from_' + \
            date_ini.strftime("%Y_%m_%d_%H_%M_%S") + "_to_" + \
            date_end.strftime("%Y_%m_%d_%H_%M_%S")
    f = open(fname + ".csv", 'w')

    f.write(text_header + "\n" + text_body)
    f.close()


def test6():
    to_remove = []

    fname = "samples_from_2021_03_01_20_00_00_to_2021_03_02_07_00_00"
    df = pd.read_csv(fname + ".csv")
    df.drop('TimeStamp', axis=1, inplace=True)
    df['TimeStampLong'] = pd.to_datetime(df['TimeStampLong'], unit='us')
    pivot = df['MACS.AzimuthAxis.followingError'][0]
    for idx, row in df.iterrows():
        if abs(pivot - row['MACS.AzimuthAxis.followingError']) < 0.00002:
            pivot = row['MACS.AzimuthAxis.followingError']
            to_remove.append(idx)

    df.drop(to_remove, axis=0, inplace=True)
    df.to_csv(fname + "_reduced" + ".csv")


def test7():
    fname = "samples_from_2021_03_01_20_00_00_to_2021_03_02_07_00_00_reduced"
    df = pd.read_csv(fname + ".csv")
    df.plot(x='TimeStampLong', y='MACS.AzimuthAxis.followingError')
    plt.show()


def test10():
    days = 31
    date_ini = datetime(2021, 3, 1, 20, 0, 0)
    date_end = datetime(2021, 3, 2, 7, 0, 0)

    wr = webreport_api("calp-vwebrepo", "8081")
    wr.add_monitor(3625)

    for n in range(0, days):

        wr.add_date_range(date_ini, date_end)
        wr.build()

        text_body = ""
        text_header = ""
        for p in range(0, round(11 * 60 * 60 * 5 / 30000)):
            raw_text = wr.next()
            raw_text = raw_text.split('\n')
            text_header = raw_text[0].replace("/", ".")
            current_text_body = [re.sub('-$', '', line) for line in raw_text[1:]]
            current_text_body = [re.sub('-[^(\d)]', ',', line) for line in current_text_body]

            text_body = text_body + '\n'.join(current_text_body)

        path = "/home/mhuertas/Work/DataScienceND/Project_1/datascientistnd_blogpost/raw/" + \
               date_ini.strftime("%Y_%m_%d") + "/MACS/AzimuthAxis/followingError/"

        os.makedirs(path)

        fname = path + 'samples'

        with gzip.open(fname + '.gz', 'wb') as f:
            f.write((text_header + "\n" + text_body).encode())
            f.close()

        date_ini = date_ini + timedelta(days=1)
        date_end = date_end + timedelta(days=1)


def retrieve(monitor,time_rage,days,period):

    wr = webreport_api("calp-vwebrepo", "8081")
    wr.add_monitor(monitor)

    for n in range(0, days):

        wr.add_date_range(date_ini, date_end)
        wr.build()

        text_body = ""
        text_header = ""
        for p in range(0, round(11 * 60 * 60 * 5 / 30000)):
            raw_text = wr.next()
            raw_text = raw_text.split('\n')
            text_header = raw_text[0].replace("/", ".")
            current_text_body = [re.sub('-$', '', line) for line in raw_text[1:]]
            current_text_body = [re.sub('-[^(\d)]', ',', line) for line in current_text_body]

            text_body = text_body + '\n'.join(current_text_body)

        path = "/home/mhuertas/Work/DataScienceND/Project_1/datascientistnd_blogpost/raw/" + \
               date_ini.strftime("%Y_%m_%d") + "/MACS/AzimuthAxis/followingError/"

        os.makedirs(path)

        fname = path + 'samples'

        with gzip.open(fname + '.gz', 'wb') as f:
            f.write((text_header + "\n" + text_body).encode())
            f.close()

        date_ini = date_ini + timedelta(days=1)
        date_end = date_end + timedelta(days=1)

    pass


if __name__ == "__main__":
    # monitors = [3623,3625,3696,12128,8116,8117,8122,8264,8265]
    # monitors_array = [11119,11128]
    # magnitudes = [4238]

    # extract(monitors,
    #        monitors_array,
    #        magnitudes,
    #        datetime(2021,3,1,20,0,0),
    #        datetime(2021,3,2,7,0,0),
    #        timedelta(hours=1),
    #        timedelta(days=1),
    #        31,
    #        "/work/mhuertas/src_python/gtc/AL/TASK_6051/")
    test10()

# %7Bunit:ArcSecond%7D
