import datetime
import json
import logging
import sys
from collections import Counter

import pandas as pd
import pymysql as db
from sqlalchemy import create_engine


def shrink(original_list):
    disorder = set(original_list)
    return sorted(list(disorder))


def get_zero_clock(date):
    return datetime.datetime.strptime("{} 00:00:00".format(date), "%Y-%m-%d %H:%M:%S")


def get_week_index():
    MIN, MAX = [int(x) for x in get_time_boundary()]
    weeks = list()
    a_week = 3600 * 24 * 7
    current = get_first_weekday(MIN)
    while current < MAX:
        weeks.append(current)
        current = current + a_week
    weeks.append(MAX)
    return weeks


def get_first_weekday(day):
    current = datetime.datetime.fromtimestamp(day)
    if current.weekday():
        return get_zero_clock((current + datetime.timedelta(days=7 - current.weekday())).date()).timestamp()
    return get_zero_clock(current.date()).timestamp()


def expand_openid():
    cursor.execute("select distinct openid from log;")
    return [ext[0] for ext in cursor.fetchall()]


def worker_process():
    user_id = user_id_pool.pop()
    log = logging.getLogger()
    cursor.execute("select ip,unix_time,something from log where openid = '%s'" % user_id)
    ip, unix_time, something = transform_result_set(cursor.fetchall())
    user_ip, *_ = shrink(ip)
    unix_time = list(int(x) for x in unix_time)
    activities = dict(zip(unix_time, something))
    user = dict()
    user['identifier'] = user_id
    user['ip'] = user_ip
    user['actions'] = json.dumps(activities)
    user_times = simplify_time(shrink(activities.keys()))
    data_counter = construct_counter(user_times)
    user['time_json'] = json.dumps(data_counter)
    user['active_days'] = len(list(data_counter.keys()))
    pool = [user.get(key) for key in ['identifier', 'ip', 'actions', 'time_json', 'active_days']]
    pool.extend(get_active_users(user_times))
    result_set.append(pool)
    log.info('{} finished({}).'.format(user_id, '/'.join((str(len(user_id_pool)), number_of_all))))


def transform_result_set(data_set):
    frame = pd.DataFrame(data_set)
    transformed = frame.values.T
    return [list(i) for i in transformed]


def get_time_boundary():
    # Ugly code as below.
    bound = list()
    cursor.execute('select min(unix_time) from log;')
    bound.append(cursor.fetchall()[0][0])
    cursor.execute('select max(unix_time) from log;')
    bound.append(cursor.fetchall()[0][0])
    return bound


def simplify_time(time_series):
    simplified = list()
    index = 0
    while True:
        simplified.append(time_series[index])
        index = search_for_nearest(time_series, time_series[index] + 3600)
        if not index:
            break
    return simplified


def construct_weekday(series):
    result = dict()
    for index, limit in enumerate(weeks_index):
        count = 0
        while series:
            if series[0] <= limit:
                count += 1
                series.pop(0)
            else:
                break
        result[index] = count
    return result


def search_for_nearest(series, value):
    distance = [serial - value for serial in series]
    for index, val in enumerate(distance):
        if val >= 0:
            return index
    return 0


def construct_counter(series):
    dates = [datetime.datetime.fromtimestamp(x).date().isoformat() for x in series]
    return Counter(dates)


def get_active_users(series):
    counter = construct_counter(series)
    monthly = len(series)
    daily = list(counter.values()).pop(0)
    weekly = max(sorted(construct_weekday(series).values()))
    return daily, weekly, monthly


handle = db.connect('localhost', 'root', 'demo', 'zq_log', charset='utf8mb4')
cursor = handle.cursor()

weeks_index = get_week_index()

user_id_pool = expand_openid()
number_of_all = str(len(user_id_pool))
result_set = list()

logging.basicConfig(
    level=logging.INFO,
    format='PID %(process)s %(name)s: %(message)s',
    stream=sys.stderr
)

while user_id_pool:
    worker_process()

header = ['user_id', 'ip', 'actions', 'tad', 'active_map', 'dau', 'wau', 'mau']
data_frame = pd.DataFrame(result_set, columns=header)

sql_engine = create_engine('mysql+pymysql://root:demo@localhost/zq_log', )
data_frame.to_sql('actions', con=sql_engine, chunksize=1000, if_exists='replace', index=None)
