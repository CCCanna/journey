import datetime
from collections import Counter

from database import *


def get_zero_clock(date):
    """"获取对应日期零点的时间戳"""
    return datetime.datetime.strptime("{} 00:00:00".format(date), "%Y-%m-%d %H:%M:%S")


def get_week_index():
    """生成一个列表，返回每个星期一零点的时间戳"""
    MIN, MAX = log_data.unix_time.min(), log_data.unix_time.max()
    weeks = list()
    a_week = 3600 * 24 * 7
    current = get_first_weekday(MIN)
    while current < MAX:
        weeks.append(current)
        current = current + a_week
    # 补上最后一天
    weeks.append(MAX)
    return shrink(weeks)


def get_first_weekday(day):
    """获取下一个星期一的时间戳，今天是周一则返回今天"""
    current = datetime.datetime.fromtimestamp(day)
    if current.weekday():
        return get_zero_clock((current + datetime.timedelta(days=7 - current.weekday())).date()).timestamp()
    return get_zero_clock(current.date()).timestamp()


def shrink(original_list):
    """"消除列表中的重复元素"""
    repeat = set(original_list)
    return sorted(repeat)


def worker(user_id):
    """集合所需要的所有数据"""
    try:
        # 之所以会try是因为openid里面有个空值，不过这样写感觉并不好
        results = log_data.query("openid=='{}'".format(user_id))
    except SyntaxError:
        return
    _, ip, unix_time, something = transform_result_set(results)
    user_ip, *_ = shrink(ip)
    user = dict()
    user['identifier'] = user_id
    user['ip'] = user_ip
    # 把时间戳和对应的行为转换成字典再导出为json字符串
    user['activities'] = json.dumps(dict(zip(unix_time, something)))
    time_series = simplify_time(shrink(unix_time))
    dates = [datetime.datetime.fromtimestamp(x).date().isoformat() for x in time_series]
    data_counter = Counter(dates)
    user['time_json'] = json.dumps(data_counter)
    pool = [user.get(key) for key in ['identifier', 'ip', 'activities', 'time_json']]
    pool.extend(count_activities(data_counter, time_series))
    result_set.append(pool)


def transform_result_set(result_frame):
    """对结果集进行转置便于后续操作"""
    transformed = result_frame.values.T
    return [list(i) for i in transformed]


def simplify_time(time_series):
    """按照1小时的界限划分用户操作时间"""
    simplified = list()
    index = 0
    while True:
        simplified.append(time_series[index])
        index = search_for_nearest(time_series, time_series[index] + 3600)
        if not index:
            break
    return simplified


def search_for_nearest(series, value):
    """获取列表中大于界限的最小值"""
    distance = [serial - value for serial in series]
    for index, val in enumerate(distance):
        if val >= 0:
            return index
    return 0


def weekday_map(series):
    """生成一个对应每个星期的访问次数字典"""
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


def count_activities(counter, series):
    """统计DAU,WAU,MAU"""
    monthly = sum(counter.values())
    daily = list(counter.values()).pop(0)
    weekly = max(sorted(weekday_map(series).values()))
    return daily, weekly, monthly


def expand_openid():
    """获取用户openid散列"""
    return shrink(log_data.openid.values)


def main():
    # 用sqlaclhemy.create_engine来创建数据库引擎，然后手动建了一个database.py
    create_table_user_action()

    log_data = fetch_log_data()
    weeks_index = get_week_index()
    result_set = list()

    for openid in expand_openid():
        worker(openid)

    data_header = ['openid', 'ip', 'activities', 'time_json', 'dau', 'wau', 'mau']
    data_frame = pd.DataFrame(result_set, columns=data_header)
    # 存成csv。。。后面的数据库翻车了人家能怎么办，人家也很绝望啊
    data_frame.to_csv(os.path.join(data_dir, 'user_action.csv'), index=None)
    data_frame.to_sql('user_action', con=engine, chunksize=1000, if_exists='append', index=None)


if __name__ == '__main__':
    main()
