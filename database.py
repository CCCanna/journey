from sqlalchemy import *
import os
import json
import time
import datetime
import pandas as pd


def get_engine():
    """"""
    db_config = get_global_config().get('db_config')
    uri = "mysql+pymysql://{user}:{password}@{hostname}/{database}".format(**db_config)
    return create_engine(uri)


def create_table_user_action():
    """创建存储第二部数据的表"""
    metadata = MetaData()
    user_action = Table('user_action', metadata,
                        Column("openid", String(40), nullable=False, primary_key=True),
                        Column("ip", String(15)),
                        Column("activities", JSON),
                        Column("time_json", JSON),
                        Column("dau", Integer),
                        Column("wau", Integer),
                        Column("mau", Integer)
                        )
    user_action.drop(engine, checkfirst=True)
    metadata.create_all(engine)


# CHARSET utf8mb4 COLLATE utf8mb4_general_ci;
def create_table_user_log():
    """创建存储第一部数据的表"""
    metadata = MetaData()
    user_log = Table('user_log', metadata,
                     Column('ip', String(15), nullable=False),
                     Column('week', Integer),
                     Column('date', Date),
                     Column('time', Time),
                     Column('http', Enum('POST', 'GET', 'HEAD')),
                     Column('http_code', Integer),
                     Column('unix_time', BIGINT),
                     Column('something', String(32)),
                     Column('location', String(64)),
                     Column('signature', String(48)),
                     Column('nonce', String(16)),
                     Column('openid', String(40))
                     )
    user_log.drop(engine, checkfirst=True)
    metadata.create_all(engine)


def query_for_result(sql_expression, legends=None):
    """按照SQL语句查询数据库"""
    result = connection.execute(sql_expression)
    data = result.fetchall()
    if not legends:
        return pd.DataFrame(data, columns=legends)
    return pd.DataFrame(data)


def transform_result_set(result_frame):
    """对结果集进行转置便于后续操作"""
    transformed = result_frame.values.T
    return [list(i) for i in transformed]


def fetch_log_data():
    """获取数据库中对应表的数据"""
    required_columns = ['openid', 'ip', 'unix_time', 'something']
    frame = pd.read_sql_table('user_log', engine, columns=required_columns)
    return frame


# ----------------报表相关代码如下----------------


def build_csv(file_name, column_name, sql_sentence):
    """生成报表数据集"""
    data = query_for_result(sql_sentence, column_name)
    export_data(data, file_name)


def export_data(data_frame, file_name):
    """存储到csv文件里面去"""
    return data_frame.to_csv(os.path.join(result_dir, file_name), index=None)


# 万万没有想到我还得写代码来填自己挖的坑，所以我当时是怎么想着把数据转成json存到数据库里面的 555~~~
# 啊啊啊啊啊啊啊这谁干的怎么是按日期来的我记得我不是这样写的啊
def merge_json(json_list):
    """设计缺陷的勉强补救"""
    dicts = [json.loads(json_data) for json_data in json_list]
    result = dicts.pop()
    key_list = result.keys()
    for a_dict in dicts:
        for key in key_list:
            result[key] = result.get(key) + a_dict.get(key, 0)
    return result


def inflate_data(single_data, keys):
    pass


def build_weeks_json(weeks, json_data):
    """weeks是时间戳列表，json_data是数据库里面的鬼畜的json"""
    date_list = [datetime.datetime.fromtimestamp(int(stamp)).date().isoformat() for stamp in weeks]
    keys = sorted(json_data.keys())
    week_day_map = dict()
    for date in date_list:
        week_day_map[date] = list()
        while keys:
            if keys[0] < date:
                week_day_map[date].append(keys.pop(0))
            else:
                break
    result = dict()
    while date_list:
        result[date] = sum([json_data.get(key, 0) for key in week_day_map[date]])
    return result


def convert_dict(dict_obj, file_name):
    """这个没有设置column_name参数，使用统一的列名字"""
    data = [(key, dict_obj.get(key, 0)) for key in dict_obj.keys()]
    frame = pd.DataFrame(data, columns=['日期', '数量'])
    export_data(frame, file_name)


# ----------------报表相关代码如上----------------

def expire():
    """关掉数据库连接"""
    connection.close()


def get_global_config():
    """获取全局配置"""
    with open('config.json') as f:
        config = json.loads(f.read())
    config['base_dir'] = os.getcwd()
    return config


global_config = get_global_config()
actions = global_config.get('projection')
data_dir = os.path.join(global_config.get('base_dir'), global_config.get('data_dir'))
log_dir = os.path.join(global_config.get('base_dir'), global_config.get('log_dir'))
result_dir = os.path.join(global_config.get('base_dir'), global_config.get('result_dir'))
engine = get_engine()
connection = engine.connect()
