from sqlalchemy import *
import os
import json
import time
import pandas as pd


def get_engine():
    db_config = get_global_config().get('db_config')
    uri = "mysql+pymysql://{user}:{password}@{hostname}/{database}".format(**db_config)
    return create_engine(uri)


def create_table_user_action():
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


def fetch_log_data():
    """获取数据库中对应表的数据"""
    required_columns = ['openid', 'ip', 'unix_time', 'something']
    frame = pd.read_sql_table('log', engine, columns=required_columns)
    return frame


def get_global_config():
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
