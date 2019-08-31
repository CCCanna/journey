import datetime
import re
import urllib.parse

from database import *


def get_location(location):
    if "/admin/" in location:
        return "管理员"
    return actions.get(location, "未知")


def get_date(string):
    """This method will parse the time string into week,date,time format. 
    Example:[Mon Jun 17 19:38:39 2019] will be parsed into 3,2019-6-17,19:38:39, and all the variables are strings."""
    string = string[1:-1]
    time_array = time.strptime(string)
    timestamp = time.mktime(time_array)
    obj = datetime.datetime.fromtimestamp(timestamp)
    return (
        obj.weekday() + 1,
        datetime.date.isoformat(obj.date()),
        datetime.time.isoformat(obj.time()),
        timestamp,
    )


def split_query(query):
    legends = ["msg_signature", "signature", "openid", "nonce"]
    query = query.replace("zq_", "")
    slices = query.split("&")
    try:
        query_dict = dict([piece.split("=") for piece in slices])
    except ValueError:
        return [""] * len(legends)
    result_set = list()
    for legend in legends:
        try:
            result_set.append(query_dict.pop(legend))
        except KeyError:
            result_set.append("")
    return result_set


def parse_url(string):
    """This method parse url into variables in legends."""
    container = list()
    parsed = urllib.parse.urlparse(string)
    container.extend(split_query(parsed.query))
    container.append(parsed.path)
    return container


def match(regex, string):
    """Please ensure the number of tokens should keep up with variables that receive result from this method."""
    pattern = re.compile(regex)
    if pattern.findall(string):
        result, *_ = pattern.findall(string)
    else:
        return [""]
    return result


def parse(string):
    string = re.sub(r"[ ]{2,}", " ", string)
    error_result = ["", False]
    if len(string) < 128:
        return error_result
    ip = match(r"\d+\.\d+\.\d+\.\d+", string)
    time_collection = match(r"\[\w{3} \w{3}.*201\d\]", string)
    week, date, time, unix_stamp = get_date(time_collection)
    method, url = match(r"(POST|GET|HEAD) (.*) =>", string)
    http_status_code = match(r"\(HTTP/1.\d (\d+)\)", string)
    _, signature, openid, nonce, location = parse_url(url)
    results = [
        ip,
        week,
        date,
        time,
        unix_stamp,
        method,
        http_status_code,
        get_location(location),
        location,
        signature,
        nonce,
        openid,
    ]
    return results, True


def main():
    create_table_user_log()
    error = open(os.path.join(data_dir, 'unmatched.txt'), "w+")
    values = list()

    for filename in os.listdir(log_dir):
        handle = open(os.path.join(log_dir, filename))
        buffer = handle.readlines()
        for line in buffer:
            item, flag = parse(line)
            if flag:
                values.append(item)
            else:
                error.write("{}\n".format(line))
        handle.close()
    error.close()

    data_header = ['ip', 'week', 'date', 'time', 'unix_time', 'http', 'http_code', 'something', 'location', 'signature',
                   'nonce', 'openid']
    frame = pd.DataFrame(values, columns=data_header)
    frame.to_csv(os.path.join(data_dir, 'user_log.csv'), index=None)
    frame.to_sql('user_log', engine, if_exists='append', index=None, chunksize=5000)


if __name__ == '__main__':
    main()
