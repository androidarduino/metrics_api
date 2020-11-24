from datetime import datetime, timedelta
import json
import time
from dateutil.parser import parse
from gcs import GCSBucket

gcs = GCSBucket("gcf_vat_data")

header = [
    "Email",
    "Timestamp",
    "Kelvin",
    "R",
    "G",
    "B",
    "IR",
    "Movement",
    "Lux"
]


# Call like generate_thumbnail('9a8e6fb7-7ffb-49a6-8486-8ca243b33864', '5m', '1h')
def generate_thumbnail(prefix, target):
    epoch = int(time.time())
    t = time.localtime(epoch)
    # Defaults to '1h':
    current_clock = datetime(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour)
    last_clock = current_clock - timedelta(hours=1)
    base, factor = '5m', 12

    if target == '1d':
        current_clock = datetime(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour)
        last_clock = current_clock - timedelta(days=1)
        base, factor = '1h', 24

    if target == '10d':
        current_clock = datetime(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour)
        last_clock = current_clock - timedelta(days=10)
        base, factor = '1d', 10

    if target == '30d':
        current_clock = datetime(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour)
        last_clock = current_clock - timedelta(days=30)
        base, factor = '10d', 3

    dates = [current_clock.strftime("%Y%m%d")]
    if t.tm_hour == 0:
        dates.append(last_clock.strftime('%Y%m%d'))

    if base == '5m' or base == '1h':
        files = get_file_list(prefix, dates, base, int(last_clock.timestamp()), int(current_clock.timestamp()))
    else:
        files = get_thumbnail_list(prefix, base, int(last_clock.timestamp()), int(current_clock.timestamp()))
    all_data = {}
    for f in files:
        data = gcs.get(f)
        json_data = json.loads(data)['data']
        # Remove the header and merge them
        json_data = json_data[1:]
        for d in json_data:
            time_stamp = int(parse(d[1]).timestamp())
            datum = d[2:]
            all_data[time_stamp] = datum

    keys = sorted(list(all_data.keys()))
    # In groups of factor, calculate thumbnails
    result = []
    begin, finish = min(keys), max(keys)
    tmp, total, count = [], {}, 0
    while len(keys) > 0:
        key = keys.pop(0)
        tmp.append(key)
        for i in range(7):
            total[i] += all_data[key][i]
        count += 1
        if len(tmp) == factor or len(keys) == 0:
            # Calculate the mean values as well as the timestamp middle value
            middle_epoch = min(tmp) + (max(tmp) - min(tmp)) / 2
            middle_ts = time.strftime('', time.localtime(middle_epoch))
            item = ['email@email.com', middle_ts]
            for i in range(7):
                total[i] /= total
                item.append(total[i])
            result.append(item)
            tmp, total, count = [], {}, 0
    result.insert(0, header)

    if target == '1h':
        for day in dates:
            gcs.put(f'{prefix}/{day}/{target}_{begin}_{finish}', json.dumps({'data': result}))
    else:
        gcs.put(f'{prefix}/{target}_{begin}_{finish}', json.dumps({'data': result}))


def get_file_list(prefix, dates, granularity, starts, ends):
    ret = set()
    for d in dates:
        blobs = gcs.list(f"{prefix}/{d}", f"/{granularity}_")
        for blob in blobs:
            arr = blob.name.split('_')
            start, end = arr[-2], arr[-1]
            # Check if the file overlaps with given time range
            a, b, x, y = int(starts), int(ends), int(start), int(end)
            if (a <= x <= b) or (a <= y <= b):
                ret.add(blob.name)
    return sorted(ret)


def get_thumbnail_list(prefix, granularity, starts, ends):
    ret = set()
    blobs = gcs.list(f"{prefix}", f"/{granularity}_")
    for blob in blobs:
        arr = blob.name.split('_')
        start, end = arr[-2], arr[-1]
        # Check if the file overlaps with given time range
        a, b, x, y = int(starts), int(ends), int(start), int(end)
        if (a <= x <= b) or (a <= y <= b):
            ret.add(blob.name)
    return sorted(ret)


def get_dates_in_range(starts, ends):
    date1 = datetime.fromtimestamp(starts)
    date2 = datetime.fromtimestamp(ends)
    dates = set()
    dates.add(date1.strftime("%Y%m%d"))
    dates.add(date2.strftime("%Y%m%d"))
    for delta in range(int((date2 - date1).days) + 1):
        dates.add((date1 + timedelta(delta)).strftime("%Y%m%d"))
    return sorted(dates)


def default_granularity(span):
    granularity = '5m'
    if span > 3600:
        granularity = '1h'
    if span > 3 * 24 * 3600:
        granularity = '1d'
    if span > 30 * 24 * 3600:
        granularity = '10d'
    if span > 365 * 24 * 3600:
        granularity = '30d'
    return granularity


def retrieve_and_merge(files):
    ret = []

    for f in files:
        data = gcs.get(f)
        json_data = json.loads(data)['data']
        # Remove the header and merge them
        ret.extend(json_data[1:])
        ret.insert(0, header)
    return {'data': ret}
