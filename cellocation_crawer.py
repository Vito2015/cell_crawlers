# coding: utf-8
"""
    cellocation_crawer.py
    ~~~~~~~~~~~~~~~~~~~~~

    Input data like:
    1cc0001000059a808452c02	{"mnc": 1, "mcc": 460, "cid": 138750978, "lac": 22952}

    Output string data like:
    {"from": "cellocation.com", "lon": "113.541458", "radius": "301", "key": "1cc00010000a66c01cad53f", "address": "", "lat": "24.771313"}

"""

import sys
import json
import urllib2
import os
# from Queue import Queue
from multiprocessing import Pool, cpu_count

_headers = {
    'User-Agent': 'mozilla/5.0 (compatible; baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
    # 'cookie': cookies_str
}

DEBUG = True

COORD = 'wgs84'
OUT_PUT = 'json'  # or 'csv' or 'xml

# usual conditions we use 'BASE_URL' , and 'BASE_URL2' for back up
# the 'BASE_URL2' param-> 96 (default) is ASU or dBm
# 坐标类型(wgs84/gcj02/bd09)，默认wgs84
BASE_URL = 'http://api.cellocation.com/cell/?coord='+COORD+'&output='+OUT_PUT+'&mcc=%s&mnc=%s&lac=%s&ci=%s'
BASE_URL2 = 'http://api.cellocation.com/loc/?coord='+COORD+'&output='+OUT_PUT+'&cl=%s,%s,%s,%s,96'

ERR_CODES = {
    10000: '查询参数错误',
    10001: '无基站数据',
    403: '每日查询超限',
}

ENCODE_STR = '%03x%04x%08x%08x'

_work_pool_size = 2000

_daily_max_request = False

process_count = cpu_count() if not DEBUG else 1  # cpu_count()
pool = Pool(processes=process_count)


class DailyMaxRequestError(StandardError):
    pass


def cell_pk(**kwargs):
    return ENCODE_STR % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])


def crawler(**kwargs):
    global _daily_max_request

    kwargs = kwargs or {}
    if not all([x in kwargs.keys() for x in ('mcc', 'mnc', 'lac', 'cid')]):
        sys.stderr.write('[Error]Keys:%s invalid.\n' % ','.join(kwargs.keys()))
        return
    url = BASE_URL % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])
    pk = cell_pk(**kwargs)
    try:
        resp = urllib2.urlopen(url, timeout=5)
    except Exception as e:
        sys.stderr.write('[Error]key:%s->%s\n' % (pk, str(e)))
        if hasattr(e, 'code'):
            if e.code in ERR_CODES.keys():
                sys.stderr.write('[Error]status=%s, errmsg=%s\n' % (e.code, ERR_CODES[e.code]))
            else:
                sys.stderr.write('[Error]The server couldn\'t fulfill the request. status=%s\n' % e.code)

        _daily_max_request = True
        return None
    else:
        data = resp.read().decode('utf-8')
        data_obj = json.loads(data)
        try:
            errcode = data_obj.pop('errcode')
        except KeyError:
            return data
        if errcode != 0 and errcode in ERR_CODES.keys():
            cell_key_info = ','.join((str(kwargs['mcc']), str(kwargs['mnc']), str(kwargs['lac']), str(kwargs['cid'])))
            sys.stderr.write('[DataError]key:%s-%s->%s, errmsg:%s\n' %
                             (pk, cell_key_info, json.dumps(data_obj), ERR_CODES[errcode]))
            return None
        data_obj['key'] = pk
        data_obj['from'] = 'cellocation.com'
        return data_obj


def parse_fetch(line):
    global pool

    line = line.strip()
    key, line = line.split('\t')
    try:
        data = json.loads(line)
    except Exception as e:
        sys.stderr.write('[Error]LineJsonData loads line:%s->error: %s\n' % (str(line), str(e)))
    else:
        result = crawler(**data)
        if result is not None:
            print('%s' % json.dumps(result))
        elif _daily_max_request:
            raise DailyMaxRequestError()

DEBUG = False


def main():
    global pool, process_count
    work_lines = []

    def _do_work():
        sys.stderr.write('Workers begin. count:%s\n' % len(work_lines))
        pool.map(parse_fetch, work_lines).get(1)
        pool.close()
        pool.join()
        sys.stderr.write('Workers done.\n')

    sys.stderr.write('Worker process count:%s.\n' % process_count)
    stdin = sys.stdin if not DEBUG else os.popen('cat missing_cells_key.json.txt | head -n 100')

    for line in stdin:
        # line = line.strip()
        if len(work_lines) < _work_pool_size:
            work_lines.append(line)
        else:
            try:
                _do_work()
            except DailyMaxRequestError:
                return
            pool = Pool(processes=cpu_count())
            work_lines = []
    try:
        _do_work()
    except DailyMaxRequestError:
        return

main()
