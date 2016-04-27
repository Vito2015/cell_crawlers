# coding: utf-8

import sys
import json
import urllib
import os
# from Queue import Queue
from multiprocessing import Pool, cpu_count

DEBUG = True
OPENCELLID_API_KEY = 'xxx'
OPENCELLID_CELL_URL = 'http://opencellid.org/cell/get?key='\
                      + OPENCELLID_API_KEY\
                      + '&mcc=%s&mnc=%s&lac=%s&cellid=%s&format=json'
ENCODE_STR = '%03x%04x%08x%08x'

_parallel_num = 4
_work_pool_size = 2000


def cell_pk(**kwargs):
    return ENCODE_STR % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])


def crawler(**kwargs):
    kwargs = kwargs or {}
    if not all([x in kwargs.keys() for x in ('mcc', 'net', 'area', 'cell')]):
        sys.stderr.write('[Error]Keys:%s invalid.\n' % ','.join(kwargs.keys()))
        return
    url = OPENCELLID_CELL_URL % (kwargs['mcc'], kwargs['net'], kwargs['area'], kwargs['cell'])

    try:
        resp = urllib.urlopen(url)
    except Exception as e:
        sys.stderr.write('[Error]%s\n' % str(e))
        return
    else:
        data = resp.read().decode('utf-8')
        data_obj = json.loads(data)
        if 'code' in data_obj.keys():
            sys.stderr.write('[DataError]%s\n' % data_obj)
            return
        pk = cell_pk(**data_obj)
        data_obj['key'] = pk
        return data_obj


def parse_fetch(line):
    idx, line = line
    # print('test work results.%s: %s' % (idx, line))
    # return
    line = line.strip()
    data = json.loads(line)
    result = crawler(**data)
    print('%s' % result)


def main():
    work_urls = []
    pool = Pool(processes=cpu_count())
    stdin = sys.stdin if not DEBUG else os.popen('cat cell_towers_china2.json | head -n 3')
    for idx, line in enumerate(stdin):
        # parse_fetch(line)
        if len(work_urls) < _work_pool_size:
            work_urls.append((idx, line))
        else:
            sys.stderr.write('Workers begin.\n')
            pool.map(parse_fetch, work_urls)
            pool.close()
            pool.join()
            sys.stderr.write('Workers done.\n')
            pool = Pool(processes=cpu_count())
            work_urls = []
    sys.stderr.write('Workers begin.\n')
    pool.map(parse_fetch, work_urls)
    pool.close()
    pool.join()
    sys.stderr.write('Workers done.\n')

main()
