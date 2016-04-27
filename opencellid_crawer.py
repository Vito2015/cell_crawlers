# coding: utf-8

import sys
import json
import urllib2
import os
# from Queue import Queue
from multiprocessing import Pool, cpu_count

OPENCELLID_API_KEY = 'fce1a555-06cb-4c03-a87b-164c08cb9cf5'
OPENCELLID_GET_CELL_POSITION_URL = \
    'http://opencellid.org/cell/get?key='+OPENCELLID_API_KEY+'&mcc=%s&mnc=%s&lac=%s&cellid=%s&format=json'

ENCODE_STR = '%03x%04x%08x%08x'

_work_pool_size = 2000


def cell_pk(**kwargs):
    return ENCODE_STR % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])


def crawler(**kwargs):
    kwargs = kwargs or {}
    if not all([x in kwargs.keys() for x in ('mcc', 'mnc', 'lac', 'cid')]):
        sys.stderr.write('[KeyError]Keys:%s invalid.\n' % ','.join(kwargs.keys()))
        return None
    url = OPENCELLID_GET_CELL_POSITION_URL % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])
    pk = cell_pk(**kwargs)
    try:
        resp = urllib2.urlopen(url)
    except Exception as e:
        sys.stderr.write('[UrlopenError]key:%s->%s\n' % (pk, str(e)))
        return None
    else:
        data = resp.read().decode('utf-8')
        data_obj = json.loads(data)
        if 'code' in data_obj.keys():
            cell_key_info = ','.join((str(kwargs['mcc']), str(kwargs['mnc']), str(kwargs['lac']), str(kwargs['cid'])))
            sys.stderr.write('[DataError]key:%s-%s->%s\n' % (pk, cell_key_info, json.dumps(data_obj)))
            return None
        data_obj['key'] = pk
        return data_obj


def parse_fetch(line):
    line = line.strip()
    key, line = line.split('\t')
    try:
        data = json.loads(line)
    except Exception as e:
        sys.stderr.write('[DataError]LineJsonData loads line:%s->error: %s\n' % (str(line), str(e)))
    else:
        result = crawler(**data)
        if result is not None:
            print('%s' % json.dumps(result))


DEBUG = False


def main():
    work_lines = []
    process_count = cpu_count() if not DEBUG else 1  # cpu_count()
    sys.stderr.write('Worker process count:%s.\n' % process_count)
    pool = Pool(processes=process_count)
    stdin = sys.stdin if not DEBUG else os.popen('cat missing_cells_key.json.txt | head -n 3')

    def _do_work():
        sys.stderr.write('Workers begin. count:%s\n' % len(work_lines))
        pool.map(parse_fetch, work_lines)
        pool.close()
        pool.join()
        sys.stderr.write('Workers done.\n')

    for line in stdin:
        if len(work_lines) < _work_pool_size:
            work_lines.append(line)
        else:
            _do_work()
            pool = Pool(processes=cpu_count())
            work_lines = []
    _do_work()

main()
