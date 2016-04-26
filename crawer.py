# coding: utf-8

import sys
import json
import urllib

OPENCELLID_API_KEY = ''
OPENCELLID_CELL_URL = 'http://opencellid.org/cell/get?key='\
                      + OPENCELLID_API_KEY\
                      + '&mcc=%s&mnc=%s&lac=%s&cellid=%s&format=json'


def cell_pk(**kwargs):
    pass


def crawler(**kwargs):
    kwargs = kwargs or {}
    if not all([x in kwargs.keys() for x in ('mcc', 'mnc', 'lac', 'cid')]):
        sys.stderr.write('[Error]Keys:%s invalid.' % ','.join(kwargs.keys()))
        return
    url = OPENCELLID_CELL_URL % (kwargs['mcc'], kwargs['mnc'], kwargs['lac'], kwargs['cid'])

    try:
        resp = urllib.urlopen(url)
    except Exception as e:
        sys.stderr.write('[Error]' + str(e))
        return
    else:
        data = resp.read().decode('utf-8')
        data_obj = json.loads(data)
        if 'code' in data_obj.keys():
            sys.stderr.write('[DataError]%s' % data_obj)
            return
        pk = cell_pk(**data_obj)
        data_obj['key'] = pk
        return data_obj


def parse_fetch(line):
    line = line.strip()
    data = json.loads(line)
    result = crawler(**data)
    print('%s' % result)


def main():
    for line in sys.stdin:
        parse_fetch(line)

main()