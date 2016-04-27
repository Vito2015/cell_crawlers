# coding: utf-8
"""
    missingcell_key_parser.py
    ~~~~~~~~~~~~~~~~~~~~~

    Input data like:
    {"_id":{"$oid":"56c43d983fdda4c054803dea"},"cellKey":"1cc00010000b19002165657","timeStamp":"2016-02-17 17:00:00"}

    Output string data like:
    1cc00010000d3fb0d0c706f	{"mnc": 1, "mcc": 460, "cid": 218919023, "lac": 54267}

"""

import re
import os
import sys
import json

reload(sys)
sys.setdefaultencoding('utf-8')

DECODE_REG = re.compile('^([0-9a-f]{3})([0-9a-f]{4})([0-9a-f]{8})([0-9a-f]{8})$')


class ParseCellKeyError(AttributeError):
    pass


def parse_cell_pk(string):
    try:
        mcc, mnc, lac, cid = DECODE_REG.match(string).groups()
    except AttributeError:
        raise ParseCellKeyError('Wrong cell key format: {0}'.format(string))
    mcc = int(mcc, base=16)
    mnc = int(mnc, base=16)
    lac = int(lac, base=16)
    cid = int(cid, base=16)
    return mcc, mnc, lac, cid


def parse_mising_cell_json_key(line):
    obj = json.loads(line)
    key_str = obj.get('cellKey')
    return key_str


def run(stdin):
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key_str = parse_mising_cell_json_key(line)
            mcc, mnc, lac, cid = parse_cell_pk(key_str)
            cell = dict()
            cell['mcc'] = mcc
            cell['mnc'] = mnc
            cell['lac'] = lac
            cell['cid'] = cid
            cell_str = '%s\t%s' % (key_str, json.dumps(cell))
            # cell_str = '%s' % json.dumps(cell)
            print cell_str
        except Exception as e:
            sys.stderr.write('%s\n' % str(e))


DEBUG = False


def main():
    if DEBUG:
        # stdin = os.system('cat %s |head -n 3' % os.path.join(os.path.curdir, 'cell_460_head1000.json.txt'))
        stdin = os.popen('cat %s |head -n 100' %
                         '/home/mi/tmpdata/metok_trajectory/missing_cells/missing_cells.json')
    else:
        stdin = sys.stdin
    run(stdin)


main()
