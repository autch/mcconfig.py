#!/usr/bin/python3

import re
import sys
import logging
from typing import List, Iterable, Optional
import argparse
import itertools
import subprocess
from dataclasses import dataclass
import multiprocessing as mp

import yaml
from lxml import etree

RECPT1 = '/usr/local/bin/recpt1'
EPGDUMP = '/usr/local/bin/epgdump'
REC_TIME = 30

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass
class ChannelTypeData:
    name: str
    channels: Iterable[int]
    epgdump_mode: Optional[str]
    recorder_ch_fmt: str


@dataclass
class GetEPGTask:
    chtype: ChannelTypeData

    ch_to_rec: str
    epgdump_mode: str
    
    recpt1: str
    epgdump: str
    seconds: int
    
        
def get_epg_from_record(task: GetEPGTask):
    logger.debug('EXEC: %s %s %d - | %s %s - -', task.recpt1, task.ch_to_rec, task.seconds, task.epgdump, task.epgdump_mode)
    p_recpt1 = subprocess.Popen([task.recpt1, task.ch_to_rec, str(task.seconds), '-'],
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    p_epgdump = subprocess.Popen([task.epgdump, task.epgdump_mode, '-', '-'],
                                 stdin=p_recpt1.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    p_recpt1.stdout.close()

    root = etree.parse(p_epgdump.stdout)
    p_epgdump.wait()
    return [xml_to_epg(task, channel) for channel in root.iter('channel')]

def xml_to_epg(task: GetEPGTask, channel):
    ch_spec = { 'tp': channel.attrib['tp'] }
    for item in channel.iterchildren():
        ch_spec[item.tag] = item.text

    ch = {
        'type': task.chtype.name,
        'name': ch_spec['display-name'],
        'channel': ch_spec['tp'],
        'serviceId': int(ch_spec['service_id']),
        'isDisabled': False
    }
    if ch_spec['service_id'] is None:
        del ch['serviceId']
    logger.info('[%s] %s: %s (sid %d)', ch['type'], ch['channel'], ch['name'], ch['serviceId'])
    return ch

def get_epg_for_channel(chdef: GetEPGTask):
    return get_epg_from_record(chdef)

def mix_args(chtype: ChannelTypeData, ch: int, args) -> GetEPGTask:
    return GetEPGTask(chtype, chtype.recorder_ch_fmt.format(name=chtype.name, ch=ch),
                      chtype.epgdump_mode if chtype.epgdump_mode else str(ch), args.recpt1, args.epgdump, args.seconds)

def natsort_for_channel(i):
    def atoi(text):
        return int(text) if text.isdigit() else text
    def natural_keys(text):
        return [atoi(c) for c in re.split(r'(\d+)', text)]
    return natural_keys(i['channel'])

def get_epg_for_chtype_mp(pool, chtype: ChannelTypeData, args):
    channels: List[GetEPGTask] = [mix_args(chtype, ch, args) for ch in chtype.channels]
    result = [
        c for ch in pool.imap_unordered(get_epg_for_channel, channels, 4) if len(ch) > 0
        for c in ch if ch
    ]
    return remove_duplicate_service(chtype, result)

def remove_duplicate_service(chtype: ChannelTypeData, channels: List[GetEPGTask]):
    if chtype.name == 'GR':
        return sorted(channels, key=natsort_for_channel)

    key_func = lambda i: i['serviceId']
    data = sorted(channels, key=key_func)
    def find_nonnull_ch(g):
        gl = list(g)
        l = [i for i in gl if i['name']]
        if l:
            return l[0]
        else:
            return gl[0]
    result = [find_nonnull_ch(g) for k, g in itertools.groupby(data, key=key_func)]

    return sorted(result, key=natsort_for_channel)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gr', '-g', action='store_true', help='receive GR channels (13..52)')
    parser.add_argument('--bs', '-b', action='store_true', help='receive BS channels (BS1..23)')
    parser.add_argument('--cs', '-c', action='store_true', help='receive CS channels (ND2..24)')
    parser.add_argument('--seconds', '-s', type=int, default=REC_TIME, help='seconds to record (default: %(default)d)')
    parser.add_argument('--tuners', '-t', type=int, default=4, help='# of tuners for each band (default: %(default)d)')
    parser.add_argument('--recpt1', default=RECPT1, help='path to recpt1 (default: %(default)s)')
    parser.add_argument('--epgdump', default=EPGDUMP, help='path to epgdump (default: %(default)s)')
    args = parser.parse_args()

    chtypes: List[ChannelTypeData] = []
    if args.gr: chtypes.append(ChannelTypeData('GR', range(13, 53), None, "{ch:d}"))
    if args.bs: chtypes.append(ChannelTypeData('BS', [1], '/BS', "{name:s}{ch:d}_0"))
    if args.cs: chtypes.append(ChannelTypeData('CS', [2], '/CS', "{name:s}{ch:d}"))

    if len(chtypes) == 0:
        print("Nothing to do", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

    with mp.Pool(processes=args.tuners) as pool:
        all_definitions = [ch for chtype_result in
                           [get_epg_for_chtype_mp(pool, chtype, args) for chtype in chtypes]
                           for ch in chtype_result]

    yaml.safe_dump(all_definitions, stream=sys.stdout, encoding='utf-8', allow_unicode=True, default_flow_style=False)
