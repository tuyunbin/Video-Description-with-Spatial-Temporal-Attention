
import os
import re
import time
import random

def date(state):
    prefix = state.get('workdir_prefix', 'jobman')
    t = time.time()
    year, month, day, hour, minute, second, wday, yday, isdst = time.localtime()
    workdir = "%s_%04i%02i%02i_%02i%02i%02i_%04i%04i" % (prefix,
                                                         year, month, day,
                                                         hour, minute, second,
                                                         (t-int(t)) * 10000, random.randint(0, 10000))
    return workdir


def numbered(state):
    prefix = state.get('workdir_prefix', 'jobman')
    pattern = re.compile('%s([0-9]+)' % prefix)
    max_idx = 0
    for x in os.listdir('.'):
        idx = pattern.findall(x)
        if idx and idx[0] > max_idx:
            max_idx = idx[0]
    return '%s%i' % (prefix, int(max_idx) + 1)




#old default - it sucks
#workdir = format_d(state, sep=',', space = False)
    


