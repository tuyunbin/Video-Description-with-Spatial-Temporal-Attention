
from jobman import make2

class Plumber(object):
    def __init__(self, itsa, me, mario):
        self.statement = itsa + me + mario

def experiment(state, channel):
    exclamation_mark = '!'
    obj = make2(state.plumber)
    print obj.statement
    return channel.COMPLETE

