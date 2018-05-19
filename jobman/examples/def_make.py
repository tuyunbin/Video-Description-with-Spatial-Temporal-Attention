from jobman import make

class MakeExample():

    def __init__(self, param1=2, param2='message'):
        self.value = param1
        self.msg = param2
        print 'Created MakeExample object successfully.'

    def test(self):
        print 'self.value = ', self.value
        print self.msg

def experiment(state, channel):

    obj = make(state.obj)
    obj.test()
    return channel.COMPLETE
