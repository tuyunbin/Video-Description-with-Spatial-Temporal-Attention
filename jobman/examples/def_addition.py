""" Example library file implementing an experiment.
"""

# Typically this sort of file would be a lot more complicated, and deserve it's own file.

def addition_example(state, channel):

    print 'state.first =', state.first
    print 'state.second =', state.second

    state.result = state.first + state.second
    print 'result =', state.result

    return channel.COMPLETE

