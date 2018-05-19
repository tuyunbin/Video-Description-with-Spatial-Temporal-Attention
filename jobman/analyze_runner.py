import os
import logging

from jobman.runner import runner_registry
from optparse import OptionParser

_logger = logging.getLogger('jobman.analyze_runner')

parser_analyze = OptionParser(usage = '%prog rsync_any [options] <tablepath> <exproot>',
        add_help_option=False)
parser_analyze.add_option('--extra', dest = 'extra', 
        type = 'str', default = '',
        help = 'comma-delimited list of extra imports for additional commands')
parser_analyze.add_option('--addr', dest = 'addr',
        type = 'str', default = 'pkl://'+os.getcwd(),
        help = 'Address of experiment root (starting with format prefix such'
        ' as postgres:// or pkl:// or dd://')
def runner_analyze(options, cmdname):
    """Analyze the state/results of the experiment

    Example usage:

        jobman analyze --extra=jobs --addr=pkl://relpath/to/experiment <cmd>
        jobman analyze --extra=jobs --addr=pkl:///abspath/to/experiment <cmd>
        jobman analyze --extra=jobs --addr=postgres://user@host:dbname?table=tablename <cmd>

    Try jobman analyze help for more information.
    """
    #parse the address
    if options.addr.startswith('pkl://'):
        import analyze.pkl
        exproot = options.addr[len('pkl://'):]
    elif options.addr.startswith('postgres://') or options.addr.startswith('sqlite://'):
        raise NotImplementedError()
        import analyze.pg
    elif options.addr.startswith('dd://'):
        raise NotImplementedError()
        import analyze.dd
    else:
        raise NotImplementedError('unknown address format, it should start with "pkl" or'
                ' "postgres" or "dd"', options.addr)

    # import modules named via --extra
    for extra in options.extra.split(','):
        if extra:
            _logger.debug('importing extra module: %s'% extra)
            __import__(extra)

    try:
        cmd = cmd_dct[cmdname]
    except:
        cmd = help
    cmd(**locals())

runner_registry['analyze'] = (parser_analyze, runner_analyze)

##############################
# Analyze sub-command registry
##############################

class Cmd(object):
    """ A callable object that attaches documentation strings to command functions.
    
    This class is a helper for the decorators `cmd` and `cmd_desc`.
    """
    def __init__(self, f, desc):
        self.f = f
        if desc is None:
            self.desc = 'No help available'
        else:
            self.desc = desc

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)
cmd_dct = {}
def cmd(f):
    """Declare a function `f` as a `mydriver.main` command.

    The docstring of `f` is taken as the description of the command.
    """
    cmd_dct[f.__name__] = Cmd(f, f.__doc__)
    return f
def cmd_desc(desc):
    """Declare a function `f` as a `mydriver.main` command, and provide an explicit description to appear to the right of your command when running the 'help' command.
    """
    def deco(f):
        cmd_dct[f.__name__] = Cmd(f, desc)
        return f
    return deco

def help(**kwargs):
    """Print help for this program"""
    print "Usage: jobman analyze <cmd>"
    #TODO
    print "Commands available:"
    for name, cmd in cmd_dct.iteritems():
        print "%20s - %s"%(name, cmd.desc)
