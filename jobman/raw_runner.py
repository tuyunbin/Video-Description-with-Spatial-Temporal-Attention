import logging, time, os

from runner import runner_registry
from optparse import OptionParser

parser_raw = OptionParser(usage = '%prog raw [options] <expr>',
                          add_help_option=False)
parser_raw.add_option('--workdir', dest = 'workdir', type = 'str', default = '',
                      help = 'Call function in a subdirectory.'
                      'The %(cmdfn)s substring will be replaced with the name of the fn'
                      'The %(timestamp)s substring will be replaced with the current time')
parser_raw.add_option('--no-latest', dest = 'nolatest', type = 'int', default = 0,
                      help = 'suppress creation of jobman.latest')
def import_cmd(cmd):
    """Return the full module name of a fully-quallified function call
    """
    #print 'cmd', cmd
    lp = cmd.index('(')
    ftoks = cmd[:lp].split('.')
    imp = '.'.join(ftoks[:-1])
    return imp, cmd

_logger = logging.getLogger('jobman.raw_runner')

def runner_raw(options, fullfn):
    """ Run a fully-qualified python function from the commandline.

    Example use:

        jobman raw 'mymodule.my_experiment(0, 1, a=2, b=3)'

    """
    imp, cmd = import_cmd(fullfn)
    cmdfn = cmd[:cmd.index('(')]
    if options.workdir:
        timestamp = '_'.join('%02i'%s for s in time.localtime()[:6])
        dirname = options.workdir % locals()
        os.makedirs(dirname)
        if not options.nolatest:
            try:
                os.remove('jobman.workdir')
            except:
                pass
            os.system('ln -s "%s" jobman.workdir' % dirname)
        os.chdir(dirname)

    _logger.info('Running: %s' % cmd)
    _logger.info('Starttime: %s' % time.localtime())
    _logger.debug('Importing: %s' % imp)
    if imp:
        exec('import '+imp)
    t0 = time.time()
    _logger.debug('executing: %s' % cmd)
    exec(cmd)
    _logger.info('Endtime: %s' % time.localtime())
    _logger.info('Duration: %s', time.time() - t0)

runner_registry['raw'] = (parser_raw, runner_raw)

