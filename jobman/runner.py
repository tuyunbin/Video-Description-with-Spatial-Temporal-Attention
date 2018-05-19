import os
import sys
import time
import tempfile
import inspect
import shutil
import optparse
from optparse import OptionParser

from tools import DD, expand, format_help, resolve, UsageError
from channel import StandardChannel

import parse
import workdirgen

################################################################################
### Running
################################################################################


def parse_and_run(command, arguments):
    if command == None:
        #allow other parameter for help used in other program
        for arg in arguments:
            if arg in ["--help", "-h"]:
                command = "help"
                arguments = []

    parser, runner = runner_registry.get(command, (None, None))
    if not runner:
        raise UsageError('Unknown runner: "%s"' % command)
    if parser:
        options, arguments = parser.parse_args(arguments)
    else:
        options = optparse.Values()
    return run(runner, [options] + arguments)


def run(runner, arguments):
    argspec = inspect.getargspec(runner)
    minargs = len(argspec[0])
    if argspec[3]:
        minargs -= len(argspec[3])
    maxargs = len(argspec[0])
    if minargs > len(arguments) or maxargs < len(arguments) and not argspec[1]:
        s = format_help(runner)
        raise UsageError(s)
    return runner(*arguments)


def run_cmdline():
    try:
        if len(sys.argv) <= 1:
            raise UsageError(
                'Usage: "%s <command> [<arguments>*]" \nor "%s help" for help'
                % (sys.argv[0], sys.argv[0]))
        cmd = None
        args = []
        for arg in sys.argv[1:]:
            if cmd is not None or arg.startswith('-'):
                args.append(arg)
            else:
                cmd = arg
        warn_if_sql_failure()
        return parse_and_run(cmd, args)
    except UsageError, e:
        print 'Usage error:'
        print e


def warn_if_sql_failure():
    """Display a warning if sqlalchemy or psycopg2 could not be imported.

    This warning is not displayed if the user is running the 'cmdline' command,
    which does not require SQL features.
    """
    if len(sys.argv) >= 2 and sys.argv[1] == 'cmdline':
        return
    from jobman import sql
    for module in ('sqlalchemy',):  # , 'psycopg2'):
        if not getattr(sql, '%s_ok' % module):
            # Note: we use `RuntimeWarning` instead of `ImportWarning` because
            # the latter are ignored by default, and we do not want it to be
            # ignored.
            print ("WARNING: SQL-related module '%s' could not be imported: SQL"
                   " features will most likely crash" % module)


################################################################################
### Registry
################################################################################

runner_registry = dict()


################################################################################
### Default runners
################################################################################

################################################################################
### cmdline
################################################################################

parser_cmdline = OptionParser(
    usage='%prog cmdline [options] <experiment> <parameters>',
    add_help_option=False)
parser_cmdline.add_option('-f', '--force', action='store_true',
                          dest='force', default=False,
                          help='force running the experiment even if it is already running or completed')
parser_cmdline.add_option('--redirect-stdout', action='store_true',
                          dest='redirect_stdout', default=False,
                          help='redirect stdout to the workdir/stdout file')
parser_cmdline.add_option('--redirect-stderr', action='store_true',
                          dest='redirect_stderr', default=False,
                          help='redirect stderr to the workdir/stdout file')
parser_cmdline.add_option('-r', '--redirect', action='store_true',
                          dest='redirect', default=False,
                          help='redirect stdout and stderr to the workdir/stdout and workdir/stderr files')
parser_cmdline.add_option('-w', '--workdir', action='store',
                          dest='workdir', default=None,
                          help='the working directory in which to run the experiment')
parser_cmdline.add_option('--workdir-dir', action='store',
                          dest='workdir_dir', default=None,
                          help='The directory where the workdir should be created')
parser_cmdline.add_option('-g', '--workdir-gen', action='store',
                          dest='workdir_gen', default='date',
                          help='function serving to generate the relative path of the workdir')
parser_cmdline.add_option('-n', '--dry-run', action='store_true',
                          dest='dry_run', default=False,
                          help='use this option to run the whole experiment in a temporary working directory (cleaned after use)')
parser_cmdline.add_option('-2', '--sigint', action='store_true',
                          dest='allow_sigint', default=False,
                          help='allow sigint (CTRL-C) to interrupt a process')
parser_cmdline.add_option('-p', '--parser', action='store',
                          dest='parser', default='filemerge',
                          help='parser to use for the argument list provided on the command line (takes a list of strings, returns a state)')
parser_cmdline.add_option('--finish-up-after', action='store',
                          dest='finish_up_after',
                          default=None,
                          help='Duration (in seconds) after which the call to channel.switch() will return "finish-up". Asks the experiment to reach the next checkpoint, save, and exit. It is up to the experimentto use channel.switch() and respect it.')
parser_cmdline.add_option('--save-every', action='store',
                          dest='save_every',
                          default=None,
                          help='Interval (in seconds) after which the call to channel.switch() will return "save". Asks the experiment to save at the next checkpoint. It is up to the experiment use channel.switch() and respect it.')


def runner_cmdline(options, experiment, *strings):
    """
    Start an experiment with parameters given on the command line.

    Usage: cmdline [options] <experiment> <parameters>

    Run an experiment with parameters provided on the command
    line. See the help topics for experiment and parameters for
    syntax information.

    Example use:
        jobman cmdline mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03

        you can use the jobman.experiments.example1 as a working
        mymodule.my_experiment
    """
    parser = getattr(parse, options.parser, None) or resolve(options.parser)
    _state = parser(*strings)
    state = expand(_state)
    state.setdefault('jobman', DD()).experiment = experiment
    state.jobman.time = time.ctime()
    experiment = resolve(experiment)

    if options.workdir and options.dry_run:
        raise UsageError('Please use only one of: --workdir, --dry-run.')
    if options.workdir and options.workdir_dir:
        raise UsageError('Please use only one of: --workdir, --workdir_dir.')
    if options.workdir:
        workdir = options.workdir
    elif options.dry_run or options.workdir_dir:
        if options.workdir_dir and not os.path.exists(options.workdir_dir):
            os.mkdir(options.workdir_dir)
        workdir = tempfile.mkdtemp(dir=options.workdir_dir)
    else:
        workdir_gen = getattr(workdirgen, options.workdir_gen,
                              None) or resolve(options.workdir_gen)
        workdir = workdir_gen(state)
    print "The working directory is:", os.path.join(os.getcwd(), workdir)

    channel = StandardChannel(workdir,
                              experiment, state,
                              redirect_stdout = options.redirect or options.redirect_stdout,
                              redirect_stderr = options.redirect or options.redirect_stderr,
                              finish_up_after = options.finish_up_after or None,
                              save_interval = options.save_every or None
                              )
    channel.catch_sigint = not options.allow_sigint
    channel.run(force=options.force)
    if options.dry_run:
        shutil.rmtree(workdir, ignore_errors=True)

runner_registry['cmdline'] = (parser_cmdline, runner_cmdline)


# ################################################################################
# ### filemerge
# ################################################################################
# parser_filemerge = OptionParser(usage = '%prog filemerge [options] <experiment> <file> <file2> ...', add_help_option=False)
# parser_filemerge.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
#                           help = 'force running the experiment even if it is already running or completed')
# parser_filemerge.add_option('--redirect-stdout', action = 'store_true', dest = 'redirect_stdout', default = False,
#                           help = 'redirect stdout to the workdir/stdout file')
# parser_filemerge.add_option('--redirect-stderr', action = 'store_true', dest = 'redirect_stderr', default = False,
#                           help = 'redirect stderr to the workdir/stdout file')
# parser_filemerge.add_option('-r', '--redirect', action = 'store_true', dest = 'redirect', default = False,
#                           help = 'redirect stdout and stderr to the workdir/stdout and workdir/stderr files')
# parser_filemerge.add_option('-w', '--workdir', action = 'store', dest = 'workdir', default = None,
#                           help = 'the working directory in which to run the experiment')
# parser_filemerge.add_option('-n', '--dry-run', action = 'store_true', dest = 'dry_run', default = False,
#                           help = 'use this option to run the whole experiment in a temporary working directory (cleaned after use)')

# def runner_filemerge(options, experiment, *files):
#     """
#     Start an experiment with parameters given in files.

#     Usage: filemerge [options] <experiment> <file> <file2> ...

#     Run an experiment with parameters provided in plain text files.
#     A single experiment will be run with the union of all the
#     parameters listed in the files.

#     Example:
#     <in file blah1.txt>
#     text.first = "hello"
#     text.second = "world"

#     <in file blah2.txt>
#     number = 12
#     numbers.a = 55
#     numbers.b = 56

#     Given these files, the following command using filemerge:
#     $ jobman filemerge mymodule.my_experiment blah1.txt blah2.txt

#     is equivalent to this one using cmdline:
#     $ jobman cmdline mymodule.my_experiment \\
#         text.first=hello text.second=world \\
#         number=12 numbers.a=55 numbers.b=56

#     you can use the jobman.experiments.example1 as a working
#     mymodule.my_experiment
#     """

#     _state = parse_files(*files)
#     state = expand(_state)
#     state.setdefault('jobman', DD()).experiment = experiment
#     experiment = resolve(experiment)
#     if options.workdir and options.dry_run:
#         raise UsageError('Please use only one of: --workdir, --dry-run.')
#     if options.workdir:
#         workdir = options.workdir
#     elif options.dry_run:
#         workdir = tempfile.mkdtemp()
#     else:
#         workdir = format_d(state, sep=',', space = False)
#     channel = StandardChannel(workdir,
#                               experiment, state,
#                               redirect_stdout = options.redirect or options.redirect_stdout,
#                               redirect_stderr = options.redirect or options.redirect_stderr)
#     channel.run(force = options.force)
#     if options.dry_run:
#         shutil.rmtree(workdir, ignore_errors=True)

# runner_registry['filemerge'] = (parser_filemerge, runner_filemerge)



################################################################################
### help
################################################################################
def runner_help(options, topic=None):
    """
    Get help for a topic.

    Usage: help <topic>
    """
    def bold(x):
        return '\033[1m%s\033[0m' % x
    if topic is None:
        print bold('Topics: (use help <topic> for more info)')
        print 'example        Example of defining and running an experiment.'
        print 'experiment     How to define an experiment.'
        print 'parameters     How to list the parameters for an experiment.'
        print
        print bold('Available commands: (use help <command> for more info)')
        for name, (parser, command) in sorted(runner_registry.iteritems()):
            print name.ljust(20), format_help(command).split('\n')[0]
        return
    elif topic == 'experiment':
        helptext = """

        jobman serves to run experiments. To define an experiment, you
        only have to define a function respecting the following protocol in
        a python file or module:

        def my_experiment(state, channel):
           # experiment code goes here

        The return value of my_experiment may be channel.COMPLETE or
        channel.INCOMPLETE. If the latter is returned, the experiment may
        be resumed at a later point. Note that the return value `None`
        is interpreted as channel.COMPLETE.

        If a command defined by jobman has an <experiment> parameter,
        that parameter must be a string such that it could be used in a
        python import statement to import the my_experiment function. For
        example if you defined my_experiment in my_module.py, you can pass
        'my_module.my_experiment' as the experiment parameter.

        When entering my_experiment, the current working directory will be
        set for you to a directory specially created for the experiment.
        The location and name of that directory vary depending on which
        jobman command you run. You may create logs, save files, pictures,
        results, etc. in it.

        state is an object containing the parameters given to the experiment.
        For example, if you run the followinc command:

        jobman cmdline my_module.my_experiment a.x=6

        `state.a.x` will contain the integer 6, and so will `state['a']['x']`.
        If the state is changed, it will be saved when the experiment ends
        or when channel.save() is called. The next time the experiment is run
        with the same working directory, the modified state will be provided.

        It is not recommended to store large amounts of data in the state.  It
        should be limited to scalar or string parameters. Results such as
        weight matrices should be stored in files in the working directory.

        channel is an object with the following important methods:

         - channel.switch() (or channel()) will give the control back to the
            user, if it is appropriate to do so. If a call to channel.switch()
            returns the string 'stop', it typically means that the signal
            SIGTERM (or SIGINT) was received. Therefore, the experiment may be
            killed soon, so it should save and return True or
            channel.INCOMPLETE so it can be resumed later. This should be
            checked periodically or data loss may be incurred.

         - channel.save() will save the current state. It is automatically
            called when the function returns, but it is a good idea to do it
            periodically.

         - channel.save_and_switch() is an useful shortcut to do both
            operations described above.

        """

    elif topic == 'parameters':
        helptext = """
        If a command takes <parameters> arguments, the arguments should each
        take one of the following forms:

        key=value

          Set a parameter with name `key` to `value`. The value will be casted
          to an appropriate type automatically and it will be accessible to
          the experiment using `state.key`.

          If `key` is a dotted name, the value will be set in nested
          dictionaries corresponding to each part.

          Examples:
            a=1           state.a <- 1
            b=2.3         state.b <- 2.3
            c.d="hello"   state.c.d <- "hello"

        key::builder

          This is equivalent to key.__builder__=builder.

          The builder should be a symbol that can be used with import or
          __import__ and it should be callable.

          If a key has a builder defined, the experiment code may easily make
          an object out of it using the `make` function. `obj = make(state.key)`.
          This will call the builder on the substate corresponding to state.key,
          as will be made clear in the example:

          Example:
            regexp::re.compile
            regexp.pattern='a.*c'

          from jobman.tools import make
          def experiment(state, channel):
              # regexp is now re.compile(pattern = 'a.*c')
              regexp = make(state.regexp)
              print regexp.sub('blahblah', 'hello abbbbc there')

          If the above experiment was called with the state produced by the
          parameters in the example, it would print 'hello blahblah there'.

        path/to/file.conf

          A file containing key=value and key::builder pairs, one on each line,
          or relative paths to other configuration files to load.

        NOTE: all of the above applies to the default command line
        arguments parser, filemerge. The option to put configuration
        files is not available in the parse.standard parser and other
        parsers may add, change or remove functionality. You can
        change the parser with the --parser option if it is available.
        """

    elif topic == 'example':
        helptext = """
        Example of an experiment that trains some model for 100000 iterations:

        # defined in: my_experiments.py
        def experiment(state, channel):
            try:
                model = cPickle.load(open('model', 'r'))
            except:
                model = my_model(state.some_param, state.other_param)
                state.n = 0
            dataset = my_dataset(skipto = state.n)
            for i in xrange(100000 - state.n):
                model.update(dataset.next())
                if i and i % 1000 == 0:
                    if channel.save_and_switch() == 'stop':
                        state.n += i + 1
                        rval = channel.INCOMPLETE
                        break
            else:
                state.result = model.cost(some_test_set)
                rval = channel.COMPLETE
            cPickle.dump(model, open('model', 'w'))
            return rval

        And then you could run it this way:

        jobman cmdline my_experiments.experiment \\
                           some_param=1 \\
                           other_param=0.4

        Or this way:

        jobman sqlschedule postgres://user:pass@host[:port]/dbname?table=tablename \\
                           my_experiments.experiment \\
                           some_param=1 \\
                           other_param=0.4

        jobman sql postgres://user:pass@host[:port]/dbname?table=tablename exproot

        You need to make sure that the module `my_experiments` is accessible
        from python. You can check with the command

        $ python -m my_experiments
        """
    else:
        helptext = runner_registry.get(topic, (None, None))[1]
        if helptext is None:
            return runner_help(options, topic=None)
    print format_help(helptext)
    if runner_registry.get(topic, [None])[0]:
        runner_registry.get(topic, [None])[0].print_help()

runner_registry['help'] = (None, runner_help)
