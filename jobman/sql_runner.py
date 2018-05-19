""" WRITEME """
from __future__ import with_statement

try:
    import sql
    from cachesync_runner import cachesync_lock
    import cachesync_runner
except:
    pass

import os
import tempfile
import shutil
import socket
import optparse
import time
import random
import re
from optparse import OptionParser

from tools import expand, flatten, resolve, UsageError
from runner import runner_registry
from channel import StandardChannel, JobError
import parse
from sql import START, RUNNING, DONE, ERR_START, ERR_SYNC, ERR_RUN, CANCELED
from api0 import open_db


###############################################################################
### Channels
###############################################################################

###############################################################################
### RSync channel
###############################################################################


class RSyncException(Exception):
    pass


class RSyncChannel(StandardChannel):
    """ WRITEME """

    def __init__(self, path, remote_path, experiment, state,
                 redirect_stdout=False, redirect_stderr=False,
                 finish_up_after=None, save_interval=None):
        super(RSyncChannel, self).__init__(path, experiment, state,
                                           redirect_stdout, redirect_stderr,
                                           finish_up_after, save_interval)

        ssh_prefix = 'ssh://'
        if remote_path.startswith(ssh_prefix):
            remote_path = remote_path[len(ssh_prefix):]
            colon_pos = remote_path.find(':')
            self.host = remote_path[:colon_pos]
            self.remote_path = remote_path[colon_pos + 1:]
        else:
            self.host = ''
            self.remote_path = os.path.realpath(remote_path)

        # If False, do not rsync during save.
        # This is useful if we have to halt with short notice.
        self.sync_in_save = True

    def rsync(self, direction, num_retries=3, exclusions=['*.no_sync']):
        """The directory at which experiment-related files are stored.
        """
        path = self.path

        remote_path = self.remote_path
        if self.host:
            remote_path = ':'.join([self.host, remote_path])

        excludes = ' '.join('--exclude="%s"' % e for e in exclusions)
        # TODO: use something more portable than os.system
        if direction == 'push':
            rsync_cmd = 'rsync -a %s "%s/" "%s/"' % (excludes, path,
                                                     remote_path)
        elif direction == 'pull':
            rsync_cmd = 'rsync -a %s "%s/" "%s/"' % (excludes, remote_path,
                                                     path)
        else:
            raise RSyncException('invalid direction', direction)

        keep_trying = True
        rsync_rval = 1  # some non-null value

        with cachesync_lock(None, self.path):
            # Useful for manual tests; leave this there, just commented.
            #cachesync_runner.manualtest_will_save()

            # allow n-number of retries, with random hold-off between retries
            attempt = 0
            while rsync_rval != 0 and keep_trying:
                rsync_rval = os.system(rsync_cmd)

                if rsync_rval != 0:
                    attempt += 1
                    keep_trying = attempt < num_retries
                    # wait anywhere from 30s to [2,4,6] mins before retrying
                    if keep_trying:
                        r = random.randint(30, attempt * 120)
                        print >> os.sys.stderr, ('RSync Error at attempt %i/%i'
                                                 ': sleeping %is') % (
                                                     attempt, num_retries, r)
                        time.sleep(r)

        if rsync_rval != 0:
            raise RSyncException('rsync failure', (rsync_rval, rsync_cmd))

    def touch(self):
        if self.host:
            host = self.host
            touch_cmd = ('ssh %(host)s  "mkdir -p \'%(path)s\'"' % dict(
                host=self.host,
                path=self.remote_path))
        else:
            touch_cmd = ("mkdir -p '%(path)s'" % dict(path=self.remote_path))
        # print "ECHO", touch_cmd
        touch_rval = os.system(touch_cmd)
        if 0 != touch_rval:
            raise Exception('touch failure', (touch_rval, touch_cmd))

    def pull(self, num_retries=3):
        return self.rsync('pull', num_retries=num_retries)

    def push(self, num_retries=3):
        return self.rsync('push', num_retries=num_retries)

    def save(self, num_retries=3):
        # Useful for manual tests; leave this there, just commented.
        #cachesync_runner.manualtest_inc_save_count()

        if self.sync_in_save:
            super(RSyncChannel, self).save()
            self.push(num_retries=num_retries)
        #TODO: else: update current.conf with only state.jobman, push current.conf

    def setup(self):
        self.touch()
        self.pull()
        super(RSyncChannel, self).setup()


###############################################################################
### DB + RSync channel
###############################################################################

class DBRSyncChannel(RSyncChannel):
    """ WRITEME """

    RESTART_PRIORITY = 2.0

    def __init__(self, db, path, remote_root,
                 redirect_stdout=False, redirect_stderr=False,
                 finish_up_after=None, save_interval=None):

        self.db = db

        self.dbstate = sql.book_dct_postgres_serial(self.db)
        if self.dbstate is None:
            raise JobError(JobError.NOJOB,
                           'No job was found to run.')

        print "Selected job id=%d in table=%s in db=%s" % (
            self.dbstate.id, self.db.tablename, self.db.dbname)

        try:
            state = expand(self.dbstate)
            # The id isn't set by the line above
            state["jobman.id"] = self.dbstate.id
            if "dbdict" in state:
                state.jobman = state.dbdict
            experiment = resolve(state.jobman.experiment)
            remote_path = os.path.join(remote_root, self.db.dbname,
                                       self.db.tablename, str(self.dbstate.id))
            super(DBRSyncChannel, self).__init__(path, remote_path,
                                                 experiment, state,
                                                 redirect_stdout,
                                                 redirect_stderr,
                                                 finish_up_after,
                                                 save_interval)
        except:
            self.dbstate['jobman.status'] = self.ERR_START
            raise

    def save(self, num_retries=3):
        # If the DB is not writable, the rsync won't happen
        # If the DB is up, but rsync fails, the status will be ERR_SYNC,
        # but self.state will not be updated in the database.

        session = self.db.session()
        try:
            # Test write access to DB
            # If it fails after num_retries trials, update_in_session will
            # raise an Exception, so save() will exit, before the rsync.
            self.dbstate.update_in_session({'jobman.status': self.ERR_SYNC},
                                           session,
                                           _recommit_times=num_retries)

            # save self.state in file current.state, and rsync
            # If the rsync fails after num_retries, an Exception will be
            # raised, and save() will exit before 'jobman.status' is
            # changed back.
            super(DBRSyncChannel, self).save(num_retries=num_retries)

            if self.sync_in_save:
                # update DB
                self.dbstate.update_in_session(flatten(self.state), session,
                        _recommit_times=num_retries)
            else:
                # update only jobman.*
                state_jobman = flatten({'jobman': self.state.jobman})
                self.dbstate.update_in_session(state_jobman, session,
                        _recommit_times=num_retries)

        finally:
            session.close()

    def setup(self):
        # Extract a single experiment from the table that is not
        # already running.  set self.experiment and self.state
        super(DBRSyncChannel, self).setup()

        self.state.jobman.sql.host_name = socket.gethostname()

        def state_del(state, keys):
            # Delete from the state the following key if present
            for key in keys:
                if hasattr(state, key):
                    del state[key]

        #put jobs scheduler info into the state
        condor_slot = os.getenv("_CONDOR_SLOT")
        sge_task_id = os.getenv('SGE_TASK_ID')
        pbs_task_id = os.getenv('PBS_JOBID')
        if condor_slot:
            self.state.jobman.sql.condor_slot = condor_slot
            job_ad_file = os.getenv("_CONDOR_JOB_AD", None)
            if job_ad_file:
                f = open(job_ad_file)
                try:
                    for line in f.readlines():
                        if line.startswith('GlobalJobId = '):
                            self.state.jobman.sql.condor_global_job_id = line.split('=')[1].strip()[1:-1]
                        elif line.startswith('Out = '):
                            self.state.jobman.sql.condor_stdout = line.split('=')[1].strip()[1:-1]
                        elif line.startswith('Err = '):
                            self.state.jobman.sql.condor_stderr = line.split('=')[1].strip()[1:-1]
                        elif line.startswith('OrigIwd = '):
                            self.state.jobman.sql.condor_origiwd = line.split('=')[1].strip()[1:-1]
                finally:
                    f.close()
        elif sge_task_id:
            self.state.jobman.sql.sge_task_id = sge_task_id
            self.state.jobman.sql.job_id = os.getenv('JOB_ID')
            self.state.jobman.sql.sge_stdout = os.getenv('SGE_STDOUT_PATH')
            self.state.jobman.sql.sge_stderr = os.getenv('SGE_STDERR_PATH')
        elif pbs_task_id:
            self.state.jobman.sql.pbs_task_id = pbs_task_id
            self.state.jobman.sql.pbs_queue = os.getenv('PBS_QUEUE')
            self.state.jobman.sql.pbs_arrayid = os.getenv('PBS_ARRAYID')
            self.state.jobman.sql.pbs_num_ppn = os.getenv('PBS_NUM_PPN')

        #delete old jobs scheduler info into the state
        #this is needed in case we move a job to a different system.
        #to know where it is running now.
        key_to_del = []
        if not condor_slot:
            key_to_del.extend(['jobman.sql.condor_global_job_id',
                               'jobman.sql.condor_stdout',
                               'jobman.sql.condor_stderr',
                               'jobman.sql.condor_origiwd',
                               'jobman.sql.condor_slot'])
        if not sge_task_id:
            key_to_del.extend(['jobman.sql.sge_task_id',
                               'jobman.sql.job_id',
                               'jobman.sql.sge_stdout',
                               'self.state.jobman.sql.sge_stderr'])
        if not pbs_task_id:
            key_to_del.extend(['jobman.sql.pbs_task_id',
                               'jobman.sql.pbs_queue',
                               'jobman.sql.pbs_arrayid',
                               'jobman.sql.pbs_num_ppn'])

        flattened_state = flatten(self.state)
        deleted = False
        for k in key_to_del:
            if k in flattened_state:
                del flattened_state[k]
                deleted = True
        if deleted:
            self.state = expand(flattened_state)

        self.state.jobman.sql.start_time = time.time()
        self.state.jobman.sql.host_workdir = self.path
        self.dbstate.update(flatten(self.state))

    def touch(self):
        try:
            super(DBRSyncChannel, self).touch()
        except:
            self.dbstate['jobman.status'] = self.ERR_START
            raise

    def run(self):
        # We pass the force flag as True because the status flag is
        # already set to RUNNING by book_dct in __init__
        v = super(DBRSyncChannel, self).run(force=True)
        if (v is self.INCOMPLETE and
            self.state.jobman.sql.priority < self.RESTART_PRIORITY):
            self.state.jobman.sql.priority = self.RESTART_PRIORITY
            self.save()
        return v

###############################################################################
### Runners
###############################################################################

###############################################################################
### sqlschedule
###############################################################################

parser_sqlschedule = OptionParser(
    usage='%prog sqlschedule [options] <tablepath> <experiment> <parameters>',
    add_help_option=False)
parser_sqlschedule.add_option('-f', '--force', action='store_true',
                              dest='force', default=False,
                              help='force adding the experiment to the database even if it is already there')
parser_sqlschedule.add_option('-p', '--parser', action='store',
                              dest='parser', default='filemerge',
                              help='parser to use for the argument list provided on the command line (takes a list of strings, returns a state)')


def runner_sqlschedule(options, dbdescr, experiment, *strings):
    """
    Schedule a job to run using the sql command.

    Usage: sqlschedule <tablepath> <experiment> <parameters>

    See the experiment and parameters topics for more information about
    these parameters.

    Assuming that a postgres database is running on port `port` of
    `host`, contains a database called `dbname` and that `user` has the
    permissions to create, read and modify tables on that database,
    tablepath should be of the following form:

        postgres://user:pass@host[:port]/dbname?table=tablename

    If no table is named `tablename`, one will be created
    automatically. The state corresponding to the experiment and
    parameters specified in the command will be saved in the database,
    but no experiment will be run.

    To run an experiment scheduled using sqlschedule, see the sql
    command.

    Example use:
        jobman sqlschedule postgres://user:pass@host[:port]/dbname?table=tablename \\
            mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03

        you can use the jobman.experiments.example1 as a working
        mymodule.my_experiment
    """
    db = open_db(dbdescr, serial=True)

    parser = getattr(parse, options.parser, None) or resolve(options.parser)

    state = parser(*strings)
    resolve(experiment)  # we try to load the function associated to the experiment
    state['jobman.experiment'] = experiment
    sql.add_experiments_to_db([state], db, verbose=1, force_dup=options.force)

runner_registry['sqlschedule'] = (parser_sqlschedule, runner_sqlschedule)

###############################################################################
### sqlschedules
###############################################################################

parser_sqlschedules = OptionParser(
    usage='%prog sqlschedule [options] <tablepath> <experiment> <parameters>',
    add_help_option=False)
parser_sqlschedules.add_option('-f', '--force', action='store_true',
                               dest='force', default=False,
                               help='force adding the experiment to the database even if it is already there')
parser_sqlschedules.add_option('-r', '--repeat',
                               dest='repeat', default=1, type='int',
                               help='repeat each jobs N times')
parser_sqlschedules.add_option('-p', '--parser', action='store',
                               dest='parser', default='filemerge',
                               help='parser to use for the argument list provided on the command line (takes a list of strings, returns a state)')
parser_sqlschedules.add_option('-q', '--quiet', action='store_true',
                               dest='quiet', default=False,
                               help='print only the number of added jobs on the number of proposed addition')


def generate_combination(repl):
    if repl == []:
        return []
    else:
        res = []
        x = repl[0]
        res1 = generate_combination(repl[1:])
        for y in x:
            if res1 == []:
                res.append([y])
            else:
                res.extend([[y] + r for r in res1])
        return res


def generate_commands(sp):
### Find replacement lists in the arguments
    repl = []
    p = re.compile('\{\{\S*?\}\}')
    for arg in sp:
        reg = p.findall(arg)
        if len(reg) == 1:
            reg = p.search(arg)
            curargs = reg.group()[2:-2].split(",")
            newcurargs = []
            for curarg in curargs:
                new = p.sub(curarg, arg)
                newcurargs.append(new)
            repl.append(newcurargs)
        elif len(reg) > 1:
            s = p.split(arg)
            tmp = []
            for i in range(len(reg)):
                if s[i]:
                    tmp.append(s[i])
                tmp.append(reg[i][2:-2].split(","))
            i += 1
            if s[i]:
                tmp.append(s[i])
            repl.append(generate_combination(tmp, ''))
        else:
            repl.append([arg])
    argscombination = generate_combination(repl)
    args_modif = generate_combination([x for x in repl if len(x) > 1])

    return (argscombination, args_modif)


def runner_sqlschedules(options, dbdescr, experiment, *strings):
    """
    Schedule multiple jobs from the command line to run using the sql command.

    Usage: sqlschedules <tablepath> <experiment> <parameters>

    See the sqlschedule command for <tablepath> <experiment>
    We accept the dbidispatch syntax:
    where <parameters> is interpreted as follows:

      The parameters may contain one or many segments of the form
      {{a,b,c,d}}, which generate multiple jobs to execute. Each
      segement will be replaced by one value in the segment separated
      by comma. The first will have the a value, the second the b
      value, etc. If their is many segment, it will generate the
      cross-product of possible value between the segment.
    """
    parser = getattr(parse, options.parser, None) or resolve(options.parser)

    db = open_db(dbdescr, serial=True)

    ### resolve(experiment) # we try to load the function associated to the experiment

    verbose = not options.quiet

    (commands,choise_args)=generate_commands(strings)
    if verbose:
        print commands, choise_args

    if options.force:
        for cmd in commands:
            state = parser(*cmd)
            state['jobman.experiment'] = experiment
            sql.add_experiments_to_db([state] * (options.repeat),
                                      db, verbose=verbose, force_dup=True)
        if options.quiet:
            print "Added %d jobs to the db" % len(commands)
    else:
        #if the first insert fail, we won't force the other as the
        #force option was not gived.
        failed = 0
        for cmd in commands:
            state = parser(*cmd)
            state['jobman.experiment'] = experiment
            ret = sql.add_experiments_to_db([state], db,
                                            verbose=verbose,
                                            force_dup=options.force)
            if ret[0][0]:
                sql.add_experiments_to_db([state] * (options.repeat-1), db,
                                          verbose=verbose, force_dup=True)
            else:
                failed+=1
                if verbose:
                    print "The last cmd failed to insert, we won't repeat it. use --force to force the duplicate of job in the db."
        print "Added", len(commands) - failed, "on", len(commands), "jobs"
runner_registry['sqlschedules'] = (parser_sqlschedules, runner_sqlschedules)

 ################################################################################
# ### sqlschedule_filemerge
 ################################################################################

# parser_sqlschedule_filemerge = OptionParser(
#     usage = '%prog sqlschedule_filemerge [options] <tablepath> <experiment> <parameters|files>',
#     add_help_option=False)
# parser_sqlschedule_filemerge.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
#                                         help = 'force adding the experiment to the database even if it is already there')

# def runner_sqlschedule_filemerge(options, dbdescr, experiment, *files):
#     """
#     Schedule a job to run using the sql command using parameter files.

#     This command is to sqlschedule what the filemerge command is to
#     cmdline.
#     """

#     try:
#         username, password, hostname, port, dbname, tablename \
#             = sql.parse_dbstring(dbdescr)
#     except Exception, e:
#         raise UsageError('Wrong syntax for dbdescr',e)

#     db = sql.postgres_serial(
#         user = username,
#         password = password,
#         host = hostname,
#         port = port,
#         database = dbname,
#         table_prefix = tablename)

#     _state = parse_files(*files)

# #     with open(mainfile) as f:
# #         _state = parse(*map(str.strip, f.readlines()))
# #     for file in other_files:
# #         if '=' in file:
# #             _state.update(parse(file))
# #         else:
# #             with open(file) as f:
# #                 _state.update(parse(*map(str.strip, f.readlines())))

#     state = _state

#     resolve(experiment) # we try to load the function associated to the experiment
#     state['jobman.experiment'] = experiment
#     sql.add_experiments_to_db([state], db, verbose = 1, force_dup = options.force)

# runner_registry['sqlschedule_filemerge'] = (parser_sqlschedule_filemerge, runner_sqlschedule_filemerge)


################################################################################
### sql
################################################################################

parser_sql = OptionParser(usage='%prog sql [options] <tablepath> <exproot>',
                          add_help_option=False)
parser_sql.add_option('-n', dest='n', type='int', default=1,
                      help='Run N experiments sequentially (default 1) '
                      '(if N is <= 0, runs as many experiments as possible).')
parser_sql.add_option('--finish-up-after', action='store',
                      dest='finish_up_after',
                      default=None,
                      help='Duration (in seconds) after which the experiment will be told to "finish up", i.e., to reach the next checkpoint, save, and exit')
parser_sql.add_option('--save-every', action='store', dest='save_every',
                      default=None,
                      help='Interval (in seconds) between checkpoints. --save-every=3600 will tell the experiment to reach the next checkpoint and save (and go on) every hour')
parser_sql.add_option('-w', '--workdir', action='store',
                      dest='workdir', default=None,
                      help='the working directory in which to run the experiment')
parser_sql.add_option('--workdir-dir', action='store',
                      dest='workdir_dir', default=None,
                      help='The directory where the workdir should be created')
parser_sql.add_option('--import', action='store',
                      dest='modules', default=None,
                      help='Modules to be loaded before the experiment begins')


def runner_sql(options, dbdescr, exproot):
    """
    Run jobs from a sql table.

    Usage: sql [options] <tablepath> <exproot>

    The jobs should be scheduled first with the sqlschedule command.

    Assuming that a postgres database is running on port `port` of
    `host`, contains a database called `dbname` and that `user` has the
    permissions to create, read and modify tables on that database,
    tablepath should be of the following form:

        postgres://user:pass@host[:port]/dbname?table=tablename

    exproot can be a local path or a remote path. Examples of exproots:
        /some/local/path
        ssh://some_host:/some/remote/path # relative to the filesystem root
        ssh://some_host:other/remote/path # relative to the HOME on some_host

    The exproot will contain a subdirectory hierarchy corresponding to
    the dbname, tablename and job id which is a unique integer.

    The sql runner will pick any job in the table which is not running
    and is not done and will terminate when that job ends. You may call
    the same command multiple times, sequentially or in parallel, to
    run as many unfinished jobs as have been scheduled in that table
    with sqlschedule.

    Example use:
        jobman sql \\
            postgres://user:pass@host[:port]/dbname?table=tablename \\
            ssh://central_host:myexperiments
    """
    if options.modules:
        modules = options.modules.split(',')
    else:
        modules = []
    for module in modules:
        __import__(module, fromlist=[])

    db = open_db(dbdescr, serial=True)
    n = options.n if options.n else -1
    nrun = 0
    try:
        while n != 0:
            if options.workdir:
                workdir = options.workdir
            else:
                if options.workdir_dir and not os.path.exists(options.workdir_dir):
                    os.mkdir(options.workdir_dir)
                workdir = tempfile.mkdtemp(dir=options.workdir_dir)
            print "The working directory is:", os.path.join(os.getcwd(), workdir)

            channel = DBRSyncChannel(db,
                                     workdir,
                                     exproot,
                                     redirect_stdout=True,
                                     redirect_stderr=True,
                                     finish_up_after=options.finish_up_after or None,
                                     save_interval = options.save_every or None
                                     )
            channel.run()

            # Useful for manual tests; leave this there, just commented.
            #cachesync_runner.manualtest_before_delete()
            with cachesync_lock(None, workdir):
                # Useful for manual tests; leave this there, just
                #commented.  cachesync_runner.manualtest_will_delete()

                shutil.rmtree(workdir, ignore_errors=True)

            n -= 1
            nrun += 1
    except JobError, e:
        if e.args[0] == JobError.NOJOB:
            print 'No more jobs to run (run %i jobs)' % nrun

runner_registry['sql'] = (parser_sql, runner_sql)

parser_sqlview = OptionParser(usage='%prog sqlview <tablepath> <viewname>',
                              add_help_option=False)
parser_sqlview.add_option('-d', '--drop', action="store_true", dest="drop",
                          help='If true, will drop the view. (default false)')
parser_sqlview.add_option('-q', '--quiet', action="store_true", dest="quiet",
                          help='be less verbose')


def runner_sqlview(options, dbdescr, viewname):
    """
    Create/drop a view of the scheduled experiments.

    Usage: jobman sqlview <tablepath> <viewname>

    The jobs should be scheduled first with the sqlschedule command.
    Also, it is more interesting to execute it after some experiment have
    finished.

    Assuming that a postgres database is running on port `port` of
    `host`, contains a database called `dbname` and that `user` has the
    permissions to create, read and modify tables on that database,
    tablepath should be of the following form:

        postgres://user:pass@host[:port]/dbname?table=tablename


    Example use:
        That was executed and at least one exeperiment was finished.
        jobman sqlschedule postgres://user:pass@host[:port]/dbname?table=tablename \\
            mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03
        Now this will create a view with a columns for each parameter and
        key=value set in the state by the jobs.
        jobman sqlview postgres://user:pass@host[:port]/dbname?table=tablename viewname

        you can use the jobman.experiments.example1 as a working
        mymodule.my_experiment
    """
    db = open_db(dbdescr, serial=True)

    if options.drop:
        db.dropView(viewname, not options.quiet)
    else:
        db.createView(viewname, not options.quiet)

runner_registry['sqlview'] = (parser_sqlview, runner_sqlview)


def to_status_number(i):
    if i == 'START':
        status = START
    elif i == 'RUNNING':
        status = RUNNING
    elif i == 'DONE':
        status = DONE
    elif i == 'ERR_START':
        status = ERR_START
    elif i == 'ERR_SYNC':
        status = ERR_SYNC
    elif i == 'ERR_RUN':
        status = ERR_RUN
    elif i == 'CANCELED':
        status = CANCELED
    else:
        try:
            status = int(i)
            assert status in [0, 1, 2, 3, 4, 5, -1]
        except Exception, e:
            raise ValueError("The status must be a str in START, RUNNING, DONE, ERR_START, ERR_SYNC, ERR_RUN, CANCELED or a int in 0,1,2,3,4,5,-1")
    return status

parser_sqlstatus = OptionParser(usage='%prog sqlstatus [--print=KEY] [--status=JOB_STATUS] [--set_status=JOB_STATUS] [--fselect=lambda blob: ...] [--resert_prio] [--select=key=value] [--quiet] [--ret_nb_jobs] <tablepath> <job id>...',
                              add_help_option=False)
parser_sqlstatus.add_option('--set_status', action="store",
                            dest="set_status", default='',
                          help='If present, will change the status of jobs to START,RUNNING,DONE,ERR_START,ERR_SYNC,ERR_RUN,CANCELED. depending of the value gived to this option (default don\'t change the status)')
parser_sqlstatus.add_option('--all', action="store_true", dest="all",
                          help='Append all jobs in the db to the list of jobs.')
parser_sqlstatus.add_option('--status', action="append", dest="status",
                            help='Append jobs in the db with the gived status'
                            ' to the list of jobs. You can pass multiple time'
                            ' this parameter.')
parser_sqlstatus.add_option('--reset_prio', action="store_true", dest="reset_prio",
                          help='Reset the priority to the default.')
parser_sqlstatus.add_option('--ret_nb_jobs', action="store_true", dest="ret_nb_jobs",
                          help='Print only the number of jobs selected.')
parser_sqlstatus.add_option('-q','--quiet', action="store_true", dest="quiet",
                          help='Be less verbose.')
parser_sqlstatus.add_option('--select', action="append", dest="select",
                          help='Append jobs in the db that match that param=value values to the list of jobs. If multiple --select option, matched jobs must support all those restriction')
parser_sqlstatus.add_option('--fselect', action="append", dest="fselect",
                          help='Append jobs in the db that match that param=value values to the list of jobs. If multiple --select option, matched jobs must support all those restriction...')
parser_sqlstatus.add_option('--print', action="append",
                            dest="prints", default=[],
                          help='print the value of the key for the jobs. Accept multiple --print parameter')
parser_sqlstatus.add_option('--print-keys', action="store_true",
                            dest="print_keys", default=[],
                          help='print all keys in the state of the first jobs.')


def runner_sqlstatus(options, dbdescr, *ids):
    """Show the status of jobs. Option allow to change it.

    The --resert_prio option set the priority of the jobs back to the
    default value.

    Example use:

        jobman sqlstatus postgres://user:pass@host[:port]/dbname?table=tablename 10 11

    """
    #we don't want to remove all output when we change the db.
    if options.set_status and options.ret_nb_jobs:
        raise UsageError("The option --set_status and --ret_nb_jobs are mutually exclusive.")

    db = open_db(dbdescr, serial=True)

    if options.set_status:
        try:
            new_status = to_status_number(options.set_status)
        except ValueError:
            raise UsageError("The option --set_status accept only the value START, RUNNING, DONE, ERR_START, ERR_SYNC, ERR_RUN, CANCELED or their equivalent int number")
    else:
        new_status = None

    have_running_jobs = False
    verbose = not options.quiet
    if options.ret_nb_jobs:
        verbose = 0
    else:
        verbose += 1
    ids = list(ids)
    try:
        session = db.session()

        if options.print_keys:
            q = db.query(session)
            job = q.first()
            print "Keys in the state of the first jobs",
            for k in job.keys():
                print k,
            print
            del q, job, k

        if options.status:
            q = db.query(session)
            jobs = []
            for  stat in options.status:
                jobs += q.filter_eq('jobman.status',
                                    to_status_number(stat)).all()

            ids.extend([j.id for j in jobs])
            del jobs, q

        if options.select:
            q = db.query(session)
            j = q.first()
            for param in options.select:
                k, v = param.split('=')
                if k == 'jobman.status':
                    q = q.filter_eq(k, to_status_number(v))
                elif isinstance(j[k], (str, unicode)):
                    q = q.filter_eq(k, v)
                elif isinstance(j[k], float):
                    q = q.filter_eq(k, float(v))
                elif isinstance(j[k], int):
                    q = q.filter_eq(k, int(v))
                else:
                    q = q.filter_eq(k, repr(v))
            jobs = q.all()
            ids.extend([j.id for j in jobs])
            del j, jobs, q

        if options.fselect:
            q = db.query(session)
            jobs = q.all()
            for param in options.fselect:
                k, v = param.split('=', 1)
                f = eval(v)
                for job in jobs:
                    if k in job:
                        if f(job[k]):
                            ids.append(job.id)
                    else:
                        print "job", job.id, "don't have the attribute",k

            del job, jobs, q

        if options.all:
            q = db.query(session)
            jobs = q.all()
            ids.extend([j.id for j in jobs])
            del q, jobs

        # Remove all dictionaries from the session
        session.expunge_all()

        ids = [int(id) for id in ids]
        ids = list(set(ids))
        ids.sort()
        nb_jobs = len(ids)

        for id in ids:
            job = db.get(id)
            if job is None:
                if verbose > 0:
                    print "Job id %s don't exit in the db" % (id)
                nb_jobs -= 1
                continue
            try:
                prio = job['jobman.sql.priority']
            except Exception:
                prio = 'BrokenDB_priority_DontExist'
            try:
                status = job['jobman.status']
            except KeyError:
                status = 'BrokenDB_Status_DontExist'

            if verbose > 1:
                print "Job id %s, status=%d jobman.sql.priority=%s" % (id, status, str(prio)),

                for p in options.prints:
                    try:
                        print '%s=%s' % (p, job[p]),
                    except KeyError:
                        print '%s=KeyDontExist' % (p),
                print

            if status == RUNNING:
                have_running_jobs = True
            if options.set_status:
                job.__setitem__('jobman.status', new_status, session)
                job.update_in_session({}, session)
            if options.reset_prio:
                job.__setitem__('jobman.sql.priority', 1.0, session)
                job.update_in_session({}, session)

        if options.set_status:
            session.commit()
            print "Changed the status to %d for %d jobs" % (new_status, len(ids))
        if options.reset_prio:
            print "Reseted the priority to the default value"
        if new_status == CANCELED and have_running_jobs:
            print "WARNING: Canceled jobs only change the status in the db. Jobs that are already running, will continue to run. If the job finish with status COMPLETE, it will change the status to DONE. Otherwise the status won't be changed"

    finally:
        session.close()

    if options.ret_nb_jobs:
        print nb_jobs


runner_registry['sqlstatus'] = (parser_sqlstatus, runner_sqlstatus)


parser_sqlreload = OptionParser(usage='%prog sqlreload <tablepath> <exproot>/dbname/tablename <id>...',
                                add_help_option=False)
parser_sqlreload.add_option('--all', action="store_true", dest="all",
                          help='If true, will reload all jobs that are in the directory. (default false)')


def runner_sqlreload(options, dbdescr, table_dir, *ids):
    """
    Put data in the experiment directory back in the the sql db.

    Usefull in case you delete the db or part of it.

    Example use:

        jobman sqlreload [--all] postgres://user:pass@host[:port]/dbname?table=tablename ~/expdir/dbname/tablename 10 11
    """
    if table_dir[-1] == os.path.sep:
        table_dir = table_dir[:-1]

    db = open_db(dbdescr, serial=True)

    assert os.path.split(table_dir)[-1] == db.tablename
    assert os.path.split(os.path.split(table_dir)[0])[-1] == db.dbname
    expdir = os.path.split(os.path.split(table_dir)[0])[0]

    if options.all:
        assert len(ids) == 0
        ids = []
        for p in os.listdir(table_dir):
            try:
                ids += [int(p)]
            except ValueError:
                print 'Skipping entry %s, as it is not a jobman id.' % p
    else:
        # Ensure that ids are all integers.
        ids = [int(d) for d in ids]

    try:
        session = db.session()
        for id in ids:
            # Get state dict from the file
            file_name = '%s/%i/current.conf' % (table_dir, id)
            file_state = parse.filemerge(file_name)

            # Get state dict from the DB
            db_state = db.get(id)
            if db_state is None:
                # No such dict exist, we have to insert it, with the right id
                file_state['jobman.id'] = id
                db.insert(file_state, session=session)
            else:
                db_state.update_in_session(file_state, session=session)
                pass
    finally:
        session.close()

runner_registry['sqlreload'] = (parser_sqlreload, runner_sqlreload)
