from __future__ import with_statement 
import sys
import os
import commands
import os.path
import glob
import time
import copy

from tools import DD, flatten
import parse
from sql import RUNNING, DONE, START

from optparse import OptionParser
from runner import runner_registry
from contextlib import contextmanager
import platform

CACHESYNC_VERBOSE = False
CACHESYNC_LOCK = True
_endmsg = "so 'jobman cachesync' will be unsafe" \
        + " (might cause corruption if called around the time the job ends)."

def _lockfile_not_available(host_string=None):
    if host_string:
        retcode, output = commands.getstatusoutput("ssh "+host_string+" \"lockfile -v\"")
        if not output.startswith("lockfile v"):
            return "Remote host seems to not have the 'lockfile' utility,"\
                    + _endmsg
    else:
        retcode, output = commands.getstatusoutput("lockfile -v")
        if not output.startswith("lockfile v"):
            return "'lockfile' utility apparently not available, " + _endmsg

    return None

@contextmanager
def cachesync_lock(host_string, tmp_dir_to_lock):
    '''
    Used to make sure we don't rsync from the two sides at the same
    time. Relies on DistributedLock, below, which itself relies on 
    the "lockfile" utility under UNIX.

    Doesn't do anything when lockfile is not available on target
    machine (ie. cluster node).

    Example usage:
    from __future__ import with_statement
    with cachesync_lock("target_machine", "/tmp/tmpDaiDWEC"):
        # do stuff, notably rsync
        # lock will be released automatically after the "with"

    which will create the lock /tmp/tmpDaiDWEC_cachesync_lock
    on machine "target_machine" through SSH.

    Parameters
    ----------
    host_string : str
        See DistributedLock
    tmp_dir_to_lock : str
        Directory in which files are cached locally, on the node.
        This is used as a basis for the lockfile name; we simply
        append "_cachesync_lock" to this path, obtaining a file
        name.

        This therefore requires that we can create files in the
        parent directory.
    '''
    lockfile_errstr = _lockfile_not_available(host_string)

    lock = None
    if not CACHESYNC_LOCK:
        lockfile_errstr = "CACHESYNC_LOCK is False, " + _endmsg

    if lockfile_errstr:
        print >> sys.stderr, "WARNING:", lockfile_errstr
    else:
        lock_path = tmp_dir_to_lock + "_cachesync_lock"
        if tmp_dir_to_lock[-1] == "/":
            lock_path = tmp_dir_to_lock[:-1] + "_cachesync_lock"

        lock = DistributedLock(host_string, lock_path)

        if CACHESYNC_VERBOSE:
            print "Attempting to acquire lock", str(lock)

        return_code = lock.acquire()

        if CACHESYNC_VERBOSE:
            print "Lock acquired"

        if return_code != 0:
            # this is REALLY unlikely, as it should not result of a timeout,
            # and should not result of improper rights... check host_string
            # and lock_path, to see if we have proper rights on the file
            # This CAN happen when killing the program, though, at least.
            raise RuntimeError('cachesync lock could not be acquired.')
        
    try:
        yield
    finally:
        if lock:
            lock.release()

class DistributedLock(object):
    def __init__(self, host_string, lock_path,
                        force_if_older=15, retries=20, sleeptime=1):
        '''
        Used to make sure we don't rsync from the two sides at the same
        time. Yet this could also be used for other locks, if the need
        ever arises.

        Uses the UNIX 'lockfile' utility on the node where the job is
        executing. It should work properly as we're using a local file in the
        /tmp directory of the node.

        Parameters
        ----------
        host_string : str or None
            String to use in calling SSH to connect to the node, e.g. 
            "username:password@my.node.address", or None
            if it's called from the job itself (ie. locally on the node)
        lock_path : str
            Path of the file to use. MUST BE IN A LOCAL DIRECTORY such
            as /tmp, otherwise there might be synchronization problems
            (and there would be no point in using these locks).
            Obviously both the local node and the remote host should
            use the same lock_path.
        force_if_older : int
            Force acquiring the lock if more than this number of minutes
            have passed since its creation (in case where the other caller
            crashed).
        retries : int
            Number of retries. Default combination of retries and sleeptime
            should be enough to outlive force_if_older, so the case where
            lock truly fails should almost be non existent (if ssh works).
        sleeptime : int
            Number of time (in minutes) to wait before retrying to create
            the file.
        '''

        # no point in using os.path.join, as this only works under unix
        # and, also, this might be called from a host which is under
        # windows
        self.lock_path = lock_path

        self.host_string = host_string

        self.force_if_older = force_if_older
        self.retries = retries
        self.sleeptime = sleeptime

        self.lock_acquired = False

    def acquire(self):
        command = "lockfile -r " + str(self.retries)
        command += " -" + str(self.sleeptime*60)
        command += " -l " + str(self.force_if_older * 60)
        command += " " + self.lock_path

        return_code = self.exec_command(command)

        if return_code == 0:
            self.lock_acquired = True

        return return_code

    def release(self):
        if self.lock_acquired:
            command = "rm -f " + self.lock_path        
            return self.exec_command(command)

    def exec_command(self, command):
        if not self.host_string:
            retcode = os.system(command)
        else:
            command = "ssh " + self.host_string + " \"" + command + "\""
            retcode = os.system(command)
     
        return retcode

    def __str__(self):
        return "DistributedLock(" + str(self.host_string) + ", " + str(self.lock_path) + ")"
     
def sync_single_directory(dir_path, all_jobs=None, force=False):
    if not all_jobs:
        conf = DD(parse.filemerge(os.path.join(dir_path, 'current.conf')))
    else:
        conf = [i for i in all_jobs if str(i.id) == os.path.split(dir_path)[-1]]
        assert len(conf)==1
        conf = conf[0]

    if 'jobman.status' not in conf\
       or 'jobman.sql.host_workdir' not in conf \
       or 'jobman.sql.host_name' not in conf:
        print "abort for", dir_path, " because at least one of jobman.status,", \
                "jobman.sql.host_workdir or jobman.sql.host_name is not specified."
        print "Try giving the --sql option if possible"
        return

    if conf['jobman.status'] != RUNNING:
        if force and conf['jobman.status'] == DONE:
            print "sync forced for complete job", dir_path
        else:
            print "won't sync", dir_path, "as job is not running (no sync to do)"
            return

    perform_sync(dir_path, conf)

def perform_sync(dir_path, conf):
    remote_dir = copy.copy(conf['jobman.sql.host_workdir'])
    remote_host = copy.copy(conf['jobman.sql.host_name'])

    # we add a trailing slash, otherwise it'll create
    # the directory on destination
    if remote_dir[-1] != "/":
        remote_dir += "/"

    host_string = remote_host + ":" + remote_dir

    with cachesync_lock(remote_host, remote_dir):
        manualtest_will_perform_sync()

        rsync_command = 'rsync -a "%s" "%s"' % (host_string, dir_path)
        print rsync_command

        return_code = os.system(rsync_command)

        print "return code was for last command was", return_code

def sync_all_directories(base_dir, all_jobs=None, force=False):
    oldcwd = os.getcwd()
    os.chdir(base_dir)

    all_dirs = glob.glob("*/current.conf")

    if len(all_dirs) == 0:
        print "No subdirectories containing a file named 'current.conf' found."

    os.chdir(oldcwd)

    for dir_and_file in all_dirs:
        dir, file = dir_and_file.split("/")

        full_path = os.path.join(base_dir, dir)

        sync_single_directory(full_path, all_jobs, force)

def cachesync_runner(options, dir):
    """
    Syncs the working directory of jobs with remote cache.

    Usage: cachesync [options] <path_to_job(s)_workingdir(s)>

    (For this to work, though, you need to do a channel.save() at least
    once in your job before calling cachesync, otherwise the host_name
    and host_workdir won't be set in current.conf)

    For the purpose of this command, see below.

    It can either sync a single directory, which must contain "current.conf"
    file which specifies the remote host and directory. Example for a single
    directory:

        # this syncs the current directory
        jobman cachesync .

        # this syncs another directory
        jobman cachesync myexperiment/mydbname/mytablename/5

    It can also sync all subdirectories of the directory you specify.
    You must use the -m (or --multiple) option for this.
    Each subdirectory (numbered 1, 2 ... etc based on job number) must
    contain a "current.conf" file specifying the remote host and directory.
    Examples:

        # syncs all subdirectories 1, 2 ...
        jobman cachesync -m myexperiment/mydbname/mytablename 

    Normally completed jobs (status = DONE) won't be synced based on
    the "status" set in current.conf. Yet you can force sync by using
    the -f or --force option.

    --sql=dbdesc is an option that allow to get from the db missing info from
    the current.conf file. Same syntax as the sql command.

    Purpose of this command
    -----------------------

    To clarify the purpose of the cachesync command: when launching jobs, 
    working directories are created for each job. For example, when launching:

    dbidispatch jobman sql 'postgres://user@gershwin/mydatabase?table=mytable' .

    A directory ``mydatabase`` with subdirectory ``mytable``.

    will be created, containing further subdirectories numbered 1, 2 and 3 
    (based on job id's in the DB). These directories are the working 
    directories of each job. They contain a copy of the stdout and stderr of 
    the job, along with copies of the jobman state (dictionaries in .conf 
    files) and further files created by the job.

    Yet the content of those directories is not updated live during the job. 
    The job runs on a cluster node, and those files are first written to a 
    temporary directory on the node itself. Then, when calling channel.save() 
    or when the job finishes, they're rsync'ed over to the working directory 
    where they should be.

    This is annoying since one can't see how the jobs are doing unless he 
    SSH'es into the cluster node and finds the temporary directory. To 
    alleviate this problem, the cachesync commands copies over the files to 
    the working directory whenever asked to, so it's easier to probe the 
    running jobs state. 
    """
    force = options.force
    multiple = options.multiple
    dbdesc = options.sql
    all_jobs = None
    if dbdesc:
        import api0
        db = api0.open_db(dbdesc, serial=True)

        try:
            session = db.session()
            q = db.query(session)
            all_jobs = q.all()
        finally:
            try:
                session.close()
            except:
                pass


    if multiple:
        sync_all_directories(dir, all_jobs, force)
    else:
        sync_single_directory(dir, all_jobs, force)

################################################################################
### register the command
################################################################################

cachesync_parser = OptionParser(
    usage = '%prog cachesync [options] <path_to_job(s)_workingdir(s)>',
    add_help_option=False)
cachesync_parser.add_option('-f', '--force', dest = 'force', default = False, action='store_true',
                              help = 'force rsync even if the job is complete')
cachesync_parser.add_option('-m', '--multiple', dest = 'multiple', default = False, action='store_true',
                               help = 'sync multiple jobs (in that case, "path_to_job" must be the directory that contains all the jobs, i.e. its subdirectories are 1, 2, 3...)')
cachesync_parser.add_option('', '--sql', dest = 'sql', default = "", action='store',
                               help = 'The db to witch we want to sync with.')

runner_registry['cachesync'] = (cachesync_parser, cachesync_runner)


###############################################################################
# Utility functions for testing (manual tests)
###############################################################################

MANUALTEST_SAVE_COUNT = 0

TEST1 = False
TEST2 = False
TEST3 = False
TEST3_t1 = None

def manualtest_lockandwait_jobman_experiment(state, channel):
    # make sure the info is available to run "jobman cachesync ."
    channel.save()

    manualtest_test1_helper1(channel)
    manualtest_test2_helper1(channel)

    return channel.COMPLETE

def manualtest_before_delete():
    global TEST3, TEST3_t1
    if TEST3:
        # simply sleep, to give time to launch cachesync
        print "OK waiting 30 secs before delete, launch cachesync in other shell"
        TEST3_t1 = time.time()
        time.sleep(30)

def manualtest_will_delete():
    global TEST3, TEST3_t1
    if TEST3:
        print "Total delay (should be > 60 secs) was ", time.time()-TEST3_t1

def manualtest_will_save():
    global TEST1, MANUALTEST_SAVE_COUNT
    if TEST1:
        print "SAVE COUNT = ", MANUALTEST_SAVE_COUNT
        if MANUALTEST_SAVE_COUNT == 4:
            print "Will wait 60 secs before save()ing"
            time.sleep(60)

def manualtest_will_perform_sync():
    global TEST2, TEST3
    if TEST2 or TEST3:
        print "Test 2/3: will sleep in perform_sync, for 60 secs"
        time.sleep(60)

def manualtest_inc_save_count():
    global MANUALTEST_SAVE_COUNT
    MANUALTEST_SAVE_COUNT += 1

def manualtest_test1_helper1(channel):
    global TEST1
    if TEST1:
        print "will run save() for second time"
        channel.save()

def manualtest_test2_helper1(channel):
    global TEST2
    if TEST2:
        has_cachesynced = False
        max_turns = 20
        for i in range(max_turns):
            time1 = time.time()
            print "performing channel.save() then sleep for 5 secs, then loop"
            channel.save()
            time.sleep(5)
            time2 = time.time()
            print "delay was ", time2-time1
            if time2-time1 > 20.0:
                # means the cachesync was performed, waited for some time
                break

'''
The reason I use manual tests is that it's much easier to run concurrency tests like these is that the test setup would be very complicated otherwise (need to insert jobs in db, get called back through 'jobman sql' etc.).

Description of manual tests:

First, create a lot of jobs which will call the function above through the sql runner:

jobman sqlschedules  postgres://ift6266h10@gershwin/ift6266h10_sandbox_db/tests_cachesync3 jobman.cachesync_runner.manualtest_lockandwait_jobman_experiment 'myparam={{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40}}'

This should be enough, otherwise just add to the list. To run one of them, place yourself in a temporary directory and run:

jobman sql postgres://ift6266h10@gershwin/ift6266h10_sandbox_db/tests_cachesync3 .

Before proceeding, set:
* CACHESYNC_VERBOSE = True, above
* uncomment lines containing "cachesync_runner.manualtest..." in sql_runner.py

Now, the test cases:

* To check that...

    * TEST 1: we can't run cachesync while channel.save() is running
        * set TEST1 = True above
        * from some shell (shell 1) call jobman sql (above)
        * in another shell (shell 2), place yourself in the directory of the job (check its number)
        * then call "jobman cachesync ." (in shell 2)
        * this should wait about 1 minute, then leave with an error message saying that rsync could not run as the directory as now been deleted
        * reset TEST1 = False

    * TEST 2: we can't run channel.save() while cachesync is running
        * set TEST2 = True above
        * from some shell (shell 1) call jobman sql as above
            * now you have ~100 secs to run the instructions below
        * in another shell (shell 2), place yourself in the directory of the job (check its number)
        * then call "jobman cachesync ." (in shell 2)
        * in shell 1, the current save() should be waiting for the lock
        * when job in shell 2 finishes, job in shell 1 should finish too
        * check in stdout to see if the last delay for save() was more then 5 seconds, something around 65 seconds in fact
        * set TEST2 = False above

    * TEST 3: we can't delete temporary directory while cachesync is running
        * set TEST3 = True above
        * from some shell (shell 1) call jobman sql as above
        * you should see a message saying that you have 30 secs before the delete
        * in another shell (shell 2), place yourself in the directory of the job (check its number)
        * then call "jobman cachesync -f ." (in shell 2)
            * notice the -f, as the job is now marked as status = COMPLETE
            * this should sleep for 60 secs
        * the delay printed (see stdout file) should be more than 30, something ~70 secs
        * set TEST3 = False

 
    * TEST 4: we can't run cachesync while temporary directory is being deleted
        * TBD, not a very important case
'''

