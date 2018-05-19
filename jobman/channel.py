import signal
import sys
import os
import time
import traceback

from tools import *



################################################################################
### JobError
################################################################################

class JobError(Exception):
    RUNNING = 0
    DONE = 1
    NOJOB = 2


################################################################################
### Channel base class
################################################################################

class Channel(object):

    COMPLETE = property(lambda s:None,
            doc=("Experiments should return this value to "
                "indicate that they are done (if not done, return `INCOMPLETE`"))
    INCOMPLETE = property(lambda s:True,
            doc=("Experiments should return this value to indicate that "
                 "they are not done (usefull if the jobs is interrupted) (if done return `COMPLETE`)"))

    START = property(lambda s: 0,
            doc="jobman.status == START means a experiment is ready to run")
    RUNNING = property(lambda s: 1,
            doc="jobman.status == RUNNING means a experiment is running on jobman_hostname")
    DONE = property(lambda s: 2,
            doc="jobman.status == DONE means a experiment has completed (not necessarily successfully)")
    ERR_START = property(lambda s: 3,
            doc="jobman.status == ERR_START means can't be started for some reason(ex: can't make the destination experiment directory.")
    ERR_SYNC = property(lambda s: 4,
            doc="jobman.status == ERR_SYNC means that the experiment was unable to synchronize the experiment directory.")
    ERR_RUN = property(lambda s: 5,
            doc="jobman.status == ERR_RUN means that the experiment did not return `COMPLETE` or `INCOMPLETE`. As COMPLETE is None, it you did not return, but default it return None and this means `COMPLETE`")
    CANCELED = property(lambda s: -1,
            doc="jobman.status == CANCELED means that user set the job to that mode and don't want to start it. If the jobs is already started and finish, it will change its status to DONE or START depending of the return value. It the started job crash, the status won't change autamatically.")

    # Methods to be used by the experiment to communicate with the channel

    def save(self):
        """
        Save the experiment's state to the various media supported by
        the Channel.
        """
        raise NotImplementedError()

    def switch(self, message = None):
        """
        Called from the experiment to give the control back to the channel.
        The following return values are meaningful:
          * 'stop' -> the experiment must stop as soon as possible. It may save what
            it needs to save. This occurs when SIGTERM or SIGINT are sent (or in
            user-defined circumstances).
          * 'finish-up' -> the experiment should continue until it reaches a point
            where it can be restarted from (check-point), then save and exit.
          * 'save' -> the experiment should continue until it reaches a check-point,
            then save(by calling channel.save()) and continue.
        """
        pass

    def __call__(self, message = None):
        return self.switch(message)

    def save_and_switch(self):
        self.save()
        self.switch()

    # Methods to run the experiment

    def setup(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def run(self):
        pass

################################################################################
### Empty Channel: useful for debugging
################################################################################
class EmptyChannel(Channel):
    def save(self):
        pass

################################################################################
### Channel for a single experiment
################################################################################

class SingleChannel(Channel):

    def __init__(self, experiment, state, finish_up_after=None, save_interval=None):
        self.experiment = experiment
        self.state = state
        self.feedback = None
        self.finish_up_notified = False

        #TODO: make this a property and disallow changing it during a with block
        self.catch_sigterm = True
        self.catch_sigint = True
        try:
            signal.SIGUSR2
            self.catch_sigusr2 = True
        except:
            print 'SingleChannel - warning, cannot use signal.SIGUSR2.'
            self.catch_sigusr2 = False

        #TODO: parse more advanced strings, like [[[dd:]hh:]mm:]ss, <n>d, <n>h, <n>m, ...
        if finish_up_after is not None:
            self.finish_up_after = int(finish_up_after)
        else:
            self.finish_up_after = None
        if save_interval is not None:
            self.save_interval = int(save_interval)
        else:
            self.save_interval = None

    def switch(self, message = None):
        now = time.time()
        if self.feedback is None and not self.finish_up_notified:
            if self.finish_up_after is not None and\
                    now - self.start_time > self.finish_up_after:
                self.finish_up_notified = True
                return 'finish-up'
            if self.save_interval is not None and\
                    now - self.last_saved_time > self.save_interval:
                self.last_saved_time = now
                return 'save'
        else:
            feedback = self.feedback
            self.feedback = None
            return feedback

    def run(self, force = False):
        self.setup()

        status = self.state.jobman.get('status', self.START)
        if status is self.DONE and not force:
            # If you want to disregard this, use the --force flag (not yet implemented)
            raise JobError(JobError.RUNNING,
                           'The job has already completed.')
        elif status is self.RUNNING and not force:
            raise JobError(JobError.DONE,
                           'The job is already running.')
        self.state.jobman.status = self.RUNNING

        v = self.ERR_RUN

        # Python 2.4 compatibility: do not use `with` statement.
        self.__enter__()
        try:
            try:
                v = self.experiment(self.state, self)
            except Exception:
                # The exception info will be passed to the __exit__ method
                # which will raise it.
                pass
        finally:
            print "The experiment returned value is",v
            if self.state.jobman.status is self.CANCELED:
                if v is self.COMPLETE:
                    self.state.jobman.status = self.DONE
                #else we don't change the status
            elif v is self.COMPLETE:
                self.state.jobman.status = self.DONE
            elif v is self.INCOMPLETE:
                self.state.jobman.status = self.START
            else:
                self.state.jobman.status = self.ERR_RUN
        self.__exit__(*sys.exc_info())

        return v

    def on_sigterm(self, signo, frame):
        # SIGTERM handler. It is the experiment function's responsibility to
        # call switch() often enough to get this feedback.
        self.feedback = 'stop'

    def __enter__(self):
        # install a SIGTERM handler that asks the experiment function to return
        # the next time it will call switch()
        if self.catch_sigterm:
            self.prev_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, self.on_sigterm)
        if self.catch_sigint:
            self.prev_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, self.on_sigterm)
        if self.catch_sigusr2:
            self.prev_sigusr2 = signal.getsignal(signal.SIGUSR2)
            signal.signal(signal.SIGUSR2, self.on_sigterm)

        self.start_time = time.time()
        self.last_saved_time = self.start_time
        self.state.jobman.start_time = self.start_time
        self.save()
        return self

    def __exit__(self, type, value, tb_traceback, save = True):
        if type:
            try:
                raise type, value, tb_traceback
            except:
                traceback.print_exc()
        if self.catch_sigterm:
            signal.signal(signal.SIGTERM, self.prev_sigterm)
            self.prev_sigterm = None
        if self.catch_sigint:
            signal.signal(signal.SIGINT, self.prev_sigint)
            self.prev_sigint = None
        if self.catch_sigusr2:
            signal.signal(signal.SIGUSR2, self.prev_sigusr2)
            self.prev_sigusr2 = None
        #This fct is called multiple time. We want to record the time only when the jobs finish.
        if hasattr(self.state.jobman,'status') and self.state.jobman.status == 2:
            self.state.jobman.end_time = time.time()
            self.state.jobman.run_time = self.state.jobman.end_time - self.state.jobman.start_time
        if save:
            self.save()
        return True


################################################################################
### Standard channel (with a path for the workdir)
################################################################################

class StandardChannel(SingleChannel):

    def __init__(self, path, experiment, state, redirect_stdout = False, redirect_stderr = False,
            finish_up_after = None, save_interval = None):
        super(StandardChannel, self).__init__(experiment, state, finish_up_after, save_interval)
        self.path = os.path.realpath(path)
        self.redirect_stdout = redirect_stdout
        self.redirect_stderr = redirect_stderr

    def realpath(self, path):
        if os.getcwd() == self.path:
            os.chdir(self.old_cwd)
            x = os.path.realpath(path)
            os.chdir(self.path)
            return x
        else:
            return os.path.realpath(path)

    def save(self):
        sys.stdout.flush()
        sys.stderr.flush()
        current = open(os.path.join(self.path, 'current.conf'), 'w')
        try:
            current.write(format_d(self.state))
            current.write('\n')
        finally:
            current.close()

    def __enter__(self):
        self.old_cwd = os.getcwd()
        os.chdir(self.path)
        if self.redirect_stdout:
            self.old_stdout = sys.stdout
            sys.stdout = open('stdout', 'a')
        if self.redirect_stderr:
            self.old_stderr = sys.stderr
            sys.stderr = open('stderr', 'a')
        return super(StandardChannel, self).__enter__()

    def __exit__(self, type, value, traceback):
        rval = super(StandardChannel, self).__exit__(type, value, traceback, save = False)
        if self.redirect_stdout:
            new_stdout = sys.stdout
            sys.stdout = self.old_stdout
            new_stdout.close()
        if self.redirect_stderr:
            new_stderr = sys.stderr
            sys.stderr = self.old_stderr
            new_stderr.close()
        os.chdir(self.old_cwd)
        self.save()
        return rval

    def setup(self):
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        # Python 2.4 compatibility: do not use `with` statement.
        self.__enter__()
        try:
            origf = os.path.join(self.path, 'orig.conf')
            if not os.path.isfile(origf):
                orig = open(origf, 'w')
                try:
                    orig.write(format_d(self.state))
                    orig.write('\n')
                finally:
                    orig.close()
            currentf = os.path.join(self.path, 'current.conf')
            if os.path.isfile(currentf):
                current_data = map(str.strip, open(currentf, 'r').readlines())
                state = expand(parse.filemerge(*current_data))
                defaults_merge(self.state, state)
        except Exception:
            # The exception info is passed to the __exit__ method which will
            # raise it.
            pass
        self.__exit__(*sys.exc_info())
