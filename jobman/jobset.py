"""provides JobSet: a python-friendly API for JobMan

warning: EXPERIMENTAL
"""

class JobSet(object):
    """Class representing an set of jobs, each represented by their DD "state" variables.

    This class works in-place on the state objects that have been added to it.
    """
    def __init__(self, path, erase_and_forget_on_delete=False):
        """If the path does not exist, a directory will be created there to store the internal
        states of this DDMap

        If the path does exist, it should be a directory that has previously been created by a
        DDMap in a process that has now ended.  This class will become the new owner and
        maintainer of the DDMap directory.

        :param erase_and_forget_on_delete: if this is True, all files associated with this
        DDMap will be erased when this object is garbage-collected.
        """
        raise NotImplementedError()

    # ITERATORS OVER JOBS

    def __iter__(self):
        """Return an iterator over all internal states
        """

    def iter_finished(self):
        """Return an iterator over all internal states that have finished computation"""
        raise NotImplementedError()

    def iter_running(self):
        """Return an iterator over all internal states that have finished computation"""
        raise NotImplementedError()

    def iter_waiting(self):
        """Return an iterator over all internal states that have finished computation"""
        raise NotImplementedError()

    # ITERATORS OVER JOBS

    def update(self, state_seq):
        for state in state_seq:
            self.add(state)

    def add(self, state):
        """Add a job-state to this pool. It will be started as soon as possible."""
        raise NotImplementedError()

    def delete(self, state):
        """Remove a job-state from this pool"""
        raise NotImplementedError()

    # RESULTS

    def wait_any(self, timeout=None):
        """Block until a job finishes, then return it.  Returns None if no job is running.
        """
        raise NotImplementedError()

    def wait_all(self, timeout=None):
        """
        Initiate computation of any un-started state jobs.
        Wait for all jobs to complete and then return.
        """
        if method == 'local':
            # do a for loop over all remaining jobs
            raise NotImplementedError()
        if method == 'multiprocess':
            # use  things described here:
            # http://docs.python.org/dev/library/multiprocessing.html
            raise NotImplementedError()
        if method == 'ssh':
            # ssh into a machine and run jobman over there.
            raise NotImplementedError()
        if method == 'bqtools':
            raise NotImplementedError()
        if method == 'condor':
            raise NotImplementedError()
        if method == 'cluster':
            raise NotImplementedError()

    # BOOKKEEPING
    
    def erase_and_forget(self):
        """
        Delete all the files corresponding to internal state objects, and the directory
        associated to this DDMap.
        """
        raise NotImplementedError()

def generic_dd_fn(state, channel):
    """Generic driver to run an arbitary function using a job DD"""
    raise NotImplementedError()
    fn, args, kwargs = None, None, None
    state.rval = fn(*args, **kwargs)


def jobset_map(fn, arg_seq, method=JobSet, path=None, cleanup=True):
    """Perform a map operation using JobMan"""
    def to_jobstate(fn, arg):
        """Return a DD that will compute"""
        # TODO: ensure that fn can be loaded from a toplevel import
        
        # TODO: ensure that all elements of arg_seq can be put into DD instances

        return dict(
                fn=fully_qualified_name_of_fn(fn),
                arg=arg
                )
    def from_jobstate(state):
        return state.rval

    if path is None:
        path = random_tmp_folder

    #create a temporary path to sychronize jobs, which will be cleaned up automatically
    jobset = method(path, erase_and_forget_on_delete=cleanup) 

    states = [to_jobstate(fn, arg) for arg in arg_seq]
    jobset.update(states)
    jobset.wait()                     # computation takes place here
    return map(from_jobstate, states)
    
