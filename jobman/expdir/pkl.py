import os, cPickle

args_filename = 'args.pkl'
results_filename = 'results.pkl'
new_jobname_format = 'job%06i'

#########################
# Utilies inspecting jobs
#########################

def jobs_iter(exproot):
    jobs = os.listdir(exproot)
    jobs.sort()
    for job in jobs:
        if job.startswith('__'):
            continue
        yield job, os.path.join(exproot, job)

def load_pkl(path):
    try:
        return cPickle.load(open(path))
    except (OSError, IOError), e:
        return None
def load_args(jobroot):
    return load_pkl(os.path.join(jobroot, args_filename))
def load_results(jobroot):
    return load_pkl(os.path.join(jobroot, results_filename))

def load_all(exproot):
    """Iterate over all (jobname, args, results) tuples
    """
    for jobname, jobpath in jobs_iter(exproot):
        yield jobname, load_args(jobpath), load_results(jobpath)


#############################
# Utilies for adding new jobs
#############################

def new_names(exproot, N, format=new_jobname_format):
    job_names = os.listdir(exproot)
    i = 0
    rvals = []
    while len(rvals) < N:
        name_i = format % i
        if name_i not in job_names:
            rvals.append(name_i)
        i += 1
    return rvals

def new_name(exproot, format=new_jobname_format):
    return new_names(exproot, 1, format=format)[0]

def add_named_jobs(exproot, name_list, args_list, protocol=cPickle.HIGHEST_PROTOCOL):
    rval = []
    for name, args in zip(name_list, args_list):
        jobroot = os.path.join(exproot, name)
        os.mkdir(jobroot)
        if args is not None:
            cPickle.dump(args, 
                    open(os.path.join(exproot, name, args_filename), 'w'),
                    protocol=protocol)
        rval.append((jobroot, args))
    return rval

def add_anon_jobs(exproot, args_list, protocol=cPickle.HIGHEST_PROTOCOL):
    return add_named_jobs(exproot, new_names(exproot, len(args_list)), args_list,protocol)

def add_unique_jobs(exproot, args_list, protocol=cPickle.HIGHEST_PROTOCOL):
    # load existing jobs from db
    existing_args = [args for args in [load_args(path) for (name,path) in jobs_iter(exproot)]
            if args is not None]
    try:
        # this will make the next step much faster if all the args are hashable
        existing_args = set(existing_args)
    except:
        pass
    args_list = [a for a in args_list if a not in existing_args]
    return add_anon_jobs(exproot, args_list, protocol=protocol)

