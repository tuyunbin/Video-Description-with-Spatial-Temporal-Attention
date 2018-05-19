import os
from ..expdir.pkl import load_all, args_filename, results_filename
from ..analyze_runner import cmd

######################
# Commands for analyze
######################

@cmd
def list_jobs(exproot, **kwargs):
    """List jobs in the experiment"""
    for jobname, args, results in load_all(exproot):
        print jobname, args, results

@cmd
def list_dups(exproot, **kwargs):
    """List duplicate jobs in the experiment"""
    seen_args = []
    seen_names = []
    for jobname, args, results in load_all(exproot):
        if args in seen_args:
            print jobname, 'is dup of', seen_names[seen_args.index(args)]
        elif args != None:
            seen_args.append(args)
            seen_names.append(jobname)
@cmd
def del_dups(exproot, **kwargs):
    """Delete duplicate jobs in the experiment"""
    seen_args = []
    seen_names = []
    for jobname, args, results in load_all(exproot):
        if args in seen_args:
            if os.listdir(os.path.join(exproot, jobname)) == [args_filename]:
                print jobname, 'is empty dup of', seen_names[seen_args.index(args)],
                print '...  deleting'
                os.remove(os.path.join(exproot, jobname, args_filename))
                os.rmdir(os.path.join(exproot, jobname))
            else:
                print jobname, 'is dup with files of', seen_names[seen_args.index(args)]
        elif args != None:
            seen_args.append(args)
            seen_names.append(jobname)


