__authors__   = "Guillaume Desjardin, Xavier Muller"
__copyright__ = "(c) 2010, Universite de Montreal"
__license__   = "3-clause BSD License"
__contact__   = "Xavier Muller <xav.muller@gmail.com>"


import os
from optparse import OptionParser

from jobman.parse import filemerge
from jobman.parse import standard as jparse
from jobman.runner import runner_registry



parser_findjob = OptionParser(usage = '%prog findjob [--group [--dont-sort]] <table experiment directory> ... <key=value> ...', add_help_option=False)
parser_findjob.add_option('--group', action = 'store_true', dest = 'group', default = False,
                          help = 'group the output directories by keys name and sort them identically inside each group')
parser_findjob.add_option('--dont-sort', action = 'store_true', dest = 'dont_sort', default = False,
                          help = 'when --group is their dont sort the directory inside each group.')

def runner_findjob(options, *strings):
    """
    Print the directory containing experiment that match some constrain

    Usage: findjob [options] <table experiment directory> ... <key=value> ...

    """
    
    dirs=[]
    keys=[]
    
    for id in range(len(strings)):
        if os.path.exists(strings[id]):
            dirs.append(strings[id])
        else:
            if not options.group:
                assert all(['=' in s for s in strings[id:]])
            else:
                assert not any(['=' in s for s in strings[id:]])
            keys=strings[id:]
            break

    if options.group:
        exp_dirs = get_dir_by_key_name(dirs, keys, not options.dont_sort)
        print "Keys:",keys
        for group_id in range(exp_dirs[0]):
            print 'Keys =',exp_dirs[1][group_id]
            for exp_dir in exp_dirs[2][group_id]:
                print exp_dir[0]
    else:
        exp_dirs = get_dir_by_key_value(dirs, keys)
        for d,id in exp_dirs:
            print d
    

runner_registry['findjob'] = (parser_findjob, runner_findjob)

def get_dir_by_key_name(dirs, keys, sort_inside_groups=True):
    '''
    Returns a 3-tuple
    The first is the number of key values that where found in the directory
    The second is a list of all the key values found
    The third is a list of list of directories. It contains nb_key_value lists
    Each list contains the folder names 

    Parameters
    ----------
    dirs : str or list of str
        Directories that correspond to the table path directory inside the experiment root directory.
    keys : str or list of str
        Experiments parameters used to group the jobs
    sort_inside_groups: bool    
        We sort seach group so that each job in each group have the same other parameter
    Examples
    --------
    
        get_dir_by_key_name(['/data/lisa/exp/mullerx/exp/dae/mullerx_db/ms_0050'],'nb_groups')
    '''

    if isinstance(keys,str):
        keys = [keys]

    nb_key_values=0
    dir_list=[]
    nb_dir_per_group=[]
    key_values=[]

    for base_dir in dirs:
        for expdir in os.listdir(base_dir):
            

            confdir = os.path.join(base_dir, expdir)
            conf = os.path.join(confdir, 'current.conf')

            # No conf file here, go to next dir.
            if not os.path.isfile(conf):
                continue 

            keys_to_match = {}
            params = filemerge(conf)
            
 
            # Get the keyvalue in the conf file.
            kval=()
            for key in keys:
                kval += params.get(key, None),

            new_key=-1;
            # Check if we have this key value already.
            for i in range(len(key_values)):
                if kval==key_values[i]:
                    new_key=i
            

            # Update dir list accordingly.
            if new_key==-1:
                key_values.append(kval)
                nb_dir_per_group.append(1)
                dir_list.append([])
                dir_list[nb_key_values].append([confdir,expdir])
                nb_key_values= nb_key_values+1
            else:
                nb_dir_per_group[new_key]=nb_dir_per_group[new_key] + 1
                dir_list[new_key].append([confdir,expdir])

    if(nb_key_values==1):
        return (nb_key_values,key_values,dir_list)

    if not sort_inside_groups or nb_key_values==1:
        return (nb_key_values,key_values,dir_list)

    # Check if we have the same number of elements in each group.
    # This means some experiments have failed
    
    for i in range(nb_key_values):
        for j in range(nb_key_values):
            if (nb_dir_per_group[i]!=nb_dir_per_group[j]):
                raise EnvironmentError('Not all experiments where found, They might have crashed.... This is not supported yet')


    # Reparse the list based on first key.
    # Sort all the other lists so
    # The same conf parameters values show up always in the same order in each group 
    # Do it the slow lazy way as this code is not time critical
    for i in range(len(dir_list[0])):
        conf = os.path.join(dir_list[0][i][0], 'orig.conf')
        original_params=filemerge(conf)
        
        for j in range(1,nb_key_values):
            for k in range(nb_dir_per_group[0]):
                # Parse each group until we match the exact dictionnary (exept for or our key),
                # then swap it within the gorup so it has the same index as in group 0.
                conf = os.path.join(dir_list[j][k][0], 'orig.conf')
                current_params=filemerge(conf)
                current_params[key]=original_params[key]
                if current_params==original_params:
                    temp=dir_list[j].pop(k)
                    dir_list[j].insert(i,temp)
            
                    
                    
    return (nb_key_values,key_values,dir_list)

    

def get_dir_by_key_value(dirs, keys=['seed=0']):
    '''
    Returns a list containing the name of the folders. Each element in the list is a list
    containing the full path and the id of the experiment as a string

    Parameters
    ----------
    dirs : str or list of str
        Directories that correspond to the table path directory inside the experiment root directory.
    keys : str or list of str
        str of format key=value that represent the experiments that we want to select.

    Examples
    --------

       get_dir_by_key_value(['/data/lisa/exp/mullerx/exp/dae/mullerx_db/ms_0050'],['seed=0'])
    '''
    if isinstance(dirs,str):
        dirs = (dirs,)

    good_dir = []

   
    # Gather results.
    for base_dir in dirs:
        for expdir in os.listdir(base_dir):

            skip = False

            confdir = os.path.join(base_dir, expdir)
            conf = os.path.join(confdir, 'current.conf')
            if not os.path.isfile(conf):
                continue

            params = filemerge(conf)


            skip = len(keys)

            for k in keys:
                keys_to_match = {}
                subkeys = k.split(':')
                for t in subkeys:
                    if t.find('=') != -1:
                        keys_to_match.update(jparse(t))
                       
                    else:
                        if len(subkeys) != 1:
                            raise ValueError('key1:key2 syntax requires keyval pairs')
                        kval = params.get(t)
                        skip -= 1

                for fkey,fval in keys_to_match.iteritems():
                    kval = params.get(fkey, None)
                    if kval == fval:
                        skip -= 1
                        break


            if not skip:
                good_dir.append([confdir,expdir])
                

                
            

    return good_dir

