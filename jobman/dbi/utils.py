#! /usr/bin/env python

import sys,time,random,md5,glob,string,socket,os
from configobj import ConfigObj

#original version: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52224
def search_file(filename, search_path):
    """Given a search path, find file
     Can be used with the PATH variable as search_path
    """
    file_found = 0
    paths = string.split(search_path, os.pathsep)
    for path in paths:
        if os.path.exists(os.path.join(path, filename)):
            file_found = 1
            break
    if file_found:
        return os.path.abspath(os.path.join(path, filename))
    else:
        return None
    

def get_new_sid(tag):
    """Build a new Session ID"""
    t1 = time.time()
    time.sleep( random.random() )
    t2 = time.time()
    base = md5.new( tag + str(t1 +t2) )
    sid = tag + '_' + base.hexdigest()
    return sid

def file_exists(filename):
    return len(glob.glob(filename)) > 0

def set_config_value(file, keyword, value):
    config = ConfigObj(file) 
    config[keyword] = value
    config.write()

def get_config_value(file, keyword):
    config = ConfigObj(file)
    try: 
        return config[keyword]
    except KeyError:
        return -1

def set_current_date(file, keyword,time_format):
    config = ConfigObj(file) 
    config[keyword] = time.strftime(time_format, time.localtime(time.time()))
    config.write()
    
def truncate(s, length):
    if len(s) < length:
        return s
    else:
        return s[:length] #+ etc
    
def string_replace(s,c,ch=''):
    """Remove any occurrences of characters in c, from string s
       s - string to be filtered, c - characters to filter"""
    for a in c:
        s = s.replace(a,ch)
        
    return s

def create_eval_command(function_name , args):
    return function_name +"('" +  string.join(args,"','") + "')"

def get_platform():
    platform = sys.platform
    if platform=='linux2':
        linux_type = os.uname()[4]
        if linux_type == 'ppc':
            platform = 'linux-ppc'
        elif linux_type =='x86_64':
            platform = 'linux-x86_64'
        else:
            platform = 'linux-i386'
    return platform

def get_condor_platform():
    platform = sys.platform
    if platform=='linux2':
        linux_type = os.uname()[4]
        if linux_type == 'ppc':
            platform = 'linux-ppc'
        elif linux_type =='x86_64':
            platform = 'X86_64'
        else:
            platform = 'INTEL'
    return platform

def get_plearndir():
    dir = os.environ['PLEARNDIR']
    #    if platform=='win32':
    #        homedir = 'R:/'
    return dir
        
def get_jobmandir():
    import jobman
    return os.path.split(jobman.__file__)[0]

def get_homedir():
    homedir = os.environ['HOME']
    return homedir

def get_username():
    username = os.environ['USER']    
    return username

def get_hostname():
    myhostname = socket.gethostname()
    return myhostname

if __name__ == '__main__':
    #    cmd = sys.argv[1] +"('" +  string.join(sys.argv[2:],"','") + "')"
    cmd = create_eval_command(sys.argv[1], sys.argv[2:])
    eval(cmd)
