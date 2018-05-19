import sys
import os
import re
try:
    import sql
except:
    pass
import copy

################################################################################
### misc
################################################################################

class DD(dict):
    def __getattr__(self, attr):
        if attr == '__getstate__':
            return super(DD, self).__getstate__
        elif attr == '__setstate__':
            return super(DD, self).__setstate__
        elif attr == '__slots__':
            return super(DD, self).__slots__
        return self[attr]
#         if attr.startswith('__'):
#             return super(DD, self).__getattr__(attr)
#         try:
#             return self[attr]
#         except KeyError:
#             raise AttributeError(attr)

    def __setattr__(self, attr, value):
        # Safety check to ensure consistent behavior with __getattr__.
        assert attr not in ('__getstate__', '__setstate__', '__slots__')
#         if attr.startswith('__'):
#             return super(DD, self).__setattr__(attr, value)
        self[attr] = value

    def __str__(self):
        return 'DD%s' % dict(self)

    def __repr__(self):
        return str(self)

    def __deepcopy__(self, memo):
        z = DD()
        for k,kv in self.iteritems():
            z[k] = copy.deepcopy(kv, memo)
        return z

def defaults_merge(d, defaults):
    for k, v in defaults.iteritems():
        if isinstance(v, dict):
            defaults_merge(d.setdefault(k, DD()), v)
        else:
            d.setdefault(k, v)

################################################################################
### resolve
################################################################################

def resolve(name, try_import=True):
    """
    Resolve a string of the form X.Y...Z to a python object by repeatedly using getattr, and
    __import__ to introspect objects (in this case X, then Y, etc. until finally Z is loaded).

    """
    symbols = name.split('.')
    try:
        builder = __import__(symbols[0])
    except ImportError, e:
        e.args+=("Error while resolving %s from %s"%(symbols, name),)
        raise
    try:
        for sym in symbols[1:]:
            try:
                builder = getattr(builder, sym)
            except AttributeError, e:
                if try_import:
                    # We need to convert it to a script as with EPD7.1-1
                    # sym is a unicode string and __import__ don't accept it
                    __import__(builder.__name__, fromlist=[str(sym)])
                    builder = getattr(builder, sym)
                else:
                    raise e
    except (AttributeError, ImportError), e:
        raise type(e)('Failed to resolve compound symbol %s' % name, e)
    return builder

################################################################################
### reval
################################################################################

def eval_in_parent(expr, depth = 2):
    caller = sys._getframe(depth)
    return eval(expr, caller.f_globals, caller.f_locals)

_reval_resolve_pattern = re.compile('@([a-zA-Z0-9_\\.]+)')
_reval_varfetch_pattern = re.compile('(?:^|[^%])%([a-zA-Z0-9_]+)')
_reval_vareval_pattern1 = re.compile('!!([a-zA-Z0-9_]+)')
_reval_vareval_pattern2 = re.compile('!([a-zA-Z0-9_]+)')
def _reval(s, depth, d):
    orig_s = s
    s = _reval_resolve_pattern.sub('resolve("\\1")', s)
    s = _reval_vareval_pattern1.sub('eval(str(%%\\1))', s)
    s = _reval_vareval_pattern2.sub('eval(str(%\\1))', s)
    required = set(_reval_varfetch_pattern.findall(s))
    s = s.replace('%%', 'state.')
    s = s.replace('%', '__auto_')

    newvars = dict(resolve = resolve)
    for k, v in d.iteritems():
        newvars['__auto_%s' % k] = v
        if k not in required:
            raise Exception('There is no %s variable to substitute in %s' % (k, orig_s))
        required.remove(k)
    if required:
        raise Exception('The variables %s are missing in the pattern %s' % (list(sorted(required)), orig_s))
    caller = sys._getframe(depth + 1)
    return eval(s, caller.f_globals, dict(caller.f_locals, **newvars))

def reval(s, **d):
    return _reval(s, 1, d)

################################################################################
### dictionary
################################################################################

def flatten(obj):
    """nested dictionary -> flat dictionary with '.' notation """
    d = {}
    def helper(d, prefix, obj):
        # Dictionaries that are not instances of `DD` and have keys which are
        # not strings are not flattened: otherwise we would lose the unique
        # mapping between the dictionary version and the flattened version.
        prevent_flatten = False
        if isinstance(obj, dict) and not isinstance(obj, DD):
            for k in obj.iterkeys():
                if not isinstance(k, basestring):
                    prevent_flatten = True
                    break
        # TODO: add numpy.floating, numpy.integer?
        if (prevent_flatten or
            # add numpy.ndarray
            isinstance(obj, (str, unicode, int, float, list, tuple, set)) or
            obj in (True, False, None)):
            # We do not flatten these objects.
            d[prefix] = obj #convert(obj)
        else:
            if isinstance(obj, dict):
                subd = obj
            elif hasattr(obj, 'state'):
                subd = obj.state()
                subd['__builder__'] = '%s.%s' % (obj.__module__, obj.__class__.__name__)
            else:
                raise TypeError('Cannot flatten object %s, of type %s, for prefix %s' % (str(obj), str(type(obj)), prefix))
            for k, v in subd.iteritems():
                if prefix:
                    pfx = '.'.join([prefix, k])
                else:
                    pfx = k
                helper(d, pfx, v)
    helper(d, '', obj)
    return d


def expand(d, dict_type=DD):
    """inverse of flatten()"""
    struct = dict_type()
    for k, v in d.iteritems():
        if k == '':
            raise NotImplementedError()
        else:
            keys = k.split('.')
        current = struct
        for k2 in keys[:-1]:
            current = current.setdefault(k2, dict_type())
        current[keys[-1]] = v  # convert(v)
    return struct

def realize(d):
    if not isinstance(d, dict):
        return d
    d = dict((k, realize(v)) for k, v in d.iteritems())
    if '__builder__' in d:
        builder = resolve(d.pop('__builder__'))
        return builder(**d)
    return d

def make(d):
    return realize(expand(d))



def realize2(d, depth):
    depth += 1 # this accounts for this frame
    if not isinstance(d, dict):
        return d
    # note: we need to add 1 to depth because the call is in a generator expression
    d = dict((k, realize2(v, depth + 1)) for k, v in d.iteritems())
    if '__builder__' in d:
        return _reval(d.pop('__builder__'), depth, d)
    return d

def _make2(d, depth):
    return realize2(expand(d), depth + 1)

def make2(d, **keys):
    return _make2(dict(d, **keys), 1)

################################################################################
### errors
################################################################################

class UsageError(Exception):
    pass

################################################################################
### formatting
################################################################################

def format_d(d, sep = '\n', space = True):
    d = flatten(d)
    if space:
        pattern = "%s = %r"
    else:
        pattern = "%s=%r"
    return sep.join(pattern % (k, v) for k, v in d.iteritems())

def format_help(topic):
    if topic is None:
        return 'No help.'
    elif isinstance(topic, str):
        help = topic
    elif hasattr(topic, 'help'):
        help = topic.help()
    else:
        help = topic.__doc__
    if not help:
        return 'No help.'

    ss = map(str.rstrip, help.split('\n'))
    try:
        baseline = min([len(line) - len(line.lstrip()) for line in ss if line])
    except:
        return 'No help.'
    s = '\n'.join([line[baseline:] for line in ss])
    s = re.sub(string = s, pattern = '\n{2,}', repl = '\n\n')
    s = re.sub(string = s, pattern = '(^\n*)|(\n*$)', repl = '')

    return s


################################################################################
### Helper functions operating on experiment directories
################################################################################

from jobman import parse
def find_conf_files(cwd, fname='current.conf', recurse=True):
    """
    This generator will iterator from the given directory, and find all job
    configuration files recursively (if specified). Job config files are read
    and a dict is returned with the proper key/value pairs.

    @param cwd: diretory to start iterating from
    @param fname: name of the job config file to look for and parse
    @param recurse: enable recursive search of job config files
    """
    for jobid in os.listdir(cwd):
        e = os.path.join(cwd, jobid)

        if os.path.isdir(e) and recurse:
            find_conf_files(e, fname)

        try:
            jobdd = DD(parse.filemerge(os.path.join(e, fname)))
        except IOError:
            print "WARNING: %s file not found. Skipping it" % os.path.join(e, fname)
            continue

        try:
            jobid = int(jobid)
        except ValueError:
            jobid = None
        yield (jobid, jobdd)


def rebuild_DB_from_FS(db, cwd='./', keep_id=True, verbose=False):
    """
    This method is meant to rebuild a database from the contents of the .conf
    files stored in an experiment directory. This can be useful for consolidating
    data stored in multiple locations or after bad things happen to the DB...

    @param db: db handle (as returned by api0.open_db) of the DB in which to insert job dicts
    @param cwd: current working dir or path from which to start looking for conf files
    @param keep_id: attempt to use the directory name as job id for inserting in DB
    @param verbose: prints info about which jobs are succesfully inserted
    """
    tot = 0
    for (jobid, jobdd) in find_conf_files(cwd):
        if keep_id and jobid:
            jobdd[sql.JOBID] = jobid
        status = sql.insert_dict(jobdd, db) 
        if status: tot += 1

        if verbose:
            print '** inserted job %i **' % jobdd[sql.JOBID]

    if verbose:
        print '==== Inserted %i jobs ====' % tot
