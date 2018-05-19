import datetime, logging, os, random, shutil, socket, sys
import tempfile, time, traceback

import SocketServer
import threading

from runner import runner_registry
from optparse import OptionParser

_logger = logging.getLogger('jobman.rsync_runner')
#_logger.addHandler(logging.StreamHandler(sys.stderr))
#_logger.setLevel(logging.DEBUG)

#################
# Salted hashing
#################
# Python 2.4 compatibility.
try:
    import hashlib
except ImportError:
    import md5 as hashlib

HASH_REPS = 2000 # some site said that this number could (should) be really high like 10K

def __saltedhash(string, salt):
    sha256 = hashlib.new('sha512')
    sha256.update(string)
    sha256.update(salt)
    for x in xrange(HASH_REPS): 
        sha256.update(sha256.digest())
        if x % 10: sha256.update(salt)
    return sha256

def saltedhash_bin(string, salt):
    """returns the hash in binary format"""
    return __saltedhash(string, salt).digest()

def saltedhash_hex(string, salt):
    """returns the hash in hex format"""
    return __saltedhash(string, salt).hexdigest()

###############
# Serving
###############


class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """
    todo = '__todo__'
    done = '__done__'
    exproot = None # set externally
    lock = None  # set externally

    def handle(self):
        _logger.info('handling connection with %s' % str(self))
        # self.request is the TCP socket connected to the client

        # this is supposed to be some kind of basic security to prevent 
        # random internet probes from messing up my experiments.
        salt = repr(time.time())
        secret = self.exproot + os.getlogin()

        #self.data = self.request.recv(1024).strip()
        # just send back the salt
        self.request.send(salt)
        t0 = time.time()
        hashed_secret = saltedhash_bin(secret, salt)
        _logger.debug('hashing took %f'%(time.time() - t0))
        cli_hashed_secret = self.request.recv(512)
        if cli_hashed_secret == hashed_secret:
            _logger.info('client authenticated')
            self.request.send('ok')
            self.handle_authenticated()
        else:
            _logger.info('client failed authentication')
            self.request.send('err')

    def handle_authenticated(self):
        cmd = self.request.recv(1024)
        if cmd == 'job please':
            self.handle_job_please()
            pass
        else:
            raise NotImplementedError()

    def handle_job_please(self):
        # if the filename is a single '?' character, then it is taken to mean: move any file
        # from the srcdir to the destdir.  When that happens the server will send the name of
        # the filename it chose back to the client.  If the srcdir is empty, then the server
        # will send back '?' to the client.
        self._lock.acquire()
        try:
            try:
                srclist = os.listdir(os.path.join(self.exproot, self.todo))
                if srclist:
                    srclist.sort()
                    choice = srclist[0] # the smallest value
                    os.rename(
                            os.path.join(os.path.join(self.exproot, self.todo, choice)),
                            os.path.join(os.path.join(self.exproot, self.done, choice)))
                else:
                    choice = ''
            except (OSError,IOError), e:
                _logger.error('Error booking ' + str(e))
                choice = ''
        finally:
            self._lock.release()

        _logger.debug('sending choice %s'% repr(choice))
        self.request.send(choice)

parser_serve = OptionParser(usage = '%prog serve [options] <tablepath> <exproot>',
                          add_help_option=False)
parser_serve.add_option('--port', dest = 'port', type = 'int', default = 9999,
                      help = 'Connect on given port (default 9999)')
def runner_serve(options, path):
    """Run a server for remote jobman rsync_any commands.

    Example usage:

        jobman serve --port=9999 path/to/experiment

    The server will watch the path/to/experiment/__todo__ directory for names, 
    and move them to path/to/experiment/__done__ as clients connect [successfully] and
    ask for fresh jobs.
    """
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    HOST, PORT = "localhost", 9999

    _logger.info("Job server for %s listening on %s:%i" %
            (path, HOST, PORT))

    #install the _lock in the TCP handler for use by handle_job_please
    MyTCPHandler._lock = threading.Lock()
    MyTCPHandler.exproot = path

    # make sure the appropriate directories have been created
    try:
        os.listdir(os.path.join(path, MyTCPHandler.todo))
    except:
        os.makedirs(os.path.join(path, MyTCPHandler.todo))
    try:
        os.listdir(os.path.join(path, MyTCPHandler.done))
    except:
        os.makedirs(os.path.join(path, MyTCPHandler.done))

    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
runner_registry['serve'] = (parser_serve, runner_serve)


###############
# Client-side 
###############

class RSyncException(Exception):
    def __init__(self, cmd, rval):
        super(RSyncException, self).__init__('Rsync Failure', (cmd, rval))

def rsync(srcdir, dstdir, num_retries=3,
        options='-ac --copy-unsafe-links',
        exclusions=[]): 
    excludes = ' '.join('--exclude="%s"' % e for e in exclusions)
    raw_cmd = 'rsync %(options)s %(excludes)s "%(srcdir)s/" "%(dstdir)s/"'
    rsync_cmd = raw_cmd % locals()

    keep_trying = True
    rsync_rval = 1 # some non-null value

    # allow n-number of retries, with random hold-off between retries
    attempt = 0
    while rsync_rval!=0 and keep_trying:
        _logger.debug('executing rsync command: %s'%rsync_cmd)
        rsync_rval = os.system(rsync_cmd)

        if rsync_rval != 0:
            _logger.info('rsync error %i' % rsync_rval)
            attempt += 1
            keep_trying = attempt < num_retries
            # wait anywhere from 30s to [2,4,6] mins before retrying
            if keep_trying: 
                r = random.randint(30,attempt*120)
                _logger.warning( 'RSync Error at %s attempt %i/%i: sleeping %is' %(
                            rsync_cmd, attempt,num_retries,r))
                time.sleep(r)

    if rsync_rval != 0:
        raise RSyncException(rsync_cmd, rsync_rval)

def server_getjob(user, host, port, expdir):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    salt = s.recv(512)
    secret =  expdir + user
    _logger.debug('secret %s'% secret)
    _logger.debug('salt %s'% salt)
    s.send(saltedhash_bin(secret, salt))
    sts = s.recv(1024)
    if sts == 'ok':
        s.send('job please')
        jobname = s.recv(1024)
        s.close()
    else:
        s.close()
        raise Exception("failed to authenticate")
    return jobname

def parse_server_str(fulladdr):
    # user@host:port/full/path/to/expdir
    user = fulladdr[:fulladdr.index("@")]
    host = fulladdr[fulladdr.index("@")+1:fulladdr.index(':')]
    port = int(fulladdr[fulladdr.index(":")+1:fulladdr.index('/')])
    expdir = fulladdr[fulladdr.index("/"):]
    return user, host, port, expdir

def run_callback_in_rsynced_tempdir(remote_rsync_loc, callback, 
        callbackname=None,
        redirect_stdout='stdout',
        redirect_stderr='stderr',
        status_filename='status'):

    # get a local tmpdir
    tmpdir = tempfile.mkdtemp()

    # rsync from remote directory to tmpdir
    rsync(remote_rsync_loc, tmpdir, exclusions=['*.no_sync_to_client', '*.no_sync'])

    # chdir to tmpdir
    os.chdir(tmpdir)

    # redirect stdout, stderr
    stdout = sys.stdout
    stderr = sys.stderr
    try:
        if redirect_stdout:
            sys.stdout = open(redirect_stdout, 'a+')
        if redirect_stderr:
            sys.stderr = open(redirect_stderr, 'a+')

        # rsync back to the remote directory
        if status_filename:
            localhost = socket.gethostname()
            statusfile = open(status_filename, 'a+')
            now = str(datetime.datetime.now())
            print >> statusfile, "%(now)s Running %(callbackname)s in %(localhost)s:%(tmpdir)s" % locals()
        rsync(tmpdir, remote_rsync_loc, exclusions=['*.no_sync_to_server', '*.no_sync'])

        try:
            callback()
        except Exception, e:
            traceback.print_exc() # goes to sys.stderr

        if status_filename:
            now = str(datetime.datetime.now())
            print >> statusfile, "%(now)s Done %(callbackname)s in %(localhost)s:%(tmpdir)s" % locals()
            statusfile.flush()

        sys.stdout.flush()
        sys.stderr.flush()
        # rsync back to the remote directory
        # if this fails and raises an exception, the rmtree below is skipped so the files
        # remain on disk.
        rsync(tmpdir, remote_rsync_loc, exclusions=['*.no_sync_to_server', '*.no_sync'])

        # delete the tempdir
        shutil.rmtree(tmpdir, ignore_errors=True)
        #lambda fn, path, excinfo : sys.stderr.writeline(
        #    'Error in rmtree: %s:%s:%s' % (fn, path, excinfo)))

    finally:
        # return stdout, stderr
        sys.stdout = stdout
        sys.stderr = stderr
    # return None

parser_rsyncany = OptionParser(usage = '%prog rsync_any [options] <server> <function>',
                          add_help_option=False)
#parser_rsyncany.add_option('--port', dest = 'port', type = 'int', default = 9999,
                      #help = 'Connect on given port (default 9999)')

def import_cmd(cmd):
    """Return the full module name of a fully-quallified function call
    """
    #print 'cmd', cmd
    lp = cmd.index('(')
    ftoks = cmd[:lp].split('.')
    imp = '.'.join(ftoks[:-1])
    return imp, cmd

_remote_info = []
def remote_info():
    return _remote_info[-1]
def remote_ssh():
    return 'ssh://%(user)s@%(host)s' % remote_info()
def remote_rsync_loc():
    return '%(user)s@%(host)s:/%(jobdir)s' % remote_info()
def _rsyncany_helper(imp, cmd):
    if imp:
        logging.getLogger('pyrun').debug('Importing: %s' % imp)
        exec('import '+imp)
    logging.getLogger('pyrun').debug('executing: %s' % cmd)
    exec(cmd)
def runner_rsyncany(options, addr, fullfn):
    """Run a function in an rsynced tempdir.

    Example usage:

        jobman rsync_any bergstra@gershwin:9999/fullpath/to/experiment 'mymodule.function()'

    """
    #parse the server address
    user, host, port, expdir = parse_server_str(addr)

    # book a job from the server (get a remote directory)
    # by moving any job from the todo subdir to the done subdir
    jobname = server_getjob(user, host, port, expdir)
    if jobname == '':
        print "No more jobs"
        return
    _logger.info('handling jobname: %s' % jobname)
    jobdir = os.path.join(expdir, jobname)
    _remote_info.append(locals())
    try:
        # run that job
        run_callback_in_rsynced_tempdir(
                remote_rsync_loc(),
                lambda : _rsync_helper(*import_cmd(fullfn)),
                callbackname=fullfn,
                )
    finally:
        _remote_info.pop()

runner_registry['rsync_any'] = (parser_rsyncany, runner_rsyncany)

