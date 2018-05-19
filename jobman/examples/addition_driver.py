""" Example script to demonstrate how to use the 'jobman.mydriver' sort of job management.
"""

#standard imports
import sys
from jobman.tools import DD, flatten
from jobman import sql, mydriver


#here, you would import the function that implements your job.
# This should be one of the jobman-compatible functions that takes arguments (state, channel)

# One reason not to put this function into this very file is that in practice,
# it can be handy to record a version number (such as the mercurial version
# hash) as part of the result of a job.  That way, when analyzing things much
# later, you can see right away what version of your code produced a given
# result.  If your version number changes every time you add a job or tweak an
# analysis function (i.e. list_all), then you will see many different version
# numbers among your experiments, even though the file with the function being
# tested didn't change at all.  This can be confusing.
from jobman.examples.def_addition import addition_example


# here we build a list of dictionaries, each of which specifies a setting of
# parameters corresponding to one job.
#
# We will pass this list to mydriver.main, so that it can potentially insert
# them, or trim down the database to match the current list, and other sorts of
# things.
state = DD()
jobs = []
for first in 0,2,4,6,8,10:
    state.first = first
    for second in 1,3,5,7,9:
        state.second = second
        jobs.append(dict(flatten(state)))


# 
# mydriver.main defines a few commands that are generally useful (insert,
# clear_db, summary, etc.) but if you want to analyse your results you can add
# additional commands.
#
# For example, if you type 'python additional_driver.py list_all', then this little function will run, because its function name is 'list_all'.
#
# For help on the meaning of the various elements of `kwargs`, see <WRITEME>.
#

@mydriver.mydriver_cmd_desc('list the contents of the database')
def list_all(db, **kwargs):
    for d in db:
        print d.id, d.items()


# Here we identify our username, and the database to log in to...
## CUSTOMIZE THIS
user='bergstrj'
server='gershwin.iro.umontreal.ca'
database='bergstrj_db'
table='test_add'

# Now we pass control over to mydriver.main.  To manage jobs, you typically will type something like
# 'python addition_driver.py <cmd> [options]'
# 
# Try typing 'python addition_driver.py help' in the shell for starters.
mydriver.main(sys.argv,
    'postgres://%s@%s/%s?table=%s'%(user, server, database, table),
    '/tmp',
    addition_example,
    jobs)
