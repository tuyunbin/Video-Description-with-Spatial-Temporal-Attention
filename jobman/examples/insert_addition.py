from jobman.tools import DD, flatten
from jobman import api0, sql

from jobman.examples.def_addition import addition_example

TABLE_NAME='test_add_'

# DB path...
db = api0.open_db('postgres://<user>:<pass>@<server>/<database>?table='+TABLE_NAME)

state = DD()
for first in 0,2,4,6,8,10:
    state.first = first
    for second in 1,3,5,7,9:
        state.second = second

        sql.insert_job(addition_example, flatten(state), db)
