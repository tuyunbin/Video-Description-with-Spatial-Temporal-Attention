from jobman import api0

TABLE_NAME='test_add_'
db = api0.open_db('postgres://<user>:<pass>@<server>/<database>?table='+TABLE_NAME)
db.createView(TABLE_NAME + 'view')
