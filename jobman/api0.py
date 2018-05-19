"""
This file defines class `DbHandle` and a few routines for creating instances of DbHandle.

"""

from jobman import sql

if sql.sqlalchemy_ok:
    import sqlalchemy.pool

    from sqlalchemy import create_engine#, desc
    from sqlalchemy import (
            Table, Column, MetaData, ForeignKeyConstraint #ForeignKey,
            )
    from sqlalchemy import (
        Integer, String, Float, DateTime, Text, Binary #Boolean,
        )
    try:
        from sqlalchemy import BigInteger
    except ImportError:
        # for SQLAlchemy 0.5 (will not work with sqlite)
        from sqlalchemy.databases.postgres import PGBigInteger as BigInteger

    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import mapper, relation, eagerload#backref

    #from sqlalchemy.engine.base import Connection

    from sqlalchemy.sql import select #operators
    from sqlalchemy.sql.expression import column, not_, literal_column #outerjoin

    from sqlalchemy.engine.url import make_url

else:
    from jobman import fake_sqlalchemy as sqlalchemy

import time
import random
import os

class Todo(Exception):
    # Here 'this' refers to the code where the exception is raised,
    # not the code of the 'Todo' exception itself!
    """Replace this with some working code!"""

class DbHandle (object):
    """
    This class implements a persistant dictionary using an SQL database as storage.

    Notes on usage
    ==============

    WRITEME

    Notes on the implementation
    ============================
    Dictionaries are stored using two tables in a database: `dict_table` and `pair_table`.

    This class provides filtering shortcuts that hide the names of the
    DbHandle internal databases.

    Attributes:
    dict_table
    pair_table



    dict_table

        An SqlAlchemy-mapped class corresponding to database table with the
        following schema:

            Column('id', Integer, primary_key=True)
            Column('create', DateTime)
            Column('write', DateTime)
            Column('read', DateTime)

            #TODO: reconsider create/read/write

    pair_table

        An SqlAlchemy-mapped class corresponding to database table with the
        following schema:

            Column('id', Integer, primary_key=True)
            Column('name', String(128))
            Column('type', String(1))
            Column('fval', Double)
            Column('ival', BigInteger)
            Column('sval', Text)
            Column('bval', Blob)

            #TODO: Consider difference between text and binary
            #TODO: Consider union?
            #TODO: Are there stanard ways of doing this kind of thing?

    """

    e_bad_table = 'incompatible columns in table'

    def __init__(h_self, Session, engine, dict_table, pair_table):
        h_self._engine = engine;
        h_self._dict_table = dict_table
        h_self._pair_table = pair_table

        #TODO: replace this crude algorithm (ticket #17)
        if ['id', 'create', 'write', 'read', 'status', 'priority','hash'] != [c.name for c in dict_table.c]:
            raise ValueError(h_self.e_bad_table, dict_table)
        if ['id', 'dict_id', 'name', 'type', 'ival', 'fval', 'sval', 'bval'] != [c.name for c in pair_table.c]:
            raise ValueError(h_self.e_bad_table, pair_table)

        h_self._session_fn = Session

        class KeyVal (object):
            """KeyVal interfaces between python types and the database.

            It encapsulates heuristics for type conversion.
            """
            def __init__(k_self, name, val):
                k_self.name = name
                k_self.val = val
            def __repr__(k_self):
                return "<Param(%s,'%s', %s)>" % (k_self.id, k_self.name, repr(k_self.val))
            def __get_val(k_self):
                val = None
                if k_self.type == 'i':
                    val = int(k_self.ival)
                elif k_self.type == 'f':
                    if k_self.fval is None:
                        val = float('nan')
                    else:
                        val = float(k_self.fval)
                elif k_self.type == 'b':
                    val = eval(str(k_self.bval))
                elif k_self.type == 's':
                    val = k_self.sval
                else:
                    raise ValueError('Incompatible value in column "type"',
                            k_self.type)
                return val

            def __set_val(k_self, val):
                k_self.ival = None
                k_self.fval = None
                k_self.bval = None
                k_self.sval = None

                if isinstance(val, (str,unicode)):
                    k_self.type = 's'
                    k_self.sval = val
                elif isinstance(val, float):
                    k_self.type = 'f'
                    # special cases
                    if str(val) in ('nan', 'inf', '-inf'):
                        # Special cases not handled by SQLAlchemy.
                        # To avoid crashes, setting value to None
                        k_self.fval = None
                    else:
                        k_self.fval = float(val)
                elif isinstance(val, int):
                    k_self.type = 'i'
                    k_self.ival = int(val)
                else:
                    k_self.type = 'b'
                    k_self.bval = repr(val)
                    assert eval(k_self.bval) == val

            val = property(__get_val, __set_val)

        mapper(KeyVal, pair_table)

        class Dict (object):
            """
            Instances are dict-like objects with additional features for
            communicating with an active database.

            This class will be mapped by SqlAlchemy to the dict_table.

            Attributes:
            handle - reference to L{DbHandle} (creator)

            """
            def __init__(d_self, session=None):
                if session is None:
                    s = h_self._session_fn()
                    s.add(d_self) #d_self transient -> pending
                    s.commit()    #d_self -> persistent
                    s.close()     #d_self -> detached
                else:
                    s = session
                    s.add(d_self)

            _forbidden_keys = set(['session'])

            #
            # dictionary interface
            #

            def __contains__(d_self, key):
                for a in d_self._attrs:
                    if a.name == key:
                        return True
                return False

            def __eq__(self, other):
                return dict(self) == dict(other)
            def __neq__(self, other):
                return dict(self) != dict(other)

            def __getitem__(d_self, key):
                for a in d_self._attrs:
                    if a.name == key:
                        return a.val
                raise KeyError(key)

            def __setitem__(d_self, key, val, session=None):
                if session is None:
                    s = h_self._session_fn()
                    s.add(d_self)
                    d_self._set_in_session(key, val, s)
                    s.commit()
                    s.close()
                else:
                    s = session
                    s.add(d_self)
                    d_self._set_in_session(key, val, s)

            def __delitem__(d_self, key, session=None):
                if session is None:
                    s = h_self._session_fn()
                    commit_close = True
                else:
                    s = session
                    commit_close = False
                s.add(d_self)

                #find the item to delete in d_self._attrs
                to_del = None
                for i,a in enumerate(d_self._attrs):
                    if a.name == key:
                        assert to_del is None
                        to_del = (i,a)
                if to_del is None:
                    raise KeyError(key)
                else:
                    i, a = to_del
                    s.delete(a)
                    del d_self._attrs[i]
                if commit_close:
                    s.commit()
                    s.close()

            def __iter__(d_self):
                return iter(d_self.keys())

            def iteritems(d_self):
                return d_self.items()

            def items(d_self):
                return [(kv.name, kv.val) for kv in d_self._attrs]

            def keys(d_self):
                return [kv.name for kv in d_self._attrs]

            def values(d_self):
                return [kv.val for kv in d_self._attrs]

            def update_simple(d_self, dct, session, **kwargs):
                """
                Make an dict-like update to self in the given session.

                :param dct: a dictionary to union with the key-value pairs in self
                :param session: an open sqlalchemy session

                :note: This function does not commit the session.

                :note: This function may raise `psycopg2.OperationalError`.
                """
                session.add(d_self)
                for k, v in dct.iteritems():
                    d_self._set_in_session(k, v, session)
                for k, v in kwargs.iteritems():
                    d_self._set_in_session(k, v, session)

            def update_in_session(d_self, dct, session, _recommit_times=5, _recommit_waitsecs=10, **kwargs):
                """Make a dict-like update in the given session.

                More robust than update_simple, it will try to recommit
                if something bad happens.

                :param session: a valid SqlAlchemy session or else None.  If it
                is None, then a session will be created and closed internally.
                If it is a valid session, then it will not be closed by this
                function, but will be left in an empty/clear state, with no
                pending things to commit.

                :precondition: session is None or else it is a valid SqlAlchemy
                session with no pending stuff to commit.  This must be so,
                because if the update fails, this function will try a few times (`_recommit_times`)
                to re-commit the transaction.
                """
                while True:
                    try:
                        d_self.update_simple(dct, session, **kwargs)
                        session.commit()
                        break
                    except Exception:
                        _recommit_times -= 1
                        if _recommit_times:
                            time.sleep(random.randint(1, _recommit_waitsecs))
                            session.rollback()
                        else:
                            raise

            def update(d_self, dct, _recommit_times=5, _recommit_waitsecs=10, **kwargs):
                """Like dict.update(), set keys from kwargs.
                """
                session = h_self._session_fn()
                if ('session' in kwargs):
                    raise Exception('"session" is no longer a kwarg to update, use update_in_session or update_simple instead')

                while True:
                    # now we have a fresh session, and we try to do our work
                    try:
                        d_self.update_simple(dct, session, **kwargs)
                        session.commit()
                        break
                    except Exception:
                        #Commonly, an exception will come from sqlalchemy or psycopg2.
                        # I don't want to hard-code psycopg2 into this file
                        # because other backends will raise different errors.
                        #
                        # An exception that doesn't go away on subsequent tries
                        # will be raised eventually in the else-clause below.
                        _recommit_times -= 1
                        if _recommit_times:
                            time.sleep(random.randint(1, _recommit_waitsecs))
                            session.rollback()
                        else:
                            session.close()
                            raise
                session.close()

            def get(d_self, key, default):
                try:
                    return d_self[key]
                except KeyError:
                    return default

            def __str__(self):
                return 'Dict'+ str(dict(self))

            #
            # database stuff
            #

            def refresh(d_self, session=None):
                """Sync key-value pairs from database to self

                @param session: use the given session, and do not commit.

                """
                if session is None:
                    session = h_self._session_fn()
                    session.add(d_self) #so session knows about us
                    session.refresh(d_self)
                    session.commit()
                    session.close()
                else:
                    session.add(d_self) #so session knows about us
                    session.refresh(self.dbrow)

            def delete(d_self, session=None):
                """Delete this dictionary from the database

                @param session: use the given session, and do not commit.
                """
                if session is None:
                    session = h_self._session_fn()
                    session.add(d_self) #so session knows about us
                    session.delete(d_self) #mark for deletion
                    session.commit()
                    session.close()
                else:
                    session.add(d_self) #so session knows about us
                    session.delete(d_self)

            # helper routine by update() and __setitem__
            def _set_in_session(d_self, key, val, session):
                """Modify an existing key or create a key to hold val"""

                #FIRST SOME MIRRORING HACKS
                if key == 'jobman.id':
                    ival = int(val)
                    d_self.id = ival
                if key == 'jobman.status':
                    ival = int(val)
                    d_self.status = ival
                if key == 'jobman.sql.priority':
                    fval = float(val)
                    d_self.priority = fval
                if key == 'jobman.hash':
                    ival = int(val)
                    d_self.hash = ival

                if key in d_self._forbidden_keys:
                    raise KeyError(key)
                created = None
                for i,a in enumerate(d_self._attrs):
                    if a.name == key:
                        assert created == None
                        created = h_self._KeyVal(key, val)
                        d_self._attrs[i] = created
                if not created:
                    created = h_self._KeyVal(key, val)
                    d_self._attrs.append(created)
                session.add(created)

        mapper(Dict, dict_table,
                properties = {
                    '_attrs': relation(KeyVal,
                        cascade="all, delete-orphan")
                    })

        class _Query (object):
            """
            Attributes:
            _query - SqlAlchemy.Query object
            """

            def __init__(q_self, query):
                q_self._query = query

            def __iter__(q_self):
                return q_self.all().__iter__()

            def __getitem__(q_self, item):
                return q_self._query.__getitem__(item)

            def filter_eq(q_self, kw, arg):
                """Return a Query object that restricts to dictionaries containing
                the given kwargs"""

                #Note: when we add new types to the key columns, add them here
                q = q_self._query
                T = h_self._Dict
                if isinstance(arg, (str,unicode)):
                    q = q.filter(T._attrs.any(name=kw, sval=arg))
                elif isinstance(arg, float):
                    q = q.filter(T._attrs.any(name=kw, fval=arg))
                elif isinstance(arg, int):
                    q = q.filter(T._attrs.any(name=kw, ival=arg))
                else:
                    q = q.filter(T._attrs.any(name=kw, bval=repr(arg)))

                return h_self._Query(q)

            def filter_eq_dct(q_self, dct):
                rval = q_self
                for key, val in dct.items():
                    rval = rval.filter_eq(key,val)
                return rval

            def filter_missing(q_self, kw):
                """Return a Query object that restricts to dictionaries
                NOT containing the given keyword"""
                q = q_self._query
                T = h_self._Dict

                q = q.filter(not_(T._attrs.any(name=kw)))

                return h_self._Query(q)

            def all(q_self):
                """Return an iterator over all matching dictionaries.

                See L{SqlAlchemy.Query}
                """
                return q_self._query.all()

            def count(q_self):
                """Return the number of matching dictionaries.

                See L{SqlAlchemy.Query}
                """
                return q_self._query.count()

            def first(q_self):
                """Return some matching dictionary, or None
                See L{SqlAlchemy.Query}
                """
                return q_self._query.first()

            def all_ordered_by(q_self, key, desc=False):
                """Return query results, sorted.

                @type key: string or tuple of string or list of string
                @param: keys by which to sort the results.

                @rtype: list of L{DbHandle._Dict} instances
                @return: query results, sorted by given keys
                """

                # order_by is not easy to do in SQL based on the data structures we're
                # using.  Considering we support different data types, it may not be
                # possible at all.
                #
                # It would be easy if 'pivot' or 'crosstab' were provided as part of the
                # underlying API, but they are not. For example, read this:
                # http://www.simple-talk.com/sql/t-sql-programming/creating-cross-tab-queries-and-pivot-tables-in-sql/

                # load query results
                results = list(q_self.all())

                if isinstance(key, (tuple, list)):
                    val_results = [([d[k] for k in key], d) for d in results]
                else:
                    val_results = [(d[key], d) for d in results]

                val_results.sort() #interesting: there is an optional key parameter
                if desc:
                    val_results.reverse()
                return [vr[-1] for vr in val_results]

        h_self._KeyVal = KeyVal
        h_self._Dict = Dict
        h_self._Query = _Query

    def __iter__(h_self):
        s = h_self.session()
        rval = list(h_self.query(s).__iter__())
        s.close()
        return rval.__iter__()

    def insert_kwargs(h_self, session=None, **dct):
        """
        @rtype:  DbHandle with reference to self
        @return: a DbHandle initialized as a copy of dct

        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
        return h_self.insert(dct, session=session)

    def insert(h_self, dct, session=None):
        """
        @rtype:  DbHandle with reference to self
        @return: a DbHandle initialized as a copy of dct

        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
        # TODO: separate insert into insert and insert_simple, as with update().
        # The idea of passing a session or not is too confusing once this function
        # handles commit exceptions correctly.
        if session is None:
            s = h_self.session()
            rval = h_self._Dict(s)
            if dct: rval.update_simple(dct, session=s)
            s.commit()
            s.close()
        else:
            rval = h_self._Dict(session)
            if dct: rval.update_simple(dct, session=session)
            session.commit()
        return rval

    def query(h_self, session):
        """Construct an SqlAlchemy query, which can be subsequently filtered
        using the instance methods of DbQuery"""
        return h_self._Query(session.query(h_self._Dict)\
                        .options(eagerload('_attrs')))

    def createView(h_self, viewname, verbose = True):

        s = h_self.session()
        kv = h_self._KeyVal
        d = h_self._Dict

        # If view already exists, drop it
        drop_view_sql = 'DROP VIEW IF EXISTS %s' % viewname

        # Get column names
        name_query = s.query(kv.name, kv.type).distinct()

        # Generate sub-queries for the big "join"
        # and build the table structure of the view
        cols = [Column('id', Integer, primary_key=True)]
        safe_names = []
        sub_queries = []
        for name, val_type_char in name_query.all():
            if val_type_char == 'i':
                val_type = Integer
            elif val_type_char == 'f':
                val_type = Float
            elif val_type_char == 'b':
                val_type = Binary
            elif val_type_char == 's':
                val_type = String
            else:
                raise ValueError('Incompatible value in column "type"',
                        val_type_char)
            val_type_string = val_type_char + 'val'

            safe_name = name.replace('_','').replace('.','_')
            if safe_name in safe_names:
                safe_name += '_' + val_type_char
            safe_names.append(safe_name)

            cols.append(Column(safe_name, val_type))
            #print 'name =', name, ', type =', type

            # SQL does not support binds in CREATE VIEW.
            # it just happened to work with psycopg2 because binds are
            # handled client-side.  Sqlite does it server-side.
            sub_query = select(
                    [kv.dict_id, column(val_type_string).label(safe_name)],
                    kv.name == literal_column("'%s'"%(name,)))
            sub_query = sub_query.alias(safe_name)
            sub_queries.append(sub_query)

        # Big "join" from which we select
        big_join = h_self._dict_table
        for sub_query in sub_queries:
            big_join = big_join.outerjoin(sub_query,
                    sub_query.c.dict_id == d.id)

        # Main "select" query, with the same information as the view
        main_query = select([d.id] + [column(name) for name in safe_names],
                from_obj = big_join)
        main_query = main_query.compile()
        quoted_params = {}
        for (key, val) in main_query.params.items():
            quoted_params[key] = repr(val)
        main_query_sql = str(main_query) % quoted_params

        # Finally, the view creation command
        # Do not add 'OR REPLACE', it will break sqlite
        create_view_sql = 'CREATE VIEW %s AS %s'\
                % (viewname, main_query_sql)
        if verbose:
            print 'Creating sql view with command:\n', create_view_sql

        # Execution
        h_self._engine.execute(drop_view_sql);
        h_self._engine.execute(create_view_sql);

        s.commit()
        s.close()

        # Create mapper class for the view
        class MappedView(object):
            pass

        t_view = Table(viewname, MetaData(), *cols)
        mapper(MappedView, t_view)

        return MappedView

    def dropView(h_self, viewname, verbose = True):

        s = h_self.session()
        kv = h_self._KeyVal
        d = h_self._Dict

        drop_view_sql = 'DROP VIEW %s'%(viewname)
        if verbose:
            print 'Deleting sql view with command:\n', drop_view_sql

        # Execution
        h_self._engine.execute(drop_view_sql);

        s.commit()
        s.close()

    def session(h_self):
        return h_self._session_fn()

    def get(h_self, id):
        s = h_self.session()
        rval = s.query(h_self._Dict).get(id)
        if rval:
            #eagerload hack
            str(rval)
            rval.id
        s.close()
        return rval


def db_from_engine(engine,
        dbname,
        table_prefix='DbHandle_default_',
        trial_suffix='trial',
        keyval_suffix='keyval'):
    """Create a DbHandle instance

    @type engine: sqlalchemy engine (e.g. from create_engine)
    @param engine: connect to this database for transactions

    @type table_prefix: string
    @type trial_suffix: string
    @type keyval_suffix: string

    @rtype: DbHandle instance

    @note: The returned DbHandle will use two tables to implement the
    many-to-many pattern that it needs:
     - I{table_prefix + trial_suffix},
     - I{table_prefix + keyval_suffix}

    """
    Session = sessionmaker(autoflush=True)#, autocommit=False)

    metadata = MetaData()

    t_trial = Table(table_prefix+trial_suffix, metadata,
            Column('id', Integer, primary_key=True),
            Column('create', DateTime),
            Column('write', DateTime),
            Column('read', DateTime),
            Column('status', Integer),
            Column('priority', Float(53)),
            Column('hash', BigInteger))

    t_keyval = Table(table_prefix+keyval_suffix, metadata,
            Column('id', Integer, primary_key=True),
            Column('dict_id', Integer, index=True),
            Column('name', String(128), index=True, nullable=False), #name of attribute
            Column('type', String(1)),
            #Column('ntype', Boolean),
            Column('ival', BigInteger),
            Column('fval', Float(53)),
            Column('sval', Text),
            Column('bval', Binary),
            ForeignKeyConstraint(['dict_id'], [table_prefix+trial_suffix+'.id']))

                #, ForeignKey('%s.id' % t_trial)),
    metadata.bind = engine
    metadata.create_all() # no-op when tables already exist
    #warning: tables can exist, but have incorrect schema
    # see bug mentioned in DbHandle constructor

    db = DbHandle(Session, engine, t_trial, t_keyval)
    db.tablename = table_prefix
    db.dbname = dbname
    return db

def get_password(hostname, dbname):
    """Return the current user's password for a given database

    If no password is found, return the empty string. That way
    the ~/.pgpass will be used.

    :TODO: Deprecate this mechanism, and only use the standard location for
           passwords for that database type (for instance, .pgpass for
           postgres)
    """
    password_path = os.getenv('HOME')+'/.jobman_%s'%dbname
    if os.path.isfile(password_path):
        password = open(password_path).readline().rstrip('\r\n')
    else:
        password = ''
    return password

def parse_dbstring(dbstring):
    """Unpacks a dbstring of the form postgres://username[:password]@hostname[:
port]/dbname?table=tablename

    :rtype: tuple of strings
    :returns: username, password, hostname, port, dbname, tablename

    :note: If the password is not given in the dbstring, this function
           attempts to retrieve it using

      >>> password = get_password(hostname, dbname)

    :note: port defaults to 5432 (postgres default).
    """
    url = make_url(dbstring)
    if 'table' not in url.query:
        # support legacy syntax for postgres
        if url.drivername == 'postgres':
            db = url.database.split('/')
            if len(db) == 2:
                url.database = db[0]
                url.query['table'] = db[1]
        else:
            raise ValueError('no table name provided (add ?table=tablename)')

    if url.drivername == 'sqlite':
        url.database = os.path.abspath(url.database)
        url.query['dbname'] = 'SQLITE_DB'

    if url.password is None and url.drivername != 'sqlite':
        url.password = get_password(url.host, url.database)

    return url

def open_db(dbstr, echo=False, serial=False, poolclass=sqlalchemy.pool.NullPool, **kwargs):
    """Create an engine to access a DbHandle.
    """
    url = parse_dbstring(dbstr)

    tablename = url.query.pop('table')
    dbname = url.query.pop('dbname', None)
    if dbname == None:
        dbname = url.database

    if serial:
        if sqlalchemy.__version__ >= '0.6':
            engine = create_engine(url, echo=echo, poolclass=poolclass, isolation_level = 'SERIALIZABLE')
        else:
            import psycopg2.extensions
            def connect():
                c = psycopg2.connect(user=url.username, password=url.password, database=url.database, host=url.host)#, port=url.port)
                c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
                return c
            engine = create_engine('postgres://'
                                   ,creator=connect
                                   ,poolclass=poolclass
                                   )

    else:
        engine = create_engine(url, echo=echo, poolclass=poolclass)

    return db_from_engine(engine, table_prefix=tablename,
                          dbname=dbname, **kwargs)
