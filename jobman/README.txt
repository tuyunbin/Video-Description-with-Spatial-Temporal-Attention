==============================================================================
IMPORTANT: the following README addresses non-standard Jobman usage (ask
James Bergstra for details). For canonical usage, please refer to the OFFICIAL
Jobman documentation (http://deeplearning.net/software/jobman/).
==============================================================================

Database usage from Python
==========================

1. Create a job database.
-------------------------

    >>> from jobman import sql
    >>> db = sql.db('postgres://<username>@<host>/<dbname>/<tableset>')

The password for the user will be read from file ****, if it exists.

This command will create a few tables in the database called <dbname>.
Those tables will all have a name that startswith '<tableset>', the handle
'db' returned by the command uses those tables to implement a
list-of-dictionaries sort of data structure.


2. Populate the database with jobs.
-----------------------------------

Call something like the following for each job you want to insert:

    >>> sql.insert_job(fn, dct, db, force_dup=False, priority=1)

The 'job' is to call 'fn' with kwargs 'dct', stored in 'db'.
This will create jobs with a status indicating that they have never been run.


3. Run jobs at the commandline.
-------------------------------

Typing this...

    $ jobman sql 'postgres://<username>@<host>/<dbname>/<tableset>' /<path>/<to>/<exproot>

will run a process that queries the database for the next job and starts to run it in the directory:

    /<path>/<to>/<exproot>/<dbname>/<tableset>/<jobnum>

It is a good practice to run a job or two in this way to make sure that the job runs cleanly.
When the job runs cleanly, you are ready to offload the rest of the work to the cluster...


4. Run jobs with the cluster.
-----------------------------

On mammouth, call on the command-line

    $ dbidispatch --bqtools --repeat_jobs=<N> jobman sql 'postgres://<username>@<host>/<dbname>/<tableset>'

Where <N> is the number of jobs you would like to run.
This will queue up <N> cluster jobs that will each run in a respective directory:
    /<path>/<to>/<exproot>/<dbname>/<tableset>/<jobnum>




Database usage using mydriver
=============================

The module 'jobman.mydriver' standardizes to some extent the way of running
jobs.  To use it, configure your experiment with a python file that looks
something like this:

.. code-block:: python

    from jobman.mydriver import main
    import sys
    import hpu.kording.driver3

    def jobs_for_mnist_finetune_in_subdir_dbversion():
        for pretrain_iters in (30000,):# 20000, 30000, 40000, 50000):
            for lr in 0.1, 0.01, 0.001:
                yield dict( full_filename='/home/bergstra/exp/hpu/kording/driver3/pt2r/model_%i.pkl'%pretrain_iters
                        , cost_l1_in=0
                        , cost_l1_out=0
                        , cost_l2_in=0
                        , cost_l2_out=0
                        , lr_in=lr
                        , lr_out=lr
                        , layers_trainable=True
                        , infvariations=False)

    main(sys.argv
            , dbstring='postgres://bergstrj@gershwin.iro.umontreal.ca/bergstrj_db/nips09_kording'
            , exp_root='/home/bergstra/exp/hpu/kording/db'
            , job_fn=hpu.kording.driver3.mnist_finetune_in_subdir_dbversion
            , job_dct_seq=jobs_for_mnist_finetune_in_subdir_dbversion()
            )

The advantage of this style of working is that there is a file that can [compactly]
encode all the jobs that you ran with the given job_fn.

Suppose the file above was called foo.py.  Then the jobs can be managed with mydriver.main, by running

    $ python foo.py

This will print a help message.  mydriver.main is not meant to reply to all
needs, but it provides many basic operations, and shows by example how you
might add custom ones.

    $ python foo.py insert --dry  # shows how many of your jobs need inserting
    $ python foo.py insert --dbi "--bqtools --nb_proc=5"   #launches dbidispatch after inserting
    $ python foo.py clear_db      # erases everything from db
    $ python foo.py create_view
    $ python foo.py list --sortd valid_loss    #basic db listing.  For more precision use a custom command


Adding custom commands to mydriver
----------------------------------

.. code-block:: python

    from jobman.mydriver import main
    import sys
    import hpu.kording.driver3

    def jobs_for_mnist_finetune_in_subdir_dbversion():
        for pretrain_iters in (30000,):# 20000, 30000, 40000, 50000):
            for lr in 0.1, 0.01, 0.001:
                yield dict( full_filename='/home/bergstra/exp/hpu/kording/driver3/pt2r/model_%i.pkl'%pretrain_iters
                        , cost_l1_in=0
                        , cost_l1_out=0
                        , cost_l2_in=0
                        , cost_l2_out=0
                        , lr_in=lr
                        , lr_out=lr
                        , layers_trainable=True
                        , infvariations=False)

    @mydriver_cmd
    def list2(db):
        print "Running list2!"
        for d in db:
            print d.id

    main(sys.argv
            , dbstring='postgres://bergstrj@gershwin.iro.umontreal.ca/bergstrj_db/nips09_kording'
            , exp_root='/home/bergstra/exp/hpu/kording/db'
            , job_fn=hpu.kording.driver3.mnist_finetune_in_subdir_dbversion
            , job_dct_seq=jobs_for_mnist_finetune_in_subdir_dbversion()
            )

