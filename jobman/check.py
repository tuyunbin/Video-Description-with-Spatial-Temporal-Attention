from subprocess import Popen, PIPE
import os
import re
import time
from optparse import OptionParser

import sql
from runner import runner_registry
from tools import UsageError
from api0 import open_db

parse_check = OptionParser(usage='%prog check <tablepath> ',
                           add_help_option=False)


def str_time(run_time):
    run_time = "%dd %dh%dm%ds" % (run_time / (24 * 3600),
                                  run_time % (24 * 3600) / 3600,
                                  run_time % 3600 / 60,
                                  run_time % 60)
    return run_time


def check_running_pbs_jobs(r, now):
    """ Verify jobs on Torque/PBS system"""
    p = Popen('qstat -x %s' % r['jobman.sql.pbs_task_id'],
              shell=True, stdout=PIPE, stderr=PIPE)
    ret = p.wait()
    out = p.stdout.read()

    if len(out) == 0:
        print ("E: Job %d marked as PBS job '%s',"
               " but 'qstat' don't know it." % (
                  r.id, r['jobman.sql.pbs_task_id']))
        return

    run_time = str_time(now - r["jobman.sql.start_time"])

    #check runtime
    #<walltime>48:00:00</walltime>
    ressource_str = re.search('<Resource_List>.*</Resource_List>',
                              out).group(0)
    walltime_str = re.search('<walltime>.*?</walltime>', ressource_str)
    walltime_str = walltime_str.group(0)[10:-11]
    assert walltime_str[2] == ':' and walltime_str[5] == ':'
    walltime = (int(walltime_str[:2]) * 60 * 60 +
                int(walltime_str[3:5]) * 60 +
                int(walltime_str[-2:]))
    if now - int(r["jobman.sql.start_time"]) > walltime:
        print ("W: Job %d is running for more then the specified"
               " max time of %s. Run time %s" % (
                   r.id, walltime_str, run_time))

    # check state
    #<job_state>R</job_state>
    state_str = re.search('<job_state>.*</job_state>', out).group(0)[11:-12]
    if state_str == "R":
        pass
    elif state_str == "Q":
        print ("E: Job %d is running in the db on pbs with job id %s, but it"
               " is in the pbs queue. Run time %s" % (
                  r.id, r["jobman.sql.pbs_task_id"], run_time))
    elif state_str == "C":
        print ("W: Job %d is running in the db, but it is marked as completed"
               " in the pbs queue. This can be synchonization issue. Retry in"
               " 1 minutes. Run time %s." % (r.id, run_time))
    else:
        print ("W: Job %d is running in the db, but we don't understand the"
               " state in the queue '%s'" % (r.id, state_str))


def check_running_sge_jobs(r, now):
    p = Popen('qstat', shell=True, stdout=PIPE)
    ret = p.wait()
    lines = p.stdout.readlines()
    """
                qstat output:

                job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
                -----------------------------------------------------------------------------------------------------------------
                 776410 0.50000 dbi_6a5f45 bastienf     r     10/18/2010 13:26:46 smp@r106-n72                       1 1
                  776410 0.50000 dbi_6a5f45 bastienf     r     10/18/2010 13:26:46 smp@r106-n72                       1 2
                   776415 0.00000 dbi_5381a1 bastienf     qw    10/18/2010 13:30:21                                    1 1,2
    """
    if len(lines) == 0:
        print "E: Job %d marked as a SGE job, but `qstat` on this host tell that their is no job running." % r.id
        return

    assert lines[0] == 'job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID \n'
    assert lines[1] == '-----------------------------------------------------------------------------------------------------------------\n'
    run_time = str_time(now - r["jobman.sql.start_time"])
    if now - int(r["jobman.sql.start_time"]) > (24 * 60 * 60):
        print "W: Job %d is running for more then 24h. The current colosse max run time is 24h. Run time %s" % (r.id, run_time)

    found = False
    for line in lines[2:]:
        sp = line.split()
        ta_sp = sp[9].split(',')
        if len(sp) != 10:
            print "W: Job %d. Don't understant one line of qstat output's. Can't tell reliably if it is still running or not" % r.id
            print "qstat output: ", line
        if (sp[0] == r["jobman.sql.job_id"] and
            r["jobman.sql.sge_task_id"] in ta_sp):
            if sp[4] == 'r':
                pass
            elif sp[4] == 'qw':
                print "E: Job %d is running in the db on sge with job id %s and task id %s, but it is waiting in the sge queue. Run time %s" % (
                   r.id, r["jobman.sql.job_id"],
                   r["jobman.sql.sge_task_id"], run_time)
            elif sp[4] == 't':
                print "W: Job %d is running in the db, but it is marked as ended in the sge queue. This can be synchonization issue. Retry in 1 minutes. Run time %s." % (
                   r.id, run_time)
            else:
                print "W: Job %d is running in the db and in the sge queue, but we don't understant the state it is in the queue:", sp[4]
                found = True  # in the sge queue
                break
    if not found:
        print "E: Job %d marked as running in the db on sge with job id %s and task id %s, but not in sge queue. Run time %s." % (
           r.id, r["jobman.sql.job_id"],
           r["jobman.sql.sge_task_id"], run_time)


def check_serve(options, dbdescr):
    """Check that all jobs marked as running in the db are marked as
    running in some cluster jobs scheduler.

    print jobs that could have crashed/been killed ...

    Example usage:

        jobman check <tablepath>

    """

    db = open_db(dbdescr, serial=True)

    try:
        session = db.session()
        q = db.query(session)
        idle = q.filter_eq('jobman.status', 0).all()
        running = q.filter_eq('jobman.status', 1).all()
        finished = q.filter_eq('jobman.status', 2).all()
        err_start = q.filter_eq('jobman.status', 3).all()
        err_sync = q.filter_eq('jobman.status', 4).all()
        err_run = q.filter_eq('jobman.status', 5).all()
        canceled = q.filter_eq('jobman.status', -1).all()
        info = []

        print ("I: number of job by status (%d:START, %d:RUNNING, %d:DONE,"
               " %d:ERR_START, %d:ERR_SYNC, %d:ERR_RUN, %d:CANCELED)"
               " in the db (%d:TOTAL)" % (len(idle), len(running),
                                          len(finished), len(err_start),
                                          len(err_sync), len(err_run),
                                          len(canceled), len(q.all())))
        print

        #warn about job in error status
        if len(err_start):
            print "E: The following jobs had an error when starting them",
            print [j.id for j in err_start]
        if len(err_sync):
            print "E: The following jobs had an error while doing the rsync",
            print [j.id for j in err_sync]
        if len(err_run):
            print "E: The following jobs had an error while running",
            print [j.id for j in err_run]
        print

        #check not 2 jobs in same slot+host
        host_slot = {}
        now = time.time()

        #check job still running
        for idx, r in enumerate(running):
            condor_job = False
            sge_job = False
            pbs_job = False

            #find the backend used for the job.
            if ("jobman.sql.condor_slot" in r.keys() and
                r["jobman.sql.condor_slot"] != "no_condor_slot"):
                condor_job = True
            if "jobman.sql.sge_task_id" in r.keys():
                sge_job = True
            if "jobman.sql.pbs_task_id" in r.keys():
                pbs_job = True
            if (sge_job + condor_job + pbs_job) > 1:
                print "W: Job %d have info such that it run on condor, sge and/or pbs. We can't determine the good one."
                continue
            if not (sge_job or condor_job or pbs_job):
                print "W: Job %d don't have condor, sge or pbs info attached to it. We can't determine if it is still running on the cluster. Old jobman to started the job?" % r.id
                continue

            #check that the job is still running.
            if sge_job:
                check_running_sge_jobs(r, now)
                continue

            if pbs_job:
                check_running_pbs_jobs(r, now)
                continue

            if not condor_job:
                print "W: Job %d is running but don't have the information needed to check if they still run on the jobs scheduler condor/pbs/torque/sge. Possible reasons: the job started with an old version of jobman or on another jobs scheduler."%r.id
                continue

            # We suppose the jobs started on condor.
            try:
                h = r["jobman.sql.host_name"]
                s = r["jobman.sql.condor_slot"]
            except KeyError, e:
                print "W: Job %d is running but don't have needed info to check them again condor. Possible reaons: the job started with an old version of jobman or without condor."%r.id
                continue
            st = s + '@' + h
            if host_slot.has_key(st):
                try:
                    t0 = str_time(now - running[host_slot[st]]["jobman.sql.start_time"])
                except KeyError:
                    t0 = 'NO_START_TIME'
                try:
                    t1 = str_time(now - r["jobman.sql.start_time"])
                except KeyError:
                    t1 = 'NO_START_TIME'
                print 'E: Job %d and Job %d are running on the same condor slot/host combination. running time: %s and %s'%(running[host_slot[st]].id,r.id,t0,t1)
            else:
                host_slot[st]=idx

            gjid = None
            if "jobman.sql.condor_global_job_id" in r.keys():
                gjid = r["jobman.sql.condor_global_job_id"]
            elif "jobman.sql.condor_GlobalJobId" in r.keys():
                gjid = r["jobman.sql.condor_GlobalJobId"]
            if gjid is not None:
                submit_host = gjid.split('#')[0]

                #import pdb;pdb.set_trace()
                #take care of the quotation, condor resquest that "" be used
                #around string.
                cmd = "condor_q -name %s -const 'GlobalJobId==\"%s\"' -format '%%s' 'JobStatus'"%(submit_host,gjid)
                p = Popen(cmd, shell=True, stdout=PIPE)
                ret = p.wait();
                lines = p.stdout.readlines()

                if ret == 127 and len(lines) == 0:
                    print "W: Job %d. condor_q failed. Is condor installed on this computer?"%r.id
                    continue

                if len(lines) == 0:
                    print "E: Job %d is marked as running in the bd on this condor jobs %s, but condor tell that this jobs is finished"%(r.id,gjid)
                    continue
                elif len(lines) == 1:
                    if lines[0] == '0':  # condor unexpanded??? What should we do?
                        print "E: Job %d is marked as running in the db, but its condor submited job is marked as unexpanded. We don't know what that mean, so we use an euristic to know if the jobs is still running."%r.id
                    elif lines[0] == '1':  # condor idle
                        print "E: Job %d is marked as running in the db, but its condor submited job is marked as idle. This can mean that the computer that was running this job crashed."%r.id
                        continue
                    elif lines[0] == '2':  # condor running
                        continue
                    elif lines[0] == '3':  # condor removed
                        print "E: Job %d is marked as running in the db, but its condor submited job is marked as removed."%r.id
                    elif lines[0] == '4':  # condor completed
                        print "E: Job %d is marked as running in the db, but its condor submited job is marked as completed."%r.id
                    elif lines[0] == '5':  # condor held
                        print "E: Job %d is marked as running in the db, but its condor submited job is marked as held."%r.id
                    elif lines[0] == '6':  # condor submission error
                        print "E: Job %d is marked as running in the db, but its condor submited job is marked as submission error(SHOULD not happen as if condor can't start the job, it don't select one in the db)."%r.id

                else:
                    print "W: condor return a not understood answer to a query. We will try some euristic to determine if it is running. test command `%s`. stdout returned `%s`"%(cmd,lines)
    #except KeyError:
    #            pass
            info = (r.id,
                    r["jobman.experiment"],
                    r["jobman.sql.condor_slot"],
                    r["jobman.sql.host_name"],
                    r["jobman.sql.start_time"])
            run_time = str_time(now - info[4])

            if info[2] == "no_condor_slot":
                print "W: Job %d is not running on condor(Should not happed...)"%info[0]
            else:
                p = Popen('''condor_status -constraint 'Name == "slot%s@%s"' -format "%%s" Name -format " %%s" State -format " %%s" Activity -format " %%s" RemoteUser -format " %%s\n" RemoteOwner''' % (info[2], info[3]),
                        shell=True, stdout=PIPE)
                p.wait()
                lines = p.stdout.readlines()
                #return when running: slot1@brams0b.iro.umontreal.ca Claimed Busy bastienf bastienf
                #return when don't exist: empty
                if len(lines) == 0:
                    print "W: Job %d is running on a host(%s) that condor lost connection with. The job run for: %s"%(r.id, info[3], run_time)
                    continue
                elif len(lines) != 1 and not (len(lines) == 2 and lines[-1] == '\n'):
                    print "W: Job %d condor_status return not understood: ",lines
                    continue
                sp = lines[0].split()
                if len(sp) >= 3 and sp[1] in ["Unclaimed", "Owner"] and sp[2] == "Idle":
                    print "E: Job %d db tell that this job is running on %s. condor tell that this host don't run a job. running time %s"%(r.id,info[3],run_time)
                elif len(sp) == 5:
                    assert sp[0] == "slot%s@%s" % (info[2], info[3])
                    if sp[3] != sp[4]:
                        print "W: Job %d condor_status return not understood: ",lines
                    if sp[1] == "Claimed" and sp[2] in ["Busy", "Retiring"]:
                        if sp[4].split('@')[0] == os.getenv("USER"):
                            print "W: Job %d is running on a condor host that is running a job of the same user. running time: %s"%(r.id,run_time)
                        else:
                            print "E: Job %d is running on a condor host that is running a job for user %s. running time: %s"%(r.id,sp[4].split('@')[0],run_time)
                    else:
                        print "W: Job %d condor state of host not understood"%r.id,sp
                else:
                    print "W: Job %d condor_status return not understood: ",lines

    finally:
        session.close()

runner_registry['check'] = (parse_check, check_serve)
