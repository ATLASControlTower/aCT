# aCTStatus.py
#
# Process to check the status of running ARC jobs
#
import re
import time
import arc

from act.common.aCTProcess import aCTProcess

class aCTStatus(aCTProcess):
    '''
    Class for checking the status of submitted ARC jobs and updating their
    status in the DB.
    '''
    
    def __init__(self):
        
        aCTProcess.__init__(self) 

        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime=time.time()


    def resetJobs(self, jobstoreset):
        '''
        Empty all StringLists in jobs so that when they are updated they do not
        contain duplicate values, since ARC always appends to these lists.
        '''
        emptylist = arc.StringList()
        j = arc.Job()
        attrstoreset = [attr for attr in dir(j) if type(getattr(j, attr)) == arc.StringList]
             
        for jobs in jobstoreset.values():   
            for job in jobs:
                for attr in attrstoreset:
                    setattr(job[2], attr, emptylist)
        
        
    def processJobErrors(self, id, appjobid, failedjob):
        '''
        Examine errors of failed job and decide whether to resubmit or not
        '''
        errors = ";".join([joberr for joberr in failedjob.Error])
        self.log.info("%s: Job failure for %s: %s" % (appjobid, failedjob.JobID, errors))
        
        # First check if it was a data staging problem
        if failedjob.RestartState == arc.JobState.PREPARING or \
           failedjob.RestartState == arc.JobState.FINISHING:
            # Don't retry when output list is not available
            if 'Error reading user generated output file list' not in errors:
                self.log.info("%s: Will rerun job %s" % (appjobid, failedjob.JobID))
                # Reset arc job state so that next time new state will be picked up
                failedjob.State = arc.JobState('Undefined')
                return "torerun"
        
        newstate = "failed"
        # Check if any job runtime error matches any error in the toresubmit list
        resub = [err for err in self.conf.getList(['errors','toresubmit','arcerrors','item']) if err in errors]
        attemptsleft = int(self.db.getArcJobInfo(id, ['attemptsleft'])['attemptsleft']) - 1
        if attemptsleft < 0:
            attemptsleft = 0
        self.db.updateArcJob(id, {'attemptsleft': str(attemptsleft)})
        if resub:
            if not attemptsleft:
                self.log.info("%s: Job %s out of retries" % (appjobid, failedjob.JobID))
            else:
                self.log.info("%s: Will resubmit job %s, %i attempts left" % (appjobid, failedjob.JobID, attemptsleft))
                failedjob.State = arc.JobState('Undefined')
                newstate = "toresubmit"
        
        else:
            self.log.info("%s: Job %s has fatal errors, cannot resubmit" % (appjobid, failedjob.JobID))
        return newstate
        
    def checkJobs(self):
        '''
        Query all running jobs
        '''
        
        # minimum time between checks
        if time.time()<self.checktime+int(self.conf.get(['jobs','checkmintime'])):
            self.log.debug("mininterval not reached")
            return
        self.checktime=time.time()

        # check jobs which were last checked more than checkinterval ago
        jobstocheck=self.db.getArcJobs("(arcstate='submitted' or arcstate='running' or arcstate='cancelling' or arcstate='holding') and " \
                                       "jobid not like '' and cluster='"+self.cluster+"' and "+ \
                                       self.db.timeStampLessThan("tarcstate", self.conf.get(['jobs','checkinterval'])) + \
                                       " limit 100000")

        njobstocheck = sum(len(v) for v in jobstocheck.itervalues())
        if not njobstocheck:
            return
        self.log.info("%d jobs to check" % njobstocheck)
        self.resetJobs(jobstocheck)
        
        # Loop over proxies
        for proxyid, jobs in jobstocheck.items():
            self.uc.CredentialString(str(self.db.getProxy(proxyid)))
    
            job_supervisor = arc.JobSupervisor(self.uc, [j[2] for j in jobs])
            job_supervisor.Update()
            jobsupdated = job_supervisor.GetAllJobs()
            jobsnotupdated = job_supervisor.GetIDsNotProcessed()
            
            for (originaljobinfo, updatedjob) in zip(jobs, jobsupdated):
                (id, appjobid, originaljob, created) = originaljobinfo
                if updatedjob.JobID in jobsnotupdated:
                    self.log.error("%s: Failed to find information on %s" % (appjobid, updatedjob.JobID))
                    continue
                if updatedjob.JobID != originaljob.JobID:
                    # something went wrong with list order
                    self.log.warning("%s: Bad job id (%s), expected %s" % (appjobid, updatedjob.JobID, originaljob.JobID))
                    continue
                # compare strings here to get around limitations of JobState API
                # map INLRMS:S and O to HOLD (not necessary when ARC 4.1 is used)
                if updatedjob.State.GetGeneralState() == 'Queuing' and (updatedjob.State.GetSpecificState() == 'INLRMS:S' or updatedjob.State.GetSpecificState() == 'INLRMS:O'):
                    updatedjob.State = arc.JobState('Hold')
                if originaljob.State.GetGeneralState() == updatedjob.State.GetGeneralState() \
                     and self.cluster != 'gsiftp://gar-ex-etpgrid1.garching.physik.uni-muenchen.de:2811/preempt':
                    # just update timestamp
                    # Update numbers every time for superMUC since walltime is missing for finished jobs
                    self.db.updateArcJob(id, {'tarcstate': self.db.getTimeStamp()})
                    continue
                
                self.log.info("%s: Job %s: %s -> %s (%s)" % (appjobid, originaljob.JobID, originaljob.State.GetGeneralState(),
                               updatedjob.State.GetGeneralState(), updatedjob.State.GetSpecificState()))
                
                # state changed, update whole Job object
                arcstate = 'submitted'
                if updatedjob.State == arc.JobState.FINISHED:
                    if updatedjob.ExitCode == -1:
                        # Missing exit code, but assume success
                        self.log.warning("%s: Job %s FINISHED but has missing exit code, setting to zero" % (appjobid, updatedjob.JobID))
                        updatedjob.ExitCode = 0
                    arcstate = 'finished'
                    self.log.debug('%s: reported walltime %d, cputime %d' % (appjobid, updatedjob.UsedTotalWallTime.GetPeriod(), updatedjob.UsedTotalCPUTime.GetPeriod()))
                elif updatedjob.State == arc.JobState.FAILED:
                    arcstate = self.processJobErrors(id, appjobid, updatedjob)
                elif updatedjob.State == arc.JobState.KILLED:
                    arcstate = 'cancelled'
                elif updatedjob.State == arc.JobState.RUNNING or \
                     updatedjob.State == arc.JobState.FINISHING:
                    arcstate = 'running'
                elif updatedjob.State == arc.JobState.HOLD:
                    arcstate = 'holding'
                elif updatedjob.State == arc.JobState.DELETED or \
                     updatedjob.State == arc.JobState.OTHER:
                    # unexpected
                    arcstate = 'failed'
                    
                # Fix crazy wallclock and CPU times
                if updatedjob.UsedTotalWallTime > arc.Time() - arc.Time(int(created.strftime("%s"))):
                    fixedwalltime = arc.Time() - arc.Time(int(created.strftime("%s")))
                    self.log.warning("%s: Fixing reported walltime %d to %d" % (appjobid, updatedjob.UsedTotalWallTime.GetPeriod(), fixedwalltime.GetPeriod()))
                    updatedjob.UsedTotalWallTime = fixedwalltime
                if updatedjob.UsedTotalCPUTime > arc.Period(10**7):
                    self.log.warning("%s: Discarding reported CPUtime %d" % (appjobid, updatedjob.UsedTotalCPUTime.GetPeriod()))
                    updatedjob.UsedTotalCPUTime = arc.Period(-1)
                self.db.updateArcJob(id, {'arcstate': arcstate, 'tarcstate': self.db.getTimeStamp(), 'tstate': self.db.getTimeStamp()}, updatedjob)

        self.log.info('Done')
        
    def checkLostJobs(self):
        '''
        Move jobs with a long time since status update to lost
        '''

        # 2 days limit. TODO: configurable?
        jobs=self.db.getArcJobsInfo("(arcstate='submitted' or arcstate='running' or arcstate='cancelling' or arcstate='finished') and " \
                                    "cluster='"+self.cluster+"' and "+self.db.timeStampLessThan("tarcstate", 172800),
                                    ['id', 'appjobid', 'JobID', 'arcstate'])
        
        for job in jobs:
            if job['arcstate'] == 'cancelling':
                self.log.warning("%s: Job %s lost from information system, marking as cancelled" % (job['appjobid'], job['JobID']))
                self.db.updateArcJob(job['id'], {'arcstate': 'cancelled', 'tarcstate': self.db.getTimeStamp()})
            else:
                self.log.warning("%s: Job %s lost from information system, marking as lost" % (job['appjobid'], job['JobID']))
                self.db.updateArcJob(job['id'], {'arcstate': 'lost', 'tarcstate': self.db.getTimeStamp()})


    def checkStuckJobs(self):
        '''
        check jobs with tstate too long ago and set them tocancel
        maxtimestate can be set in arc config file for any state in arc.JobState,
        e.g. maxtimerunning, maxtimequeuing
        '''
        
        # Loop over possible states
        # Note: MySQL is case-insensitive. Need to watch out with other DBs
        for jobstate in [state.lower() for state in dir(arc.JobState) if re.match('^[A-Z][A-Z]', state)]:
            maxtime = self.conf.get(['jobs', 'maxtime%s' % jobstate])
            if not maxtime:
                continue
            
            # be careful not to cancel jobs that are stuck in cleaning
            select = "state='%s' and %s" % (jobstate, self.db.timeStampLessThan("tstate", maxtime))
            jobs = self.db.getArcJobsInfo(select, columns=['id', 'JobID', 'appjobid', 'arcstate'])
            
            for job in jobs:
                if job['arcstate'] == 'toclean' or job['arcstate'] == 'cancelling':
                   # delete jobs stuck in toclean/cancelling
                   self.log.info("%s: Job stuck in toclean/cancelling for too long, deleting" % (job['appjobid']))
                   self.db.deleteArcJob(job['id'])
                   continue

                self.log.warning("%s: Job %s too long in state %s, cancelling" % (job['appjobid'], job['JobID'], jobstate))
                if job['JobID']:
                    # If jobid is defined, cancel
                    self.db.updateArcJob(job['id'], {'arcstate': 'tocancel', 'tarcstate': self.db.getTimeStamp(), 'tstate': self.db.getTimeStamp()})
                else:
                    # Otherwise delete it
                    self.db.deleteArcJob(job['id'])
        
    
    def process(self):
        # check job status
        self.checkJobs()
        # check for lost jobs
        self.checkLostJobs()
        # check for stuck jobs too long in one state and kill them
        self.checkStuckJobs()

if __name__ == '__main__':
    st=aCTStatus()
    st.run()
    st.finish()
