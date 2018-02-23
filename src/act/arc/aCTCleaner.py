# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#

import arc

from act.common.aCTProcess import aCTProcess

class aCTCleaner(aCTProcess):
    
    def processToClean(self):
        
        jobstoclean = self.db.getArcJobs("arcstate='toclean' and cluster='"+self.cluster+"' limit 100")
        
        if not jobstoclean:
            return

        self.log.info("Cleaning %i jobs", sum(len(v) for v in jobstoclean.values()))
        for proxyid, jobs in jobstoclean.items():
            self.uc.CredentialString(str(self.db.getProxy(proxyid)))

            job_supervisor = arc.JobSupervisor(self.uc, [j[2] for j in jobs])
            job_supervisor.Update()
            job_supervisor.Clean()
            
            notcleaned = job_supervisor.GetIDsNotProcessed()
    
            for (id, appjobid, job, created) in jobs:
                if job.JobID in notcleaned:
                    self.log.error("%s: Could not clean job %s" % (appjobid, job.JobID))
    
                self.db.deleteArcJob(id)  
  
    def process(self):

        # clean jobs
        self.processToClean()


if __name__ == '__main__':
    st=aCTCleaner()
    st.run()
    st.finish()
