from urlparse import urlparse
from aCTATLASProcess import aCTATLASProcess
from aCTPanda2Xrsl import aCTPanda2Xrsl

class aCTPanda2Arc(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the arcjobs table.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        
        self.sites = {}
        self.setSites()
        print self.sites

    def setSites(self):
        for sitename in self.conf.getList(["sites","site","name"]):
            self.sites[sitename] = {}
            self.sites[sitename]['endpoints'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["endpoints","item"])
            self.sites[sitename]['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["schedconfig"])[0]
            try:
                self.sites[sitename]['corecount'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["corecount"])[0])
            except:
                self.sites[sitename]['corecount'] = 1
            try:
                self.sites[sitename]['catalog'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["catalog"])[0]
            except:
                self.sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            try:
                self.sites[sitename]['truepilot'] = int(self.conf.getListCond(["sites", "site"], "name=" + sitename, ["truepilot"])[0])
            except:
                self.sites[sitename]['truepilot'] = 0

    def createArcJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL limit 10000")

        for job in jobs:
            
            parser = aCTPanda2Xrsl(job['pandajob'], job['siteName'], self.sites[job['siteName']]['schedconfig'],
                                   self.sites[job['siteName']]['catalog'], self.sites[job['siteName']]['corecount'],
                                   self.sites[job['siteName']]['truepilot'])
            parser.parse()
            try:
                xrsl = parser.getXrsl()
            except:
                pass
            if xrsl is not None:
                #print xrsl
                endpoints = self.sites[job['siteName']]['endpoints']
                cl = []
                for e in endpoints:
                    cl.append(urlparse(e).hostname + urlparse(e).path)
                cls = ",".join(cl)
                self.log.info("Inserting job %i with clusterlist %s" % (job['pandaid'], cls))
                maxattempts = 5
                if job['siteName'] == 'BOINC':
                    maxattempts = 30

                # Set the list of files to download at the end of the job
                downloadfiles = 'gmlog/errors'
                if not self.sites[job['siteName']]['truepilot']:
                    downloadfiles += ';jobSmallFiles.tgz'
                try:
                    downloadfiles += ';%s' % parser.jobdesc['logFile'][0].replace('.tgz', '')
                except:
                    pass

                aid = self.dbarc.insertArcJobDescription(xrsl, maxattempts=maxattempts, clusterlist=cls,
                                                         proxyid=job['proxyid'], appjobid=str(job['pandaid']),
                                                         downloadfiles=downloadfiles)
                if not aid:
                    self.log.error("%s: Failed to insert arc job description: %s" % (job['pandaid'], xrsl))
                    continue
                
                jd = {}
                jd['arcjobid'] = aid['LAST_INSERT_ID()']
                jd['pandastatus'] = 'starting'
                # make sure actpandastatus is really 'sent', in case of resubmitting 
                jd['actpandastatus'] = 'sent'
                self.dbpanda.updateJob(job['pandaid'], jd)
                

    def process(self):
        self.setSites()
        self.createArcJobs()


if __name__ == '__main__':

    am=aCTPanda2Arc()
    am.run()
    am.finish()
