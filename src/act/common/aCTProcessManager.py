import subprocess
import os

import aCTUtils
from act.arc import aCTDBArc

class aCTProcessManager:
    '''
    Manager of aCT processes, starting and stopping as necessary
    '''
    
    def __init__(self, log, conf):
        
        # logger
        self.log = log
        self.conf = conf
        self.actlocation = conf.get(["actlocation","dir"])
        self.logdir = self.conf.get(["logger", "logdir"])
        # DB connection
        self.db = aCTDBArc.aCTDBArc(self.log, self.conf.get(["db","file"]))
        # list of processes to run per cluster
        self.processes = ['act/arc/aCTStatus', 'act/arc/aCTFetcher', 'act/arc/aCTCleaner']
        # submitter process
        self.submitter = 'act/arc/aCTSubmitter'
        # dictionary of processes:aCTProcessHandler of which to run a single instance
        # TODO: app-specific processes in conf file instead of hard-coded
        self.processes_single = {#'act/atlas/aCTAutopilot':None,
                                 #'act/atlas/aCTPandaGetJobs':None,
                                 #'act/atlas/aCTPanda2Arc':None,
                                 #'act/common/aCTProxyHandler':None,
                                 #'act/atlas/aCTATLASStatus':None,
                                 #'act/atlas/aCTValidator':None,
                                 #'act/atlas/aCTAGISFetcher':None
                                 'act/client/client2arc':None
                                 }
        # dictionary of cluster to list of aCTProcessHandlers
        self.running = {}
        # dictionary of cluster to Submitter processes handlers, there should
        # be one per unique cluster in clusterlist
        self.submitters = {}
        
        # Start single instance processes
        for process in self.processes_single:
            proc = self.aCTProcessHandler(process, self.logdir, actlocation=self.actlocation)
            proc.start()
            self.processes_single[process] = proc
        
    def __del__(self):
        
        self.log.info('Shutdown')
        for cluster, procs in self.running.items():
            for proc in procs:
                self.log.info('Terminating %s for %s' % (proc.name, cluster))
                proc.terminate()
        for cluster, proc in self.submitters.items():
            self.log.info('Terminating aCTSubmitter for %s' % cluster)
            proc.terminate()
        for appproc, proc in self.processes_single.items():
            self.log.info('Terminating %s' % appproc)
            proc.terminate()
        
        # Sleep to allow processes to exit before checking them in aCTProcess
        # destructor
        aCTUtils.sleep(1)
        
    def reconnectDB(self):
        ''' 
        Reconnect DB
        '''
        try:
            del self.db
        except AttributeError: # Already deleted
            pass
        self.db = aCTDBArc.aCTDBArc(self.log, self.conf.get(["db", "file"]))
 

    def checkClusters(self):
        '''
        Get the list of current clusters and (re)start necessary processes
        '''
        
        clusters = self.db.getActiveClusters()
        activeclusters = dict((k, v) for (k, v) in zip([c['cluster'] for c in clusters],
                                                       [c['COUNT(*)'] for c in clusters]))
        clusters = self.db.getClusterLists()
        clusterlists = dict((k, v) for (k, v) in zip([c['clusterlist'] for c in clusters],
                                                     [c['COUNT(*)'] for c in clusters]))
        
        # Check for processes that exited and if they should be restarted
        # All running per-cluster processes
        procs = [p for c in self.running for p in self.running[c]]
        # Submitter processes
        procs.extend(self.submitters.values())
        # Single instance processes
        procs.extend(self.processes_single.values())
        
        for proc in procs:
            rc = proc.check()
            if rc == None :
                self.log.debug("Process %s%s is running", proc.name, ' for %s' % proc.cluster if proc.cluster else '')
            elif proc.cluster and proc.cluster not in activeclusters.keys():
                self.log.info("Not restarting %s for %s as not needed", proc.name, proc.cluster)
                if proc.name == self.submitter:
                    del self.submitters[proc.cluster]
                elif proc.cluster in self.running:
                    del self.running[proc.cluster]
            else:
                self.log.info("Restarting process %s %s", proc.name, 'for %s' % proc.cluster if proc.cluster else '')
                proc.restart()
        
        # Check for new processes to start
        for cluster in activeclusters:
            if cluster and cluster not in self.running.keys():
                self.running[cluster] = []
                for proc in self.processes:
                    self.log.info("Starting process %s for %s", proc, cluster)
                    ph = self.aCTProcessHandler(proc, self.logdir, cluster, actlocation=self.actlocation)
                    ph.start()
                    self.running[cluster].append(ph)
        
        # Get unique list of clusters from cluster lists
        clusterlist = []
        for cluster in clusterlists:
            if not cluster:
                cluster = ''
            clist = cluster.split(',')
            for c in clist:
                if c not in clusterlist:
                    clusterlist.append(c)

        # Start any new submitters required
        for cluster in clusterlist:
            if cluster not in self.submitters:
                self.log.info("Starting process aCTSubmitter for %s", cluster)
                ph = self.aCTProcessHandler(self.submitter, self.logdir, cluster, actlocation=self.actlocation)
                ph.start()
                self.submitters[cluster] = ph
        
                
    class aCTProcessHandler:
        """
        Internal process control class wrapping Popen
        """
        def __init__(self, name, logdir, cluster='', actlocation=''):
            self.name = name
            self.cluster = cluster
            self.child = None
            self.actlocation = actlocation
            # Redirect stdout and stderr to process log
            self.fdout = open(os.path.join(logdir, name[name.rfind('/')+1:]+'.log'), 'a')            
        def __del__(self):
            self.kill()
        def start(self):
            self.child = subprocess.Popen(['/usr/bin/python2', os.path.join(self.actlocation, self.name+".py"), self.cluster], stdout=self.fdout, stderr=subprocess.STDOUT)            
        def check(self):
            return self.child.poll()
        def restart(self):
            if self.check() != None:
                self.start()
        def terminate(self):
            if self.child:
                self.child.terminate()
        def kill(self):
            # first kill nicely (SIGTERM)
            self.terminate()
            if self.child and self.check() == None:
                try:
                    print 'checking pid', self.child.pid
                    os.kill(self.child.pid, 0)
                except OSError: # process already exited
                    print 'process gone'
                    return
                print 'process still running, sleeping'
                aCTUtils.sleep(1)
                # make sure it is gone
                self.child.kill()

