import cgi
import json
import os
import re
import time
import uuid


class aCTPanda2Xrsl:

    def __init__(self, pandajob, sitename, siteinfo, osmap, tmpdir, atlasconf, eventranges, log):
        self.log = log
        self.pandajob = pandajob
        self.jobdesc = cgi.parse_qs(pandajob)
        self.pandaid = self.jobdesc['PandaID'][0]
        self.xrsl = {}
        self.siteinfo = siteinfo
        self.ncores = siteinfo['corecount']

        self.defaults = {}
        self.defaults['memory'] = 2000
        self.defaults['cputime'] = 2*1440*60
        self.sitename = sitename
        self.schedconfig = siteinfo['schedconfig']
        self.truepilot = siteinfo['truepilot']
        self.osmap = osmap
        self.maxwalltime = siteinfo['maxwalltime']
        if self.maxwalltime == 0:
            self.maxwalltime = 7*24*60

        self.tmpdir = tmpdir
        self.inputfiledir = os.path.join(self.tmpdir, 'inputfiles')
        self.inputjobdir = os.path.join(self.inputfiledir, self.jobdesc['PandaID'][0])
        self.atlasconf = atlasconf
        self.eventranges = eventranges
        self.longjob = False
        self.traces = []
        if len(self.pandajob) > 50000:
            self.longjob = True

        self.rtesites = ["BEIJING-CS-TH-1A_MCORE","BEIJING-ERAII_MCORE","BEIJING-TIANJIN-TH-1A_MCORE","LRZ-LMU_MUC1_MCORE"]#,"LRZ-LMU_MUC_MCORE1"]#"MPPMU-DRACO_MCORE","MPPMU-HYDRA_MCORE"]
        self.artes = None
        # ES merge jobs need unique guids because pilot uses them as dict keys
        if not self.truepilot and self.jobdesc.has_key('eventServiceMerge') and self.jobdesc['eventServiceMerge'][0] == 'True':
            if self.pandajob.startswith('GUID'):
                esjobdesc = self.pandajob[self.pandajob.find('&'):]
            else:
                esjobdesc = self.pandajob[:self.pandajob.find('&GUID')] + self.pandajob[self.pandajob.find('&', self.pandajob.find('&GUID')+5):]
            esjobdesc += '&GUID=%s' % '%2C'.join(['DUMMYGUID%i' % i for i in range(len(self.jobdesc['GUID'][0].split(',')))])
            self.pandajob = esjobdesc

        #print self.jobdesc.keys()

    def getNCores(self):

        # Unified panda queues: always use coreCount from job description
        try:
	    self.ncores = int(self.jobdesc.get('coreCount', [1])[0])
        except: # corecount is NULL
            self.ncores = 1
        # For accounting BOINC running slots, force corecount to 1
        if 'BOINC' in self.sitename:
            self.ncores = 1
        if self.sitename == 'CSCS-LCG2-HPC_MCORE_TEST':
	    self.ncores = 18

        self.xrsl['count'] = '(count=%d)' % self.ncores

        # force single-node jobs for now
        if self.ncores > 1:
            self.xrsl['countpernode'] = '(countpernode=%d)' % self.ncores
            if self.sitename.find('RAL-LCG2') < 0 and self.sitename.find('TOKYO') < 0 and self.sitename.find('FZK') < 0 and self.sitename.find('TRIUMF') < 0 and self.sitename.find('IFIC') < 0 and self.sitename.find('pic') < 0:
                self.xrsl['countpernode'] = '(runtimeenvironment = APPS/HEP/ATLAS-MULTICORE-1.0)'

        return self.ncores

    def setJobname(self):

        if 'jobName' in self.jobdesc:
            jobname = self.jobdesc['jobName'][0]
        else:
            jobname = "pandajob"
        self.xrsl['jobname'] = '(jobname = "%s")' % jobname

    def setDisk(self):

        if 'UIO' not in self.sitename or self.sitename == 'UIO_MCORE_LOPRI':
            return
        # Space for data created by the job
        if 'maxDiskCount' in self.jobdesc:
            disk = int(self.jobdesc['maxDiskCount'][0])
        else:
            disk = 500
        # Add input file sizes
        if 'fsize' in self.jobdesc:
            disk += sum([int(f) for f in self.jobdesc['fsize'][0].split(',')]) / 1000000
        # Add safety factor
        disk += 2000
        self.log.debug('%s: disk space %d' % (self.pandaid, disk))
        self.xrsl['disk'] = "(disk = %d)" % disk

    def setTime(self):

        if 'maxCpuCount' in self.jobdesc:
            cpucount = int(self.jobdesc['maxCpuCount'][0])

            # hack for group production!!!
            if cpucount == 600:
                cpucount = 24*3600

            cpucount = int(2 * cpucount)
            self.log.info('%s: job maxCpuCount %s' % (self.pandaid, cpucount))
        else:
            cpucount = 2*24*3600
            self.log.info('%s: Using default maxCpuCount %s' % (self.pandaid, cpucount))

        if cpucount == 0:
            cpucount = 2*24*3600

        #if cpucount < 50000:
        #    cpucount = 50000

        # shorten installation jobs
        try:
            if self.jobdesc['prodSourceLabel'][0] == 'install':
                cpucount = 12*3600
        except:
            pass

        if int(cpucount) <= 0:
            cpucount = self.defaults['cputime']

        walltime = int(cpucount / 60)

        # panda changed to walltime
        #if self.getNCores() > 1:
        #    walltime = int (walltime / self.getNCores() )

        # JEDI analysis hack
        walltime = max(120, walltime)
        walltime = min(self.maxwalltime, walltime)
        cputime = self.getNCores() * walltime
        if self.sitename.startswith('BOINC'):
            walltime = min(240, walltime)
            cputime = walltime
        self.log.info('%s: walltime: %d, cputime: %d' % (self.pandaid, walltime, cputime))

        self.xrsl['time'] = '(walltime=%d)(cputime=%d)' % (walltime, cputime)

    def setMemory(self):

        if 'minRamCount' in self.jobdesc:
            memory = int(self.jobdesc['minRamCount'][0])
        elif not self.sitename.startswith('ANALY'):
            memory = 4000
        else:
            memory = 2000

        if memory <= 0:
            memory = self.defaults['memory']

        # fix until maxrrs in pandajob is better known
        if memory <= 1000:
            memory = 1000

        if self.sitename == 'BOINC' or self.sitename == 'BOINC_MCORE':
            memory = 2400

        # hack mcore pile, use new convention for memory
        #if self.getNCores() > 1 and memory > 3000:
        #    if memory > 1000:
        #        memory = memory / self.getNCores()
        #    else:
        #        memory = 3000

        # fix memory to 500MB units (AF fix before divide)
        memory = int(memory-1)/500*500 + 500

        if self.getNCores() > 1:
            # hack for 0 ramcount, defaulting to 4000, see above, fix to 2000/core
            if memory == 4000:
                memory = 2000
            else:
                memory = memory / self.getNCores()

        #AF # fix memory to 500MB units
        #AF memory = int(memory-1)/500*500 + 500
	# temp hack
        if self.sitename == 'CSCS-LCG2-HPC_MCORE_TEST':
		memory = 850
        if self.sitename == 'MPPMU_MCORE' and memory < 2000:
		memory = 2000

        self.xrsl['memory'] = '(memory = %d)' % (memory)

    def setRTE(self):

        self.artes = ''
        # Non-RTE setup only requires ATLAS-SITE and possibly ENV/PROXY
        if self.truepilot and 'BOINC' not in self.sitename:
            self.xrsl['rtes'] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)"
            return
        if self.siteinfo['type'] == 'analysis' and 'BOINC' not in self.sitename:
            self.xrsl['rtes'] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE)"
            return
        if self.sitename not in self.rtesites:
            self.xrsl['rtes'] = "(runtimeenvironment = APPS/HEP/ATLAS-SITE)"
            return
        
        # Old-style RTE setup
        atlasrtes = []
        for (package, cache) in zip(self.jobdesc['swRelease'][0].split('\n'), self.jobdesc['homepackage'][0].split('\n')):
            if cache.find('Production') > 1 and cache.find('AnalysisTransforms') < 0:
                rte = package.split('-')[0].upper()  + '-' + cache.split('/')[1]
            elif cache.find('AnalysisTransforms') != -1:
                rte = package.upper()
                res = re.match('AnalysisTransforms-(.+)_(.+)', cache)
                if res is not None:
                    if res.group(1).find('AtlasProduction') != -1:
                        rte = "ATLAS-" + res.group(2)
                    else:
                        rte = "ATLAS-" + res.group(1).upper() + "-" + res.group(2)
            else:
                rte = cache.replace('Atlas', 'Atlas-').replace('/', '-').upper()
            rte = str(rte)
            rte = rte.replace('ATLAS-', '')
            rte += "-"+self.jobdesc['cmtConfig'][0].upper()

            if cache.find('AnalysisTransforms') < 0:
                rte = rte.replace('PHYSICS-', 'ATLASPHYSICS-')
                rte = rte.replace('PROD2-', 'ATLASPROD2-')
                rte = rte.replace('PROD1-', 'ATLASPROD1-')
                rte = rte.replace('DERIVATION-', 'ATLASDERIVATION-')
                rte = rte.replace('P1HLT-', 'ATLASP1HLT-')
                rte = rte.replace('TESTHLT-', 'ATLASTESTHLT-')
                rte = rte.replace('CAFHLT-', 'ATLASCAFHLT-')
                rte = rte.replace('21.0.13.1','ATLASPRODUCTION-21.0.13.1')
                rte = rte.replace('21.0.20.1','ATLASPRODUCTION-21.0.20.1')
            if cache.find('AnalysisTransforms') != -1:
                res=re.match('(21\..+)',rte)
                if res is not None:
                    rte = rte.replace('21','OFFLINE-21')

            if rte.find('NULL') != -1:
                rte = 'PYTHON-CVMFS-X86_64-SLC6-GCC47-OPT'

            atlasrtes.append(rte)

        self.xrsl['rtes'] = ""
        for rte in atlasrtes[-1:]:
            self.xrsl['rtes'] += "(runtimeenvironment = APPS/HEP/ATLAS-" + rte + ")"

        if self.siteinfo['type'] == 'analysis':
            self.xrsl['rtes'] += "(runtimeenvironment = ENV/PROXY)"

        self.artes = ",".join(atlasrtes)


    def setExecutable(self):

        self.xrsl['executable'] = "(executable = ARCpilot)"

    def setArguments(self):

        if self.artes is None:
            self.setRTE()

        # Set options for NG/true pilot
        if self.truepilot:
            pargs = '"pilot3/pilot.py" "-h" "%s" "-s" "%s" "-f" "false" "-p" "25443" "-d" "{HOME}" "-w" "https://pandaserver.cern.ch"' % (self.schedconfig, self.sitename)
        else:
            pargs = '"pilot3/pilot.py" "-h" "%s" "-s" "%s" "-F" "Nordugrid-ATLAS" "-d" "{HOME}" "-j" "false" "-f" "false" "-z" "true" "-b" "2" "-t" "false"' % (self.sitename, self.sitename)

        pandajobarg = self.pandajob
	# CSCS terst hack
        if self.sitename == 'CSCS-LCG2-HPC_MCORE_TEST':
	    pandajobarg = re.sub(r'&coreCount=\d+&', '&coreCount=18&', pandajobarg)
        # Set corecount to 1 to allow single core jobs to run. For other numbers
        # corecount is set dynamically using ATHENA_PROC_NUMBER. This hack can be
        # removed when ATHENA_PROC_NUMBER=1 works.
        if 'BOINC' in self.sitename:
            self.log.debug('hacking boinc corecount')
            pandajobarg = re.sub(r'&coreCount=\d+&', '&coreCount=1&', pandajobarg)
            # Set maxCPUCount to one week to avoid pilot killing suspended jobs
            pandajobarg = re.sub(r'&maxCpuCount=\d+&', '&maxCpuCount=604800&', pandajobarg)
            self.log.debug(pandajobarg)
        # Hack to tell job to start from checkpoint
        if self.sitename == 'BOINC_CHECKPOINT':
            # Filter by task ID, one task using checkpoint and the other not
            if int(self.jobdesc['taskID'][0]) == 12522345:
                self.log.debug('%s: setting checkpoint restart arg for task %s' % (self.pandaid, self.jobdesc['taskID'][0]))
                pandajobarg = re.sub(r'MC15aPlus', 'MC15aPlus+--restart%3D%2Fhome%2Fatlas01%2Ftest-checkpoint%2Fckpt%2Fcheckpoint.tar', pandajobarg)
            # use newest DB release
            pandajobarg = re.sub(r'--DBRelease%3D%22all%3Acurrent%22', '--DBRelease%3D%22100.0.2%22', pandajobarg)
        # Commented on request of Rod
        #if self.sitename in ['LRZ-LMU_MUC_MCORE1', 'LRZ-LMU_MUC1_MCORE']:
        if self.sitename in ['IN2P3-CC_HPC_IDRIS_MCORE']:
            pandajobarg = re.sub(r'--DBRelease%3D%22all%3Acurrent%22', '--DBRelease%3D%22100.0.2%22', pandajobarg)
        # Add log to OS
        if 'BOINC' not in self.sitename:
            pandajobarg += '&putLogToOS=true'
        if self.longjob:
            pandajobarg = "FILE"
        self.xrsl['arguments'] = '(arguments = "' + self.artes + '" "' + pandajobarg + '" ' + pargs + ')'
        #AF self.xrsl['arguments']  = '(arguments = "'+self.artes+'" "' + self.pandajob  + '" '+pargs+ ')'

    def setInputsES(self, inf):
        
        for f, s, i in zip (self.jobdesc['inFiles'][0].split(","), self.jobdesc['scopeIn'][0].split(","), self.jobdesc['prodDBlockToken'][0].split(",")):
            if i == 'None':
                # Rucio file
                lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp;cache=invariant/replicas", s, f])
            else:
                i = int(i.split("/")[0])
                if i in self.osmap:
                    lfn = '/'.join([self.osmap[i], f])
                else:
                    # TODO this exception is ignored by panda2arc
                    raise Exception("No OS defined in AGIS for bucket id %d" % i)
            inf[f] = lfn

    def setInputs(self):

        x = ""
        if self.sitename == 'ANALY_SiGNET_DIRECT':
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-true-test")'
        elif self.sitename == 'HPC2N':
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-test")'
        elif self.sitename == 'BOINC-TEST':
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot")'
        elif self.truepilot:
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-true-test")'
        elif self.eventranges:
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-es")'      
        elif self.sitename == 'BOINC_CHECKPOINT':
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-nolimit")'
        else:
            x += '(ARCpilot "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot")'

        if self.jobdesc['prodSourceLabel'][0] == 'rc_test':
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-rc.tar.gz")'
        elif self.jobdesc['prodSourceLabel'][0] == 'ptest':
            #x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch;cache=check/project-atlas-gmsb/pilotcode-dev.tar.gz")'
            x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch/project-atlas-gmsb/pilot2.tar.gz")'
        elif self.sitename in ['LRZ-LMU_MUC1_MCORE', 'LRZ-LMU_MUC_MCORE1', 'IN2P3-CC_HPC_IDRIS_MCORE']:
            x += '(pilotcode.tar.gz "http://wguan-wisc.web.cern.ch;cache=check/wguan-wisc/wguan-pilot-dev-HPC_arc.tar.gz")'
        elif self.sitename in self.rtesites:
            x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-RTE.tar.gz")'
        #elif 'BOINC' in self.sitename or (self.sitename.startswith('ANALY') and not self.truepilot) or ('MPPMU' in self.sitename):
            # Use official pilot for BOINC and ND analy queues
            #x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'
            #x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-NOKILL.tar.gz")'
        else:
            # Use no kill pilot to avoid pilot killing too much at end of job
            # Paul's version using PILOT_NOKILL
            x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch;cache=check/project-atlas-gmsb/pilotcode-PICARD-72.12-NORDUGRID.tar.gz")'
            #x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-NOKILL.tar.gz")'
            #x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'
            #x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-AF.tar.gz")'
            #x += '(pilotcode.tar.gz "http://dcameron.web.cern.ch;cache=check/dcameron/dev/pilotcode-DC.tar.gz")'

        # Special HPCs which cannot get agis files from cvmfs or over network
        if re.match('BEIJING-.*_MCORE', self.sitename):
            x += '(agis_schedconf.agis.%s.json "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&panda_queue=%s")' % (self.sitename,self.sitename)
            x += '(agis_ddmendpoints.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json")'
            x += '(agis_ddmblacklisting.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmblacklisting.json")'
            x += '(agis_schedconf.cvmfs.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json")'

        if self.eventranges:
            x += '(ARCpilot-test.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-es.tar.gz")'

        if self.longjob:
            # TODO create input file
            pandaid = self.jobdesc['PandaID'][0]
            try:
                os.makedirs(self.inputjobdir)
            except:
                pass
            tmpfile = self.inputjobdir+"/pandaJobData.out"
            f = open(tmpfile, "w")
            f.write(self.pandajob)
            f.close()
            x += '(pandaJobData.out "%s/pandaJobData.out")' % self.inputjobdir

        if not self.truepilot:
            x += '(queuedata.pilot.json "http://pandaserver.cern.ch:25085;cache=check/cache/schedconfig/%s.all.json")' % self.schedconfig

        if 'inFiles' in self.jobdesc and not self.truepilot:
            inf = {}
            if self.jobdesc.has_key('eventServiceMerge') and self.jobdesc['eventServiceMerge'][0] == 'True':
                self.setInputsES(inf)

            for filename, scope, dsn, guid, token in zip(self.jobdesc['inFiles'][0].split(","),
                                                         self.jobdesc['scopeIn'][0].split(","),
                                                         self.jobdesc['realDatasetsIn'][0].split(","),
                                                         self.jobdesc['GUID'][0].split(","),
                                                         self.jobdesc['prodDBlockToken'][0].split(",")):

                # Skip files which use direct I/O: site has it enabled, token is
                # not 'local', file is root file and --useLocalIO is not used

                # don't use direct I/O - pending new mover switch
                #if token != 'local' and self.siteinfo.get('direct_access_lan', False) and \
                #  not ('.tar.gz' in filename or '.lib.tgz' in filename or '.raw.' in filename) and \
                #  '--useLocalIO' not in self.jobdesc['jobPars'][0]:
                #    continue
                # Hard-coded pilot rucio account - should change based on proxy
                # Rucio does not expose mtime, set cache=invariant so not to download too much
                if 'SiGNET' in self.sitename or 'ARNES' in self.sitename:
                    lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=https;httpgetpartial=no;cache=invariant/replicas", scope, filename]) 
                else:
                    lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp;cache=invariant/replicas", scope, filename])
                # lfn='/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp,https;cache=invariant/replicas", scope, filename])
                # lfn='/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=https,gsiftp;cache=invariant/replicas", scope, filename])
                inf[filename] = lfn
                dn = self.jobdesc.get('prodUserID', [])
                prodSourceLabel = self.jobdesc.get('prodSourceLabel', [''])[0]
                eventType = 'get_sm'
                if re.match('user', prodSourceLabel):
                    eventType = 'get_sm_a'
                self.traces.append({'uuid': str(uuid.uuid4()), 'scope': scope, 'filename': filename, 'dataset': dsn, 'guid': guid, 'eventVersion': 'aCT', 'timeStart': time.time(), 'usrdn': dn[0], 'eventType': eventType})

            # some files are double:
            for k, v in inf.items():
                x += "(" + k + " " + '"' + v + '"' + ")"
        
            if self.jobdesc.has_key('eventService') and self.jobdesc['eventService'] and self.eventranges:
                # Create tmp json file to upload with job
                pandaid = self.jobdesc['PandaID'][0]
                tmpjsonfile = os.path.join(self.tmpdir, 'eventranges', str('%s.json' % pandaid))
                jsondata = json.loads(self.eventranges)
                with open(tmpjsonfile, 'w') as f:
                    json.dump(jsondata, f)
                x += '("eventranges.json" "%s")' %  tmpjsonfile
            
        self.xrsl['inputfiles'] = "(inputfiles =  %s )" % x

    def setLog(self):

        if 'logFile' in self.jobdesc:
            logfile = self.jobdesc['logFile'][0]
        else:
            logfile = "LOGFILE"

        self.xrsl['log'] = '(stdout = "' + logfile.replace('.tgz', '') + '")(join = yes)'

    def setGMLog(self):

        self.xrsl['gmlog'] = '("gmlog" = "gmlog")'
        self.xrsl['rerun'] = '("rerun" = "2")'

    def setOutputs(self):

        # dynamic outputs

        output = '("jobSmallFiles.tgz" "")'
        output += '("@output.list" "")'
        # needed for SCEAPI
        # generated output file list"
        output += '("output.list" "")'
        self.xrsl['outputs'] = "(outputfiles = %s )" % output

        if self.truepilot:
            self.xrsl['outputs'] = ""

    def setPriority(self):

        if 'currentPriority' in self.jobdesc:
            #self.xrsl['priority'] = '("priority" = ' + str(int(self.jobdesc['currentPriority'][0])/100) + ')'
            # need a better number
            prio = 50
            try:
                prio = int(self.jobdesc['currentPriority'][0])
                if prio < 1:
                    prio = 1
                if prio > 0 and prio < 1001:
                    prio = prio * 90 / 1000.
                    prio = int(prio)
                if prio > 1000 and prio < 10001:
                    prio = 90 + (prio - 1000) / 900.
                    prio = int(prio)
                if prio > 10000:
                    prio = 100
            except:
                pass
            #self.xrsl['priority'] = '("priority" = 60 )'
            self.xrsl['priority'] = '("priority" = %d )' % prio
            if self.sitename == 'wuppertalprod_MCORE':
                self.xrsl['priority'] = ""
            if self.sitename == 'wuppertalprod':
                self.xrsl['priority'] = ""
            if self.sitename == 'wuppertalprod_HI':
                self.xrsl['priority'] = ""
            if self.sitename == 'ANALY_wuppertalprod':
                self.xrsl['priority'] = ""

    def setEnvironment(self):

        if not self.truepilot:
            self.xrsl['environment'] = '(environment = ("PILOT_NOKILL" "YES"))'
            return

        # Set schedulerID and job log URL for true pilot jobs
        schedid = self.atlasconf.get(["panda", "schedulerid"])
        schedurl = self.atlasconf.get(["joblog", "urlprefix"])
        self.xrsl['environment'] = '(environment = ("PILOT_NOKILL" "YES")("PANDA_JSID" "%s")("SCHED_URL" "%s"))' % (schedid, schedurl)

    def parse(self):
        self.setTime()
        self.setJobname()
        self.setDisk()
        self.setMemory()
        self.setRTE()
        self.setExecutable()
        self.setArguments()
        self.setInputs()
        self.setLog()
        self.setGMLog()
        self.setOutputs()
        self.setPriority()
        self.setEnvironment()

    def getXrsl(self):
        x = "&"
        for k in self.xrsl.keys():
            x += self.xrsl[k] + "\n"
        return x


if __name__ == '__main__':
    from act.common.aCTLogger import aCTLogger
    logger=aCTLogger('test')
    log=logger()
    pandajob = 'logGUID=d786ee44-a065-4566-b0ad-4361dab1b06f&cmtConfig=i686-slc5-gcc43-opt&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL%2CNULL&destinationDBlockToken=ATLASDATADISK%2CATLASDATADISK%2CATLASDATADISK%2CATLASDATADISK&destinationSE=CERN-PROD&realDatasets=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.ESD.e1900_s1638_r4899_tid01341574_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.AOD.e1900_s1638_r4899_tid01341574_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.TAG.e1900_s1638_r4899_tid01341574_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.log.e1900_s1638_r4899_tid01341574_00&prodUserID=jose.enrique.garcia%40cern.ch&GUID=72CD95A0-93E6-1840-B89C-0E00240E27FD%2C35369775-2BA2-E145-A142-BAA78880C6E1%2CEE1CC970-A1D1-3E4D-9A03-998467FB843E%2C02A0460A-5F63-8C46-AEAC-CA4EE5FB3086%2C49BBFB21-9F2A-1D48-90D9-0C82C0A71D06%2C5D152B2E-6C0D-0643-978F-314F449CF2D2%2C29BED061-C906-4D47-B24D-9BEADF04289C%2C76009FCC-3B48-BE4C-ADA4-28A154EBF572%2C04AD849D-F5AD-3D4A-BA1B-26850BFDB2A4%2C4569FF77-3842-2149-8BF8-82B799096CCD%2C2F96B80E-C313-2541-BE94-385F2D5D23E7%2CD24C033E-2E72-1643-B621-0DE4079D93D3%2CF54DAC3C-6C4D-DA4B-A0DD-96A5F12B49B9%2C7CA3CC33-CC49-EE44-8719-A8B5C7E64192%2CEF32DBED-7CB4-3A42-8887-B0379399F8C9%2C719b77d7-54c7-4112-95f5-8dec51cfcd4e%2CCF5D74CA-9EE0-C048-BFF8-F93894383B65%2C768FD917-6B5D-E348-B450-203723C0FA2D%2C340A28C3-8C42-0644-B30A-B0C8FB42C26A%2C8DC99D5B-B595-CB49-A09D-D388CFAB3B74%2CD48B5105-D0F8-E54B-BF23-E934264F549F%2C7A59B8AC-C76F-4645-97D2-001885A664E5%2CF3F7C224-580E-B147-9FD4-B52E2C2BE012%2CE896C839-5CB1-D148-9B0F-44530046E432%2C17FD899C-3585-3041-8D72-DD0F82DA9A06%2CE90E87DC-2B16-CD4B-90A9-ABC5979BB1DC%2CC52DDC02-14E1-444E-BC12-95448DF43B47%2CABF60825-4ED9-5343-BBB9-B940CA92B22D%2C55D5E1D2-BA76-1D46-B03E-4ED329396FD6%2C2EDE36F0-D774-0C4F-AC34-D1006FA95BE7%2CAC5716C4-A1A3-CF47-90E2-0734D157F4F8&realDatasetsIn=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cddo.000001.Atlas.Ideal.DBRelease.v230101%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00&nSent=4&cloud=CERN&StatusCode=0&homepackage=IBLProd%2F17.3.10.1.9&inFiles=HITS.01246207._000291.pool.root.1%2CHITS.01246207._000307.pool.root.1%2CHITS.01246207._000308.pool.root.1%2CHITS.01246207._000310.pool.root.1%2CHITS.01246207._000311.pool.root.1%2CHITS.01246207._000318.pool.root.1%2CHITS.01246207._000320.pool.root.1%2CHITS.01246207._000323.pool.root.1%2CHITS.01246207._000324.pool.root.1%2CHITS.01246207._000325.pool.root.1%2CHITS.01246207._000327.pool.root.1%2CHITS.01246207._000317.pool.root.1%2CHITS.01246207._000330.pool.root.1%2CHITS.01246207._000455.pool.root.1%2CHITS.01246207._000449.pool.root.1%2CDBRelease-23.1.1.tar.gz%2CHITS.01222100._000106.pool.root.1%2CHITS.01222104._000109.pool.root.1%2CHITS.01222104._000110.pool.root.1%2CHITS.01246207._000014.pool.root.1%2CHITS.01246207._000024.pool.root.1%2CHITS.01246207._000030.pool.root.1%2CHITS.01246207._000075.pool.root.1%2CHITS.01246207._000222.pool.root.1%2CHITS.01222100._000107.pool.root.1%2CHITS.01222100._000108.pool.root.1%2CHITS.01222100._000109.pool.root.1%2CHITS.01222100._000110.pool.root.1%2CHITS.01222104._000106.pool.root.1%2CHITS.01222104._000107.pool.root.1%2CHITS.01222104._000108.pool.root.1&processingType=validation&fsize=151174736%2C147668340%2C142550293%2C154935036%2C152996472%2C162045722%2C147247345%2C151492131%2C150528904%2C157304985%2C142994620%2C156109886%2C148761687%2C136075690%2C141567353%2C713566706%2C908043152%2C299988220%2C304423706%2C162260812%2C158996143%2C155373606%2C145577873%2C157495287%2C908094177%2C901244403%2C905077013%2C921653187%2C298933095%2C294433725%2C297102623&fileDestinationSE=CERN-PROD%2CCERN-PROD%2CCERN-PROD%2CCERN-PROD&scopeOut=valid1%2Cvalid1%2Cvalid1&minRamCount=3800&jobDefinitionID=359119736&scopeLog=valid1&transformation=DigiMReco_trf.py&maxDiskCount=8000&prodDBlockToken=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL&transferType=NULL&destinationDblock=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.ESD.e1900_s1638_r4899_tid01341574_00_sub0130314221%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.AOD.e1900_s1638_r4899_tid01341574_00_sub0130314225%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.TAG.e1900_s1638_r4899_tid01341574_00_sub0130314230%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.log.e1900_s1638_r4899_tid01341574_00_sub0130314234&dispatchDBlockToken=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL&jobPars=inputHitsFile%3DHITS.01246207._%5B000014%2C000024%2C000030%2C000075%2C000222%2C000291%2C000307%2C000308%2C000310%2C000311%2C000317%2C000318%2C000320%2C000323%2C000324%2C000325%2C000327%2C000330%2C000449%2C000455%5D.pool.root+outputESDFile%3DESD.01341574._000022.pool.root.1+outputAODFile%3DAOD.01341574._000022.pool.root.1+outputTAGFile%3DTAG.01341574._000022.pool.root.1+maxEvents%3D2000+triggerConfigByRun%3D%22%7B195847%3A%5C%22MCRECO%3ADB%3ATRIGGERDBMC%3A337%2C146%2C276%5C%22%7D%22+preInclude_h2r%3DDigitization%2FForceUseOfPileUpTools.py%2CSimulationJobOptions%2FpreInclude.PileUpBunchTrains2011Config8_DigitConfig.py%2CRunDependentSimData%2FconfigLumi_mc12_v1.py+postInclude_h2r%3DInDetIBL_Example%2FpostOptions.IBL_Digitization.py+preExec_h2r%3D%22from+Digitization.DigitizationFlags+import+digitizationFlags%3B+digitizationFlags.overrideMetadata%2B%3D%5B%5C%22SimLayout%5C%22%2C%5C%22PhysicsList%5C%22%5D%22+postExec_h2r%3D%22job.PileUpToolsAlg.PileUpTools%5B%5C%22MergeMcEventCollTool%5C%22%5D.OnlySaveSignalTruth%3DTrue%22+preInclude_r2e%3DInDetIBL_Example%2FpreInclude.IBL_Rec.py%2CInDetIBL_Example%2FNewLayerMaterial_03.py+postInclude_r2e%3DInDetIBL_Example%2FpostInclude.IBL_Rec.py+preExec_r2e%3D%22rec.Commissioning.set_Value_and_Lock%28True%29%3B+from+CaloRec.CaloCellFlags+import+jobproperties%3B+jobproperties.CaloCellFlags.doLArCellEmMisCalib%3DFalse%3Bfrom+InDetRecExample.InDetJobProperties+import+InDetFlags%3BInDetFlags.doBackTracking.set_Value_and_Lock%28True%29%3BInDetFlags.doTRTStandalone.set_Value_and_Lock%28True%29%3BInDetFlags.doPixelClusterSplitting.set_Value_and_Lock%28False%29%3BInDetFlags.writeRDOs.set_Value_and_Lock%28True%29%3BTriggerFlags.doLVL2.set_Value_and_Lock%28True%29%3BTriggerFlags.doEF.set_Value_and_Lock%28True%29%22+postInclude_e2a%3DInDetIBL_Example%2FpostInclude.IBL_NoAlign.py+preExec_e2a%3D%22InDetFlags.doPixelClusterSplitting.set_Value_and_Lock%28False%29%3BTriggerFlags.AODEDMSet%3D%5C%22AODSLIM%5C%22%3Brec.Commissioning.set_Value_and_Lock%28True%29%22+autoConfiguration%3Deverything+DBRelease%3DDBRelease-23.1.1.tar.gz+conditionsTag%3DOFLCOND-MC12-IBL-20-30-50+geometryVersion%3DATLAS-IBL-03-00-00+--athenaopts%3D--keep-configuration+numberOfLowPtMinBias%3D39.8608+numberOfHighPtMinBias%3D0.138128+LowPtMinbiasHitsFile%3DHITS.01222100._%5B000106%2C000107%2C000108%2C000109%2C000110%5D.pool.root+DataRunNumber%3D-1+jobNumber%3D22+digiSeedOffset1%3D22+digiSeedOffset2%3D22+HighPtMinbiasHitsFile%3DHITS.01222104._%5B000106%2C000107%2C000108%2C000109%2C000110%5D.pool.root+preExec%3D%22from+LArROD.LArRODFlags+import+larRODFlags%3B+larRODFlags.doOFCPileupOptimization.set_Value_and_Lock%28True%29%3B+larRODFlags.NumberOfCollisions.set_Value_and_Lock%2820%29%3B+larRODFlags.UseDelta.set_Value_and_Lock%283%29%3B+jobproperties.Beam.energy.set_Value_and_Lock%287000%2AUnits.GeV%29%3B+jobproperties.Beam.numberOfCollisions.set_Value_and_Lock%2820.0%29%3B+jobproperties.Beam.bunchSpacing.set_Value_and_Lock%2850%29%22+postExec%3D%22CfgMgr.MessageSvc%28%29.setError%2B%3D%5B%5C%22HepMcParticleLink%5C%22%5D%22+&attemptNr=1&swRelease=Atlas-17.3.10&maxCpuCount=100000&outFiles=ESD.01341574._000022.pool.root.1%2CAOD.01341574._000022.pool.root.1%2CTAG.01341574._000022.pool.root.1%2Clog.01341574._000022.job.log.tgz.1&currentPriority=900&scopeIn=valid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cddo%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV&PandaID=1928208201&dispatchDblock=panda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.GEN.1cc2007f-458b-436f-9c40-e312a8926432_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162&prodSourceLabel=managed&checksum=ad%3A88716ba5%2Cad%3A3317b9cb%2Cad%3Acced6da5%2Cad%3Af85fd73f%2Cad%3A37c7f13f%2Cad%3Ab52565aa%2Cad%3Ac3b5df33%2Cad%3A31137849%2Cad%3Aa1b30416%2Cad%3Ab12b9841%2Cad%3Af23cac2e%2Cad%3A3ee3cef7%2Cad%3Aa35f7501%2Cad%3Ab48bcbfc%2Cad%3A1cb4abf6%2Cad%3A9f56b207%2Cad%3A603209bd%2Cad%3A0a551f20%2Cad%3Ace924d00%2Cad%3A6e020562%2Cad%3A0553226f%2Cad%3Aaf342214%2Cad%3Aa098c5ce%2Cad%3A138854f7%2Cad%3A95ead297%2Cad%3A47158820%2Cad%3Aa43f1cab%2Cad%3A18627a6d%2Cad%3Ad530f334%2Cad%3A4af0fe91%2Cad%3A7b2ca679&jobName=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.e1900_s1638_r4899_tid01341574._000022.job&taskID=1341574&logFile=log.01341574._000022.job.log.tgz.1'

    pandajob = "logGUID=26e620d0-e252-41ce-94ea-fa2471fca4a7&cmtConfig=x86_64-slc5-gcc43-opt&dispatchDBlockTokenForOut=NULL%2CNULL&destinationDBlockToken=ATLASDATADISK%2CATLASDATADISK&destinationSE=INFN-T1&realDatasets=mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.HITS.e877_s1349_tid01361280_00%2Cmc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.log.e877_s1349_tid01361280_00&prodUserID=jose.enrique.garcia%40cern.ch&GUID=D6211C66-6E37-E311-83E9-0025901AA3BE%2C1b2daa89-e8ab-47ca-a5af-173c4b1d0725&realDatasetsIn=mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.evgen.EVNT.e877_tid01358974_00%2Cddo.000001.Atlas.Ideal.DBRelease.v160801&nSent=100&cloud=IT&StatusCode=0&homepackage=AtlasProduction%2F16.6.7.14&inFiles=EVNT.01358974._001138.pool.root.1%2CDBRelease-16.8.1.tar.gz&processingType=simul&fsize=71384635%2C534823901&fileDestinationSE=INFN-T1%2CINFN-T1&scopeOut=mc11_7TeV&minRamCount=2000&jobDefinitionID=373528873&scopeLog=mc11_7TeV&transformation=AtlasG4_trf.py&maxDiskCount=8000&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.HITS.e877_s1349_tid01361280_00_sub0138498014%2Cmc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.log.e877_s1349_tid01361280_00_sub0138498018&dispatchDBlockToken=NULL%2CNULL&jobPars=inputEvgenFile%3DEVNT.01358974._001138.pool.root.1+outputHitsFile%3DHITS.01361280._085006.pool.root.1+maxEvents%3D1+skipEvents%3D250+DBRelease%3DDBRelease-16.8.1.tar.gz+preInclude%3DSimulationJobOptions%2FpreInclude.CalHits.py%2CSimulationJobOptions%2FpreInclude.BeamPipeKill.py%2CSimulationJobOptions%2FpreInclude.ParticleID.py+geometryVersion%3DATLAS-GEO-18-01-03_VALIDATION+conditionsTag%3DOFLCOND-SDR-BS7T-05-12+AMITag%3Ds1349+randomSeed%3D85006+physicsList%3DQGSP_BERT+firstEvent%3D251+RunNumber%3D126130+&attemptNr=1&swRelease=Atlas-16.6.7&maxCpuCount=100000&outFiles=TEST-HITS.01361280._085006.pool.root.1%2Clog.01361280._085006.job.log.tgz.1&currentPriority=110&scopeIn=mc11_7TeV%2Cddo&PandaID=1977745545&dispatchDblock=panda.1361280.11.07.GEN.ad783244-f71e-4e13-944f-59c164ff5cf5_dis001977745458%2Cpanda.1361280.11.07.GEN.6b2356f6-d865-4a90-b7c3-7d4a0ab5cdee_dis001977745458&prodSourceLabel=managed&checksum=ad%3Ac9e2ff0a%2Cad%3Ab6ac2f66&jobName=TEST-mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.e877_s1349_tid01361280._085006.job&taskID=1361280&logFile=TEST-log.01361280._085006.job.log.tgz.1"

    pandajob = "logGUID=df19539e-d442-4432-9a42-1aa8d0736a09&cmtConfig=i686-slc5-gcc43-opt&dispatchDBlockTokenForOut=NULL%2CNULL&destinationDBlockToken=NULL%2CNULL&destinationSE=LRZ-LMU_C2PAP&realDatasets=hc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473%2Chc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Dgangarbt%2FCN%3D703216%2FCN%3DRobot%3A+Ganga+Robot%2FCN%3Dproxy&GUID=e188c93b-d07c-41fc-841f-e9945cd85d15%2C46349036-6095-E011-952E-002655213860&realDatasetsIn=ddo.000001.Atlas.Ideal.DBRelease.v240601%2Cmc11_7TeV.105000.pythia_minbias_inelastic.evgen.EVNT.e816_tid420738_00&nSent=0&cloud=DE&StatusCode=0&homepackage=AtlasProduction%2F16.6.7.34&inFiles=DBRelease-24.6.1.tar.gz%2CEVNT.420738._000001.pool.root.1&processingType=gangarobot-pft&fsize=833392469%2C23892103&fileDestinationSE=LRZ-LMU_C2PAP%2CLRZ-LMU_C2PAP&scopeOut=hc_test&minRamCount=0&jobDefinitionID=52&scopeLog=hc_test&transformation=AtlasG4_trf.py&maxDiskCount=0&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=hc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473_sub0139394088%2Chc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473_sub0139394088&dispatchDBlockToken=NULL%2CNULL&jobPars=maxEvents%3D2+skipEvents%3D0+randomSeed%3D568+geometryVersion%3DATLAS-GEO-11-00-00+conditionsTag%3DOFLCOND-SDR-BS7T-02+--ignoreerrors%3DTrue+DBRelease%3DDBRelease-24.6.1.tar.gz+RunNumber%3D105000+outputHitsFile%3DHits.hc_20028229.HITS.pool.root.LRZ-LMU_C2PAP.1384367986.820f+inputEvgenFile%3DEVNT.420738._000001.pool.root.1+--overwriteQueuedata%3D%7Ballowfax%3DFalse%7D+&attemptNr=0&swRelease=Atlas-16.6.7&maxCpuCount=0&outFiles=Hits.hc_20028229.HITS.pool.root.LRZ-LMU_C2PAP.1384367986.820f%2Ce8976462-758a-46c8-82bb-522a68cfc69b.job.log.tgz&currentPriority=10000&scopeIn=ddo%2Cmc11_7TeV&PandaID=1986227157&dispatchDblock=NULL%2CNULL&prodSourceLabel=prod_test&checksum=ad%3A46c6683e%2Cad%3A2da86ea1&jobName=e8976462-758a-46c8-82bb-522a68cfc69b&taskID=NULL&logFile=e8976462-758a-46c8-82bb-522a68cfc69b.job.log.tgz"
    pandajob = "jobsetID=21000&logGUID=1c412f70-851f-429f-ba75-6d6cc0861222&cmtConfig=x86_64-slc6-gcc49-opt&prodDBlocks=mc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cpanda.0225095920.817593.lib._7770508%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00%2Cmc15_13TeV%3Amc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471_tid07091912_00&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL&destinationDBlockToken=TOMERGE%2CTOMERGE%2CTOMERGE&destinationSE=NULL&realDatasets=panda.um.user.lcerdaal.CVBV35_MxAOD.root.67419484%2Cpanda.um.user.lcerdaal.CVBV35_hist.67419485%2Cpanda.um.user.lcerdaal.CVBV35.log.67419483&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Dlcerdaal%2FCN%3D743490%2FCN%3DLeonor+Cerda+Alberich%2FCN%3Dproxy&GUID=B0B372A6-EBD5-2346-8B4B-DD44B5A83603%2C1D33B61C-C4E9-354B-B5C1-99D2B21DD2AE%2CB8E1E5E9-79E0-A343-B236-53D7726D2110%2C76469B57-AE5D-7649-9B13-6D77F3336743%2C12076092-FE84-7849-BBA3-01A2C1D9878A%2CA69E929C-A4C3-9244-B2C9-1C92D2F2F9D1%2C54DBD613-7AC3-504A-9868-85EAD2E6EEDE%2CAA40EE05-D6CE-424C-B40F-108CAE6082E3%2C7DE3CA10-FA4C-F54F-BF84-EFDD0867668F%2C35420939-2AEF-C449-BBFA-50B3E457C7DF%2CDCF848E2-E5A6-E44E-8335-7F5B2A240D5E%2C8A996316-0904-3A45-9984-DFFA524D6FF9%2C72a24b72-6b69-47a8-8182-9f6d28d16041%2CA5ADFA55-B2C5-AD4F-8CA1-A3D33F20C2D5%2CFA987BD2-32C7-3B44-BF5B-B350FF58CB7A%2C0BA2D9AB-F0E6-2240-8B5B-A03081761DC8%2C821BB226-DF9A-8E44-9094-B00583D1543F&realDatasetsIn=mc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cpanda.0225095920.817593.lib._7770508%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F%2Cmc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%2F&nSent=0&cloud=ND&StatusCode=0&homepackage=NULL&inFiles=DAOD_HIGG1D1.07091912._000016.pool.root.1%2CDAOD_HIGG1D1.07091912._000017.pool.root.1%2CDAOD_HIGG1D1.07091912._000018.pool.root.1%2CDAOD_HIGG1D1.07091912._000020.pool.root.1%2CDAOD_HIGG1D1.07091912._000021.pool.root.1%2CDAOD_HIGG1D1.07091912._000022.pool.root.1%2CDAOD_HIGG1D1.07091912._000019.pool.root.1%2CDAOD_HIGG1D1.07091912._000023.pool.root.1%2CDAOD_HIGG1D1.07091912._000024.pool.root.1%2CDAOD_HIGG1D1.07091912._000029.pool.root.1%2CDAOD_HIGG1D1.07091912._000030.pool.root.1%2CDAOD_HIGG1D1.07091912._000031.pool.root.1%2Cpanda.0225095920.817593.lib._7770508.5510350115.lib.tgz%2CDAOD_HIGG1D1.07091912._000025.pool.root.1%2CDAOD_HIGG1D1.07091912._000026.pool.root.1%2CDAOD_HIGG1D1.07091912._000027.pool.root.1%2CDAOD_HIGG1D1.07091912._000028.pool.root.1&processingType=panda-client-0.5.60-jedi-run&currentPriority=983&fsize=2704668013%2C2707060396%2C2721101358%2C2710676168%2C2705137174%2C2723773566%2C2706671129%2C2712045167%2C2673536146%2C2682993356%2C2730692285%2C2711456131%2C161138817%2C2709494869%2C2672282868%2C2652078576%2C2738865386&fileDestinationSE=ANALY_SiGNET%2CANALY_SiGNET%2CANALY_SiGNET&scopeOut=panda%2Cpanda&minRamCount=0&jobDefinitionID=21782&scopeLog=panda&transformation=http%3A%2F%2Fpandaserver.cern.ch%3A25085%2Ftrf%2Fuser%2FrunGen-00-00-02&maxDiskCount=50000&coreCount=1&prodDBlockToken=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL&transferType=NULL&destinationDblock=panda.um.user.lcerdaal.CVBV35_MxAOD.root.67419484_sub0282382208%2Cpanda.um.user.lcerdaal.CVBV35_hist.67419485_sub0282382207%2Cpanda.um.user.lcerdaal.CVBV35.log.67419483_sub0282382209&dispatchDBlockToken=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL&jobPars=-j+%22%22+--sourceURL+https%3A%2F%2Faipanda012.cern.ch%3A25443+-r+.+-p+%22runjob.sh%2520mc15_13TeV.361039.Sherpa_CT10_SinglePhotonPt35_70_CVetoBVeto.merge.DAOD_HIGG1D1.e3587_s2608_s2183_r7326_r6282_p2471%22+-l+panda.0225095920.817593.lib._7770508.5510350115.lib.tgz+-o+%22%7B%27hist-output.root%27%3A+%27panda.um.user.lcerdaal.7770508._000256.hist-output.root%27%2C+%27MxAOD.root%27%3A+%27panda.um.user.lcerdaal.7770508._000256.MxAOD.root%27%7D%22+-i+%22%5B%27DAOD_HIGG1D1.07091912._000016.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000017.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000018.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000019.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000020.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000021.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000022.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000023.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000024.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000025.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000026.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000027.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000028.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000029.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000030.pool.root.1%27%2C+%27DAOD_HIGG1D1.07091912._000031.pool.root.1%27%5D%22+--useRootCore+--rootVer+6.04.12+--writeInputToTxt+IN%3Ainput.txt+&attemptNr=5&swRelease=NULL&maxCpuCount=0&outFiles=panda.um.user.lcerdaal.7770508._000256.MxAOD.root%2Cpanda.um.user.lcerdaal.7770508._000256.hist-output.root%2Cpanda.um.user.lcerdaal.CVBV35.log.7770508.000256.log.tgz&ddmEndPointOut=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&scopeIn=mc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cpanda%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV%2Cmc15_13TeV&PandaID=2773552067&sourceSite=NULL&dispatchDblock=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2Cpanda.0225095920.817593.lib._7770508%2CNULL%2CNULL%2CNULL%2CNULL&prodSourceLabel=user&checksum=ad%3A1a666e58%2Cad%3A2b0434ed%2Cad%3Aa2700040%2Cad%3Af3a6a55a%2Cad%3A0f9c6dab%2Cad%3A13f8e429%2Cad%3A7d8cca56%2Cad%3Adfcaf583%2Cad%3A43c44df6%2Cad%3A68de20b0%2Cad%3Ae1355f2d%2Cad%3A3faa770c%2Cad%3Aa2bb6c07%2Cad%3Ab32ddb3e%2Cad%3Ae9d53551%2Cad%3A54177370%2Cad%3A568e1bd3&jobName=user.lcerdaal.CVBV35%2F.2773037917&ddmEndPointIn=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&taskID=7770508&logFile=panda.um.user.lcerdaal.CVBV35.log.7770508.000256.log.tgz"
    pandajob = "jobsetID=799&logGUID=5ba37307-e4d7-4224-82f9-ff0503622677&cmtConfig=x86_64-slc6-gcc48-opt&prodDBlocks=user.rwatari%3Auser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2.99328897%2Cpanda.1110091801.467362.lib._9845189&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL&destinationDBlockToken=NULL%2CNULL%2CNULL&destinationSE=NULL&realDatasets=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log%2F&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Drwatari%2FCN%3D764796%2FCN%3DRyutaro+Watari%2FCN%3Dproxy&GUID=51997D0A-850A-9044-A264-83A8986FE1C6%2C1de48e07-f37c-43e6-a343-3947342858b1&realDatasetsIn=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2%2Cpanda.1110091801.467362.lib._9845189&nSent=0&cloud=ND&StatusCode=0&homepackage=AnalysisTransforms-AtlasProduction_20.7.3.7&inFiles=user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2Cpanda.1110091801.467362.lib._9845189.7456421499.lib.tgz&processingType=panda-client-0.5.69-jedi-athena-trf&currentPriority=814&fsize=1140292964%2C727003478&fileDestinationSE=ANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT&scopeOut=user.rwatari%2Cuser.rwatari&minRamCount=4772&jobDefinitionID=836&scopeLog=user.rwatari&transformation=http%3A%2F%2Fpandaserver.cern.ch%3A25085%2Ftrf%2Fuser%2FrunAthena-00-00-12&maxDiskCount=3167&coreCount=1&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0.104826316_sub0341667607%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1.104826317_sub0341667608%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.104826315_sub0341667610&dispatchDBlockToken=NULL%2CNULL&jobPars=-l+panda.1110091801.467362.lib._9845189.7456421499.lib.tgz+--sourceURL+https%3A%2F%2Faipanda078.cern.ch%3A25443+-r+WorkArea%2Frun%2Ffast%2F+--trf+--useLocalIO++-i+%22%5B%27user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%27%5D%22+-o+%22%7B%27IROOT%27%3A+%5B%28%27InDetDxAOD.pool.root%27%2C+%27user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%27%29%2C+%28%27esd.pool.root%27%2C+%27user.rwatari.9845189.EXT1._002324.esd.pool.root%27%29%5D%7D%22++-j+%22Reco_tf.py%2520--inputRDOFile%253Duser.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2520--outputESDFile%253Desd.pool.root%2520%2520--doAllNoise%2520False%2520--autoConfiguration%253Deverything%2520--numberOfCavernBkg%253D0%2520--postInclude%253DFTKFastSim%2FInDetDxAOD.py%2520--preExec%2520%2527rec.UserAlgs%253D%255B%2522FTKFastSim%2FFTKFastSimulation_jobOptions.py%2522%255D%253Brec.doCalo.set_Value_and_Lock%2528False%2529%253Brec.doMuon.set_Value_and_Lock%2528False%2529%253Brec.doJetMissingETTag.set_Value_and_Lock%2528False%2529%253Brec.doEgamma.set_Value_and_Lock%2528False%2529%253Brec.doMuonCombined.set_Value_and_Lock%2528False%2529%253Brec.doTau.set_Value_and_Lock%2528False%2529%253Brec.doTrigger.set_Value_and_Lock%2528False%2529%253Brec.doFTK.set_Value_and_Lock%2528True%2529%253Bfrom%2520AthenaCommon.DetFlags%2520import%2520DetFlags%253BDetFlags.all_setOn%2528%2529%253BDetFlags.FTK_setOn%2528%2529%2527%2520--maxEvents%253D-1%2520--postExec%2520r2e%253A%2520%2527ServiceMgr%252B%253DService%2528%2522BeamCondSvc%2522%2529%253BbeamCondSvc%253DServiceMgr.BeamCondSvc%253BbeamCondSvc.useDB%253DFalse%253BbeamCondSvc.posX%253D-0.0497705%253BbeamCondSvc.posY%253D1.06299%253BbeamCondSvc.posZ%253D0.0%253BbeamCondSvc.sigmaX%253D0.0251281%253BbeamCondSvc.sigmaY%253D0.0231978%253BbeamCondSvc.sigmaZ%253D0.1%253BbeamCondSvc.sigmaXY%253D-2.7745e-06%253BbeamCondSvc.tiltX%253D-1.51489e-05%253BbeamCondSvc.tiltY%253D-4.83891e-05%253B%2527%22&attemptNr=2&swRelease=Atlas-20.7.3&nucleus=NULL&maxCpuCount=0&outFiles=user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%2Cuser.rwatari.9845189.EXT1._002324.esd.pool.root%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz&ddmEndPointOut=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&scopeIn=user.rwatari%2Cpanda&PandaID=3072596651&sourceSite=NULL&dispatchDblock=NULL%2Cpanda.1110091801.467362.lib._9845189&prodSourceLabel=user&checksum=ad%3Afd1c3aac%2Cad%3A516b31b3&jobName=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev%2F.3071213044&ddmEndPointIn=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&taskID=9845189&logFile=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz"
    # a = aCTPanda2Xrsl(pandajob, 'LRZ-LMU_C2PAP')
    siteinfo = {'schedconfig': 'ANALY_SiGNET_DIRECT', 'corecount': 1, 'truepilot': False, 'maxwalltime': 10800, 'direct_access_lan': True, 'type': 'analysis'}
    a = aCTPanda2Xrsl(pandajob, 'ANALY_SiGNET_DIRECT', siteinfo, {}, '/tmp', None, log)
    a.parse()
    print a.getXrsl()
