import os

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMX2Arc(aCTLDMXProcess):
    '''
    Pick up new jobs in the db and create ARC jobs
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)

    def processNewJobs(self):

        # Submit new jobs
        newjobs = self.dbldmx.getJobs("ldmxstatus='new' order by modified limit 100")
        for job in newjobs:

            with open(job['description']) as f:
                config = {l.split('=')[0]: l.split('=')[1].strip() for l in f}

            xrsl = self.createXRSL(job['description'], job['template'], config)
            if not xrsl:
                self.log.warning(f'Could not create xrsl for {job["id"]}')
                # Set back to new to put at the back of the queue
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'new'})
                continue

            # Send to cluster with the data if possible
            clusterlist = self.rses.get(config.get('InputDataLocationLocalRSE'), ','.join(self.endpoints))
            self.log.info(f'Inserting job {job["id"]} to CEs {clusterlist}\n with xrsl {xrsl}')
            arcid = self.dbarc.insertArcJobDescription(xrsl,
                                                       proxyid=job['proxyid'],
                                                       clusterlist=clusterlist,
                                                       downloadfiles='gmlog/errors;stdout;rucio.metadata',
                                                       appjobid=str(job['id']),
                                                       fairshare=job['batchid'][:50])

            if not arcid:
                self.log.error('Failed to insert arc job')
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'failed'})
                continue

            desc = {'ldmxstatus': 'waiting', 'arcjobid': arcid['LAST_INSERT_ID()']}
            self.dbldmx.updateJobLazy(job['id'], desc)

            # Dump job description
            logdir = os.path.join(self.conf.get(["joblog", "dir"]),
                                  job['created'].strftime('%Y-%m-%d'))
            os.makedirs(logdir, 0o755, exist_ok=True)
            xrslfile = os.path.join(logdir, f'{job["id"]}.xrsl')
            with open(xrslfile, 'w') as f:
                f.write(xrsl)
                self.log.debug(f'Wrote description to {xrslfile}')

        if newjobs:
            self.dbldmx.Commit()


    def createXRSL(self, descriptionfile, templatefile, config):

        xrsl = {}

        # Parse some requirements from descriptionfile
        xrsl['memory'] = f"(memory = {float(config.get('JobMemory', 2)) * 1000})"
        xrsl['walltime'] = f"(walltime = {int(config.get('JobWallTime', 240))})"
        xrsl['cputime'] = f"(cputime = {int(config.get('JobWallTime', 240))})"
        # LDMX RTE must be before SIMPROD one
        xrsl['runtimeenvironment'] = ''
        if 'RunTimeEnvironment' in config:
            xrsl['runtimeenvironment'] = f"(runtimeenvironment = APPS/{config.get('RunTimeEnvironment')})"
        xrsl['runtimeenvironment'] += f"(runtimeenvironment = APPS/{self.conf.get(['executable', 'simprodrte'])})"
        if config.get('FinalOutputDestination'):
            xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" "")("@output.files" ""))'
        else:
            xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" ""))'

        wrapper = self.conf.get(['executable', 'wrapper'])
        xrsl['executable'] = f"(executable = ldmxsim.sh)"

        inputfiles = f'(ldmxsim.sh {wrapper}) \
                       (ldmxproduction.config {descriptionfile}) \
                       (ldmxjob.py {templatefile}) \
                       (ldmx-simprod-rte-helper.py {self.conf.get(["executable", "ruciohelper"])})'
        if 'InputFile' in config and 'InputDataLocationLocalRSE' not in config:
            # No local copy so get ARC to download it
            inputfiles += f'({config["InputFile"].split(":")[1]} \"{config["InputDataLocationRemote"]}\" "cache=no")'
        if 'PileupLocation' in config:
            inputfiles += f'({config["PileupLocation"].split("/")[-1]} \"{config["PileupLocation"]}\" "cache=yes")'
        xrsl['inputfiles'] = f'(inputfiles = {inputfiles})'

        xrsl['stdout'] = '(stdout = stdout)'
        xrsl['gmlog'] = '(gmlog = gmlog)'
        xrsl['join'] = '(join = yes)'
        xrsl['rerun'] = '(rerun = 2)'
        xrsl['count'] = '(count = 1)'
        xrsl['jobName'] = '(jobname = "LDMX Prod Simulation")'

        return '&' + '\n'.join(xrsl.values())

    def process(self):

        self.processNewJobs()


if __name__ == '__main__':

    ar = aCTLDMX2Arc()
    ar.run()
    ar.finish()
