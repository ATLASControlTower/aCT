import os
import tempfile
import time

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMXGetJobs(aCTLDMXProcess):
    '''
    Pick up new jobs and register them in the LDMX db
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)

    def generateJobs(self, config):

        randomseed1 = int(config['RandomSeed1SequenceStart'])
        randomseed2 = int(config['RandomSeed2SequenceStart'])
        njobs = int(config['NumberofJobs'])
        self.log.info(f'Creating {njobs} jobs')
        for n in range(njobs):
            config['RandomSeed1'] = randomseed1
            config['RandomSeed2'] = randomseed2
            config['runNumber'] = n+1
            yield config
            randomseed1 += 1
            randomseed2 += 1

    def getNewJobs(self):
        '''
        Read new job files in buffer dir and create necessary job descriptions
        '''

        bufferdir = self.conf.get(['jobs', 'bufferdir'])
        configsdir = os.path.join(bufferdir, 'configs')
        os.makedirs(configsdir, 0o755, exist_ok=True)
        jobs = [os.path.join(configsdir, j) for j in os.listdir(configsdir) if os.path.isfile(os.path.join(configsdir, j))]
        now = time.time()
        try:
            # Take the first proxy available
            proxyid = self.dbarc.getProxiesInfo('TRUE', ['id'], expect_one=True)['id']
        except Exception:
            self.log.error('No proxies found in DB')
            return

        for jobfile in jobs:

            # Avoid partially written files by delaying the read
            if now - os.path.getmtime(jobfile) < 5:
                self.log.debug(f'Job {jobfile} is too new')
                continue

            self.log.info(f'Picked up job at {jobfile}')

            with open(jobfile) as f:
                try:
                    config = {l.split('=')[0]: l.split('=')[1].strip() for l in f}
                    batchid = config.get('BatchID', f'Batch-{time.strftime("%Y-%m-%dT%H:%M:%S")}')
                except Exception as e:
                    self.log.error(f'Failed to parse job config file {jobfile}: {e}')
                    os.remove(jobfile)
                    continue

            try:
                templatefile = os.path.join(bufferdir, 'templates', config['MacTemplate'])
                with open(templatefile) as tf:
                    template = tf.readlines()
            except Exception as e:
                self.log.error(f'Bad template file or template not defined in {jobfile}: {e}')
                os.remove(jobfile)
                continue

            try:
                # Generate copies of config and template
                for jobconfig in self.generateJobs(config):
                    newjobfile = os.path.join(self.tmpdir, os.path.basename(jobfile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newjobfile}.', delete=False, encoding='utf-8') as njf:
                        newjobfile = njf.name
                        njf.write('\n'.join(f'{k}={v}' for k,v in config.items()))

                    newtemplatefile = os.path.join(self.tmpdir, os.path.basename(templatefile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newtemplatefile}.', delete=False, encoding='utf-8') as ntf:
                        newtemplatefile = ntf.name
                        for l in template:
                            if l.startswith('/ldmx/persistency/root/runNumber '):
                                ntf.write(f'/ldmx/persistency/root/runNumber {jobconfig["runNumber"]}\n')
                            elif l.startswith('/random/setSeeds '):
                                ntf.write(f'/random/setSeeds {jobconfig["RandomSeed1"]} {jobconfig["RandomSeed2"]}\n')
                            else:
                                ntf.write(l)

                    self.dbldmx.insertJob(newjobfile, newtemplatefile, proxyid, batchid=batchid)
                    self.log.info(f'Inserted job from {newjobfile} into DB')
            except Exception as e:
                raise
                self.log.error(f'Failed to create jobs from {jobfile}: {e}')
            os.remove(jobfile)


    def archiveOldJobs(self):
        '''Move old jobs to the archive table'''

        # modified column is reported in local time so may not be exactly one day
        select = self.dbldmx.timeStampLessThan('modified', 60*60*24)
        select += ' and ldmxstatus in ("finished", "failed", "cancelled")'
        columns = ['id', 'sitename', 'ldmxstatus', 'starttime', 'endtime', 'modified']
        jobs = self.dbldmx.getJobs(select, columns)
        if not jobs:
            return

        self.log.info('Archiving %d jobs' % len(jobs))
        for job in jobs:
            self.log.debug('Archiving LDMX job %d' % job['id'])
            # Fill out empty start/end time
            if job['starttime']:
                if not job['endtime']:
                    job['endtime'] = job['modified']
            elif job['endtime']:
                job['starttime'] = job['endtime']
            else:
                job['starttime'] = self.dbldmx.getTimeStamp()
                job['endtime'] = self.dbldmx.getTimeStamp()

            # archive table doesn't have modified
            jobarchive = job.copy()
            del jobarchive['modified']
            self.dbldmx.insertJobArchiveLazy(jobarchive)
            self.dbldmx.deleteJob(job['id']) # commit is called here


    def process(self):

        self.getNewJobs()

        # Move old jobs to archive - every hour
        if time.time() - self.starttime > 3600:
            self.log.info("Checking for jobs to archive")
            self.archiveOldJobs()
            self.starttime = time.time()


if __name__ == '__main__':

    ar = aCTLDMXGetJobs()
    ar.run()
    ar.finish()
