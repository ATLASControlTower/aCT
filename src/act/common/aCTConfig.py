import os
import time
from xml.dom import minidom

class aCTConfig:

    def __init__(self, configfile):
        self.configfile = configfile
        self.top=[]
        self.tparse=0
        if self.configfile:
            self.parse()

    def parse(self):
        mtime=os.stat(self.configfile)[8]
        if mtime<=self.tparse:
            return
        xml=minidom.parse(self.configfile)
        self.top=xml.getElementsByTagName('config')
        self.tparse=mtime


    def getList(self,nodes):
        n0=self.top
        for name in nodes:
            tn=[]
            for n in n0:
                n1=n.getElementsByTagName(name)
                tn.extend(n1)
            n0=tn
        l=[]
        for nn in n0:
            l.append(nn.firstChild.data)
        return l


    def getListCond(self,nodesc,cond,nodes):
        c=cond.split("=")
        n0=self.top
        for name in nodesc:
            tn=[]
            for n in n0:
                n1=n.getElementsByTagName(name)
                tn.extend(n1)
            n0=tn

        el=[]
        for t in tn:
            n1=t.getElementsByTagName(c[0])
            if( n1 and n1[0].firstChild.data == c[1] ):
                el.append(t)

        n0=el
        for name in nodes:
            tn=[]
            for n in n0:
                n1=n.getElementsByTagName(name)
                tn.extend(n1)
            n0=tn

        l=[]
        for nn in n0:
            l.append(nn.firstChild.data)
        return l


    def get(self,nodes):
        l = self.getList(nodes)
        if l:
            return l[0]
        return None

    def getCond(self, nodesc, cond, nodes):
        l = self.getListCond(nodesc, cond, nodes)
        if l:
            return l[0]
        return None

class aCTConfigARC(aCTConfig):

    def __init__(self):
        if 'ACTCONFIGARC' in os.environ and os.path.exists(os.environ['ACTCONFIGARC']):
            configfile = os.environ['ACTCONFIGARC']
        elif 'VIRTUAL_ENV' in os.environ and os.path.exists(os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigARC.xml')):
            configfile = os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigARC.xml')
        elif os.path.exists('/etc/act/aCTConfigARC.xml'):
            configfile = '/etc/act/aCTConfigARC.xml'
        elif os.path.exists('aCTConfigARC.xml'):
            configfile="aCTConfigARC.xml"
        else:
            raise Exception('Could not find aCTConfigARC.xml')
        aCTConfig.__init__(self, configfile)

class aCTConfigAPP(aCTConfig):

    def __init__(self):
        if 'ACTCONFIGAPP' in os.environ and os.path.exists(os.environ['ACTCONFIGAPP']):
            configfile = os.environ['ACTCONFIGAPP']
        elif 'VIRTUAL_ENV' in os.environ and os.path.exists(os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigAPP.xml')):
            configfile = os.path.join(os.environ['VIRTUAL_ENV'], 'etc', 'act', 'aCTConfigAPP.xml')
        elif os.path.exists('/etc/act/aCTConfigAPP.xml'):
            configfile = '/etc/act/aCTConfigAPP.xml'
        elif os.path.exists('aCTConfigAPP.xml'):
            configfile="aCTConfigAPP.xml"
        else:
            configfile=None
        aCTConfig.__init__(self, configfile)


if __name__ == '__main__':

    actconf=aCTConfig("aCTConfigARC.xml")
    #actconf.printConfig()
    while 1:
        actconf.parse()
        print(actconf.get(['jobs','maxqueued']))
        print(actconf.get(['rls','server']))
        print(actconf.getList(['test','a1','a2']))
        print(actconf.getList(['atlasgiis','item']))
        print(actconf.getList(['clustersreject','item']))
        print(actconf.getList(['srm','token','name']))
        print(actconf.getList(['srm','token','dir']))
        print(actconf.get(['srm','prefix']))
        print(actconf.getList(['brokerlist','broker','qreject','item']))
        for (i,j) in zip(actconf.getList(['srm','token','name']),actconf.getList(['srm','token','dir'])):
            print(i,j)
        exit(1)
        time.sleep(1)

