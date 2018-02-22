/subprocess.Popen/ {
	print "            self.child = subprocess.Popen(['python', os.path.join(self.actlocation, self.name+'.py'), self.cluster], stdout=self.fdout, stderr=subprocess.STDOUT)"
	next
}1
