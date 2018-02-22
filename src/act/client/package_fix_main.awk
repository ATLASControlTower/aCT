/#!/ {
	print "#!/usr/bin/env python"
	next
}

/import aCTConfig/ {
	print "import act.common.aCTConfig as aCTConfig"
	next
}

/import aCTLogger/ {
	print "import act.common.aCTLogger as aCTLogger"
	next
}

/import aCTSignal/ {
	print "import act.common.aCTSignal as aCTSignal"
	next
}

/import aCTUtils/ {
	print "import act.common.aCTUtils as aCTUtils"
	next
}

/import aCTProcessManager/ {
	print "import act.common.aCTProcessManager as aCTProcessManager"
	next
}

/\/usr\/sbin\/logrotate/ {
	print "            command = ['logrotate', '-s', logrotatestatus, temp.name]"
	next
}1
