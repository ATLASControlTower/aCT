BEGIN {
	"dirname "ENVIRON["PWD"] | getline actroot
	in_tmp = 0 # parsing is in tmp section
	in_actlocation = 0 # parsing is in actlocation section
}

/<socket>/ {
	print "\t<socket>/tmp/act.mysql.socket</socket>"
	next
}

/<file>/ {
	print "\t<file>/tmp/act.mysql.socket</file>"
	next
}

/<tmp>/ {
	print
	in_tmp = 1
	next
}

/<\/tmp>/ {
	print
	in_tmp = 0
	next
}

/<actlocation>/ {
	print
	in_actlocation = 1
	next
}

/<\/actlocation>/ {
	print
	in_actlocation = 0
	next
}

/<dir>/ {
	if (in_tmp) {
		printf "\t<dir>%s/run/tmp</dir>\n", actroot
		next
	} else if (in_actlocation) {
		printf "\t<dir>%s/src</dir>\n", actroot
		next
	} else {
		print
		next
	}
}

/<pidfile>/ {
	printf "\t<pidfile>%s/run/act.pid</pidfile>\n", actroot
	next
}

/<logdir>/ {
	printf "\t<logdir>%s/run/log</logdir>\n", actroot
	next
}

/<proxystoredir>/ {
	printf "\t<proxystoredir>%s/run/proxies</proxystoredir>\n", actroot
	next
}1
