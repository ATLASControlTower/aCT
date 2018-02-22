# This script searches for line "^deactivate () {" (start of the function that
# deactivates virtual environment) and prints code that restores PYTHONPATH.

/^deactivate[[:space:]]?\(\)[[:space:]]?\{/ {
	print
	print "    ### code from setup_venv.sh to preserve PYTHONPATH ###"
	print "    if ! [ -z \"${OLD_PYTHONPATH+_}\" ] ; then"
	print "        PYTHONPATH=\"$OLD_PYTHONPATH\""
	print "        export PYTHONPATH"
	print "        unset OLD_PYTHONPATH"
	print "    fi"
	print ""
	print "    ### code from setup_venv.sh to preserve PATH ###"
	print "    if ! [ -z \"${OLD_PATH+_}\" ] ; then"
	print "        PATH=\"$OLD_PATH\""
	print "        export PATH"
	print "        unset OLD_PATH"
	print "    fi"
	print ""
	print "    ### code from setup_venv.sh to add ACTCONFIGARC environment variable ###"
	print "    if ! [ -z \"${ACTCONFIGARC+_}\" ] ; then"
	print "        unset ACTCONFIGARC"
	print "    fi"
	print ""
	# jump over all printed lines
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
	next
}1
