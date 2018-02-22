#!/bin/sh

# Script requires python virtual environment to be activated when run.

# stop aCT
aCTMain.py stop

# stop database
echo "Stopping database ..."
/bin/sh stop_db.sh

echo "Done"
