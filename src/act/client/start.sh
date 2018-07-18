#!/bin/bash

# Script requires python virtual environment to be activated when run.

# start database
echo "Starting database ..."
/bin/bash start_db.sh &
sleep 1

# start aCT
aCTMain.py start

echo "Done"
