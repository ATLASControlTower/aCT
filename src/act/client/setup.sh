#!/bin/sh

# configure database
echo "Configuring database ..."
awk -f configure_db.awk start_db.sh > temp_start.sh
mv temp_start.sh start_db.sh

# start database
echo "Starting database ..."
/bin/sh start_db.sh &
sleep 1

# configure aCT
echo "Configuring aCT ..."
awk -f configure_act.awk aCTConfigARC.xml > temp_conf.xml
mv temp_conf.xml aCTConfigARC.xml

# install virtualenv
echo "Installing virtualenv ..."
pip install --user virtualenv

# create venv
echo "Creating python virtual environment ..."
/bin/sh setup_venv.sh

# activate virtualenv
echo "Activating virtual environment ..."
source venv/bin/activate

# install mysql.connector
echo "Installing mysql.connector ..."
pip install mysql-connector==2.1.6

echo "Installing pyOpenSSL ..."
pip install pyOpenSSL

# install flask
echo "Installing flask ..."
pip install Flask

# create database tables
echo "Creating database ..."
python setup_db.py
/bin/sh stop_db.sh

# deactivate python virtual environent
echo "Deactivating virtual environment ..."
deactivate

echo "Done"
