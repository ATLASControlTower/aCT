#!/bin/bash

function abspath()
{
	cd "$1"
	pwd
}

# create python virtual environment
PATH="$PATH:~/.local/bin" virtualenv venv --system-site-packages

# Modify virtual environment scripts so they preserve PYTHONPATH and
# use custom PYTHONPATH.
awk -f configure_paths.awk venv/bin/activate >> temp_activate # see configure_paths.awk
mv temp_activate venv/bin/activate

echo >> venv/bin/activate # empty line
echo '### code from setup_venv.sh  to preserve PYTHONPATH ###' >> venv/bin/activate
echo 'OLD_PYTHONPATH="$PYTHONPATH"' >> venv/bin/activate
echo 'export OLD_PYTHONPATH' >> venv/bin/activate
echo "PYTHONPATH=\"$(abspath ../src)\"" >> venv/bin/activate
echo 'export PYTHONPATH' >> venv/bin/activate

echo >> venv/bin/activate # empty line
echo '### code from setup_venv.sh  to preserve PATH ###' >> venv/bin/activate
echo 'OLD_PATH="$PATH"' >> venv/bin/activate
echo 'export OLD_PATH' >> venv/bin/activate
echo "PATH=\"\$PATH:$(abspath ../bin)\"" >> venv/bin/activate
echo 'export PATH' >> venv/bin/activate

echo >> venv/bin/activate # empty line
echo '### code from setup_venv.sh to add ACTCONFIGARC environment variable ###' >> venv/bin/activate
echo "ACTCONFIGARC=\"$(abspath ../bin)/aCTConfigARC.xml\"" >> venv/bin/activate
echo 'export ACTCONFIGARC' >> venv/bin/activate
