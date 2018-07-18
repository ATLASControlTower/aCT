#!/bin/bash

if [ $# -ne 2 ]; then
	echo "usage: $0 <source dir> <path to archive>"
	exit 1
fi

SRCDIR="$1"
DSTPKG="$2"
PKGDIR="$DSTPKG" # extension removal in future?

# copy entire source tree
cp -R "$SRCDIR" "$PKGDIR"

# first, delete some useless directories
rm -rf "$PKGDIR/.git"
rm -f "$PKGDIR/.gitignore"

# readme
mv "$PKGDIR/src/act/client/README.md" "$PKGDIR/"

# create directories for runtime
RUNDIR="$PKGDIR/run"
mkdir "$RUNDIR"

# setup directories for mysql
MYSQLDIR="$RUNDIR/mysql"
mkdir $MYSQLDIR
mv "$PKGDIR/src/act/client/mysql.conf" "$MYSQLDIR"

# create bin directory for programs
BINDIR="$PKGDIR/bin"
mkdir "$BINDIR"

# move configuration
mv "$PKGDIR/src/act/client/aCTConfigARC.xml" "$BINDIR"

# move aCT commands and allow execution
mv "$PKGDIR/src/act/client/act"*".py" "$BINDIR"
chmod u+x "$BINDIR/act"*".py"

# move awk scripts
mv "$PKGDIR/src/act/client/configure_"*".awk" "$BINDIR"

# move setup scripts, enable execution of main script
mv "$PKGDIR/src/act/client/setup"* "$BINDIR"
chmod u+x "$BINDIR/setup.sh"

# move start scripts
mv "$PKGDIR/src/act/client/start"* "$BINDIR"
chmod u+x "$BINDIR/start.sh"

# move stop scripts
mv "$PKGDIR/src/act/client/stop"* "$BINDIR"
chmod u+x "$BINDIR/stop.sh"

# move sites configuration
mv "$PKGDIR/src/act/client/sites.json" "$BINDIR"

# move and fix aCTMain.py, add execution permission
mv "$PKGDIR/src/act/common/aCTMain.py" "$BINDIR"
awk -f "$SRCDIR/src/act/client/package_fix_main.awk" "$BINDIR/aCTMain.py" > "$BINDIR/tempMain.py"
mv "$BINDIR/tempMain.py" "$BINDIR/aCTMain.py"
chmod u+x "$BINDIR/aCTMain.py"

# fix aCTProcessManager
awk -f "$SRCDIR/src/act/client/package_fix_procman.awk" "$PKGDIR/src/act/common/aCTProcessManager.py" > "$PKGDIR/src/act/common/tempProcManager.py"
mv "$PKGDIR/src/act/common/tempProcManager.py" "$PKGDIR/src/act/common/aCTProcessManager.py"

# clean client package
rm -f "$PKGDIR/src/act/client/package.sh"
rm -f "$PKGDIR/src/act/client/package_fix_main.awk"
rm -f "$PKGDIR/src/act/client/package_fix_procman.awk"

# cd to directory where package should be created
cd "$(dirname $PKGDIR)"
zip -r "$(basename $PKGDIR).zip" "$(basename $PKGDIR)/"
rm -rf "$(basename $PKGDIR)"
