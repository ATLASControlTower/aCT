DBDIR=$PWD/mysql
if [ ! -d $DBDIR/db_data ] ; then
	mkdir  $DBDIR/db_data
fi
/usr/sbin/mysqld --defaults-file=$DBDIR/mysql.conf --datadir=$DBDIR/db_data/ --socket=/tmp/act.mysql.socket
