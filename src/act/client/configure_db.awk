/^DBDIR=/ {
	"dirname "ENVIRON["PWD"] | getline actroot
	printf "DBDIR=%s/run/mysql\n", actroot
	next
}1
