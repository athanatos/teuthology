#/bin/sh

#./virtualenv/bin/teuthology-lock --list-targets > /tmp/tmp
./virtualenv/bin/teuthology-nuke -t $1 $2
#./virtualenv/bin/teuthology-lock -t /tmp/tmp --unlock
