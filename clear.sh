#/bin/sh

./virtualenv/bin/teuthology-lock --list-targets > /tmp/tmp
./virtualenv/bin/teuthology-nuke -t /tmp/tmp -r
./virtualenv/bin/teuthology-lock -t /tmp/tmp --unlock
