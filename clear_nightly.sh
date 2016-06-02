#/bin/sh

./virtualenv/bin/teuthology-lock --list-targets --owner scheduled_samuelj@slider > /tmp/tmp
cat /tmp/tmp
sleep 5
./virtualenv/bin/teuthology-nuke --owner scheduled_samuelj@slider -r -t /tmp/tmp -u
