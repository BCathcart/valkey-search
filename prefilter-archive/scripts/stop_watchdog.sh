#!/bin/bash
# Stop only the snapshot watchdog, leaving the benchmark running.
# In a script file so the match pattern never appears in the caller's own
# command line (pkill -f otherwise signals the invoking shell).
SELF=$$
for p in $(pgrep -f "watch_thr""oughput" 2>/dev/null); do
    [ "$p" = "$SELF" ] && continue
    echo "stopping watchdog pid $p"
    kill "$p" 2>/dev/null
done
sleep 1
echo "watchdogs remaining: $(pgrep -f "watch_thr""oughput" | grep -cv "^$SELF$")"
