#!/bin/bash
# Kill benchmark harness processes and their valkey servers.
# Kept in a script file so the pattern never appears in the invoking shell's
# own command line, which otherwise makes pkill -f signal this very shell.
SELF=$$
for p in $(pgrep -f "crossover_e2e[.]py" 2>/dev/null; pgrep -f "diag_load[.]py" 2>/dev/null); do
  [ "$p" = "$SELF" ] && continue
  kill "$p" 2>/dev/null && echo "killed harness $p"
done
sleep 1
for p in $(pgrep -x valkey-server 2>/dev/null); do
  kill "$p" 2>/dev/null && echo "killed valkey-server $p"
done
sleep 1
echo "remaining harness: $(pgrep -f 'crossover_e2e[.]py' | grep -vc "^$SELF$")"
echo "remaining valkey-server: $(pgrep -cx valkey-server)"
