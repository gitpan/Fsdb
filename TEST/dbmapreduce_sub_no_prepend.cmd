prog='dbmapreduce'
args='--no-prepend-key -k experiment -C "dbcolstats(qw(--nolog duration))"'
cmp='diff -cb '
predecessor=dbmapreduce_sub.cmd
in=TEST/dbmapreduce_ex.in
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
