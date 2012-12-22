prog='dbmapreduce'
args='-k experiment -C "dbcolstats(qw(--nolog duration))"'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
