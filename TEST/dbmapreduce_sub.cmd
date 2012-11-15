prog='dbmapreduce'
args='-k experiment -C "dbcolstats(qw(--nolog duration))"'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
