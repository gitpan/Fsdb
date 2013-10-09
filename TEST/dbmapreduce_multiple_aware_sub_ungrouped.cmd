prog='dbmapreduce'
args='-M -k experiment -f TEST/dbmapreduce_multiple_aware_sub.pl -C simple_reducer'
predecessor=dbmapreduce_multiple_aware_sub.cmd
cmp='diff -cb '
in=TEST/dbmapreduce_grouped_incorrectly.in
