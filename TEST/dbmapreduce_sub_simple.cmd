prog='dbmapreduce'
args='-k experiment -f TEST/dbmapreduce_sub_simple.pl -C "simple_reducer"'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
