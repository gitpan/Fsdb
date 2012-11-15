prog='dbmapreduce'
args='-k experiment -f TEST/dbmapreduce_external_file.pl -C make_reducer'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
