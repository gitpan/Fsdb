prog='dbmapreduce'
args='--nowarnings --prepend-key -K -k experiment perl TEST/dbmapreduce_external_with_key.pl'
cmp='diff -cb '
in=TEST/dbmapreduce_no_input.in
