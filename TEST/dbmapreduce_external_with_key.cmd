prog='dbmapreduce'
args='--nowarnings -K -k experiment perl TEST/dbmapreduce_external_with_key.pl'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
suppress_warnings='5.1[0-2]:Unbalanced string table refcount;5.1[0-2]:Scalars leaked'
