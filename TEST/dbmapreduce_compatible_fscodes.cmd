prog='dbmapreduce'
args='-k experiment dbcolstats -F S duration'
cmp='diff -cb '
subprogs=dbcolstats
in=TEST/dbmapreduce_incompatible_fscodes.in
suppress_warnings='5.10:Unbalanced string table refcount;5.10:Scalars leaked'
