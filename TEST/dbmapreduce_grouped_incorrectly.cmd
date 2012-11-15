prog='dbmapreduce'
args='-S -S -k experiment dbcolstats duration'
cmp='diff -cb '
subprogs=dbcolstats
# suppress_warnings='5.10:Unbalanced string table refcount;5.10:Scalars leaked'
