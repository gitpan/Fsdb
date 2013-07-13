prog='dbmapreduce'
args='-k experiment dbcolstats duration'
cmp='diff -cb '
portable=false
subprogs=dbcolstats
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
suppress_warnings='5.10:Unbalanced string table refcount;5.10:Scalars leaked'
