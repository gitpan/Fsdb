prog='dbmapreduce'
args='-k experiment dbcolstats duration'
cmp='diff -cb '
portable=false
subprogs=dbcolstats
altcmp='dbfilediff --quiet -E --exit '
altcmp_needs_input_flags=true
suppress_warnings='5.1[0-2]:Unbalanced string table refcount;5.1[0-2]:Scalars leaked'
