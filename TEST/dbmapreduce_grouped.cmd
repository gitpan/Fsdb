prog='dbmapreduce'
args='-S -k experiment dbcolstats duration'
in=TEST/dbmapreduce_ex.in
cmp='diff -cb '
subprogs=dbcolstats
# suppress_warnings='5.10:Unbalanced string table refcount;5.10:Scalars leaked'
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
