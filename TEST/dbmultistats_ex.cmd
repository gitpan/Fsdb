prog='dbmultistats'
args='-k experiment duration'
in=TEST/dbmapreduce_ex.in
cmp='diff -cb '
portable=false
subprogs=dbstats
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
