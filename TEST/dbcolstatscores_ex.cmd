prog='dbcolstatscores'
args='--tmean 50 --tstddev 10 test1'
cmp='diff -cb '
portable=false
subprogs=dbstats
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
