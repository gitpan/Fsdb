# test a problem where the output to the dbsort was truncating floats
prog='dbcolstats'
args='-q 4 duration'
cmp='diff -c '
portable=false
subprogs=dbsort
