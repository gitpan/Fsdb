prog='dbmultistats'
args='-S -S -k experiment duration'
in=TEST/dbmapreduce_grouped_incorrectly.in
cmp='diff -cb '
portable=false
subprogs=dbstats
