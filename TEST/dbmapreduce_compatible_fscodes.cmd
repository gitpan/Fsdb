prog='dbmapreduce'
args='-k experiment dbcolstats -F S duration'
cmp='diff -cb '
subprogs=dbcolstats
in=TEST/dbmapreduce_incompatible_fscodes.in
