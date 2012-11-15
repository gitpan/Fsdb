prog='dbmapreduce'
args='-k experiment -C "dbrow(\"_experiment eq 'ufs_mab_sys'\")"'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
