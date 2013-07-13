prog='dbmapreduce'
args='-k experiment dbmultistats duration'
cmp='diff -cb '
in=TEST/dbmapreduce_ex.in
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
suppress_warnings='5.10:Unbalanced string table refcount;5.10:Scalars leaked'
