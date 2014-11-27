prog='dbrowdiff'
args='-I clock'
in=TEST/dbrowdiff_ex.in
cmp='diff -cb '
altcmp='dbfilediff --quiet -E --exit '
altcmp_needs_input_flags=true
