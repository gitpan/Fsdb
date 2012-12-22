prog='dbcolmovingstats'
args='-n 4 -e - count'
in=TEST/dbcolmovingstats_ex.in
cmp='diff -cb '
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true

