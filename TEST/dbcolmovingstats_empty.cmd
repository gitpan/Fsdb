prog='dbcolmovingstats'
args='-n 4 -e - count'
in=TEST/dbcolmovingstats_ex.in
cmp='diff -cb '
altcmp='dbfilediff --quiet -E --exit '
altcmp_needs_input_flags=true

