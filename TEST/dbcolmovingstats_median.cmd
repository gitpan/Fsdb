prog='dbcolmovingstats'
args='-w 4 -m count'
cmp='diff -cb '
in=TEST/dbcolmovingstats_ex.in
altcmp='dbfilediff --quiet -E --exit '
altcmp_needs_input_flags=true
