prog='dbcolmovingstats'
args='-w 4 -m -a count'
cmp='diff -cb '
in=TEST/dbcolmovingstats_nonnumeric_no.in
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
