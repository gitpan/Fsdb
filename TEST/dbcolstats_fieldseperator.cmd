prog='dbcolstats'
args='-F S absdiff'
cmp='diff -c '
in=TEST/dbcolstats_ex.in
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
