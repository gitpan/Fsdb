prog='dbcolmovingstats'
# The following input SOMETIMES goes negative on the qsrt around line 3694
# (the run of 0.8244 values).   We now catch that that in dbcolmovingstats.
args='-w 20 a_short'
cmp='diff -cb '
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
