# example 7.10 from Scheaffer and McClave
prog='dbrvstatdiff'
args='-h "<=0" x2 sd2 n2 x1 sd1 n1'
cmp='diff -cb '
altcmp='dbfilediff -E --exit '
altcmp_needs_input_flags=true
