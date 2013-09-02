# this command demonstrates regular commands can operate on compressed input
prog='dbrow'
args='"_fullname =~ /John/"'
cmp='diff -cb '
cmd_tail='| dbfilealter -Z none'

