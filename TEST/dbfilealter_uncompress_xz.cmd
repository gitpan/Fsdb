prog='dbfilealter'
args='-Z none'
in='TEST/dbfilealter_compress_xz.out'
cmp='diff -cb '
requires='IO::Compress::Xz'
