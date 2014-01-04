prog='dbfilealter'
# gzip gives non-determinstic output ecause it embeds the time in the header
enabled=0
args='-Z gz'
in=TEST/dbfilealter_ex.in
cmp='cmp '
requires='IO::Compress::Gzip'
