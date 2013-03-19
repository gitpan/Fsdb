prog='dbmerge'
# delay shoudl force testing of multithread queueing
args='--test delay-finish -n -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in  -i TEST/dbmerge_1k.in n'
cmd_tail='| dbrowuniq -c'
in='/dev/null'
cmp='diff -cb '
