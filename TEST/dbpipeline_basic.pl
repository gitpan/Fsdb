#!/usr/bin/perl -w

#
# dbpipeline_basic.pl
# Copyright (C) 2007 by John Heidemann
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#

use threads;
use Fsdb::Filter::dbpipeline;
use Fsdb::Filter::dbcol;
use Fsdb::Filter::dbroweval;

# do the equivalent of
#
#   cat DATA/grades.jdb | dbcol name test1 | dbroweval '_test1 += 5;'

my $pipeline = new Fsdb::Filter::dbpipeline(
		    '--noautorun',
		    new Fsdb::Filter::dbcol(qw(name test1)),
		    new Fsdb::Filter::dbroweval('_test1 += 5;'));
# now run it by hand
$pipeline->setup_run_finish();

exit 0;
