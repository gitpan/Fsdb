#!/usr/bin/perl -w

#
# pipeline_basic.pl
# Copyright (C) 2007 by John Heidemann
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#

# next line is just to suppress the comment about threads::shared
use threads;

use Fsdb::BoundedQueue;
use Fsdb::Filter::dbcol;
use Fsdb::Filter::dbroweval;

# do the equivalent of
#
#   cat DATA/grades.jdb | dbcol name test1 | dbroweval '_test1 += 5;'

my $pipe = new Fsdb::BoundedQueue;
my $dbcol = new Fsdb::Filter::dbcol('--output' => $pipe, qw(name test1));
my $dbrow = new Fsdb::Filter::dbroweval('--input' => $pipe, '_test1 += 5;');

#
# first, just see if it works without threads
# (count on $pipe) to buffer the whole data, which it should.
#
$dbcol->setup_run_finish;
$dbrow->setup_run_finish;

exit 0;
