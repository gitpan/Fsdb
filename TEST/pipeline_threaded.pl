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
# this time, do it with real threads
#
my $colthr = threads->new(sub { $dbcol->setup_run_finish; });
my $rowthr = threads->new(sub { $dbrow->setup_run_finish; });

# see comment in pipeline_reverse_threaded.pl
$rowthr->join;
$colthr->join;

exit 0;
