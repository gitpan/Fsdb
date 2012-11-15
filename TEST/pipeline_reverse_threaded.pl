#!/usr/bin/perl -w

#
# pipeline_basic.pl
# Copyright (C) 2007 by John Heidemann
# $Id: pipeline_threaded.pl 567 2007-11-18 05:23:47Z johnh $
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
my $rowthr = threads->new(sub { $dbrow->setup_run_finish; });
# However, reverse the thread start, forcing rowthr to block.
# On my system (Fedora 8, perl 5.8.8), this works correctly sometimes
# and fails others.  It fails more often with sleep().
# The bug is that I didn't thread->join the threads, so the exit 
# below was terminating them.  Who knew that perl didn't do an implicit join?
# See perl bug #48214 for my proposed resolution.
threads->yield;
my $colthr = threads->new(sub { sleep 1; $dbcol->setup_run_finish; });

$rowthr->join;
$colthr->join;

exit 0;
