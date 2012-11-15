#!/usr/bin/perl

#
# Fsdb::Support::NamedTmpfile.pm
# Copyright (C) 2007 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#


package Fsdb::Support::NamedTmpfile;

=head1 NAME

Fsdb::Support::NamedTmpfile - dreate temporary files that can be opened

=head1 SYNOPSIS

See L<Fsdb::Support::NamedTmpfile::new>

=head1 FUNCTIONS

=head2 alloc

    $pathname = Fsdb::Support::NamedTmpfile::alloc($tmpdir);

Create a unique filename for temporary data.
$TMPDIR is optional.
The file is automatically removed on program exit,
but the pathname exists for the duration of execution
(and can be opened).

Note that there is a potential race condition between when we pick the file
and when the caller opens it, when an external program could interceed.
The caller therefor should open files with exclusive access.

This routine is Perl thread-save.

While this routine is basically "new", we don't call it such
because we do not return an object.

=cut

@ISA = ();
($VERSION) = ('$Revision$' =~ m/(\d+)/);

use threads;
use threads::shared;

my $named_tmpfile_counter : shared = 0;
my @named_tmpfiles : shared;

sub alloc {
    my($class, $tmpdir) = @_;

    $tmpdir = (defined($ENV{'TMPDIR'}) ? $ENV{'TMPDIR'} : "/tmp") if (!defined($tmpdir));

    my $i = $named_tmpfile_counter++;
    # Generate files with leading zeros so they can be lexically sorted
    # and dbsort can be stable (hopefully).
    my $fn = sprintf("%s/fsdb.%d.%05d~", $tmpdir, $$, $i);

    if ($i == 0) {
	 # install signals on first time
	 foreach (qw(HUP INT TERM)) {
	     $SIG{$_} = \&cleanup_signal;
	 };
    };

    push @named_tmpfiles, $fn;

    return $fn;
}

=head2 cleanup_one

    Fsdb::Support::NamedTmpfile::cleanup_one('some_filename');

cleanup one tmpfile, forgetting about it if necessary.


=cut

sub cleanup_one {
    my($fn) = @_;
    # xxx: doesn't check for inclusion first
    unlink($fn) if (-f $fn);
    @named_tmpfiles = grep { $_ ne $fn } @named_tmpfiles;
}


=head2 cleanup_all

    Fsdb::Support::NamedTmpfile::cleanup_all

Cleanup all tmpfiles
Not a method.

=cut

sub cleanup_all {
    while ($fn = shift @named_tmpfiles) {
	unlink($fn) if (-f $fn);
    };
}

sub END {
    # graceful termination
    cleanup_all;
}


=head2 cleanup_signal 

cleanup tmpfiles after a signal. 
Not a method.

=cut

sub cleanup_signal {
    my($sig) = @_;
    # print "terminated with signal $sig, cleaning up ". join(',', @tmpfiles) . "\n";
}

1;
