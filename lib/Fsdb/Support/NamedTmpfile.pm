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

This routine is Perl thread-safe.

While this routine is basically "new", we don't call it such
because we do not return an object.

=cut

@ISA = ();
($VERSION) = 1.0;

use threads;
use threads::shared;

use Carp;
use File::Temp qw(tempfile);

my @named_tmpfiles : shared;
my $tmpdir = undef;
my $template = undef;

sub alloc {
    my($tmpdir) = @_;

    if (!defined($tmpdir)) {
	$tmpdir = (defined($ENV{'TMPDIR'}) ? $ENV{'TMPDIR'} : "/tmp") if (!defined($tmpdir));
    };
    if (!defined($template)) {
	$template = sprintf("fsdb.%d.XXXXXX", $$);
    };

    croak "tmpdir $tmpdir is not a directory\n" if (! -d $tmpdir);
    croak "tmpdir $tmpdir is not writable\n" if (! -w $tmpdir);
    my($fh, $fn) = tempfile($template, SUFFIX => "~", DIR => $tmpdir);
    close $fh;
    push @named_tmpfiles, $fn;

    return $fn;
}

=head2 cleanup_one

    Fsdb::Support::NamedTmpfile::cleanup_one('some_filename');

cleanup one tmpfile, forgetting about it if necessary.


=cut

sub cleanup_one {
    my($fn) = @_;
    return if (!defined($fn));
    # xxx: doesn't check for inclusion first
    unlink($fn) if (-f $fn);
    @named_tmpfiles = grep { defined($_) && $_ ne $fn } @named_tmpfiles;
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


1;
