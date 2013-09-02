#!/usr/bin/perl -w

#
# Fsdb::Support::Pipe.pm
# Copyright (C) 2013 by John Heidemann <johnh@ficus.cs.ucla.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblib for details.
#

package Fsdb::Support::Pipe;


=head1 NAME

Fsdb::Support::Pipe - extend IO::Pipe for threaded use

=head1 SYNOPSIS

    use Fsdb::Support::Pipe;
    my $pipe = new IO::Pipe;
    $read_end = $pipe->read_end();
    $write_end = $pipe->write_end();

This package provides functions to let one pick out parts of the pipe
from C<IO::Pipe> without disturbing the other side.
It depends on C<IO::Pipe> internals.

=cut
#'

1;


package IO::Pipe;

sub read_side {
    my $me = shift;
    my $fh  = ${*$me}[0];
    return $fh;
}
sub write_side {
    my $me = shift;
    my $fh  = ${*$me}[1];
    return $fh;
}

1;
