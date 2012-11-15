#!/usr/bin/perl -w

#
# dbrowuniq.pm
# Copyright (C) 1997-2012 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#

package Fsdb::Filter::dbrowuniq;

=head1 NAME

dbrowuniq - eliminate adjacent rows with duplicate fields, maybe counting

=head1 SYNOPSIS

dbrowuniq [-c] [uniquifying fields...]

=head1 DESCRIPTION

Eliminate adjacent rows with duplicate fields, perhaps counting them.
Roughly equivalent to the Unix L<uniq> command,
but optionally only operating on the specified fields.

By default, I<all> columns must be unique.
If column names are specified, only those columns must be unique
and the first row with those columns is returned.

Dbrowuniq eliminates only identical rows that I<adjacent>.
If you want to eliminate identical rows across the entirefile,
you must make them adajcent, perhaps by using dbsort on your
uniquifying field.
(That is, the input with three lines a/b/a will produce
three lines of output with both a's, but if you dbsort it,
it will become a/a/b and dbrowuniq will output a/b.

=head1 OPTIONS

=over 4

=item B<-c> or B<--count>

Create a new column (count) which counts the number of times
each line occurred.

=back

=for comment
begin_standard_fsdb_options

This module also supports the standard fsdb options:

=over 4

=item B<-d>

Enable debugging output.

=item B<-i> or B<--input> InputSource

Read from InputSource, typically a file name, or C<-> for standard input,
or (if in Perl) a IO::Handle, Fsdb::IO or Fsdb::BoundedQueue objects.

=item B<-o> or B<--output> OutputDestination

Write to OutputDestination, typically a file name, or C<-> for standard output,
or (if in Perl) a IO::Handle, Fsdb::IO or Fsdb::BoundedQueue objects.

=item B<--autorun> or B<--noautorun>

By default, programs process automatically,
but Fsdb::Filter objects in Perl do not run until you invoke
the run() method.
The C<--(no)autorun> option controls that behavior within Perl.

=item B<--help>

Show help.

=item B<--man>

Show full manual.

=back

=for comment
end_standard_fsdb_options


=head1 SAMPLE USAGE

=head2 Input:

    #fsdb      event
    _null_getpage+128
    _null_getpage+128
    _null_getpage+128
    _null_getpage+128
    _null_getpage+128
    _null_getpage+128
    _null_getpage+4
    _null_getpage+4
    _null_getpage+4
    _null_getpage+4
    _null_getpage+4
    _null_getpage+4
    #  | /home/johnh/BIN/DB/dbcol event
    #  | /home/johnh/BIN/DB/dbsort event

=head2 Command:

    cat data.fsdb | dbrowuniq -c

=head2 Output:

    #fsdb	event	count
    _null_getpage+128	6
    _null_getpage+4	6
    #	2	/home/johnh/BIN/DB/dbcol	event
    #  | /home/johnh/BIN/DB/dbrowuniq -c

=head1 SEE ALSO

L<Fsdb>.


=head1 CLASS FUNCTIONS

=cut

@ISA = qw(Fsdb::Filter);
$VERSION = 2.0;

use strict;
use Pod::Usage;
use Carp;

use Fsdb::Filter;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;


=head2 new

    $filter = new Fsdb::Filter::dbrowuniq(@arguments);

Create a new dbrowuniq object, taking command-line arguments.

=cut

sub new ($@) {
    my $class = shift @_;
    my $self = $class->SUPER::new(@_);
    bless $self, $class;
    $self->set_defaults;
    $self->parse_options(@_);
    $self->SUPER::post_new();
    return $self;
}


=head2 set_defaults

    $filter->set_defaults();

Internal: set up defaults.

=cut

sub set_defaults ($) {
    my($self) = @_;
    $self->SUPER::set_defaults();
    $self->{_count} = undef;
    $self->{_uniquifying_cols} = [];
}

=head2 parse_options

    $filter->parse_options(@ARGV);

Internal: parse command-line arguments.

=cut

sub parse_options ($@) {
    my $self = shift @_;

    my(@argv) = @_;
    $self->get_options(
	\@argv,
 	'help|?' => sub { pod2usage(1); },
	'man' => sub { pod2usage(-verbose => 2); },
	'autorun!' => \$self->{_autorun},
	'c|count!' => \$self->{_count},
	'close!' => \$self->{_close},
	'd|debug+' => \$self->{_debug},
	'i|input=s' => sub { $self->parse_io_option('input', @_); },
	'log!' => \$self->{_logprog},
	'o|output=s' => sub { $self->parse_io_option('output', @_); },
	) or pod2usage(2);
    push (@{$self->{_uniquifying_cols}}, @argv);
}

=head2 setup

    $filter->setup();

Internal: setup, parse headers.

=cut

sub setup ($) {
    my($self) = @_;

    $self->finish_io_option('input', -comment_handler => $self->create_pass_comments_sub);

    if ($#{$self->{_uniquifying_cols}} == -1) {
	push (@{$self->{_uniquifying_cols}}, @{$self->{_in}->cols});
    } else {
	foreach (@{$self->{_uniquifying_cols}}) {
	    croak $self->{_prog} . ": unknown column ``$_''.\n"
		if (!defined($self->{_in}->col_to_i($_)));
	};
    };

    $self->finish_io_option('output', -clone => $self->{_in}, -outputheader => 'delay');
    if ($self->{_count}) {
        $self->{_out}->col_create('count')
	    or croak $self->{_prog} . ": cannot create column count (maybe it already existed?)\n";
    };
}

=head2 run

    $filter->run();

Internal: run over each rows.

=cut
sub run ($) {
    my($self) = @_;

    my $read_fastpath_sub = $self->{_in}->fastpath_sub();
    my $write_fastpath_sub = $self->{_out}->fastpath_sub();
    my $count_coli = $self->{_out}->col_to_i('count');

    my $last_fref = [];
    my $fref;
    my $count = 0;

    my $check_code = '1';
    foreach (@{$self->{_uniquifying_cols}}) {
	my $coli = $self->{_in}->col_to_i($_);
	croak $self->{_prog} . ": internal error, cannot find column $_ even after checking already.\n"
	    if (!defined($coli));
	$check_code .= " && (\$last_fref->[$coli] eq \$fref->[$coli])";
    };
    print $check_code if ($self->{_debug});

    my $handle_new_row_code = q'
	@{$last_fref} = @{$fref};
	$count = 1;
    ';
    my $handle_old_row_code =
	(defined($self->{_count}) ? '$last_fref->[' . $count_coli . '] = $count;' . "\n" : '') .
	'&$write_fastpath_sub($last_fref) if ($#{$last_fref} != -1);' . "\n";

    my $loop_code = q'
	while ($fref = &$read_fastpath_sub()) {
	    if ($#{$last_fref} != -1) {
		if (' . $check_code . q') {
		    # identical
		    $count++;
		    next;
		} else {
		    # not identical
		    ' . $handle_old_row_code 
		       . $handle_new_row_code . q'
		};
	    } else {
		# first row ever
		' . $handle_new_row_code . q'
	    };
        };
	# handle last row
	' . $handle_old_row_code . "\n";
    eval $loop_code;
    $@ && croak $self->{_prog} . ": internal eval error: $@\n";
};


=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2012 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;
