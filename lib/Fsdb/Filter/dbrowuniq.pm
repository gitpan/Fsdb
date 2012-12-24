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

dbrowuniq [-cL] [uniquifying fields...]

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

By default, L<dbrowuniq> outputs the I<first> unique row.
Optionally, with F<-L>, it will output the I<last> unique row.
(This choice only matters when uniqueness is determined by specific fields.)

=head1 OPTIONS

=over 4

=item B<-c> or B<--count>

Create a new column (count) which counts the number of times
each line occurred.

The new column is named by the C<-N> argument, defaulting to C<count>.

=item B<-N> on B<--new-name>

Specify the name of the count column, if any.
(Default is C<count>.)

=item B<-L> or B<--last>

Output the last unique row. 
By default, it outputs the first unique row.

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

=head1 SAMPLE USAGE 2

=head2 Input:

	#h event i
	_null_getpage+128 10
	_null_getpage+128 11
	_null_getpage+128 12
	_null_getpage+128 13
	_null_getpage+128 14
	_null_getpage+128 15
	_null_getpage+4 16
	_null_getpage+4 17
	_null_getpage+4 18
	_null_getpage+4 19
	_null_getpage+4 20
	_null_getpage+4 21
	#  | /home/johnh/BIN/DB/dbcol event
	#  | /home/johnh/BIN/DB/dbsort event

=head2 Command:

    cat data.fsdb | dbrowuniq -c -L event

=head2 Output:

	#fsdb event i count
	_null_getpage+128	15	6
	#  | /home/johnh/BIN/DB/dbcol event
	#  | /home/johnh/BIN/DB/dbsort event
	_null_getpage+4	21	6
	#   | dbrowuniq -c 

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
    $self->{_last} = undef;
    $self->{_uniquifying_cols} = [];
    $self->{_destination_column} = 'count';
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
	'L|last!' => \$self->{_last},
	'close!' => \$self->{_close},
	'd|debug+' => \$self->{_debug},
	'i|input=s' => sub { $self->parse_io_option('input', @_); },
	'log!' => \$self->{_logprog},
	'N|new-name=s' => \$self->{_destination_column},
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
        $self->{_out}->col_create($self->{_destination_column})
	    or croak $self->{_prog} . ": cannot create column " . $self->{_destination_column} . " (maybe it already existed?)\n";
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

    my $first_prev_fref = [];
    my $last_prev_fref = [];
    my $output_fref = [];
    my $this_fref;
    my $count = 0;

    my $check_code = '1';
    foreach (@{$self->{_uniquifying_cols}}) {
	my $coli = $self->{_in}->col_to_i($_);
	croak $self->{_prog} . ": internal error, cannot find column $_ even after checking already.\n"
	    if (!defined($coli));
	$check_code .= " && (\$first_prev_fref->[$coli] eq \$this_fref->[$coli])";
    };
    print $check_code if ($self->{_debug});

    my $handle_new_key_code = q'
	@{$first_prev_fref} = @{$this_fref};
	$count = 1;
    ';
    my $remember_prev_row_code = q'
	@{$last_prev_fref} = @{$this_fref};
    ';
    $remember_prev_row_code = '' if (!$self->{_last});  # optimization
    my $handle_end_of_prev_code =
	'@{$output_fref} = ' . ($self->{_last} ? '@{$last_prev_fref}' : '@{$first_prev_fref}') . ";\n" . 
	(defined($self->{_count}) ? '$output_fref->[' . $count_coli . '] = $count;' . "\n" : '') .
	'&$write_fastpath_sub($output_fref) if ($count > 0);' . "\n";

    my $loop_code = q'
	while ($this_fref = &$read_fastpath_sub()) {
	    if ($count > 0) {
		if (' . $check_code . q') {
		    # identical, so just update prev
		    ' . $remember_prev_row_code . q'
		    $count++;
		    next;
		} else {
		    # not identical
		    ' . $handle_end_of_prev_code 
		       . $handle_new_key_code 
		       . $remember_prev_row_code . q'
		};
	    } else {
		# first row ever
		' . $handle_new_key_code 
		. $remember_prev_row_code . q'
	    };
        };
	# handle last row
	' . $handle_end_of_prev_code . "\n";
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
