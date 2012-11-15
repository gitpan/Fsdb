#!/usr/bin/perl -w

#
# dbmerge.pm
# Copyright (C) 1991-2008 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#

package Fsdb::Filter::dbmerge;

=head1 NAME

dbmerge - merge all inputs in sorted order based on the the specified columns

=head1 SYNOPSIS

    dbmerge --input A.fsdb --input B.fsdb [-T TemporaryDirectory] [-nNrR] column [column...]

or
    cat A.fsdb | dbmerge --input - --input B.fsdb [-T TemporaryDirectory] [-nNrR] column [column...]


or
    dbmerge [-T TemporaryDirectory] [-nNrR] column [column...] --inputs A.fsdb [B.fsdb ...]


=head1 DESCRIPTION

Merge all provided, pre-sorted input files, producing one sorted result.
Inputs can both be specified with C<--input>, or one can come
from standard input and the other from C<--input>.

Inputs must have identical schemas (columns, column order,
and field separators).

Unlike F<dbmerge2>, F<dbmerge> supports an arbitrary number of 
input files.

Because this program is intended to merge multiple sources,
it does I<not> default to reading from standard input.
If you wish to list F<-> as an explicit input source.

Also, because we deal with multiple input files,
this module doesn't output anything until it's run.

Dbmerge consumes a fixed amount of memory regardless of input size.
It therefore buffers output on disk as necessary.
(Merging is implemented a series of two-way merges,
so disk space is O(number of records).)


=head1 OPTIONS

General option:

=over 4

=item <--removeinputs>

Delete the source files after they have been consumed.
(Defaults off, leaving the inputs in place.)

=item <-T TmpDir>

where to put tmp files.
Also uses environment variable TMPDIR, if -T is 
not specified.
Default is /tmp.

=back

Sort specification options (can be interspersed with column names):

=over 4

=item B<-r> or B<--descending>

sort in reverse order (high to low)

=item B<-R> or B<--ascending>

sort in normal order (low to high)

=item B<-n> or B<--numeric>

sort numerically

=item B<-N> or B<--lexical>

sort lexicographically

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

File F<a.fsdb>:

    #fsdb cid cname
    11 numanal
    10 pascal

File F<b.fsdb>:

    #fsdb cid cname
    12 os
    13 statistics

These two files are both sorted by C<cname>,
and they have identical schemas.

=head2 Command:

    dbmerge --input a.fsdb --input b.fsdb cname

or

    cat a.fsdb | dbmerge --input b.fsdb cname

=head2 Output:

    #fsdb      cid     cname
    11 numanal
    12 os
    10 pascal
    13 statistics
    #  | dbmerge --input a.fsdb --input b.fsdb cname

=head1 SEE ALSO

L<dbmerge2(1)>,
L<dbsort(1)>,
L<Fsdb(3)>

=head1 CLASS FUNCTIONS

=cut


@ISA = qw(Fsdb::Filter);
($VERSION) = 2.0;

use strict;
use Carp qw(croak);
use Pod::Usage;

use Fsdb::Filter;
use Fsdb::Filter::dbmerge2;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;
use Fsdb::Support::NamedTmpfile;

=head2 new

    $filter = new Fsdb::Filter::dbmerge(@arguments);

Create a new object, taking command-line arugments.

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
    my $self = shift @_;
    $self->SUPER::set_defaults();
    $self->{_remove_inputs} = undef;
    $self->{_info}{input_count} = 2;
    $self->{_sort_argv} = [];
    $self->set_default_tmpdir;
}

=head2 parse_options

    $filter->parse_options(@ARGV);

Internal: parse command-line arguments.

=cut

sub parse_options ($@) {
    my $self = shift @_;

    my(@argv) = @_;
    my $past_sort_options = undef;
    $self->get_options(
	\@argv,
 	'help|?' => sub { pod2usage(1); },
	'man' => sub { pod2usage(-verbose => 2); },
	'autorun!' => \$self->{_autorun},
	'close!' => \$self->{_close},
	'd|debug+' => \$self->{_debug},
	'i|input=s@' => sub { $self->parse_io_option('inputs', @_); },
	'inputs!' => \$past_sort_options,
	'log!' => \$self->{_logprog},
	'o|output=s' => sub { $self->parse_io_option('output', @_); },
	'removeinputs!' => \$self->{_remove_inputs},
	'T|tmpdir|tempdir=s' => \$self->{_tmpdir},
	# sort key options:
	'n|numeric' => sub { $self->parse_sort_option(@_); },
	'N|lexical' => sub { $self->parse_sort_option(@_); },
	'r|descending' => sub { $self->parse_sort_option(@_); },
	'R|ascending' => sub { $self->parse_sort_option(@_); },
	'<>' => sub { if ($past_sort_options) {
			    $self->parse_io_option('inputs', @_);
			} else  {
			    $self->parse_sort_option('<>', @_);
			};
		    },
	) or pod2usage(2);
}


=head2 setup

    $filter->setup();

Internal: setup, parse headers.

=cut

sub setup ($) {
    my($self) = @_;

    croak $self->{_prog} . ": no sorting key specified.\n"
	if ($#{$self->{_sort_argv}} == -1);

    if ($#{$self->{_inputs}} == -1) {
	croak $self->{_prog} . ": no input sources specified, use --input.\n";
    };
    @{$self->{_files_to_merge}} = @{$self->{_inputs}};
    # prove files exist (early error checking)
    foreach (@{$self->{_inputs}}) {
	next if (ref($_) ne '');   # skip objects
	next if ($_ eq '-');   # special case: stdin
	if (! -f $_) {
	    croak $self->{_prog} . ": input source $_ does not exist.\n";
	};
    };
    # don't
    my $default_callback = $self->{_remove_inputs} ?
	    ($self->{_debug} ? sub { print STDERR "cleaning up $_[0]\n"; unlink($_[0]); } 
		: sub { unlink($_[0]); } ) :
	    undef;
    @{$self->{_files_cleanup}} = ($default_callback) x ($#{$self->{_files_to_merge}}+1);
}

=head2 segment_next_output

    $out = $self->segment_next_output($input_finished)

Internal: return a Fsdb::IO::Writer as $OUT
that either points to our output or a temporary file, 
depending on how things are going.

=cut

sub segment_next_output {
    my ($self, $input_finished) = @_;
    my $final_output = ($#{$self->{_files_to_merge}} == -1 && $input_finished);
    my $out;
    if ($final_output) {
#        $self->finish_io_option('output', -clone => $self->{_two_ins}[0]);
#        $out = $self->{_out};
	$out = $self->{_output};   # will pass this to the dbmerge2 module
	print "# final output\n" if ($self->{_debug});
    } else {
	# dump to a file for merging
	my $tmpfile = Fsdb::Support::NamedTmpfile::alloc($self->{_tmpdir});
	$out = $tmpfile;   # just return the name
	push(@{$self->{_files_to_merge}}, $tmpfile);
	push(@{$self->{_files_cleanup}}, \&Fsdb::Support::NamedTmpfile::cleanup_one);
	print "# intermediate file: $tmpfile\n" if ($self->{_debug});
    };
    return ($out, $final_output);
}


=head2 segment_merge

    $self->segment_merge();

Merge queued files, if any.

We process the work queue in
a file-system-cache-friendly order, based on ideas from
"Information and Control in Gray-box Systems" by
the Arpaci-Dusseau's at SOSP 2001.

Idea:  each "pass" through the queue, reverse the processing
order so that the most recent data (that's hopefully
in the file system cache in memory) is handled first.

This algorithm isn't perfect (sometimes if there's an odd number
of files in the queue you reach way back in time, but most of 
the time it's quite good).

=cut

sub segment_merge ($) {
    my($self) = @_;
    return if ($#{$self->{_files_to_merge}} == -1);

    my $files_to_merge_ref = $self->{_files_to_merge};
    my $files_cleanup_ref = $self->{_files_cleanup};

    # keep track of how often to reverse
    my($files_before_reversing_queue) = $#{$files_to_merge_ref} + 1;
    # Merge the files in a binary tree.
    while ($#{$files_to_merge_ref} >= 0) {
	# Each "pass", reverse the queue to reuse stuff in memory.
	if ($files_before_reversing_queue <= 0) {
	    @{$files_to_merge_ref} = reverse @{$files_to_merge_ref};
	    $files_before_reversing_queue = $#{$files_to_merge_ref} + 1;
	    print "# reversed queue, $files_before_reversing_queue before next reverse.\n" if ($self->{_debug});
	};
	$files_before_reversing_queue -= 2;
	# Pick off the two next files for merging.
	die $self->{_prog} . ": segment_merge, odd number of segments.\n" if ($#{$files_to_merge_ref} == 0);   # assert
	my (@two_fn) = (shift @${files_to_merge_ref}, shift @${files_to_merge_ref});
	my (@two_cleanup) = (shift @${files_cleanup_ref}, shift @${files_cleanup_ref});
	if (!ref($two_fn[0]) && !ref($two_fn[1])) {
	    # Take files in order with the goal of providing a stable sort,
	    # (if they're files... do nothing if they're objects).
	    if (($two_fn[0] cmp $two_fn[1]) > 0) {
		my $tmp_fn = $two_fn[0];
		$two_fn[0] = $two_fn[1];
		$two_fn[1] = $tmp_fn;

		my $tmp_cleanup = $two_cleanup[0];
		$two_cleanup[0] = $two_cleanup[1];
		$two_cleanup[1] = $tmp_cleanup;
	    };
	};
	my($out_fn, $final_output) = $self->segment_next_output(1);
	print "# merging $two_fn[0] with $two_fn[1] to $out_fn\n" if ($self->{_debug});

	#
	# run our merge
	#
	@{$self->{_two_inputs}} = @two_fn;
	my @merge_options = qw(--autorun --nolog);
	push(@merge_options, '--noclose', '--saveoutput' => \$self->{_out})
	    if ($final_output);
	new Fsdb::Filter::dbmerge2(@merge_options,
				'--input' => $two_fn[0], '--input' => $two_fn[1],
				'--output' => $out_fn,
				@{$self->{_sort_argv}});

        foreach (0..1) {
	    &{$two_cleanup[$_]}($two_fn[$_])
		if (defined($two_cleanup[$_]));
	};
    };
}



=head2 run

    $filter->run();

Internal: run over each rows.

=cut
sub run ($) {
    my($self) = @_;

    $self->segment_merge();
};

    

=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2008 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;
