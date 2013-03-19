#!/usr/bin/perl -w

#
# dbmerge.pm
# Copyright (C) 1991-2013 by John Heidemann <johnh@isi.edu>
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
With C<--xargs>, each line of standard input is a filename for input.

Inputs must have identical schemas (columns, column order,
and field separators).

Unlike F<dbmerge2>, F<dbmerge> supports an arbitrary number of 
input files.

Because this program is intended to merge multiple sources,
it does I<not> default to reading from standard input.
If you wish to list F<-> as an explicit input source.

Also, because we deal with multiple input files,
this module doesn't output anything until it's run.

L<dbmerge> consumes a fixed amount of memory regardless of input size.
It therefore buffers output on disk as necessary.
(Merging is implemented a series of two-way merges,
so disk space is O(number of records).)

L<dbmerge> will merge data in parallel, if possible.
The <--parallelism> option can control the degree of parallelism,
if desired.


=head1 OPTIONS

General option:

=over 4

=item B<--xargs>

Expect that input filenames are given, one-per-line, on standard input.
(In this case, merging can start incrementally.

=item B<--removeinputs>

Delete the source files after they have been consumed.
(Defaults off, leaving the inputs in place.)

=item B<-T TmpDir>

where to put tmp files.
Also uses environment variable TMPDIR, if -T is 
not specified.
Default is /tmp.

=item B<--parallelism> N

Allow up to N merges to happen in parallel.
Default is the number of CPUs in the machine.

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

use threads;
use Thread::Semaphore;
use Thread::Queue;

use Fsdb::Filter;
use Fsdb::Filter::dbmerge2;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;
use Fsdb::Support::NamedTmpfile;
use Fsdb::Support::OS qw($parallelism_available);


=head2 new

    $filter = new Fsdb::Filter::dbmerge(@arguments);

Create a new object, taking command-line arugments.

=cut

sub new($@) {
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

sub set_defaults($) {
    my $self = shift @_;
    $self->SUPER::set_defaults();
    $self->{_remove_inputs} = undef;
    $self->{_info}{input_count} = 2;
    $self->{_sort_argv} = [];
    $self->{_max_parallelism} = undef;
    $self->{_test} = '';
    $self->{_xargs} = undef;
    $self->set_default_tmpdir;
}

=head2 parse_options

    $filter->parse_options(@ARGV);

Internal: parse command-line arguments.

=cut

sub parse_options($@) {
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
	'parallelism=i' => \$self->{_max_parallelism},
	'removeinputs!' => \$self->{_remove_inputs},
	'test=s' => \$self->{_test},
	'T|tmpdir|tempdir=s' => \$self->{_tmpdir},
	'xargs!' => \$self->{_xargs},
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


=head2 segment_next_output

    $out = $self->segment_next_output($is_final_output)

Internal: return a Fsdb::IO::Writer as $OUT
that either points to our output or a temporary file, 
depending on how things are going.

=cut

sub segment_next_output($$) {
    my ($self, $is_final_output) = @_;
    my $out;
    if ($is_final_output) {
#        $self->finish_io_option('output', -clone => $self->{_two_ins}[0]);
#        $out = $self->{_out};
	$out = $self->{_output};   # will pass this to the dbmerge2 module
	print "# final output\n" if ($self->{_debug});
    } else {
	# dump to a file for merging
	my $tmpfile = Fsdb::Support::NamedTmpfile::alloc($self->{_tmpdir});
	$self->{_files_cleanup}{$tmpfile} = 'NamedTmpfile';
	$out = $tmpfile;   # just return the name
    };
    return $out;
}


=head2 segment_cleanup

    $out = $self->segment_cleanup($file);

Internal: Clean up a file, if necessary.
(Sigh, used to be function pointers, but 
not clear how they would interact with threads.)

=cut

sub segment_cleanup($$) {
    my($self, $file) = @_;
    my($cleanup_type) = $self->{_files_cleanup}{$file};
    die "bad (empty) file in dbmerge::segment_cleanup\n"
	if (!defined($file));
    if (!defined($cleanup_type)) {
	print "# dbmerge: segment_cleanup:  no cleanup for $file\n" if ($self->{_debug});
	# nothing
    } elsif ($cleanup_type eq 'unlink') {
	print "# dbmerge: segment_cleanup: cleaning up $file\n" if ($self->{_debug});
	unlink($file);
    } elsif ($cleanup_type eq 'NamedTmpfile') {
	print "# dbmerge: segment_cleanup:  NamedTmpfile::cleanup_one $file\n" if ($self->{_debug});
	Fsdb::Support::NamedTmpfile::cleanup_one($file);
    } else {
	die $self->{_prog} . ": internal error, unknown segment_cleanup type $cleanup_type\n";
    };
}


=head2 segments_merge2_finish

    $out = $self->segments_merge2_finish($out_fn, $is_final_output, 
			$two_in_fn_ref);


Internal: do the actual merge2 work (maybe our parent put us in a
thread, maybe not).

=cut
sub segments_merge2_finish($$$$) {
    my($self, $out_fn, $is_final_output, $two_in_fn_ref) = @_;

    my @merge_options = qw(--autorun --nolog);
    push(@merge_options, '--noclose', '--saveoutput' => \$self->{_out})
	if ($is_final_output);

    my $debug_msg = "$two_in_fn_ref->[0] with $two_in_fn_ref->[1] to $out_fn" . ($is_final_output ? " (final)" : "");
    print "# segments_merge2_finish: starting merge $debug_msg\n"
	if ($self->{_debug});
    new Fsdb::Filter::dbmerge2(@merge_options,
			'--input' => $two_in_fn_ref->[0],
			'--input' => $two_in_fn_ref->[1],
			'--output' => $out_fn,
			@{$self->{_sort_argv}});
    print "# segments_merge2_finish: done with merge $debug_msg\n"
	if ($self->{_debug});

    foreach (0..1) {
	$self->segment_cleanup($two_in_fn_ref->[$_]);
    };
}

=head2 segments_merge2_start

    $out = $self->segments_merge2_start($go_sem, $filename, $force_start);

Internal: release a forked thread (bound by $GO_SEM).
Always if $FORCE_START, otherwise only if we have free parallelism.

Returns either the semaphore (if it's not started)
or the thread (if it is started).

=cut

sub segments_merge2_start($$$$) {
    my($self, $go_sem, $out_fn, $force_start) = @_;
    #
    # Note there's a race in our check of parallelism available
    # and letting it go.
    # It's not REALLY a race because only one thread releases threads,
    # and it doesn't matter because an extra active thread is not
    # the end of the world.
    #
    if ($parallelism_available <= 0 && !$force_start) {
	print "# segments_merge2_start: merge thread for $out_fn postponed\n" if ($self->{_debug});
	return [$go_sem, $out_fn];
    };
    # send it off!
    if ($self->{_debug}) {
	my $reason = ($parallelism_available <= 0) ? 'forced' : 'free';
        print "# segments_merge2_start: merge thread for $out_fn started, $reason\n";
    };
    $go_sem->up();
    return [undef, $out_fn];
}

=head2 segments_merge_one_depth

    $self->segments_merge_one_depth($depth);

Merge queued files, if any.

Also release any queued threads.

=cut

sub segments_merge_one_depth($$) {
    my($self, $depth) = @_;

    my $work_ref = $self->{_work}[$depth];
    my $closed = $self->{_work_closed}[$depth];
    my $ipc = $self->{_ipc};

    #
    # Merge the files in a binary tree.
    #
    # In the past, we did this in a very clever
    # a file-system-cache-friendly order, based on ideas from
    # "Information and Control in Gray-box Systems" by
    # the Arpaci-Dusseaus at SOSP 2001.
    #
    # Unfortunately, this optimization makes the sort unstable
    # and complicated,
    # so it was dropped when paralleism was added.
    #
    while ($#{$work_ref} >= ($closed ? 0 : 3)) {
	if ($#{$work_ref} == 0) {
	    last if (!$closed);
	    # one left, just punt it next
	    print "# segments_merge_one_depth: runt at depth $depth pushed to next depth.\n" if ($self->{_debug});
	    push ($self->{_work}[$depth + 1], shift @$work_ref);
	    die "internal error\n" if ($#{$work_ref} != -1);
	    last;
	};
	# are they blocked?  force-start them if we are
	my $waiting_on_inputs = 0;
	foreach my $i (0..1) {
	    my $ref = ref($work_ref->[$i]);
	    next if ($ref eq '');  # filename
	    die "suprise ref $ref in the work queue $depth\n" if ($ref ne 'ARRAY');
	    my($go_sem, $fn) = ($work_ref->[$i][0], $work_ref->[$i][1]);
	    if (defined($go_sem)) {
		print "# segments_merge_one_depth: depth $depth forced start on $fn.\n" if ($self->{_debug});
		$self->segments_merge2_start($go_sem, $fn, 1);
		$work_ref->[$i][0] = undef;   # note that it's started
	    } else {
		print "# segments_merge_one_depth: depth $depth waiting on working $fn.\n" if ($self->{_debug});
	    };
	    $waiting_on_inputs++;
	};
	# bail out if inputs are not done yet.
	return if ($waiting_on_inputs);

	# now we KNOW we have filenames, not queued work
	my(@two_fn) = (shift @${work_ref}, shift @${work_ref});
	my $is_final_output = ($closed && $#{$work_ref} == -1 && $depth == $#{$self->{_work}});
	my($out_fn) = $self->segment_next_output($is_final_output);
	print "# segments_merge_one_depth: depth $depth planning $two_fn[0] and $two_fn[1] to $out_fn.\n" if ($self->{_debug});

	foreach my $i (0..1) {
	    next if ($two_fn[$i] eq '-');
	    croak $self->{_prog} . ": file $two_fn[$i] is missing.\n"
		if (! -f $two_fn[$i]);
	};

	if ($is_final_output) {
		# last time: do it here, in-line
		# so that we update $self->{_out} in the main thread
		$self->segments_merge2_finish($out_fn, $is_final_output, \@two_fn);
		# signal ourselves that we're really done
		$ipc->enqueue([-1, 'merge-final']);
		return;
	};
	#
	# fork a thread to do the merge
	#
	my $go_sem = Thread::Semaphore->new(0);
	my $merge_thread = threads->new(
	    sub {
		$go_sem->down();  # wait to be told to start
		$parallelism_available--;
		$self->segments_merge2_finish($out_fn, $is_final_output, \@two_fn);
		sleep(1) if (defined($self->{_test}) && $self->{_test} eq 'delay-finish');
		$parallelism_available++;
		$ipc->enqueue([$depth+1, $out_fn]);
		return $out_fn;
	    }
        );
	# Put the thread in our queue, and maybe run it.
	$merge_thread->detach();
	push(@{$self->{_work}[$depth+1]}, $self->segments_merge2_start($go_sem, $out_fn, 0));
    }; 
    # At this point all possible work has been queued and maybe started.
    # If the depth is closed, the work should be empty.
    # if not, there may be some files in the queue
}

=head2 segments_xargs

    $self->segments_xargs();

Internal: read new filenames to process (from stdin)
and send them to the work queue.

=cut

sub segments_xargs($) {
    my($self) = @_;
    my $ipc = $self->{_ipc};

    my $num_inputs = 0;

    # read files as in fsdb format
    if ($#{$self->{_inputs}} == 0) {
	# hacky...
	$self->{_input} = $self->{_inputs}[0];
    };
    $self->finish_io_option('input', -header => '#fsdb filename');
    my $read_fastpath_sub = $self->{_in}->fastpath_sub();
    while (my $fref = &$read_fastpath_sub()) {
	# send each file for processing as level zero
	print "# dbmerge: segments_xargs: got $fref->[0]\n" if ($self->{_debug});
	$ipc->enqueue([0, $fref->[0]]);
	$num_inputs++;
    };
    if ($num_inputs <= 1) {
	$ipc->enqueue([-1, "error: --xargs, but zero or one input files; dbmerge needs at least two."]);  # signal eof-f
    };
    $ipc->enqueue([-1, 'xargs-eof']);
}


=head2 segments_merge_all

    $self->segments_merge_all()

Internal:
Merge queued files, if any.
Iterates over all depths of the merge tree,
and handles any forked threads.

=cut

sub segments_merge_all($) {
    my($self) = @_;

    # queue to handle inter-thread communication
    my $ipc = Thread::Queue->new();
    $self->{_ipc} = $ipc;

    my $xargs_thread = undef;
    if ($self->{_xargs}) {
	$xargs_thread = threads->new(
	    sub {
		$self->segments_xargs();
	    }
	);
	$xargs_thread->detach();
    };

    #
    # Alternate forking off new merges from finished files,
    # and checking $ipc for finished work or new files.
    #
    for (;;) {
	# test for done by signal that is_final_output has run
#	# done?
#	my $deepest = $#{$self->{_work}};
#	last if ($self->{_work_closed}[$deepest] && $#{$self->{_work}[$deepest]} <= 0);

	#
	# First, fork off new threads, where possible.
	#
	foreach my $depth (0..$#{$self->{_work}}) {
	    $self->segments_merge_one_depth($depth);
	    if ($#{$self->{_work}[$depth]} == -1 && $self->{_work_closed}[$depth]) {
		# When one level is all in progress, we can close the next.
		my $next_depth = $depth + 1;
		if (!$self->{_work_closed}[$next_depth]) {
		    $self->{_work_closed}[$next_depth] = 1;
		    print "# segments_merge_all: closed work level $next_depth\n" if ($self->{_debug});
		};
	    };
	};

	#
	# Next, handle stuff that's finished.
	# We do ONE thing, maybe blocking, then go back and see
	# if we can fork off more threads.
	#
	my $work_aref = $ipc->dequeue();
	my($work_depth, $work_fn) = @$work_aref;
	if ($work_depth == -1) {
	    if ($work_fn =~ /^error/) {
		croak $self->{_prog} . ": $work_fn\n";
	    } elsif ($work_fn eq 'xargs-eof') {
		# eof from xargs, propagate it
		print "# segments_merge_all: eof from xargs\n" if ($self->{_debug});
		$self->{_work_closed}[0] = 1;
		next;
	    } elsif ($work_fn eq 'merge-final') {
		print "# segments_merge_all: got is_final_output signal\n" if ($self->{_debug});
		last;
	    } else {
		croak $self->{_prog} . ": internal error: suprising ipc signal $work_fn\n";
	    };
	};
	print "# segments_merge_all: new file for $work_fn at $work_depth\n" if ($self->{_debug});

	if ($work_depth == 0) {
	    # queue first level files here
	    push(@{$self->{_work}[$work_depth]}, $work_fn);
	    $self->{_files_cleanup}{$work_fn} = 'unlink'
		if ($self->{_remove_inputs});
	} else {
	    #
	    # If we get a new deeper file, then 
	    # some thread finished.  We therefore need to
	    # (1) find that file in the work queue and make it finished,
	    # and (2) to try to release a pending thread, if any. 
	    #
	    my $found_finished = undef;
	    my $found_new = undef;
	    CLEANUP:
	    foreach my $depth (0..$#{$self->{_work}}) {
	        foreach my $i (0..$#{$self->{_work}[$depth]}) {
		    last CLEANUP if ($found_finished && $found_new);
		    my $work_ref = $self->{_work}[$depth][$i];
		    if (ref($work_ref) eq 'ARRAY') {
			my($go_sem, $fn) = @$work_ref;
			if (!defined($go_sem)) {
			    next if ($found_finished);
			    next if ($fn ne $work_fn);
			    print "# segments_merge_all: merge thread for $depth, $i reports $fn is done\n" if ($self->{_debug});
			    $self->{_work}[$depth][$i] = $work_fn;
			    $found_finished = 1;
			} else {
			    die "internal error: suprising thing in the work queue at $depth/$i\n"
				if (ref($go_sem) ne 'Thread::Semaphore');
			    next if ($found_new);
			    # release it
			    print "# segments_merge_all: merge thread released, depth $depth, file $fn\n" if ($self->{_debug});
			    $go_sem->up();
			    $work_ref->[0] = undef;
			    $found_new = 1;
			};
		    };
		};
	    };
	};
    };
}



=head2 setup

    $filter->setup();

Internal: setup, parse headers.

=cut

sub setup($) {
    my($self) = @_;

    croak $self->{_prog} . ": no sorting key specified.\n"
	if ($#{$self->{_sort_argv}} == -1);

    if (!$self->{_xargs} && $#{$self->{_inputs}} == -1) {
	croak $self->{_prog} . ": no input sources specified, use --input or --xargs.\n";
    };
    if (!$self->{_xargs} && $#{$self->{_inputs}} == 0) {
	croak $self->{_prog} . ": only one input source, but can't merge one file.\n";
    };
    if ($self->{_xargs} && $#{$self->{_inputs}} > 0) {
	croak $self->{_prog} . ": --xargs and multiple inputs (perhaps you meant NOT --xargs?).\n";
    };
    # prove files exist (early error checking)
    foreach (@{$self->{_inputs}}) {
	next if (ref($_) ne '');   # skip objects
	next if ($_ eq '-');   # special case: stdin
	if (! -f $_) {
	    croak $self->{_prog} . ": input source $_ does not exist.\n";
	};
    };
    if ($self->{_remove_inputs}) {
	foreach (@{$self->{_inputs}}) {
	    $self->{_files_cleanup}{$_} = 'unlink'
		if ($_ ne '-');
	};
    };
    #
    # the _work queue consists of
    # 1. filenames that need to be merged.
    # 2. [$go_sem, filename] for blocked threads
    # 3. [undef, filename] for files still being computed.
    if ($self->{_xargs}) {
	$self->{_work}[0] = [];
	$self->{_work_closed}[0] = 0;
    } else {
	push(@{$self->{_work}[0]}, @{$self->{_inputs}});
	$self->{_work_closed}[0] = 1;
    };
    #
    # control parallelism
    #
    if (!defined($parallelism_available)) {
        if (!defined($self->{_max_parallelism})) {
	    $self->{_max_parallelism} = Fsdb::Support::OS::max_parallelism();
	};
	$parallelism_available = $self->{_max_parallelism};
    };
}

=head2 run

    $filter->run();

Internal: run over each rows.

=cut
sub run($) {
    my($self) = @_;

    $self->segments_merge_all();
};

    

=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2013 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;
