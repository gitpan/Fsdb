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

=item B<--endgame> (or B<--noendgame>)

Enable endgame mode, extra parallelism when finishing up.
Still in devleopment.
Off by default.

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
use threads::shared;
use Thread::Semaphore;
use Thread::Queue;

use IO::Pipe;
use Fsdb::Support::Pipe;

use Fsdb::Filter;
use Fsdb::Filter::dbmerge2;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;
use Fsdb::BoundedQueue;
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
    $self->{_endgame} = undef;
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
	'endgame!' => \$self->{_endgame},
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

=head2 _pretty_fn

    _pretty_fn($fn)

Internal: pretty-print a filename or Fsdb::BoundedQueue.

=cut

sub _pretty_fn {
    my($fn) = @_;
    return ref($fn) if (ref($fn));
    return $fn;
}

=head2 _remember_ipc_item

    $hint = $self->_remember_ipc_item($item)

Internal: return a string "hint" that can be passed through
threads safely, and later recovered via C<_recall_ipc_item($HINT)>.

(This nonsense is because a worker thread must send a pipe over
IPC to the main thread, but that generates "glob" errors.)

=cut

sub _remember_ipc_item($$) {
    my($self, $item) = @_;
    $self->{_work_memory_next_hint} = 0 if (!defined($self->{_work_memory_next_hint}));
    my $hint = "///hint" . $self->{_work_memory_next_hint}++;
    my $clean_item = $item;
    if (ref($clean_item) =~ /^IO::Pipe/) {
	# Hack:
	# Convert the pipe to a reader, so that we don't get excess open writers
	# that prevent EOF.
	# (Do this by hand, rather than via ->reader(),
	# so we don't distrurb either end.)
	$clean_item =  $clean_item->read_side();
    };
    $self->{_work_memory}{$hint} = $clean_item;
    return $hint;
}

=head2 _recall_ipc_item

    $token = $self->_recall_ipc_item($item)

Internal: recover a memorized work item.
Only works once.

=cut

sub _recall_ipc_item($$) {
    my($self, $hint) = @_;
    my $memory = $self->{_work_memory}{$hint};
    croak "dbmerge::_recall_ipc_item: forgotten $hint\n"
	if (!defined($memory));
    delete $self->{_work_memory}{$hint};
    return $memory;
}


=head2 segment_next_output

    $out = $self->segment_next_output($output_type)

Internal: return a Fsdb::IO::Writer as $OUT
that either points to our output or a temporary file, 
depending on how things are going.

The $OUTPUT_TYPE can be 'final' or 'ipc' or 'file'.

=cut

sub segment_next_output($$) {
    my ($self, $output_type) = @_;
    my $out;
    if ($output_type eq 'final') {
#        $self->finish_io_option('output', -clone => $self->{_two_ins}[0]);
#        $out = $self->{_out};
	$out = $self->{_output};   # will pass this to the dbmerge2 module
	print "# final output\n" if ($self->{_debug});
    } elsif ($output_type eq 'file') {
	# dump to a file for merging
	my $tmpfile = Fsdb::Support::NamedTmpfile::alloc($self->{_tmpdir});
	$self->{_files_cleanup}{$tmpfile} = 'NamedTmpfile';
	$out = $tmpfile;   # just return the name
    } elsif ($output_type eq 'ipc') {
	# endgame-mode: send stuff down in-memory queues
	# $out = shared_clone(new Fsdb::BoundedQueue);
	$out = new IO::Pipe;
    } else {
	die "internal error: dbmege.pm:segment_next_output bad output_type: $output_type\n";
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
    if (ref($file)) {
	if (ref($file) =~ /^IO::/) {
	    print "# closing IO::Pipe\n" if ($self->{_debug});
	    $file->close;
	} elsif (ref($file) eq 'Fsdb::BoundedQueue') {
	    # nothing to do
	} else {
	    die "internal error: unknown type in dbmerge::segment_cleanup\n";
	};
	return;
    };
    my($cleanup_type) = $self->{_files_cleanup}{$file};
    die "bad (empty) file in dbmerge::segment_cleanup\n"
	if (!defined($file));
    if (!defined($cleanup_type)) {
	print "# dbmerge: segment_cleanup:  no cleanup for " . _pretty_fn($file) . "\n" if ($self->{_debug});
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


=head2 segments_merge2_run

    $out = $self->segments_merge2_run($out_fn, $is_final_output, 
			$in0, $in1);


Internal: do the actual merge2 work (maybe our parent put us in a
thread, maybe not).

=cut
our $unique_id :shared;
sub segments_merge2_run($$$$$) {
    my($self, $out_fn, $is_final_output, $in0, $in1) = @_;
    my $id = $unique_id++;

    my @merge_options = qw(--autorun --nolog);
    push(@merge_options, '--noclose', '--saveoutput' => \$self->{_out})
	if ($is_final_output);

    my $debug_msg = '';
    if ($self->{_debug}) {
	$debug_msg = "(id $id) " . _pretty_fn($in0) . " with " . _pretty_fn($in1) . " to " . _pretty_fn($out_fn) . " " . ($is_final_output ? " (final)" : "") . " " . join(" ", @merge_options);
    };
    print "# segments_merge2_run: merge start $debug_msg\n"
	if ($self->{_debug});
    new Fsdb::Filter::dbmerge2(@merge_options,
			'--input' => $in0,
			'--input' => $in1,
			'--output' => $out_fn,
			@{$self->{_sort_argv}});
    print "# segments_merge2_run: merge finish $debug_msg\n"
	if ($self->{_debug});

    $self->segment_cleanup($in0);
    $self->segment_cleanup($in1);
    if (ref($out_fn) =~ /^IO::/ && !$is_final_output) {
        print "# segments_merge2_run: merge closing out " . ref($out_fn) . " h$debug_msg\n"
	    if ($self->{_debug});
	$out_fn->close;
    };
}

=head2 segments_merge2_prepare

    $out = $self->segments_merge2_prepare($go_sem, $filename, $force_start);

Internal: release a forked thread (bound by $GO_SEM).
Always if $FORCE_START, otherwise only if we have free parallelism.

Returns either the semaphore (if it's not started)
or the thread (if it is started).

=cut

sub segments_merge2_prepare($$$$) {
    my($self, $go_sem, $out_fn, $force_start) = @_;
    #
    # Note there's a race in our check of parallelism available
    # and letting it go.
    # It's not REALLY a race because only one thread releases threads,
    # and it doesn't matter because an extra active thread is not
    # the end of the world.
    #
    my($out_to_queue) = (ref($out_fn) =~ m@^(Fsdb::BoundedQueue|IO::)@) ? 1 : 0;
    $force_start = 1 if ($out_to_queue);
    if ($parallelism_available <= 0 && !$force_start) {
	print "# segments_merge2_prepare: merge thread for " . _pretty_fn($out_fn) . " postponed\n" if ($self->{_debug});
	return [$go_sem, $out_fn];
    };
    # send it off!
    if ($self->{_debug}) {
	my $reason = ($parallelism_available <= 0) ? 'forced' : 'free';
        print "# segments_merge2_prepare: merge thread for " . _pretty_fn($out_fn) . " started, $reason\n";
    };
    $go_sem->up() if (defined($go_sem));
    return [$out_to_queue, $out_fn];
}


=head2 enqueue_work

    $self->enqueue_work($depth, $work);

Internal: put $WORK on the queue at $DEPTH, updating the max count.

=cut

sub enqueue_work($$$) {
    my($self, $depth, $work) = @_;
    $self->{_work_max_files}[$depth] = 0 if (!defined($self->{_work_max_files}[$depth]));
    $self->{_work_max_files}[$depth]++;
    push(@{$self->{_work}[$depth]}, $work);
};

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

    print "# segments_merge_one_depth: scanning $depth\n" if ($self->{_debug});
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
	    $self->enqueue_work($depth + 1, shift @{$work_ref});
	    die "internal error\n" if ($#{$work_ref} != -1);
	    last;
	};
	# are they blocked?  force-start them if they are
	my $waiting_on_inputs = 0;
	foreach my $i (0..1) {
	    my($status, $fn) = ($work_ref->[$i][0], $work_ref->[$i][1]);
	    if (ref($status) eq 'Thread::Semaphore') {
		print "# segments_merge_one_depth: depth $depth forced start on $fn.\n" if ($self->{_debug});
		$self->segments_merge2_prepare($status, $fn, 1);
		$work_ref->[$i][0] = undef;   # note that it's started
	    } elsif ($status == 0) {
		print "# segments_merge_one_depth: depth $depth waiting on working " . _pretty_fn($fn) . ".\n" if ($self->{_debug});
		$waiting_on_inputs++;
	    } elsif ($status == 1) {
		# input is done
	    } else {
		die "interal error: unknown status $status\n";
	    };
	};
	# bail out if inputs are not done yet.
	return if ($waiting_on_inputs);

	# now we KNOW we do not have blocked work
	my(@two_fn) = (shift @${work_ref}, shift @${work_ref});
	my $output_type = 'file';
	if ($closed && $#{$work_ref} == -1 && $depth == $#{$self->{_work}}) {
	    $output_type = 'final';
	} elsif ($self->{_endgame} && $closed && $self->{_work_max_files}[$depth] <= $self->{_endgame_max_files}) {
	    # endgame mode currently disabled... its' 10x slower than writing to files (!)
	    print "# segments_merge_one_depth: endgame parallelism.\n" if ($self->{_debug});
	    $output_type = 'ipc';
	};
	my($out_fn) = $self->segment_next_output($output_type);
	print "# segments_merge_one_depth: depth $depth planning " . _pretty_fn($two_fn[0][1]) . " and " . _pretty_fn($two_fn[1][1]) . " to " . _pretty_fn($out_fn) . ".\n" if ($self->{_debug});

	foreach my $i (0..1) {
	    next if (ref($two_fn[$i][1]) =~ /^(Fsdb::BoundedQueue|IO::)/ || $two_fn[$i][1] eq '-');
	    croak $self->{_prog} . ": file $two_fn[$i][1] is missing.\n"
		if (! -f $two_fn[$i][1]);
	};

	if ($output_type eq 'final') {
		# last time: do it here, in-line
		# so that we update $self->{_out} in the main thread
		$self->segments_merge2_run($out_fn, 1, $two_fn[0][1], $two_fn[1][1]);
		# signal ourselves that we're really done
		$ipc->enqueue([-1, 'merge-final']);
		return;
	};
	#
	# fork a thread to do the merge
	#
	my $go_sem = Thread::Semaphore->new(0);
	my $out_fn_hint = $self->_remember_ipc_item($out_fn);
	my $merge_thread = threads->new(
	    sub {
		$go_sem->down();  # wait to be told to start
		$parallelism_available--;
		$self->segments_merge2_run($out_fn, 0, $two_fn[0][1], $two_fn[1][1]);
		sleep(1) if (defined($self->{_test}) && $self->{_test} eq 'delay-finish');
		$parallelism_available++;
		$ipc->enqueue([$depth+1, $out_fn_hint]);
		return $out_fn;
	    }
        );
	# Put the thread in our queue, and maybe run it.
	$merge_thread->detach();
 	$self->enqueue_work($depth + 1, $self->segments_merge2_prepare($go_sem, $out_fn, $output_type ne 'file'));
	print "# segments_merge_one_depth: looping after depth $depth planning " . _pretty_fn($two_fn[0][1]) . " and " . _pretty_fn($two_fn[1][1]) . " to " . _pretty_fn($out_fn) . ".\n" if ($self->{_debug});
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

=head3 Merging Strategy

Merging is done in a binary tree is managed through the C<_work> queue.
It has an array of C<depth> entries,
one for each level of the tree.

Items are processed in order at each level of the tree,
and only level-by-level, so the sort is stable.

=head3 Parallelism Model

Parallelism is also managed through the C<_work> queue,
each element of which consists of one file or stream suitable for merging.
The work queue contains both ready output (files or BoundedQueue streams)
that can be immediately handled, and pairs of semaphore/pending output
for work that is not yet started.
All manipulation of the work queue happens in the main thread
(with C<segments_merge_all> and C<segments_merge_one_depth>).

We start a thread to handle each item in the work queue,
and limit parallelism to the C<_max_parallelism>,
defaulting to the number of available processors.

There two two kinds of parallelism, regular and endgame.
For regular parallelism we pick two items off the work queue,
merge them, and put the result back on the queue as a new file.
Items in the work queue may not be ready.  For in-progress items we
wait until they are done.  For not-yet-started items
we start them, then wait until they are done.

Endgame parallelism handles the final stages of a large merge.
When there are enough processors that we can start a merge jobs
for all remaining levels of the merge tree.  At this point we switch
from merging to files to merging into C<Fsdb::BoundedQueue> pipelines
that connect merge processes which start and run concurrently.

The final merge is done in the main thread so that that the main thread
can handle the output stream and recording the merge action.

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
    # $ipc gets pairs [$depth, $item]
    # if $depth == -1: it's a special message and item is a stirng
    # if $depth == 0: item is a filename
    #  if $depth > 0: item is a _remember_ipc_item that wraps either a file or a pipe or bounded queue
    #
    for (;;) {
	# test for done by signal that is_final_output has run
#	# done?
#	my $deepest = $#{$self->{_work}};
#	last if ($self->{_work_closed}[$deepest] && $#{$self->{_work}[$deepest]} <= 0);

	#
	# First, fork off new threads, where possible.
	#
	# Go through this loop multiple times because closing the last depth
	# can actually allow work to start at the last+1 depth,
	# and in endgame mode we risk blocking (due to flow control)
	# if we don't start threads at all depths.
	#
	my $try_again = 1;
	while ($try_again) {
	    foreach my $depth (0..$#{$self->{_work}}) {
	        $self->segments_merge_one_depth($depth);
		$try_again = undef;
	        if ($#{$self->{_work}[$depth]} == -1 && $self->{_work_closed}[$depth]) {
		    # When one level is all in progress, we can close the next.
		    my $next_depth = $depth + 1;
		    if (!$self->{_work_closed}[$next_depth]) {
		        $self->{_work_closed}[$next_depth] = 1;
			$try_again = 1;
		        print "# segments_merge_all: closed work depth $next_depth\n" if ($self->{_debug});
		    };
		};
	    };
	};

	#
	# Next, handle stuff that's finished.
	# We do ONE thing, maybe blocking, then go back and see
	# if we can fork off more threads.
	#
	print "# segments_merge_all: blocking on ipc\n" if ($self->{_debug});
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
	croak "dbmerge: failed assert(work_depth!=-1)\n" if ($work_depth == -1);
	$work_fn = $self->_recall_ipc_item($work_fn)
	    if ($work_depth > 0);
	print "# segments_merge_all: new file for " . _pretty_fn($work_fn) . " at $work_depth\n" if ($self->{_debug});

	if ($work_depth == 0) {
	    # queue first level files here
	    $self->enqueue_work($work_depth, [1, $work_fn]);
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
		    my($status, $fn) = @{$self->{_work}[$depth][$i]};
		    croak "internal error: suprise that remembered hint leaked out.\n" if ($fn =~ m@^///hint@);
		    if (ref($status) eq 'Thread::Semaphore') {
			next if ($found_new);
			# release it
			print "# segments_merge_all: merge thread released, depth $depth, file $fn\n" if ($self->{_debug});
			$status->up();
			$self->{_work}[$depth][$i][0] = 0;
			$found_new = 1;
		    } elsif ($status == 0) {
			next if ($found_finished);
			next if ($fn ne $work_fn);
			print "# segments_merge_all: merge thread for $depth, $i reports $fn is done\n" if ($self->{_debug});
			$self->{_work}[$depth][$i][0] = 1;
			$found_finished = 1;
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
    # 1. [1, filenames] that for completed files need to be merged.
    # 2. [$go_sem, filename] for blocked threads that, when started, will go to filename.
    # 3. [0, filename] for files still being computed.
    #
    # Filename can be an Fsdb::BoundedQueue or IO::Pipe objects for endgame mode threads
    #
    # _work_closed is set when that depth is no longer growing;
    # at that time _work_depth_files is the maximum number of files there.
    #
    # Put stuff on it with $self->enqueue_work to keep the 
    # related variables correct.
    #
    $self->{_work}[0] = [];
    $self->{_work_closed}[0] = 0;
    $self->{_work_depth_files}[0] = 0;
    if (!$self->{_xargs}) {
	foreach (@{$self->{_inputs}}) {
	    $self->enqueue_work(0, [1, $_]);
	};
	$self->{_work_closed}[0] = 1;
    };
    #
    # control parallelism
    #
    # For the endgame, we overcommit by a large factor
    # because in the merge tree many become blocked on the IO pipeline.
    #
    if (!defined($self->{_max_parallelism})) {
	$self->{_max_parallelism} = Fsdb::Support::OS::max_parallelism();
    };
    my $viable_endgame_processes = $self->{_max_parallelism};
    my $files_to_merge = 1;
    while ($viable_endgame_processes > 0) {
	$viable_endgame_processes -= $files_to_merge;
	$files_to_merge *= 2;
    };
    $self->{_endgame_max_files} = int($files_to_merge / 2);
    STDOUT->autoflush(1) if ($self->{_debug});
    print "# dbmerge: endgame_max_files: " . $self->{_endgame_max_files} . "\n" if($self->{_debug});
    if (!defined($parallelism_available)) {
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
