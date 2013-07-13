#!/usr/bin/perl -w

#
# dbmapreduce.pm
# Copyright (C) 1991-2011 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#


package Fsdb::Filter::dbmapreduce;

=head1 NAME

dbmapreduce - reduce all input rows with the same key

=head1 SYNOPSIS

    dbmapreduce [-dMS] [-k KeyField] [-f CodeFile] [-C filter-code] [ReduceCommand [ReduceArguments...]]

=head1 DESCRIPTION

Group input data by KeyField,
then reduce each group.
The reduce function can be an external program
given by ReduceCommand and ReduceArguments,
or an Perl subroutine given by CODE.

This program thus implements Google-style map/reduce,
but executed sequentially.
With the C<-M> option, the reducer is given multiple groups
(as with Google), but not in any guaranteed order
(while Google guarantees they arive in lexically sorted order).
However without it, a stronger invariant
is provided: each reducer is given exactly one group.

By default the KeyField is the first field in the row.
Unlike Hadoop streaming, the -k KeyField option can explicitly
name where the key is in any column of each input row.

The KeyField used for each Reduce is added to the beginning of each 
row of reduce output.

The reduce function can do anything, emitting one or more output records.
However, it must have the same output field separator as the input data.
(In some cases this case may not occur, for example, input data with -FS
and a reduce function of dbstats.  This bug needs to be fixed in the future.)

Reduce functions default to be shell commands.
However, with C<-C>, one can use arbitrary Perl code
(see the C<-C> option below for details).
the C<-f> option is useful to specify complex Perl code
somewhere other than the command line.

Finally, as a special case, if there are no rows of input,
the reducer will be invoked once with the empty value (if it's an external 
reducer) or with undef (if it's a subroutine).
It is expected to generate the output header,
and it may generate no data rows itself, or a null data row
of its choosing.


Assumptions and requirements:	

By default, data can be provided in arbitrary order
and the program consumes O(number of unique tags) memory,
and O(size of data) disk space.

With the -S option, data must arrive group by tags (not necessarily sorted),
and the program consumes O(number of tags) memory and no disk space.
The program will check and abort if this precondition is not met.

With two -S's, program consumes O(1) memory, but doesn't verify
that the data-arrival precondition is met.

Although painful internally,
the field seperators of the input and the output
can be different.
(Early versions of this tool prohibited such variation.)


=head1 OPTIONS

=over 4

=item B<-k> or B<--key> KeyField

specify which column is the key for grouping (default: the first column)

=item B<-S> or B<--pre-sorted>

Assume data is already grouped by tag.
Provided twice, it removes the validiation of this assertion.

=item B<-M> or B<--multiple-ok>

Assume the ReduceCommand can handle multiple grouped keys,
and the ReduceCommand is responsible for outputting the  
with each output row.
(By default, a separate ReduceCommand is run for each key,
and dbmapreduce adds the key to each output row.)

=item B<-K> or B<--pass-current-key>

Pass the current key as an argument to the external,
non-map-aware ReduceCommand.
This is only done optionally since some external commands 
do not expect an extra argument.
(Internal, non-map-aware Perl reducers are always given 
the current key as an argument.)

=item B<-C FILTER-CODE> or B<--filter-code=FILTER-CODE>

Provide FILTER-CODE, Perl code that generates and returns
a Fsdb::Filter object that implements the reduce function.
The provided code should be an anonymous sub
that creates a Fsdb Filter that implements the reduce object.

The reduce object will then be called with --input and --output
paramters that hook it into a the reduce with queues.

One sample fragment that works is just:

    dbcolstats(qw(--nolog duration))

So this command:

    cat DATA/stats.fsdb | \
	dbmapreduce -k experiment -C 'dbcolstats(qw(--nolog duration))'

is the same as the example

    cat DATA/stats.fsdb | \
	dbmapreduce -k experiment dbcolstats duration

except that with C<-C> there is no forking and so things run faster.

If C<dbmapreduce> is invoked from within Perl, then one can use
a code SUB as well:
    dbmapreduce(-k => 'experiment', 
	-C => sub { dbcolstats(qw(--nolong duration)) }); 

The reduce object must consume I<all> input as a Fsdb stream,
and close the output Fsdb stream.  (If this assumption is not
met the map/reduce will be aborted.)

For non-map-reduce-aware filters,
when the filter-generator code runs, C<@_[0]> will be the current key.

=item B<-f CODE-FILE> or B<--code-file=CODE-FILE>

Includes F<CODE-FILE> in the program.
This option is useful for more complicated perl reducer functions.

Thus, if reducer.pl has the code.

    sub make_reducer {
	my($current_key) = @_;
	dbcolstats(qw(--nolog duration));
    }

Then the command

    cat DATA/stats.fsdb | dbmapreduce -k experiment -f reducer.pl -C make_reducer

does the same thing as the example.


=item B<-w> or B<--warnings>

Enable warnings in user supplied code.
Warnings are issued if an external reducer fails to consume all input.
(Default to include warnings.)

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

    #fsdb experiment duration
    ufs_mab_sys 37.2
    ufs_mab_sys 37.3
    ufs_rcp_real 264.5
    ufs_rcp_real 277.9

=head2 Command:

    cat DATA/stats.fsdb | dbmapreduce -k experiment dbcolstats duration

=head2 Output:

    #fsdb      experiment      mean    stddev  pct_rsd conf_range      conf_low       conf_high        conf_pct        sum     sum_squared     min     max     n
    ufs_mab_sys     37.25 0.070711 0.18983 0.6353 36.615 37.885 0.95 74.5 2775.1 37.2 37.3 2
    ufs_rcp_real    271.2 9.4752 3.4938 85.13 186.07 356.33 0.95 542.4 1.4719e+05 264.5 277.9 2
    #  | dbmapreduce -k experiment dbstats duration


=head1 SEE ALSO

L<Fsdb>.
L<dbmultistats>
L<dbrowsplituniq>


=head1 CLASS FUNCTIONS

A few notes about the internal structure:
L<dbmapreduce> uses two to four threads to run.
An optional thread C<$self->{_in_thread}> sorts the input.
The main thread then reads input and groups input by key.
Each group is passed to a
secondary thread C<$self->{_reducer_thread}>
that invokes the reducer on each group
and does any ouptut.
If the reducer is I<not> map-aware, then
we create a final postprocessor thread that 
adds the key back to the output.
Either the reducer or the postprocessor thread do output.

=cut

@ISA = qw(Fsdb::Filter);
$VERSION = 2.0;

require 5.010;  # sigh, threads in 5.008 are too primitive for The Brave New World
use strict;
use Pod::Usage;
use threads;
use threads::shared;
use Carp;

use Fsdb::Filter;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;
use Fsdb::Filter::dbsubprocess;
use Fsdb::Filter::dbpipeline qw(dbpipeline_filter dbpipeline_open2 dbsort dbsubprocess);

my $REDUCER_GROUP_SYNCHRONIZATION_FLAG = 'reducer group synchronization flag';

=head2 new

    $filter = new Fsdb::Filter::dbmapreduce(@arguments);

Create a new dbmapreduce object, taking command-line arguments.

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
    $self->{_key_column} = undef;
    $self->{_pre_sorted} = 0;
    $self->{_filter_generator_code} = undef;
    $self->{_reduce_generator} = undef;
    $self->{_reducer_is_multikey_aware} = undef;
    $self->{_external_command_argv} = [];
    $self->{_pass_current_key} = undef;
    $self->{_filter_generator_code} = undef;
    $self->{_code_files} = [];
    $self->{_warnings} = 1;
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
	'C|filter-code|code=s' => \$self->{_filter_generator_code},
	'close!' => \$self->{_close},
	'd|debug+' => \$self->{_debug},
	'f|code-files=s@' => $self->{_code_files},
	'i|input=s' => sub { $self->parse_io_option('input', @_); },
	'k|key=s' => \$self->{_key_column},
	'K|pass-current-key!' => \$self->{_pass_current_key},
	'log!' => \$self->{_logprog},
	'M|multiple-ok!' => \$self->{_reducer_is_multikey_aware},
	'o|output=s' => sub { $self->parse_io_option('output', @_); },
	'S|pre-sorted+' => \$self->{_pre_sorted},
	'saveoutput=s' => \$self->{_save_output},
        'w|warnings!' => \$self->{_warnings},
	) or pod2usage(2);
    push (@{$self->{_external_command_argv}}, @argv);
}

=head2 setup

    $filter->setup();

Internal: setup, parse headers.

=cut

sub setup ($) {
    my($self) = @_;

    my $included_code = '';
    #
    # get any extra code
    #
    foreach my $code_file (@{$self->{_code_files}}) {
	open(IN, "< $code_file") or croak $self->{_prog} . ": cannot read code from $code_file\n";
	$included_code .= join('', <IN>);
	close IN;
    };

    #
    # what are we running?
    #
    # Figure it out, and generate a 
    # $self->{_reducer_generator_sub} that creates a
    # filter object that will be passed to dbpipeline_open2
    # to reduce one (or many, if map_aware_reducer) keys.
    #
    if ($#{$self->{_external_command_argv}} >= 0) {
	# external command
	my @argv = @{$self->{_external_command_argv}};
	my $empty = $self->{_empty};
	unshift @argv, ($self->{_warnings} ? '--warnings' : '--nowarnings');
	my $reducer_sub;
	if ($self->{_pass_current_key}) {
	    $reducer_sub = sub { $_[0] = $empty if (!defined($_[0])); dbsubprocess('--nolog', @argv, @_); };
	} else {
	    $reducer_sub = sub { dbsubprocess('--nolog', @argv); };
	};
	$self->{_reducer_generator_sub} = $reducer_sub;
    } elsif (defined($self->{_filter_generator_code})) { 
	my $reducer_sub;
	if (ref($self->{_filter_generator_code}) eq 'CODE') {
	    print "direct code assignment for reducer sub\n" if ($self->{_debug});
	    $reducer_sub = $self->{_filter_generator_code};
	} else {
	    my $sub_code;
	    $sub_code = 
		"use Fsdb::Filter::dbpipeline qw(:all);\n" . 
		$included_code . 
		'$reducer_sub = sub {' . "\n" .
		$self->{_filter_generator_code} . 
		"\n\t;\n};\n";
	    print "$sub_code" if ($self->{_debug});
	    eval $sub_code;
	    $@ && croak $self->{_prog} . ": error evaluating user-provided reducer sub:\n$sub_code\nerror is: $@.\n";
	};
	$self->{_reducer_generator_sub} = $reducer_sub;
    } else {
	croak $self->{_prog} . ": reducer not specified.\n";
    };

    # do we need to group the keys for the user?
    if ($self->{_pre_sorted}) {
	$self->finish_io_option('input', -comment_handler => $self->create_tolerant_pass_comments_sub('_to_reducer_writer'));
    } else {
	# not pre-sorted, so do lexical sort
	my $sort_column = defined($self->{_key_column}) ? $self->{_key_column} : '0';
        my($new_reader, $new_thread) = dbpipeline_filter(
		$self->{_input},
		[-comment_handler => $self->create_tolerant_pass_comments_sub('_to_reducer_writer')],
		dbsort($sort_column));
	$self->{_pre_sorted_input} = $self->{_input};
	$self->{_in} = $new_reader;
	$self->{_in_thread} = $new_thread;
	#
	# We detach the sorter here.
	#
	# This is OK, sort of, but seems broken.
	# We do it because otherwise we block joining the thread
	# at the end of $self->run.
	# We shouldn't, because the sorter should always terminate.
	# And we KNOW it's run it's finish(), because it must have
	# generated EOF.
	#
	# So this may indicate a bug in sort or dbpipline_filter
	# with a lingering thread.
	# But to debug later, as detaching seems like a plausible work-around
	# and not TOO evil.
	#
	$new_thread->detach;
    };

    #
    # figure out key column's name, now that we've done setup
    #
    if (defined($self->{_key_column})) {
	$self->{_key_coli} = $self->{_in}->col_to_i($self->{_key_column});
	croak $self->{_prog} . ": key column " . $self->{_key_column} . " is not in input stream.\n"
	    if (!defined($self->{_key_coli}));
    } else {
	# default to first column
	$self->{_key_coli} = 0;
	$self->{_key_column} = $self->{_in}->i_to_col(0);
    };

    #
    # setup the postprocessing thread
    #
    $self->_setup_reducer;

    $self->{_reducer_invocation_count} = 0;
    $SIG{'PIPE'} = sub {
	# xxx: MacOS 10.6.3 causes failures here,
	# even though everything seems to run.
	# As a hack, suppress this message:
	exit 0 if ($^O eq 'darwin');
        die $self->{_prog} . ": cannot run external dbmapreduce reduce program (" . join(" ", @{$self->{_external_command_argv}}) . ")\n";
    };

}

=head2 _setup_reducer

    _setup_reducer

(internal)  One thread that runs the reducer thread and produces output.
C<_reducer_queue> is sends the new key,
then a Fsdb stream, then EOF (undef)
for each group.
We setup the output, suppress all but the first header,
and add in the keys if necessary.

=cut

sub _setup_reducer {
    my($self) = @_;

    $self->{_reducer_queue} = new Fsdb::BoundedQueue;
    my $reducer_thread = threads->new( 
	    $self->{_reducer_is_multikey_aware} ? sub {
		    $self->_multikey_aware_reducer();
		} : sub {
		    $self->_multikey_ignorant_reducer();
		});
    $self->{_reducer_thread} = $reducer_thread;
}

=head2 _multikey_aware_reducer 

    _multikey_aware_reducer 

Handle a map-aware reduce process.  We assume the caller
suppresses signaling about key transition, 
so we just run the user's reducer in our thread.

=cut

sub _multikey_aware_reducer {
    my($self) = @_;

    my $from_preprocessor = $self->{_reducer_queue};
    my $write_fastpath_sub = undef;

    my $sync_marker;
    my $current_key = undef;
    my $current_reducer = undef;

    print STDERR "dbmapreduce: _multikey_aware_reducer master thread started\n" if ($self->{_debug} >= 2);

    while ($sync_marker = $from_preprocessor->dequeue) {
	last if (!defined($sync_marker));
	croak "dbmapreduce: lost synchronization in queue (perhaps reducer didn't consume data until end-of-file?)\n"
	    if ($sync_marker ne $REDUCER_GROUP_SYNCHRONIZATION_FLAG);
	$current_key = $from_preprocessor->dequeue;

	#
	# Ok, we got a key (maybe null if no input rows).
	#  The rest of the queue
	# will have fsdb objects until undef.
	#
	print STDERR "dbmapreduce: _multikey_aware_reducer on " . $self->_key_to_string($current_key) . "\n" if ($self->{_debug} >= 2);

	# setup the reducer, if necessary
	if (!defined($current_reducer)) {
	    print STDERR "dbmapreduce: _multikey_aware_reducer making reducer\n" if ($self->{_debug} >= 2);

	    my $users_reducer = &{$self->{_reducer_generator_sub}}();
	    # hook input queue directly to user's reducer
	    # hook output directly to our output
	    # Who hoo... an easy case.

	    # just like in dbpipeline, we have to setup output manually,
	    # and use noclose so we can log our own final bit.
	    $users_reducer->parse_options('--input' => $from_preprocessor,
		    '--output' => $self->{_output},
		    '--saveoutput' => \$self->{_out},
		    '--noclose');

	    # run!  (in the current thread)
	    $users_reducer->setup_run_finish;
	} else {
	    # we assume the caller supresses multiple key signalling
	    croak $self->{_prog} . ": interal error _multikey_aware_reducer called twice.\n";
        };
    };

    $self->{_out}->write_comment("mapreduce reducer: done with all reducers.\n")
	    if ($self->{_debug});
    # Done with all reducers.
    # Finish up the IO.
    $self->SUPER::finish();
}


=head2 _multikey_ignorant_reducer 

    _multikey_ignorant_reducer 

Handle a map-ignorant reduce process. 
We handle multiple keys.
We create a postprocessor thread to add the key back in to
the output and do our finish().
We then become a middle-process, just handling key transitions
and invoking new reducers.

=cut

sub _multikey_ignorant_reducer {
    my($self) = @_;

    my $from_preprocessor = $self->{_reducer_queue};
    my $reducer_is_multikey_aware = $self->{_reducer_is_multikey_aware};

    my $sync_marker;
    my $current_key = undef;
    my $current_reducer = undef;

    print STDERR "dbmapreduce: _multikey_ignorant_reducer master thread started\n" if ($self->{_debug} >= 2);

    #
    # Make a post-processor.
    #
    # Need a queue to talk to them.
    # Protocol is the same as with us (key, header+data, undef)*,
    # except they get final data.
    #
    my $postprocessor_thread = undef;
    my $to_postprocessor = undef;
    $self->{_postprocessor_queue} = $to_postprocessor = new Fsdb::BoundedQueue;
    $self->{_postprocessor_thread} = $postprocessor_thread = threads->new( sub {
	    $self->_multikey_ignorant_postprocessor;
	});

    while ($sync_marker = $from_preprocessor->dequeue) {
	last if (!defined($sync_marker));
	croak "dbmapreduce: lost synchronization in queue (perhaps reducer didn't consume data until end-of-file?)\n"
	    if ($sync_marker ne $REDUCER_GROUP_SYNCHRONIZATION_FLAG);
	$current_key = $from_preprocessor->dequeue;

	#
	# Ok, we got a key (possibly null if no input rows at all).
	# The rest of the queue
	# will have fsdb objects until undef.
	#

	# Tell the postprocessor what's coming.
	$to_postprocessor->enqueue($REDUCER_GROUP_SYNCHRONIZATION_FLAG);
	$to_postprocessor->enqueue($current_key);

	# Run the reducer,
	# hooked up to the postprocessor.
	print STDERR "dbmapreduce: _multikey_ignorant_reducer making reducer for " . $self->_key_to_string($current_key) . "\n" if ($self->{_debug} >= 2);
	my $users_reducer = &{$self->{_reducer_generator_sub}}($current_key);
	# just like in dbpipeline, we have to setup output manually,
	# Note that here we close (implicitly) when we're done.
	$users_reducer->parse_options('--input' => $from_preprocessor,
		    '--output' => $to_postprocessor);

        # run!  (in the current thread)
	$users_reducer->setup_run_finish;
	print STDERR "dbmapreduce: _multikey_ignorant_reducer reducer " . $self->_key_to_string($current_key) . " done\n" if ($self->{_debug} >= 2);

	# Loop back and get another group.
    };

    # if we're really done, 
    # the tell the postprocessor we're done and
    # then just wait until the postprocessor is done
    # (and the main thread waits until we're done)
    #
    # (Note: there may be no postprocessor if there was no input data.)
    $to_postprocessor->enqueue(undef);   # done done
    $postprocessor_thread->join;
    if (my $error = $postprocessor_thread->error()) {
	die $self->{_prog} . ": postprocessor thread erred: $error";
    };
}


=head2 _multikey_ignorant_postprocessor 

    _multikey_ignorant_postprocessor 

Post-process a map-ignorant reduce process. 
Add back in our key to each output.
The one scary bit is we reuse the reducer to postprocessor queue
past our EOF signal (undef).

=cut

sub _multikey_ignorant_postprocessor {
    my($self) = @_;

    my $from_reducer_queue = $self->{_postprocessor_queue};

    my $sync_marker;
    my $current_key = undef;
    my $write_fastpath_sub = undef;
    my $key_coli = undef;
    my $reducer_returns_key_column = undef;

    print STDERR "dbmapreduce: _multikey_ignorant_postprocessor started\n" if ($self->{_debug} >= 2);

    while ($sync_marker = $from_reducer_queue->dequeue) {
	last if (!defined($sync_marker));
	croak "dbmapreduce: lost synchronization in queue (perhaps reducer didn't consume data until end-of-file?)\n"
	    if ($sync_marker ne $REDUCER_GROUP_SYNCHRONIZATION_FLAG);
	$current_key = $from_reducer_queue->dequeue;

	print STDERR "dbmapreduce: _multikey_ignorant_postprocessor on " . $self->_key_to_string($current_key) . "\n" if ($self->{_debug} >= 2);

	#
	# build a reader around our queue.
	# (Creepy---we actually reuse the queue past the eof signal.)
	#
	my $from_reducer_fsdb = new Fsdb::IO::Reader(-queue => $from_reducer_queue);
	my $fscode_alter = undef;

	if (!defined($write_fastpath_sub)) {
	    # first time, so set up write process
	    # xxx: this probably fails if we get zero input---should catch that somewhere much earlier.
	    # xxx: no, we assume the reducer always will give us something.

	    # Does the reducer give us our key back?
	    # If so, take it, but verify.
	    $key_coli = $from_reducer_fsdb->col_to_i($self->{_key_column});
	    $reducer_returns_key_column = defined($key_coli);
	    my @output_cols;

	    if ($reducer_returns_key_column) {
		# just pass back what we're given
		@output_cols = @{$from_reducer_fsdb->cols};
	    } else {
		# add in our key at the front (ick, hacky)
		@output_cols = @{$from_reducer_fsdb->cols};
		unshift(@output_cols, $self->{_key_column});
	    };

	    $self->finish_io_option('output', -clone => $from_reducer_fsdb, -cols => \@output_cols, -fscode => $self->{_in}->fscode());
	    $write_fastpath_sub = $self->{_out}->fastpath_sub();
	    # Save the first reader object forever
	    # so we can check for compatibility	
	    # with future reducers.
	    $self->{_first_reducer_reader} = $from_reducer_fsdb;
	    #
	    # Verify our output is compatible with our input.
	    # Without this check, one can have -F S input with spaces
	    # in the key, and -F D output with single spaces,
	    # and so the output file is not properly formatted.
	    #
	    # If it's incompatible, we do our best to fix it up.
	    #
	    if ($self->{_in}->fscode() ne $from_reducer_fsdb->fscode()) {
		$fscode_alter = 1;
		# carp $self->{_prog} . ": input and reducer outputs do not have compatible fscodes (they are " . $self->{_in}->fscode() . " and " . $from_reducer_fsdb->fscode() . ")\n"
	    };
	} else {
	    print STDERR "dbmapreduce: _multikey_ignorant_reducer reusing writer\n" if ($self->{_debug} >= 2);
	    # Writer already existed, BUT out of paranoia we check
	    # that other reducers don't change the schema.
	    croak $self->{_prog} . ": reducers have non-identical output schema.\n"
		if ($self->{_first_reducer_reader}->compare($from_reducer_fsdb) ne 'identical');
	};

	# Log something if we're debugging
	my $out = $self->{_out};
	$out->write_comment("dbmapreduce: _multikey_ignorant_reducer new reducer instance on key: " . $self->_key_to_string($current_key))
	    if ($self->{_debug});

	# The reducer is now sending us stuff.
	my $fref;
	my $read_fastpath_sub = $from_reducer_fsdb->fastpath_sub();
	my $string_current_key = ($reducer_returns_key_column ? undef : $self->_key_to_string($current_key));
	my $loop_code =  q'
	    while ($fref = &$read_fastpath_sub()) {
	    ' .
		(!$reducer_returns_key_column ? 'unshift (@$fref, $string_current_key);' : '') . # if necessary, add in the key
		($fscode_alter ? '$out->correct_fref_containing_fs($fref);' : '') .   # or if necessary, check and fix fscode problems
	    '
		&$write_fastpath_sub($fref);
	    };
	';
	eval $loop_code;
	$@ && die $self->{_prog} . ":  internal eval error: $@.\n";
	$self->{_out}->write_comment("dbmapreduce: _multikey_ignorant_reducer done with non-map-aware reducer on key: " . $self->_key_to_string($current_key))
	    if ($self->{_debug});

	# Loop back and do next group.
    };

    #
    # We're now all done with data, so clean up.
    #
    croak $self->{_prog} . ": internal error: never opened output.\n"
	if (!defined($write_fastpath_sub));

    $self->{_out}->write_comment("dbmapreduce: _multikey_ignorant_reducer done with all reducer groups.\n")
	if ($self->{_debug});
    # Done with all reducers.
    # Finish up the IO.
    $self->SUPER::finish();
}


=head2 _open_new_key

    _open_new_key

(internal)

=cut

sub _open_new_key {
    my($self, $new_key) = @_;

    print STDERR "dbmapreduce: _open_new_key on " . $self->_key_to_string($new_key) . "\n" if ($self->{_debug} >= 2);

    $self->{_current_key} = $new_key;

    # If already running and can handle multiple tags, just keep going.
    if ($self->{_reducer_is_multikey_aware}) {
	return if (defined($self->{_current_reducer_fastpath_sub}));
	die "reducer_thread not started, and not our job to start it.\n"
	    if (!defined($self->{_reducer_thread}));
	# fall through to setup fastpath
    };

    #
    # make the reducer
    #

    # tell our reducer thread it's coming:
    $self->{_reducer_queue}->enqueue($REDUCER_GROUP_SYNCHRONIZATION_FLAG);
    $self->{_reducer_queue}->enqueue($new_key);

    # now, set up our output to go to the reducer
    my($to_reducer_writer) = new Fsdb::IO::Writer(-queue => $self->{_reducer_queue}, -clone => $self->{_in});
    $self->{_to_reducer_writer} = $to_reducer_writer;
    $self->{_current_reducer_fastpath_sub} = $to_reducer_writer->fastpath_sub();
}

=head2 _close_old_key

    _close_old_key

Internal, finish a tag.

=cut

sub _close_old_key {
    my($self, $key, $final) = @_;

    print STDERR "dbmapreduce: _close_old_key on " . $self->_key_to_string($key) . "\n" if ($self->{_debug} >= 2);

    if (!defined($key)) {
	croak $self->{_prog} . ": internal error: _close_old_key called on non-final null-key.\n"
	    if (!$final);
    };
    return if ($self->{_reducer_is_multikey_aware} && !$final);  # can keep handling them in the reducer

    croak $self->{_prog} . ": internal error: current key doesn't equal prior key " . $self->_key_to_string($self->{_current_key}) . " != key " . $self->_key_to_string($key) . "\n"
	if (defined($key) && $self->{_current_key} ne $key);
    # finish the reducer
    print STDERR "dbmapreduce: _close_old_key closing current reducer group\n" if ($self->{_debug} >= 2);
    $self->{_to_reducer_writer}->close;

    # tell the reducer thread we're really done (if we are)
    $self->{_reducer_queue}->enqueue(undef)
	if ($final);
}

=head2 _key_to_string

    $self->_key_to_string($key)

Convert a key (maybe undef) to a string for status messages.

=cut

sub _key_to_string($$) {
    my($self, $key) = @_;
    return defined($key) ? $key  : $self->{_empty};
}

=head2 run

    $filter->run();

Internal: run over each rows.

=cut
sub run ($) {
    my($self) = @_;

    my $read_fastpath_sub = $self->{_in}->fastpath_sub();
    my $reducer_fastpath_sub = undef;

    # read data
    my($last_key) = undef;
    my $fref;
    my $key_coli = $self->{_key_coli};
    $self->{_key_counts} = {};
    my $nrows = 0;
    my $debug = $self->{_debug};
    while ($fref = &$read_fastpath_sub()) {
	# print STDERR "data line: " . join("  ", @$fref) . "\n";
        my($key) = $fref->[$key_coli];
# next block is removed because it fails in perl-5.8
# with Bareword "threads::all" not allowed while "strict subs"
# (that's a 5.10-ism, apparently.)
#	if ($nrows++ % 100 == 0 && $debug >= 3) {
#	    # dump thread stats
#	    print STDERR "# dbmapreduce thread stats: " .
#		"all: " . scalar(threads->list(threads::all)) . ", " .
#		"running: " . scalar(threads->list(threads::running)) . ", " .
#		"joinable: " . scalar(threads->list(threads::joinable)) . "\n";
#	};
    
        if (!defined($last_key) || $key ne $last_key) {
            # start a new one
            # check for out-of-order duplicates
            if ($self->{_pre_sorted} == 1) {
                croak $self->{_prog} . ": single key ``$key'' split into multiple groups, selection of -S was invalid\n"
                    if (defined($self->{_key_counts}{$key}));
                $self->{_key_counts}{$key} = 1;
            };
            # finish off old one?
            if (defined($last_key)) {
                $self->_close_old_key($last_key);
            };
            $self->_open_new_key($key);
            $last_key = $key;
	    $reducer_fastpath_sub = $self->{_current_reducer_fastpath_sub};
	    die "no reducer\n" if (!defined($reducer_fastpath_sub));
        };
        # pass the data to be reduced
	&{$reducer_fastpath_sub}($fref);
    };

    if (!defined($last_key)) {
	# no input data, so write a single null key
        $self->_open_new_key(undef);
    };

    # print STDERR "done with input, last_key=$last_key\n";
    # close out any pending processing? (use the force option)
    $self->_close_old_key($last_key, 1);

    # we join the thread in finish
}

=head2 finish

    $filter->finish();

Internal: write trailer.

=cut

sub finish ($) {
    my($self) = @_;

    # Let the reducer and postprocessor threads cleanup things.
    # It holds the output handle and calls the REAL finish.
    #
    # We just join on them to make sure we don't terminate the 
    # main thread prematurely.
    #
    # (no open _out, so must print to stderr!)
    print STDERR "# mapreduce main: join on postprocess thread\n"
	if ($self->{_debug});
    $self->{_reducer_thread}->join;
    if (my $error = $self->{_reducer_thread}->error()) {
	die $self->{_prog} . ": reducer thread erred: $error";
    };
}


=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2011 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;
