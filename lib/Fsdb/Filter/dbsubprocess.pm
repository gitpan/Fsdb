#!/usr/bin/perl -w

#
# dbsubprocess.pm
# Copyright (C) 1991-2008 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#

package Fsdb::Filter::dbsubprocess;

=head1 NAME

dbsubprocess - invoke a subprocess as a Fsdb filter object

=head1 SYNOPSIS

dbsubprocess program [arguments...]

=head1 DESCRIPTION

Run PROGRAM as a process, with optional ARGUMENTS as program arguments,
feeding its standard input and standard output
as fsdb streams.

This program is primarily for internal use by dbmapreduce.

Like L<dbpipeline>, L<dbsubprocess> program does have a 
Unix command; instead it is used only from within Perl.

=head1 OPTIONS

=over 4

=item B<-w> or B<--warnings>

Enable warnings in user supplied code.
(Default to include warnings.)

=back

=for comment
begin_standard_fsdb_options

and the standard fsdb options:

=over 4

=item B<-d>

Enable debugging output.

=item B<-i> or B<--input> InputSource

Read from InputSource, typically a file, or - for standard input,
or (if in Perl) a IO::Handle, Fsdb::IO or Fsdb::BoundedQueue objects.

=item B<-o> or B<--output> OutputDestination

Write to OutputDestination, typically a file, or - for standard output,
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

	#fsdb name id test1
	a	1	80
	b	2	70
	c	3	65
	d	4	90
	e	5	70
	f	6	90

=head2 Command:

the following perl code:

    use Fsdb::Filter::dbsubprocess;
    my $f = new Fsdb::Filter::dbsubprocess(qw(cat));
    $f->setup_run_finish;
    exit 0;

=head2 Output:

	#fsdb name id test1
	a	1	80
	b	2	70
	c	3	65
	d	4	90
	e	5	70
	f	6	90
	#   | dbsubprocess cat

=head1 SEE ALSO

L<dbpipeline(1)>,
L<Fsdb(3)>

=head1 CLASS FUNCTIONS

=cut

@ISA = qw(Fsdb::Filter);
($VERSION) = 2.0;

use strict;
use Pod::Usage;
# use IPC::Open2;
use Carp;

use Fsdb::Filter;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;


=head2 new

    $filter = new Fsdb::Filter::dbsubprocess(@arguments);

Create a new dbsubprocess object, taking command-line arugments.

=cut

sub new {
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
    $self->{_external_command_argv} = [];
    $self->{_warnings} = 1;
}

=head2 parse_options

    $filter->parse_options(@ARGV);

Internal: parse options

=cut

sub parse_options ($@) {
    my $self = shift @_;

    my(@argv) = @_;
    $self->get_options(
	\@argv,
 	'help|?' => sub { pod2usage(1); },
	'man' => sub { pod2usage(-verbose => 2); },
	'autorun!' => \$self->{_autorun},
	'close!' => \$self->{_close},
	'd|debug+' => \$self->{_debug},
	'i|input=s' => sub { $self->parse_io_option('input', @_); },
	'log!' => \$self->{_logprog},
	'o|output=s' => sub { $self->parse_io_option('output', @_); },
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

    croak $self->{_prog} . ": no program given.\n"
        if ($#{$self->{_external_command_argv}} < 0);

    $self->finish_io_option('input', -comment_handler => $self->create_pass_comments_sub('_to_subproc_out'));
}

=head2 _subproc_preprocessor

    $filter->_subproc_preprocessor

(internal)
Send output to the subprocess.

=cut

sub _subproc_preprocessor ($) {
    my($self) = @_;

    $self->{_from_subproc_fh}->close;
    delete $self->{_from_subproc_fh};

    #
    # Here we're going to send the subproc stuff (as text).
    #
    my $read_fastpath_sub = $self->{_in}->fastpath_sub();
    $self->{_to_subproc_out} = new Fsdb::IO::Writer(-fh => $self->{_to_subproc_fh}, -clone => $self->{_in});
    my $to_subproc_fastpath_sub = $self->{_to_subproc_out}->fastpath_sub();
    my $fref;
    while ($fref = &$read_fastpath_sub()) {
	&$to_subproc_fastpath_sub($fref);
    };
    $self->{_to_subproc_out}->close;
    $self->{_to_subproc_fh}->close;
    delete $self->{_to_subproc_fh};
}

=head2 _subproc_postprocessor

    $filter->_subproc_postprocessor

(internal)
Read output from the subprocess and convert back to a fsdb stream.

=cut

sub _subproc_postprocessor ($) {
    my($self) = @_;

    $self->{_from_subproc_fsdb} =  new Fsdb::IO::Reader(-fh => $self->{_from_subproc_fh}, -comment_handler => $self->create_pass_comments_sub);
    $self->finish_io_option('output', -clone => $self->{_from_subproc_fsdb});

    my $from_subproc_fastpath_sub = $self->{_from_subproc_fsdb}->fastpath_sub();
    my $write_fastpath_sub = $self->{_out}->fastpath_sub();
    my $fref;

    while ($fref = &{$from_subproc_fastpath_sub}()) {
	&{$write_fastpath_sub}($fref);
    };
}

=head2 _dbsubprocess_open2

    ($pid, $from_child, $to_child) = _open2('some', 'cmd', 'and', 'args');

Cribbed off of L<IPC::Open3.pm> to avoid leaking symbols in perl-5.10.0,
but simplified considerably.
Basically the C<Symbol.pm> GENnn symbols were leaking, for whatever reason.

This command cannot fail (it will die if it fails).

=cut

sub _dbsubprocess_open2 {
    my $parent_rdr = new IO::Handle;
    my $parent_wtr = new IO::Handle;
    pipe CHILD_RDR, $parent_wtr or die "cannot pipe\n";
    pipe $parent_rdr, CHILD_WTR or die "cannot pipe\n";

    my $pid = fork;
    croak "dbsubprocess: fork failed\n" if (!defined($pid));
    if ($pid == 0) {
	# in child
	untie *STDIN;
	untie *STDOUT;
	$parent_wtr->close;
	open \*STDIN, "<&=" . fileno CHILD_RDR;
	$parent_rdr->close;
	open \*STDOUT, ">&=" . fileno CHILD_WTR;
	# ignore stderr
	exec @_ or croak "cannot exec: " . join(" ", @_) . "\n";
	# never returns, either way.
	die;   # just in case
    };
    # in parent
    close CHILD_RDR;
    close CHILD_WTR;
    return ($pid, $parent_rdr, $parent_wtr);
}

=head2 run

    $filter->run();

Internal: run over all data rows.

=cut
sub run ($) {
    my($self) = @_;

    # catch sigpipe for failure cases in the child
    if ($self->{_warnings}) {
	$SIG{'PIPE'} = sub {
	    warn $self->{_prog} . ": external dbmapreduce reduce program exited with SIGPIPE (" . join(" ", @{$self->{_external_command_argv}}) . "), probably not consuming all input.\n";
	};
    } else {
	$SIG{'PIPE'} = sub { };
    };

    # run the subproc
    my $subproc_pid;
    my $from_child;
    my $to_child;
    ($subproc_pid, $from_child, $to_child) = _dbsubprocess_open2(@{$self->{_external_command_argv}});
    $self->{_from_subproc_fh} = $from_child;
    $self->{_to_subproc_fh} = $to_child;
    # open2 must write those variables
#    $subproc_pid = IPC::Open2($self->{_from_subproc_fh}, $self->{_to_subproc_fh}, @{$self->{_external_command_argv}});

    #
    # Deadlock warning: we've now forked (in open2)
    # and need separate threads to read and write to the subproc.
    # We fork a subprocessor thread to write to the subproc.
    # and we stick around to get stuff from the subproc.
    # (We used to do this the other way,
    # but we want output to be in the main thread so we can save _out.)
    #

    # Need a thread to send input to subproc
    my $subproc_preprocessor_thread = threads->new( sub {
	$self->_subproc_preprocessor;
    } );
    $self->{_subproc_preprocessor_thread} = $subproc_preprocessor_thread;

    # Convert the output back to fsdb here.
    $self->{_to_subproc_fh}->close;
    delete $self->{_to_subproc_fh};
    $self->_subproc_postprocessor;
    $self->{_from_subproc_fh}->close;  # clean up the other out of paranoia
    delete $self->{_from_subproc_fh};

    $self->{_subproc_preprocessor_thread}->join;
    # and reap the subprocess
    waitpid $subproc_pid, 0 if (defined($subproc_pid));
}


=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2008 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;

