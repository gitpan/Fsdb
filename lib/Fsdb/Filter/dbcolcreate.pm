#!/usr/bin/perl -w

#
# dbcolcreate.pm
# Copyright (C) 1991-2007 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblibdir for details.
#

package Fsdb::Filter::dbcolcreate;

=head1 NAME

dbcolcreate - create new columns

=head1 SYNOPSIS

    dbcolcreate NewColumn1 [NewColumn2]

or

    dbcolcreate -e DefaultValue NewColumnWithDefault

=head1 DESCRIPTION

Create columns C<NewColumn1>, etc.
with an optional C<DefaultValue>.


=head1 OPTIONS

=over 4

=item B<-e> EmptyValue or B<--emtpy>

Specify the value newly created columns get.

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

    #fsdb test
    a
    b

=head2 Command:

    cat data.fsdb | dbcolcreate foo 

=head2 Output:

    #fsdb      test    foo
    a       -
    b       -


=head1 SEE ALSO

L<Fsdb>.


=head1 CLASS FUNCTIONS

=cut

@ISA = qw(Fsdb::Filter);
($VERSION) = 2.0;

use strict;
use Pod::Usage;
use Carp;

use Fsdb::Filter;
use Fsdb::IO::Reader;
use Fsdb::IO::Writer;


=head2 new

    $filter = new Fsdb::Filter::dbcolcreate(@arguments);

Create a new dbcolcreate object, taking command-line arguments.

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
    $self->{_creations} = [];
    $self->{_create_values} = {};
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
	'close!' => \$self->{_close},
	'd|debug+' => \$self->{_debug},
	'e|empty=s' => \$self->{_empty},
	'i|input=s' => sub { $self->parse_io_option('input', @_); },
	'log!' => \$self->{_logprog},
	'o|output=s' => sub { $self->parse_io_option('output', @_); },
	'<>' => sub { 
	    my($target) = @_;
	    if ($target eq '-') {
		warn "dbcolcreate: appear to be using fsdb-1 dual argument syntax.  Replace \"NewCol DefaultValue\" with \"-e DefaultValue NewCol\".\n";
		return;
	    };
	    push(@{$self->{_creations}}, $target);
	    $self->{_create_values}{$target} = $self->{_empty};
	},
	) or pod2usage(2);
    pod2usage(2) if ($#argv != -1);
}

=head2 setup

    $filter->setup();

Internal: setup, parse headers.

=cut

sub setup ($) {
    my($self) = @_;

    $self->finish_io_option('input', -comment_handler => $self->create_pass_comments_sub);

    my @new_cols = @{$self->{_in}->cols};
    my %existing_cols;
    foreach (@new_cols) {
	$existing_cols{$_} = 1;
    };
    my $coli = $#new_cols;
    my $insert_code = '';
    foreach (@{$self->{_creations}}) {
	croak $self->{_prog} . ": attempt to create pre-existing column $_.\n"
	    if (defined($existing_cols{$_}));
	$coli++;
	push @new_cols, $_;
	$existing_cols{$_} = 2;
	my $val = $self->{_create_values}{$_};
	my $quote = "'";
	if ($val =~ /\'/) {
	    $quote = '|';
	    croak $self->{_prog} . ": internal error: cannot find reasonable way to do quoting.\n"
		if ($val =~ /\|/);
	};
	$insert_code .= "\t" . '$fref->[' . $coli . '] = q' . $quote . $val . $quote . ";\n";
    };

    $self->finish_io_option('output', -clone => $self->{_in}, -cols => \@new_cols);
    
    #
    # write the loop
    #
    {
	my $loop_sub;
	my $read_fastpath_sub = $self->{_in}->fastpath_sub();
	my $write_fastpath_sub = $self->{_out}->fastpath_sub();
	my $loop_sub_code = q'
	    $loop_sub = sub {
		my $fref;
		while ($fref = &$read_fastpath_sub()) {
		' . $insert_code . q'
		    &$write_fastpath_sub($fref);
		};
	    };
	';
	eval $loop_sub_code;
	$@ && die $self->{_prog} . ":  internal eval error: $@.\n";
	$self->{_loop_sub} = $loop_sub;
    }
}


=head2 run

    $filter->run();

Internal: run over each rows.

=cut
sub run ($) {
    my($self) = @_;
    &{$self->{_loop_sub}}();
}


=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2007 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;