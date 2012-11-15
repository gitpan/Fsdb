#!/usr/bin/perl -w

#
# mysql_to_db.pm
# Copyright (C) 2006-2007 by John Heidemann <johnh@isi.edu>
# $Id$
#
# This program is distributed under terms of the GNU general
# public license, version 2.  See the file COPYING
# in $dblib for details.
#


package Fsdb::Filter::mysql_to_db;

=head1 NAME

mysql_to_db - convert a mysql table to fsdb format

=head1 SYNOPSIS

    mysql_to_db [options] SQL-command

=head1 DESCRIPTION

Execute the SQL command I<SQL-command> and output the results in fsdb format.

Yes, this is a bit perverse.

Assumes it will talk to the Unix domain socket, not network.

My default, it converts NULL and all-space records to -.

=head1 OPTIONS

=over 4

=item B<-U USER> or <--user USER> 

Specify the mysql user.

=item B<-P PASSWORD> or <--password PASSWORD> 

Specify the mysql password.

=item B<-D DATABASE> or <--database DATABASE> 

Specify the mysql database.

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

    xxx

=head2 Command:

    mysql_to_db  -U SENSYS2006 -P SENSYS2006 -D SENSYS2006 'show tables'


=head2 Output:

    xxx


=head1 SEE ALSO

L<Fsdb>.


=head1 CLASS FUNCTIONS

=cut

@ISA = qw(Fsdb::Filter);
$VERSION = 2.0;

use strict;
use Pod::Usage;
use Carp;

use DBI;
use DBD::mysql;

use Fsdb::Filter;
use Fsdb::IO::Writer;


=head2 new

    $filter = new Fsdb::Filter::mysql_to_db(@arguments);

Create a new mysql_to_db object, taking command-line arguments.

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
    $self->{_password} = undef;
    $self->{_user} = undef;
    $self->{_database} = undef;
    $self->{_sql_code} = '';
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
	'd|debug+' => \$self->{_debug},
	'D|database=s' => \$self->{_database},
	'i|input=s' => sub { $self->parse_io_option('input', @_); },
	'log!' => \$self->{_logprog},
	'o|output=s' => sub { $self->parse_io_option('output', @_); },
	'P|password=s' => \$self->{_password},
	'U|user=s' => \$self->{_user},
	) or pod2usage(2);
    foreach (@argv) {
	$self->{_sql_code} .= ($self->{_sql_code} eq '' ? '' : ' ') . $_);
    };
}

=head2 setup

    $filter->setup();

Internal: setup, parse headers.

=cut

sub setup ($) {
    my($self) = @_;

    $self->{_pretty_args} .= Fsdb::Support::code_prettify($self->{_sql_code});

    my($dsn) = "DBI:mysql:" . $self->{_database};

    $self->{_dbhand} = DBI->connect($dsn, $self->{_user}, $self->{_password})
	or croak $self->{_prog} . ": cannot contact database.\n";

    $self->{_qhand} = $self->{_dbhand}->prepare($self->{_sql_code});
    $self->{_qhand}->execute() or croak $self->{_prog} . ": sql command failed: " . $self->{_sql_code} . "\n";

    # punt on setting up output until run()
}


=head2 run

    $filter->run();

Internal: run over each rows.

=cut

sub run ($) {
    my($self) = @_;

    my $write_fastpath_sub;
    my @cols;  # database columsn

    while ($hashref = $self->{_qhand}->fetchrow_hashref) {
        if ($#cols == -1) {
            @cols = sort keys %$hashref;
	    @cols = Fsdb::IO::clean_potential_columns(@cols);
	    $self->finish_io_option('output', -fscode => 'S', -cols => \@cols);
	    $write_fastpath_sub = $self->{_out}->fastpath_sub();
	    next;
        };
        my(@f) = ();
        foreach (@cols) {
            my $v = $hashref->{$_};
            if (!defined($v)) {
                $v = $self->{_out}{_empty};
            } else {
                $v =~ s/^\s+//g;
                $v =~ s/\s+$//g;
                if ($v eq '') {
                    $v = $self->{_out}{_empty};
                } else {
		    # fix spaces:
		    $v =~ s/(\r\n)/\n/g;   # unixify
		    $v =~ s/\r/\n/g;   # unixify
		    $v =~ s/\n/ \\n /g;   # quote newline
		    $v =~ s/  +/ /g;  # no double spaces
		    $v =~ s/[[:cntrl:]]//g;
                };
            };
            push(@f, $v);
        };
	&{$write_fastpath_sub}(\@f);
    };
}


=head1 AUTHOR and COPYRIGHT

Copyright (C) 1991-2008 by John Heidemann <johnh@isi.edu>

This program is distributed under terms of the GNU general
public license, version 2.  See the file COPYING
with the distribution for details.

=cut

1;
