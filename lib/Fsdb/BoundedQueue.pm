
#
# BoundedQueue.pm
# $Id$
#
# This file is dervied from perl-5.8.8's Thread::Queue,
# but extended to support bounded length queues (by John Heidemann).
#
# It is licened under the same terms as Perl (GPLv2 or the Perl Artistic License.)
#

package Fsdb::BoundedQueue;

use threads;
use threads::shared;
use strict;
use Carp;

our $VERSION = '2.00';

=head1 NAME

Fsdb::BoundedQueue - thread-safe queues

=head1 SYNOPSIS

    use Fsdb::BoundedQueue;
    my $q = new Fsdb::BoundedQueue;
    $q->enqueue("foo", "bar");
    my $foo = $q->dequeue;    # The "bar" is still in the queue.
    my $foo = $q->dequeue_nb; # returns "bar", or undef if the queue was empty
    my $left = $q->pending;   # returns the number of items still in the queue

=head1 DESCRIPTION

A queue, as implemented by C<Fsdb::BoundedQueue> is a thread-safe 
data structure much like a list.  Any number of threads can safely 
add elements to the end of the list, or remove elements from the head 
of the list. (BoundedQueues don't permit adding or removing elements from 
the middle of the list).

Unlike <Thread::Queue>, the BoundedQueue will block after
queueing a fixed number of elements, set by set_bound.

=head1 FUNCTIONS AND METHODS

=over 8

=item new

The C<new> function creates a new empty queue.

=item enqueue LIST

The C<enqueue> method adds a list of scalars on to the end of the queue.
The queue will grow as needed to accommodate the list.

=item dequeue

The C<dequeue> method removes a scalar from the head of the queue and
returns it. If the queue is currently empty, C<dequeue> will block the
thread until another thread C<enqueue>s a scalar.

=item dequeue_nb

The C<dequeue_nb> method, like the C<dequeue> method, removes a scalar from
the head of the queue and returns it. Unlike C<dequeue>, though,
C<dequeue_nb> won't block if the queue is empty, instead returning
C<undef>.

=item pending

The C<pending> method returns the number of items still in the queue.

=item set_bound

Set the bound on the queue size (the default is thousands).

=back

=head1 SEE ALSO

L<threads>, L<threads::shared>, L<Thread::Queue>

=cut

# one global bound
my $bound = 2048;

sub new {
    my $class = shift;
    my @q : shared;   # queue starts empty!
    bless \@q, $class;
    return \@q;
}

sub set_bound {
    $bound = $_[0];
    croak "cannot set Fsdb::BoundedQueue bound smaller than 2\n"
	if ($bound <= 1);
}

sub dequeue  {
    my $q = shift;
    lock(@$q);
    while (!@$q) {
	cond_wait @$q;
    };
    my $elem = shift @$q;
    cond_signal @$q;   #  if @$q > 1;
    return $elem;
}

sub dequeue_nb {
    my $q = shift;
    lock(@$q);
    my $elem = shift @$q;
    cond_signal @$q;
    return $elem;
}

sub enqueue {
    my $q = shift;
    lock(@$q);
    cond_wait @$q while ($#$q >= $bound);
    foreach (@_) {
	#
	# This copying is gross, but without it I found I
	# was getting aliases in the queue.  I.e., enqueue one,
	# then the caller modifies what should be a new copy,
	# but it changes the old version.
	# Better correct than wrong.
	#
	# The positive thing is it means the caller can be share oblivious.
	#
	# By perl-5.14 we could do
	#   push(@$q, shared_clone($_));
	# but our code here is a little more specialized
	# and otherwise equivalent.
	#
	if (!ref($_)) {
	    my $copy : shared = $_;
	    push @$q, $copy;
	} elsif (ref($_) eq 'ARRAY') {
	    my @copy : shared = @$_;
	    push @$q, \@copy;
	} else {
	    die;
	};
    };
    cond_signal @$q; # if ($#$q >= 0);
}

sub pending  {
    my $q = shift;
    lock(@$q);
    return scalar(@$q);
}

1;


