package Hydrant::PubSub;
use Moose;

use Scalar::Util qw(weaken refaddr);
use Guard;

use Hash::Util::FieldHash::Compat qw(fieldhash);

use namespace::clean -except => 'meta';

has _subscriptions => (
    isa => "HashRef",
    is  => "ro",
    default => sub { return {} },
);

has _cleanup => => (
    isa => "HashRef",
    is  => "ro",
    default => sub { fieldhash my %h },
);

sub remove {
    my ( $self, $cv ) = @_;

    delete $self->_cleanup->{$cv};

    return;
}

sub wait_for_cell {
    my ( $self, $path, $name, %opts ) = @_;

    $self->_push_sub( cell => "$path/$name", %opts );
}

sub wait_for_delete {
    my ( $self, $path, $name, %opts ) = @_;

    $self->_push_sub( delete => "$path/$name", %opts );
}

sub wait_for_path {
    my ( $self, $path, %opts ) = @_;

    $self->_push_sub( path => $path, %opts );
}

sub push_create {
    my ( $self, $path, $name, $cell, $cv ) = @_;

    if ( my $sub = $self->pop_cell_subscriber($path, $name) ) {
        $sub->send($path, $name, $cell, $cv);
        return $sub;
    } else {
        return;
    }
}

sub notify_delete {
    my ( $self, $path, $name ) = @_;

    if ( my $sub = $self->pop_delete_subscriber($path, $name) ) {
        $sub->send($path, $name);
    }

    return;
}

sub pop_cell_subscriber {
    my ( $self, $path, $name ) = @_;

    return $self->_pop_sub( cell => "$path/$name" )
        || $self->_pop_sub( path => $path );
}

sub pop_delete_subscriber {
    my ( $self, $path, $name ) = @_;

    $self->_pop_sub( delete => "$path/$name" );
}

sub _scrub_queue {
    my ( $self, $ns, $key, $cv ) = @_;

    if ( my $queue = $self->_subscriptions->{$ns}{$key} ) {
        if ( $cv ) {
            my $r = refaddr($cv);
            @$queue = grep { defined and not $r == refaddr($_) } @$queue;
        } else {
            @$queue = grep { defined } @$queue;
        }

        unless ( @$queue ) {
            delete $self->_subscriptions->{$ns}{$key};
        }
    }
}

sub _push_sub {
    my ( $self, $ns, $key, %opts ) = @_;

    my $cv = $opts{cv};
    weaken($cv);

    my $t;

    if ( my $timeout = $opts{timeout} ) {
        $t = AE::timer($timeout, 0, sub {
            $self->remove($cv);
            $cv->croak("timed out") if $cv and not $cv->ready;
        });
    }

    # this guard keeps a ref to the timer for as long as $cv is alive
    $self->_cleanup->{$cv} = guard {
        undef $t; # reset the timer, if there is one
        $self->_scrub_queue($ns, $key, $cv);
    };

    my $queue = $self->_subscriptions->{$ns}{$key} ||= [];

    # add to the queue, but don't take a ref
    # if the $cv goes out of scope the guard will fire, and the timer, if any,
    # will also go out of scope
    push @$queue, $cv;
    weaken($queue->[-1]);

    return;
}

sub _pop_sub {
    my ( $self, $ns, $key ) = @_;

    if ( my $queue = $self->_subscriptions->{$ns}{$key} ) {
        my $ret = shift @$queue;

        $self->remove($ret);

        unless ( @$queue ) {
            delete $self->_subscriptions->{$ns}{$key};
        }

        return $ret;
    }

    return;
}


__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__
