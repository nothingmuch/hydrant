package Hydrant;
use Moose;

use AnyEvent;
use Try::Tiny;

sub _get_cv {
    my ( $opts, $args ) = @_;

    if ( my $cv = $opts->{cv} ) {
        return $cv;
    } else {
        my $cv = AE::cv;

        $opts->{cv} = $cv;
        push @$args, cv => $cv;

        return $cv;
    }
}

use namespace::clean -except => 'meta';

use Hydrant::PubSub;

with qw(Hydrant::Role::API);

has backend => (
    does => "Hydrant::Role::Backend",
    is   => "ro",
    required => 1,
);

has pubsub => (
    isa => "Hydrant::PubSub",
    is  => "ro",
    default => sub { Hydrant::PubSub->new },
);

# for each blocking query operation (dequeue, remove, get, etc) add a subscriber.

# when performing a put check the subscriber list, and if none is found, fall
# back to the backend

# TODO
# {first,last}_key as variants of get with blocking/nonblocking
# unify with _dequeue code
#
# key count

sub keys { shift->backend->keys(@_) }

sub get    { shift->_get( get => @_ ) }
sub remove { shift->_get( remove => @_ ) }

sub _get {
    my ( $self, $prim, @args ) = @_;

    my ( $path, $name, %opts ) = @args;

    my $cv = _get_cv(\%opts, \@args);

    my $s = $self->pubsub;

    if ( $opts{block} ) {
        my $cv_listen = AE::cv {
            my $get_cv = shift;

            try {
                if ( my $cell = $get_cv->recv ) {
                    # found a match, return
                    $cv->send( $get_cv->recv );
                } else {
                    # no match yet, add a subscriber
                    my $cv_listen = AE::cv {
                        my $put_cv = $_[0];

                        my ( $path, $name, $cell, $put_status_cv ) = $put_cv->recv;

                        $cv->send( $cell, $path, $name );

                        if ( $prim eq 'remove' ) {
                            $put_status_cv->send( 1, $path, $name, $cell );
                        } else {
                            $self->put($path, $name, $cell, cv => $put_status_cv)
                        }
                    };

                    # make sure the listener doesn't die
                    push @{ $cv->{_refcnt} ||= [] }, $cv_listen;

                    $s->wait_for_cell($path, $name, %opts, cv => $cv_listen );
                }
            } catch {
                # delegate whatever error we caught
                $cv->croak($_);
            }
        };

        $self->backend->$prim($path, $name, $cv_listen);

        push @{ $cv->{_refcnt} ||= [] }, $cv_listen unless $cv_listen->ready;
    } else {
        # nonblocking lookups don't need the pubsub
        $self->backend->$prim($path, $name, $cv);
    }

    return $cv;
}

sub delete {
    my ( $self, @args ) = @_;

    my ( $path, $name, %opts ) = @args;

    my $cv = _get_cv(\%opts, \@args);

    my $s = $self->pubsub;

    if ( $opts{block} ) {
        my $cv_listen = AE::cv {
            my $delete_cv = shift;

            try {
                if ( my $cell = $delete_cv->recv ) {
                    # found a match, return
                    $cv->send( $delete_cv->recv );
                } else {
                    # no match yet, add a subscriber
                    my $cv_listen = AE::cv {
                        my $put_cv = $_[0];

                        my ( $path, $name, $cell, $put_status_cv ) = $put_cv->recv;

                        $cv->send( $cell, $path, $name );

                        $put_status_cv->send( 1, $path, $name, $cell );
                    };

                    $s->wait_for_cell($path, $name, %opts, cv => $cv_listen );

                    push @{ $cv->{_refcnt} ||= [] }, $cv_listen;
                }
            } catch {
                $cv->croak($_);
            }
        };

        $self->backend->delete($path, $name, $cv_listen);

        push @{ $cv->{_refcnt} ||= [] }, $cv_listen unless $cv_listen->ready;
    } else {
        $self->backend->delete($path, $name, $cv);
    }

    return $cv;
}

sub shift {
    my ( $self, @args ) = @_;

    $self->_dequeue( qw(remove first_key), @args );
}

sub pop {
    my ( $self, @args ) = @_;

    $self->_dequeue( qw(remove last_key), @args );
}

sub _dequeue {
    my ( $self, $prim, $key, @args ) = @_;

    my ( $path, %opts ) = @args;

    my $cv = _get_cv(\%opts, \@args);

    my $s = $self->pubsub;

    my $method = "${prim}_$key";

    if ( $opts{block} ) {
        my $cv_listen = AE::cv {
            my $removed_cv = $_[0];

            try {
                my ( $cell, $removed_path, $removed_name ) = $removed_cv->recv;

                if ( $cell ) {
                    $cv->send($cell, $path, $removed_name);
                    $s->notify_delete($path, $removed_name);
                } else {
                    my $cv_listen = AE::cv {
                        my $sub_cv = $_[0];

                        my ( $path, $name, $cell, $put_cv, %opts ) = $sub_cv->recv;

                        $cv->send($cell, $name, $cell); # the cv for the shift
                        $put_cv->send(1);

                        $s->notify_delete($path, $name);
                    };

                    $s->wait_for_path($path, %opts, cv => $cv_listen );

                    push @{ $cv->{_refcnt} ||= [] }, $cv_listen;
                }
            } catch {
                $cv->croak($_);
            }
        };

        $self->backend->$method($path, $cv_listen);

        push @{ $cv->{_refcnt} ||= [] }, $cv_listen;
    } else {
        my $cv_listen = AE::cv {
            my $removed_cv = $_[0];

            try {
                $cv->send( my ( $cell, $path, $name ) = $removed_cv->recv );
                $s->notify_delete($path, $name) if $cell;
            } catch {
                $cv->error($_);
            }
        };

        $self->backend->$method($path, $cv_listen);

        push @{ $cv->{_refcnt} ||= [] }, $cv_listen;
    }

    return $cv;
}

sub put {
    my ( $self, @args ) = @_;

    my ( $path, $name, $cell, %opts ) = @args;

    my $cv = _get_cv(\%opts, \@args);

    my $s = $self->pubsub;

    unless ( $s->push_create($path, $name, $cell, $cv) ) {
        # there was no listener for the namespace or the cell
        # write to the backend

        if ( $opts{block} ) {
            # write if none exist, wait for delete otherwise
            $self->backend->put($path, $name, $cell, AE::cv {
                my $put_cv = $_[0];

                try {
                    $cv->send( $put_cv->recv );
                } catch {
                    if ( /Cell exists/ ) {
                        # wait until the cell has been deleted
                        $s->wait_for_delete($path, $name, %opts, cv => AE::cv {
                            try {
                                # check for errors
                                $_[0]->recv;

                                # try again now that it has been deleted
                                $self->put($path, $name, $cell, %opts, cv => $cv);
                            } catch {
                                $cv->croak($_);
                            }
                        });
                    } else {
                        $cv->error($_);
                    }
                }
            }, %opts, no_overwrite => 1);
        } else {
            # otherwise, spill to storage
            $self->backend->put($path, $name, $cell, $cv, %opts);
        }
    }

    return $cv;
}

my $i = "aaaaaaaaa";

sub ascending_identifier {
    $i++;
}

sub push {
    my ( $self, $path, $cell, @args ) = @_;

    my $id = ascending_identifier();

    unless ( $cell->has_etag ) {
        $cell = $cell->clone( etag => $id );
    }

    return $self->put( $path, $id, $cell, @args );
}

sub insert {
    my ( $self, $path, $cell, @args ) = @_;

    return $self->put( $path, $cell->etag, $cell, @args );
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__
