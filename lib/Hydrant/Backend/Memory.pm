package Hydrant::Backend::Memory;
use Moose;

use AnyEvent;
use Try::Tiny;

use Data::Stream::Bulk::Util qw(bulk);

use namespace::clean -except => 'meta';

use Hydrant::Backend::Memory::Cell;

with qw(Hydrant::Role::Backend);

has storage => (
    traits => [qw(Hash)],
    isa => "HashRef",
    is  => "ro",
    default => sub { return {} },
    handles => {
        _get_container    => "get",
        _delete_container => "delete",
    },
);

sub get {
    my ( $self, $path, $name, $cv ) = @_;

    $cv->send( $self->storage->{$path}{$name}, $path, $name );
}

sub put {
    my ( $self, $path, $name, $cell, $cv, %opts ) = @_;

    if ( my $prev = $self->storage->{$path}{$name} ) {
        if ( $opts{no_overwrite} ) {
            $cv->croak("Cell exists");
        } else {
            try {
                my $updated = $prev->update($cell, %opts);
                my $stored = $self->storage->{$path}{$name} = $updated->for_storage;
                $cv->send(1, $path, $name, $stored);
            } catch {
                $cv->croak($_);
            }
        }
    } else {
        my $stored = $self->storage->{$path}{$name} = $cell->for_storage;
        $cv->send(1, $path, $name, $stored);
    }
}

sub delete {
    my ( $self, $path, $name, $cv ) = @_;

    my $parent = $self->_get_container($path);

    my $deleted = delete $parent->{$name};

    if ( CORE::keys %$parent == 0 ) {
        $self->_delete_container($path);
    }

    if ( $deleted ) {
        $cv->send(1, $path, $name);
    } else {
        $cv->send(undef);
    }
}

sub remove {
    my ( $self, $path, $name, $cv ) = @_;

    $self->get($path, $name, $cv);

    $self->delete($path, $name, AE::cv);
}

sub _keys {
    my ( $self, $path ) = @_;

    my @keys = CORE::keys(%{ $self->_get_container($path) || {} });

    return [ sort @keys ];
}

sub keys {
    my ( $self, $path ) = @_;

    # FIXME Data::Stream::Bulk::AnyEvent
    return bulk(@{ $self->_keys($path) });
}

sub _first_key {
    my ( $self, $path ) = @_;

    return $self->_keys($path)->[0];
}

sub first_key {
    my ( $self, $path, $cv ) = @_;

    $cv->send( $self->_first_key($path), $path );
}

sub _last_key {
    my ( $self, $path ) = @_;

    return $self->_keys($path)->[-1];
}

sub last_key {
    my ( $self, $path, $cv ) = @_;

    $cv->send( $self->_last_key($path), $path );
}

sub remove_first_key {
    my ( $self, $path, $cv ) = @_;

    if ( my $key = $self->_first_key($path) ) {
        $self->remove( $path, $key, $cv );
    } else {
        $cv->send(undef);
    }
}

sub remove_last_key {
    my ( $self, $path, $cv ) = @_;

    if ( my $key = $self->_last_key($path) ) {
        $self->remove( $path, $key, $cv );
    } else {
        $cv->send(undef);
    }
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__
