package Hydrant::Role::Cell;
use Moose::Role;

use Digest::SHA1;
use Carp;
use Time::HiRes qw(time);

use MooseX::Types::Moose qw(Num ScalarRef Bool Str);

use namespace::clean;

with qw(MooseX::Clone);

requires qw(open new_from_io);

has stored => (
    traits => [qw(NoClone)],
    isa => "Bool",
    is  => "ro",
    writer => "_stored",
);

sub for_storage {
    my $self = shift;
    return $self->clone( stored => 1 );

    if ( $self->stored ) {
        return $self->clone( stored => 1 );
    } else {
        $self->_stored(1);
        return $self;
    }
}

has mutable => (
    isa => Bool,
    is  => "ro",
);

has expires => (
    isa => Num,
    is  => "ro",
    predicate => "has_expires",
);

has last_modified => (
    isa => Num,
    is  => "ro",
    default => sub { time },
);

has content_type => (
    isa => Str,
    is  => "ro",
    default => "application/octet-stream",
);

has etag => (
    isa => Str,
    is  => "ro",
    lazy_build => 1,
);

sub _build_etag {
    my $self = shift;

    my $io = $self->open;

    my $d = Digest::SHA1->new;

    {
        local $/ = \4096;
        $d->add($io->getline); # addfile uses PerlIO_read which might not a good fit
    }

    return $d->hexdigest;
}

sub update {
    my ( $self, $new, %opts ) = @_;

    if ( $self->mutable ) {
        $new->clone( $self->inherited_fields );
    } else {
        croak "Can't update a readonly cell";
    }
}

# ex: set sw=4 et:

__PACKAGE__

__END__
