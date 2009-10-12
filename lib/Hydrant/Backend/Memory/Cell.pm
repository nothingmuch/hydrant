package Hydrant::Backend::Memory::Cell;
use Moose;

use IO::Handle::Util qw(io_to_string io_from_any);

use namespace::clean -except => 'meta';

with qw(Hydrant::Role::Cell);

has _blob => (
    isa => "ScalarRef",
    is  => "ro",
    required => 1,
);

sub new_from_io {
    my ( $class, $io, @args ) = @_;

    my $buf = io_to_string($io);

    $class->new( @args, _blob => \$buf );
}

sub open {
    my $self = shift;

    return io_from_any($self->_blob);
}

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__
