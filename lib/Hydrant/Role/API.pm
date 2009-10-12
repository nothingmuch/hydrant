package Hydrant::Role::API;
use Moose::Role;

use namespace::clean;

requires qw(
    put
    get
    delete
    remove

    push
    shift
    pop

    keys
);


# ex: set sw=4 et:

__PACKAGE__

__END__
