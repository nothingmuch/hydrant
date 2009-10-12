package Hydrant::Role::Backend;
use Moose::Role;

use namespace::clean;

requires qw(
    get
    put
    delete
    remove

    keys
    first_key
    last_key
    remove_first_key
    remove_last_key
);


# ex: set sw=4 et:

__PACKAGE__

__END__
