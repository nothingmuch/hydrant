#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use IO::Handle::Util qw(io_from_any io_to_string);
use AnyEvent;

use ok 'Hydrant::Backend::Memory';
use ok 'Hydrant::Backend::Memory::Cell';

my $backend = Hydrant::Backend::Memory->new;

isa_ok( $backend, "Hydrant::Backend::Memory" );

{
    $backend->get("hello", "animal", my $cv = AE::cv);

    ok( not($cv->recv), "no cell yet" );
}

{
    my $stream = $backend->keys("hello");

    is_deeply( [ $stream->all ], [], "keys" );
}

{
    $backend->first_key("hello", my $cv = AE::cv);
    is( $cv->recv, undef, "first key" );
}

{
    $backend->last_key("hello", my $cv = AE::cv);
    is( $cv->recv, undef, "last key" );
}

{
    my $cell = Hydrant::Backend::Memory::Cell->new_from_io(
        io_from_any("this is a moose"),
        etag => "moose",
    );

    isa_ok( $cell, "Hydrant::Backend::Memory::Cell" );

    can_ok( $cell, 'open' );

    ok( my $io = $cell->open, "got IO" );

    is( $io->getline, "this is a moose", "got data from IO" );

    is( $cell->etag, "moose", "etag" );

    ok( $cell->last_modified <= time() + 2, "last modified makes sense" );

    is( $cell->content_type, "application/octet-stream", "default mime type" );

    $backend->put("hello", "animal", $cell, my $cv = AE::cv);

    ok( $cv->recv, "stored" );
}

{
    $backend->get("hello", "animal", my $cv = AE::cv);

    ok( my $cell = $cv->recv, "got cell" );

    isa_ok( $cell, "Hydrant::Backend::Memory::Cell" );
}

{
    my $stream = $backend->keys("hello");
    is_deeply( [ $stream->all ], [qw(animal)], "keys" );
}

{
    $backend->first_key("hello", my $cv = AE::cv);
    is( $cv->recv, "animal", "first key" );
}

{
    $backend->last_key("hello", my $cv = AE::cv);
    is( $cv->recv, "animal", "last key" );
}

done_testing;
