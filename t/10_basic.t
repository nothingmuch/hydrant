#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use IO::Handle::Util qw(io_from_any);
use AnyEvent;

use ok 'Hydrant';
use ok 'Hydrant::Backend::Memory';

my $h = Hydrant->new(
    backend => Hydrant::Backend::Memory->new,
);

isa_ok( $h, "Hydrant" );

is( $h->get("foo", "bar")->recv, undef, "no /foo/bar" );

{
    my $cell = Hydrant::Backend::Memory::Cell->new_from_io(
        io_from_any("this is a moose"),
        etag => "moose",
    );

    ok( $h->put("foo", "bar", $cell)->recv, "put" );
}

{
    ok( my $cell = $h->get("foo", "bar")->recv, "got /foo/bar" );

    is( $cell->etag, "moose", "right cell" );
}

{
    my $cell = Hydrant::Backend::Memory::Cell->new_from_io(
        io_from_any("OH HAI"),
    );

    ok( my $cv = $h->get(qw(foo baz), block => 1), "got cond var for blocking read" );

    ok( not($cv->ready), "not yet ready" );

    ok( $h->put(qw(foo baz), $cell)->recv, "put" );

    ok( $cv->ready, "CV ready" );

    is( $cv->recv, $cell, "got cell" );

    is( $h->get(qw(foo baz))->recv->etag, $cell->etag, "still added to storage" );
}

{
    ok( $h->remove(qw(foo baz))->recv, "got cell for remove" );
    is( $h->remove(qw(foo baz))->recv, undef, "not a second time" );

    ok( my $cv1 = $h->remove(qw(foo baz), block => 1), "got cond var for blocking read" );
    ok( my $cv2 = $h->remove(qw(foo baz), block => 1), "got cond var for blocking read" );

    my $cell = Hydrant::Backend::Memory::Cell->new_from_io(
        io_from_any("OH HAI"),
    );

    ok( not($cv1->ready), "not yet ready" );

    ok( $h->put(qw(foo baz), $cell)->recv, "put" );

    ok( $cv1->ready, "CV ready" );
    ok( not($cv2->ready), "second CV not ready" );

    is( $cv1->recv, $cell, "got cell" );

    is( $h->get(qw(foo baz))->recv, undef, "not inserted" );

    ok( $h->put(qw(foo baz), $cell)->recv, "put" );

    ok( $cv2->ready, "second CV ready" );

    is( $cv2->recv, $cell, "got cell" );

    is( $h->get(qw(foo baz))->recv, undef, "not inserted" );

    ok( $h->put(qw(foo baz), $cell)->recv, "put" );

    is( $h->get(qw(foo baz))->recv->etag, $cell->etag, "inserted" );

    is_deeply( [ $h->keys("foo")->all ], [qw(bar baz)], "keys" );

    ok( $h->delete(qw(foo bar))->recv, "delete" );

    ok( not($h->delete(qw(foo bar))->recv), "can't delete twice" );

    ok( my $cv = $h->delete(qw(foo bar), block => 1), "blocking delete cond var" );

    ok( !$cv->ready, "delete not ready" );

    ok( $h->put(qw(foo bar), $cell)->recv, "put" );

    ok( $cv->ready, "delete ready" );
    ok( $cv->recv, "delete" );

    ok( not($h->delete(qw(foo bar))->recv), "can't delete twice" );
}

{
    is_deeply( [ $h->keys("queue")->all ], [], "keys" );

    ok( my $cv1 = $h->shift("queue", block => 1), "got cv for shift" );

    ok( not($cv1->ready), "not ready" );

    is_deeply( [ $h->keys("queue")->all ], [], "keys" );

    ok( my $cv2 = $h->shift("queue", block => 1), "got cv for shift" );

    ok( not($cv2->ready), "not ready" );

    is_deeply( [ $h->keys("queue")->all ], [], "keys" );

    my $cell = Hydrant::Backend::Memory::Cell->new_from_io(
        io_from_any("OH HAI"),
    );

    $cell->etag; # force build

    ok( $h->push("queue", $cell)->recv, "push" );

    is_deeply( [ $h->keys("queue")->all ], [], "keys" );

    ok( $cv1->ready, "cv1 is ready" );
    ok( not($cv2->ready), "cv2 not ready" );

    ok( $h->push("queue", $cell)->recv, "push" );

    ok( $cv1->ready, "cv1 is ready" );
    ok( $cv2->ready, "cv2 is ready" );

    is_deeply( [ $h->keys("queue")->all ], [], "keys" );

    my ( $ok, $path, $name ) = $h->push("queue", $cell)->recv;
    ok( $ok, "put" );

    is( $path, "queue", "path from put status cv" );

    is_deeply( [ $h->keys("queue")->all ], [ $name ], "keys" );

    ok( $h->push("queue", $cell)->recv, "push" );

    is( $h->pop("queue")->recv->etag, $cell->etag, "nb pop" );

    is( $h->shift("queue")->recv->etag, $cell->etag, "nb shift" );

    is( $h->shift("queue")->recv, undef, "nb shift" );

    ok( my $pop_cv = $h->pop("queue", block => 1), "blocking pop" );

    ok( not($pop_cv->ready), "not ready" );

    ok( $h->push("queue", $cell)->recv, "push" );

    ok( $pop_cv->ready, "pop ready" );

    is( $pop_cv->recv, $cell, "got cell" );
}

done_testing;

