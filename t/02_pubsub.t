#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Exception;

use Scalar::Util qw(weaken);

use IO::Handle::Util qw(io_from_any);
use AnyEvent;

use ok "Hydrant::PubSub";

use Hydrant::Backend::Memory::Cell;

my $cell = Hydrant::Backend::Memory::Cell->new_from_io(
    io_from_any("this is a moose"),
    etag => "moose",
);

my $pubsub = Hydrant::PubSub->new;

lives_ok {
    $pubsub->notify_delete("hello", "animal");
} "no delete listeners";

ok( not($pubsub->push_create("hello", "animal", $cell)), "no write listeners" );

{
    $pubsub->wait_for_cell("hello", "animal", cv => my $cv = AE::cv);

    ok( $pubsub->push_create("hello", "animal", $cell, my $cv2 = AE::cv), "got a write listener" );

    ok( $cv->ready, "cond var triggered" );

    is_deeply( [ $cv->recv ], [ qw(hello animal), $cell, $cv2 ], "parameters" );
}

{
    $pubsub->wait_for_cell(qw(blah blah), cv => my $cv = AE::cv);

    $pubsub->remove($cv);

    ok( not($pubsub->push_create(qw(blah blah), $cell, AE::cv)), "listener removed" );
}

{
    $pubsub->wait_for_cell(qw(blah blah), cv => my $cv = AE::cv);

    weaken($cv);

    ok( not($pubsub->push_create(qw(blah blah), $cell, AE::cv)), "listener automatically removed" );
}

{
    $pubsub->wait_for_cell("hello", "animal", cv => my $cv1 = AE::cv);
    $pubsub->wait_for_cell("hello", "animal", cv => my $cv2 = AE::cv);

    ok( $pubsub->push_create("hello", "animal", $cell, my $push_cv1 = AE::cv), "got a write listener" );

    ok( $cv1->ready, "cond var triggered" );
    ok( not($cv2->ready), "second cond var not triggered" );

    is_deeply( [ $cv1->recv ], [ qw(hello animal), $cell, $push_cv1 ], "parameters" );

    ok( $pubsub->push_create("hello", "animal", $cell, my $push_cv2 = AE::cv), "got a write listener" );

    ok( $cv2->ready, "second cond var triggered" );
    is_deeply( [ $cv2->recv ], [ qw(hello animal), $cell, $push_cv2 ], "parameters" );

    ok( not($pubsub->push_create("hello", "animal", $cell, AE::cv)), "no more write listeners" );
}

{
    $pubsub->wait_for_path("hello", cv => my $cv1 = AE::cv);
    $pubsub->wait_for_cell("hello", "animal", cv => my $cv2 = AE::cv);

    ok( $pubsub->push_create("hello", "animal", $cell, my $push_cv1 = AE::cv), "got a write listener" );

    ok( $cv2->ready, "cond var triggered" );
    ok( not($cv1->ready), "second cond var not triggered" );

    is_deeply( [ $cv2->recv ], [ qw(hello animal), $cell, $push_cv1 ], "parameters" );

    ok( $pubsub->push_create("hello", "animal", $cell, my $push_cv2 = AE::cv), "got a write listener" );

    ok( $cv1->ready, "second cond var triggered" );
    is_deeply( [ $cv1->recv ], [ qw(hello animal), $cell, $push_cv2 ], "parameters" );

    ok( not($pubsub->push_create("hello", "animal", $cell, AE::cv)), "no more write listeners" );
}

{
    $pubsub->wait_for_path("hello", cv => my $cv = AE::cv(), timeout => 0.01);

    ok( not($cv->ready), "not ready" );

    throws_ok { $cv->recv } qr/timed out/, "timed out";
}

{
    $pubsub->wait_for_path("hello", cv => my $cv = AE::cv(), timeout => 0.01);

    ok( not($cv->ready), "not ready" );

    ok( $pubsub->push_create("hello", "animal", $cell, my $push_cv1 = AE::cv), "got a write listener" );

    lives_ok { $cv->recv } "no time out";
}

done_testing;

