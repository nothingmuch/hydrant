package Hydrant::Service::REST;
use Moose;

use Coro;
use Coro::AnyEvent;

use Scalar::Util qw(looks_like_number);
use Plack::Server::Coro;

use MooseX::Types::Moose qw(Bool);

use namespace::clean -except => 'meta';

has store => (
    does => "Hydrant::Role::API",
    is   => "ro",
    required => 1,
);

has started => (
    isa => Bool,
    is  => "ro",
    writer => "_started",
    init_arg => undef,
);

sub BUILD {
    my ( $self, $p ) = @_;

    $self->_server( Plack::Server::Coro->new(%$p) );
}

has server => (
    isa => "Object",
    is  => "ro",
    writer => "_server",
);

sub start {
    my $self = shift;

    return if $self->started;

    $self->server->run(sub {
        $self->handle_request(@_);
    });

    $self->_started(1);
}

sub handle_request {
    my ( $self, $env ) = @_;

    my $method = "handle_" . lc($env->{REQUEST_METHOD});

    if ( my $body = $self->can($method) ) {
        return $self->$body($env);
    } else {
        return [ 501, [], [] ];
    }
}

sub handle_get {
    my ( $self, $env ) = @_;

    if ( my @path = $self->_path_to_cell($env) ) {
        my $cell = $self->store->get(
            @path,
            $self->_env_opts($env),
        )->recv;

        if ( $cell ) {
            return [ 200, [], $cell->open ];
        } else {
            return [ 404, [], [] ];
        }
    } else {
        return [ 404, [], [] ];
    }
}

sub handle_put {
    my ( $self, $env ) = @_;

    if ( my @path = $self->_path_to_cell($env) ) {

        my $input = $env->{'psgi.input'};

        my $remaining = $env->{CONTENT_LENGTH} || 0;

        my $str = '';

        while ( $remaining > 0 ) {
            $remaining -= $input->read($str, $remaining, length($str));
        }

        my $cell = Hydrant::Backend::Memory::Cell->new( _blob => \$str );

        my $stored = $self->store->put(
            @path,
            $cell,
            $self->_env_opts($env),
        )->recv;
        
        if ( $stored  ) {
            return [ 200, [], [] ];
        } else {
            return [ 500, [], [] ];
        }
    } else {
        return [ 500, [], [] ];
    }
}

sub handle_delete {
    my ( $self, $env ) = @_;

    if ( my @path = $self->_path_to_cell($env) ) {

        my $deleted = $self->store->delete(
            @path,
            $self->_env_opts($env),
        )->recv;


        if ( $deleted ) {
            return [ 200, [], [] ];
        } else {
            return [ 500, [], [] ];
        }
    } else {
        return [ 500, [], [] ];
    }
}

sub _env_opts {
    my ( $self, $env ) = @_;

    my @opts;

    if ( my $value = $env->{HTTP_X_HYDRANT_BLOCK} ) {
        if ( $value =~ /^\s*true\s*$/i ) {
            push @opts, block => 1;
        } elsif ( looks_like_number($value) and $value > 0 ) {
            push @opts, block => 1, timeout => +$value;
        }
    }

    return @opts;
}

sub _path_to_cell {
    my ( $self, $env ) = @_;

    if ( my ( $path, $name ) = ( $env->{PATH_INFO} =~ m{ ^ /? (.*?) / ([^/]+) $ }x ) ) {
        return ( $path, $name );
    } else {
        return;
    }
}

sub _path_to_prefix {
    my ( $self, $env ) = @_;

    my ( $path ) = ( $env->{PATH_INFO} =~ m{ ^ /? (.*?) / $ }x );

    return $path;
}

sub _req_to_cell {

}

sub _req_to_path {
    
}

=pod

X-Hydrant-Block: true | Int | Float

get cell

get key list


Cell
    get/head
        if match etag
        if none match etag
        if range
            middleware
        
    put
        if match etag
            update
        if none match *
            insert
        otherwise
            overwrite 

    delete
        if match etag
        otherwise
            unconditional


post
    

=cut

__PACKAGE__->meta->make_immutable;

# ex: set sw=4 et:

__PACKAGE__

__END__
