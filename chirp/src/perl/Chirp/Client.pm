######################################################################
## Copyright (C) 2015- The University of Notre Dame
## This software is distributed under the GNU General Public License.
##    See the file COPYING for details.
#######################################################################
#
#### See module documentation at the end of this file.
#

package Chirp::Client;

use v5.8.8;
use strict;
use warnings;

use CChirp;

use Chirp::Stat;

use Carp qw(croak);

sub Chirp::Client::new {
	# hostport, timeout=>, authentication=>, debug=>

	my ($class, %args) = @_;

	$args{hostport}       ||= 'localhost:9094';
	$args{timeout}        ||= 60;
	$args{debug}          ||=  0;
	$args{tickets}        ||= [];
	$args{authentication} ||= [];

	if( $args{debug} ) {
		cctools_debug_config('chirp_perl_client');
		cctools_debug_flags_set('chirp');
	}

	if( @{$args{tickets}} && (! @{$args{authentication}}) ) {
		$args{authentication} = ['ticket'];
	}

	Chirp::Client->__set_tickets($args{tickets});

	if( @{$args{authentication}} ) {
		for (@{$args{authentication}}) {
			unless( auth_register_byname($_) > 0 ) {
				croak("'$_' is not a valid authentication method.");
			}
		}
	}
	else {
		auth_register_all();
	}

	my $c = bless {
		__hostport => $args{hostport},
		__timeout  => $args{timeout} },
	$class;

	$c->{__identity} = $c->whoami();

	croak("Could not authenticate with $args{hostport}.") unless $c->identity;

	return $c;
}

sub __set_tickets {
	my ($self, $tickets) = @_;

	my $tickets_str;

	if( defined $tickets && @{$tickets} ) {
		$tickets_str = join(',', @{$tickets});
	}
	else {
		$tickets_str = $ENV{CHIRP_CLIENT_TICKETS};
	}

	auth_ticket_load($tickets_str) if defined $tickets_str;
}

sub __stoptime {
	my ($self, %args) = @_;

	$args{timeout}            ||= $self->timeout;
	$args{absolute_stop_time} ||= time() + $args{timeout};

	return $args{absolute_stop_time}+0;
}

sub hostport {
	my ($self) = @_;
	return $self->{__hostport};
}


sub timeout {
	my ($self, $value) = @_;
	$self->{__timeout} = $value if $value;
	return $self->{__timeout};
}


sub identity {
	my ($self) = @_;
	return $self->{__identity};
}

sub whoami {
	my ($self, %args) = @_;
	return chirp_wrap_whoami($self->hostport, $self->__stoptime(%args));
}

sub listacl {
	my ($self, $path, %args) = @_;
	my $acls = chirp_wrap_listacl($self->hostport, $path, $self->__stoptime(%args));

	croak("Could not get ACL from path '$path'.\n") unless $acls;

	return split(/\n/, $acls);
}

sub ls {
	my ($self, $path, %args) = @_;
	my $dr = chirp_reli_opendir($self->hostport, $path, $self->__stoptime(%args));

	croak("Could not list path '$path'.\n") unless $dr;

	my @files;
	while (my $f = chirp_reli_readdir($dr)) {
		my $s = Chirp::Stat->__new($f->{name}, $f->{info});
		push @files, $s;
	}

	return @files;
}

sub stat {
	my ($self, $path, %args) = @_;
	my $info = chirp_wrap_stat($self->hostport, $path, $self->__stoptime(%args));

	croak("Could not stat path '$path'.\n") unless $info;

	return Chirp::Stat->__new($path, $info);
}

sub put {
	# self, source, destination=>, absolute_stop_time=>, timeout=>
	my ($self, $source, %args) = @_;

	$args{destination} ||= $source;

	my $status = chirp_recursive_put($self->hostport, $source, $args{destination}, $self->__stoptime(%args));

	croak("Could not put path '$source' into '$args{destination}' (status $status).\n") if $status < 0;
	return $status;
}

sub get {
	# self, source, destination=>, absolute_stop_time=>, timeout=>
	my ($self, $source, %args) = @_;

	$args{destination} ||= $source;

	my $status = chirp_recursive_get($self->hostport, $source, $args{destination}, $self->__stoptime(%args));

	croak("Could not get path '$source' to '$args{destination}' (status $status).\n") if $status < 0;
	return $status;
}

sub rm {
	my ($self, $path, %args) = @_;

	my $result = chirp_reli_rmall($self->hostport, $path, $self->__stoptime(%args));

	croak("Could not recursevely remove path '$path' (status $result).\n") if $result < 0;
	return $result;
}

1;

__END__

=head1 NAME

Chirp::Client - Perl Chirp client bindings

=head1 SYNOPSIS

The objects and methods provided by this package correspond to the
native C API in chirp_reli.h for client creation and control. This
module is automatically loaded with C<< Chirp::Client >>.

		use Chirp::Client;
		my $client = Chirp::Client('localhost:9094');
		print $client->whoami(timeout => 120), "\n";
		put $client->put('myfile', destination=>'/some/path/myfile');


=head1 METHODS

=head2 Chirp::Client

=head3 C<< Chirp::Client->new(hostport=>'addr:port', ...); >>

Creates a new chirp client connected to the server at addr:port.

=over 12

=item hostport          The host:port of the server (default is localhost:9094).
=item timeout           The time to wait for a server response on every request (default is 60).
=item authentication    A list of prefered authentications. E.g., ['tickets', 'unix']
=item debug             Generate client debug output.

=back


=head3 C<< hostport >>

Returns the hostport of the chirp server the client is connected.


=head3 C<< timeout >>

Returns the default timeout of the client when waiting for to the server.


=head3 C<< identity >>

Returns a string with identity of the client according to the server. It is the value of a call to C<< whoami >> just after the client connects to the server.



All the following methods receive the following optional keys:

=over 12

=item absolute_stop_time  If given, maximum number of seconds since epoch to wait for a server response. (Overrides any timeout.)

=item timeout             If given, maximum number of seconds to wait for a server response.

=back


=head3 C<< whoami >>

Returns a string with identity of the client according to the server.


=head3 C<< listacl(path) >>

Returns a string with the ACL of the given directory. Dies if path cannot be accessed.

=over 12

=item path                Target directory.

=back


=head3 C<< ls(path) >>

Returns a list with stat objects of the files in the path.  Dies if path cannot be accessed.

=over 12

=item path                Target file/directory.

=back



=head3 C<< stat(path) >>

Returns a Chirp::Stat object with information on path. Dies if path cannot be accessed.

=over 12

=item path                Target file/directory.

=back


=head3 C<< put(source, destination => ...) >>

Copies local file/directory source to the chirp server as file/directory
destination.  If destination is not given, source name is used. Dies on error.

=over 12

=item source              A local file or directory.

=item destination         File or directory name to use in the server (defaults to source).

=back


=head3 C<< get(source, destination => ...) >>

Copies server file/directory source to the local file/directory destination.
If destination is not given, source name is used. Dies on error.

=over 12

=item source              A server file or directory.

=item destination         File or directory name to be used locally (defaults to source).

=back


=head3 C<< rm(path) >>

Removes the given file or directory from the server. Dies on error.

=over 12

=item path                Target file/directory.

=back

=cut
