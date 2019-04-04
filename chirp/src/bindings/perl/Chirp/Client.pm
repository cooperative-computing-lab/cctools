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

use POSIX qw(strerror);
use Errno qw(:POSIX);
use Carp  qw(croak);

use CChirp;
use Chirp::Stat;


sub croak_errno {
	my ($errno, $msg) = @_;
	my $errno_msg     = strerror($errno);

	croak("$msg ($errno_msg)");
}


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

sub DESTROY {
    my ($self) = @_;
    chirp_reli_disconnect($self->hostport);
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
	my $identity = chirp_wrap_whoami($self->hostport, $self->__stoptime(%args));

	croak_errno($!, "Could not get my identity from server.") unless $identity;
}

sub listacl {
	my ($self, $path, %args) = @_;
	my $acls = chirp_wrap_listacl($self->hostport, $path, $self->__stoptime(%args));

	croak_errno($!, "Could not get ACL from path '$path'.") unless $acls;

	return split(/\n/, $acls);
}

sub setacl {
	my ($self, $path, $subject, $rights, %args) = @_;

	my $result = chirp_reli_setacl($self->hostport, $path, $subject, $rights, $self->__stoptime(%args));

	croak_errno($!, "Could not modify acl on '$path' for '$subject' to '$rights'.") if $result < 0;
	return $result;
}

sub resetacl {
	my ($self, $path, $rights, %args) = @_;

	my $result  = chirp_reli_resetacl($self->hostport, $path, $rights, $self->__stoptime(%args));

	croak_errno($!, "Could not set acl on '$path' for '@{[$self->identity]}' to '$rights'.") if $result < 0;
	return $result;
}

sub ls {
	my ($self, $path, %args) = @_;

	croak('A path should be given.')             unless $path;

	my $dr = chirp_reli_opendir($self->hostport, $path, $self->__stoptime(%args));

	croak_errno($!, "Could not list path '$path'.") unless $dr;

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

	croak_errno($!, "Could not stat path '$path'.") unless $info;

	return Chirp::Stat->__new($path, $info);
}

sub chmod {
	my ($self, $path, $mode, %args) = @_;
	my $result = chirp_reli_chmod($self->hostport, $path, $mode, $self->__stoptime(%args));

	croak_errno($!, "Could not change mode of path '$path' to '$mode'") unless $result >= 0;

	return $result;
}

sub put {
	# self, source, destination=>, absolute_stop_time=>, timeout=>
	my ($self, $source, %args) = @_;

	$args{destination} ||= $source;

	my $result = chirp_recursive_put($self->hostport, $source, $args{destination}, $self->__stoptime(%args));

	croak_errno($!, "Could not put path '$source' into '$args{destination}'.") if $result < 0;
	return $result;
}

sub get {
	# self, source, destination=>, absolute_stop_time=>, timeout=>
	my ($self, $source, %args) = @_;

	$args{destination} ||= $source;

	my $result = chirp_recursive_get($self->hostport, $source, $args{destination}, $self->__stoptime(%args));

	croak_errno($!, "Could not get path '$source' to '$args{destination}'.") if $result < 0;
	return $result;
}

sub rm {
	my ($self, $path, %args) = @_;

	my $result = chirp_reli_rmall($self->hostport, $path, $self->__stoptime(%args));

	croak_errno($!, "Could not recursively remove path '$path'.") if $result < 0;
	return $result;
}

sub mkdir {
	my ($self, $path, %args) = @_;

	$args{mode}         ||= 0755;
	$args{ignore_exist} ||= 0;

	my $result = chirp_reli_mkdir_recursive($self->hostport, $path, $args{mode}, $self->__stoptime(%args));

	if( $result < 0 && ! ($args{ignore_exist} && $!{EEXIST}) ) {
		croak_errno($!, "Could not recursively create directory '$path'.") if $result < 0;
	}

	return $result;
}

sub hash {
	my ($self, $path, %args) = @_;

	$args{algorithm} ||= 'sha1';

	my $hash_hex = chirp_wrap_hash($self->hostport, $path, $args{algorithm}, $self->__stoptime(%args));

	croak_errno($!, "Could not hash path '$path'.\n") unless $hash_hex;

	return $hash_hex;
}


eval "use JSON qw(to_json from_json);";
if ($@) {
	# JSON module is not installed.
	sub to_json ($@) {
		croak("The chirp job interface needs the JSON module from CPAN.\n");
	}

	sub from_json ($@) {
		croak("The chirp job interface needs the JSON module from CPAN.\n");
	}
}

sub __json_of_ids {
	my ($self, @numbers) = @_;
	return to_json([ map { $_ + 0 } @numbers ]);
}

sub job_create {
	my ($self, $job_description) = @_;

	my $job_json = to_json($job_description);

	my $job_id   = chirp_wrap_job_create($self->hostport, $job_json, $self->__stoptime);

	croak_errno($!, "Could not create job.") if $job_id < 0;
	return $job_id;
}

sub job_commit {
	my ($self, @job_ids) = @_;

	my $job_ids_str = $self->__json_of_ids(@job_ids);
	my $result      = chirp_wrap_job_commit($self->hostport, $job_ids_str, $self->__stoptime);

	croak_errno($!, "Could not commit jobs: $job_ids_str.") if $result < 0;
	return $result;
}

sub job_kill {
	my ($self, @job_ids) = @_;

	my $job_ids_str = $self->__json_of_ids(@job_ids);
	my $result      = chirp_wrap_job_kill($self->hostport, $job_ids_str, $self->__stoptime);

	croak_errno($!, "Could not kill jobs: $job_ids_str.") if $result < 0;
	return $result;
}

sub job_reap {
	my ($self, @job_ids) = @_;

	my $job_ids_str = $self->__json_of_ids(@job_ids);
	my $result      = chirp_wrap_job_reap($self->hostport, $job_ids_str, $self->__stoptime);

	croak_errno($!, "Could not reap jobs: $job_ids_str.") if $result < 0;
	return $result;
}

sub job_status {
	my ($self, @job_ids) = @_;

	my $job_ids_str = $self->__json_of_ids(@job_ids);
	my $status      = chirp_wrap_job_status($self->hostport, $job_ids_str, $self->__stoptime);

	croak_errno($!, "Could not get status of jobs: $job_ids_str.\n") unless $status;
	return from_json($status);
}

sub job_wait {
	my ($self, $waiting_time, %args) = @_;
	$args{job_id} ||= 0;

	my $state = chirp_wrap_job_wait($self->hostport, $args{job_id}, $waiting_time, $self->__stoptime);

	croak_errno($!, "Error when waiting for job: $args{job_id}.\n") unless $state;

	my $jsons = from_json($state);

	# if only one status was requested, return first element of the list.
	return $jsons->[0] if ($args{job_id} > 0);
	return $jsons;
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

=item hostport=>          The host:port of the server (default is localhost:9094).
=item timeout=>           The time to wait for a server response on every request (default is 60).
=item authentication=>    A list of prefered authentications. E.g., ['tickets', 'unix']
=item debug=>             Generate client debug output (default 0).

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


=head3 C<< setacl(path) >>

Modifies the ACL for the given directory.

=over 12

=item path                Target directory.

=item subject             Target subject.

=item rights              Permissions to be granted.

=back


=head3 C<< resetacl(path) >>

Set the ACL for the given directory to be only for the rights to the calling user.

=over 12

=item path                Target directory.

=item rights              Permissions to be granted.

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

=head3 C<< chmod(path, mode) >>

Changes permissions on path.

=over 12

=item path                Target file/directory.
=item mode                Desired permissions (e.g., 0755)

=back


=head3 C<< put(source, destination => ...) >>

Copies local file/directory source to the chirp server as file/directory
destination.  If destination is not given, source name is used. Dies on error.

=over 12

=item source              A local file or directory.

=item destination=>       File or directory name to use in the server (defaults to source).

=back


=head3 C<< get(source, destination => ...) >>

Copies server file/directory source to the local file/directory destination.
If destination is not given, source name is used. Dies on error.

=over 12

=item source              A server file or directory.

=item destination=>       File or directory name to be used locally (defaults to source).

=back


=head3 C<< rm(path) >>

Removes the given file or directory from the server. Dies on error.

=over 12

=item path                Target file/directory.

=back


=head3 C<< mkdir(path) >>

Recursively create the directories in path. Dies on error.

=over 12

=item path                Target directory.

=item mode=>              Unix permissions for the created directory (default 0755).

=item ignore_exist=>      Ignore error if directory exists (default 0)

=back


=head3 C<< hash(path, algorithm => ...) >>

Computes the checksum of path.

=over 12

=item path                Target file.

=item algorithm=>         One of 'md5' or 'sha1' (default if not given).

=back


=head2 Job interface

Chirp job interface. (Needs the JSON module from CPAN).

=head3 C<< job_create(job_description) >>

Creates a chirp job. See http://ccl.cse.nd.edu/software/manuals/chirp.html for details.

		my $job_description = {
			executable => "/bin/tar",
			arguments  =>  qw(tar -cf archive.tar a b),
			files      => { task_path => 'a',
							serv_path => '/users/magrat/a.txt'
							type      => 'INPUT' },
						  { task_path => 'b',
							serv_path => '/users/magrat/b.txt'
							type      => 'INPUT' },
						  { task_path => 'archive.tar',
							serv_path => '/users/magrat/archive.tar'
							type      => 'OUTPUT' }
		};

		my $job_id = $client->job_create($job_description);

=over 12

=item job_description A hash reference with a job chirp description.

=back


=head3 C<< job_commit(job_id, job_id, ...) >>

Commits (starts running) the jobs identified with the different job ids.

		$client->commit($job_id);

=item job_id,... Job ids of the chirp jobs to be committed.

=back


=head3 C<< job_kill(job_id, job_id, ...) >>

		$client->commit($job_id);

Kills the jobs identified with the different job ids.

=item job_id,... Job ids of the chirp jobs to be killed.

=back


=head3 C<< job_reap(job_id, job_id, ...) >>

Reaps the jobs identified with the different job ids.

=item job_id,... Job ids of the chirp jobs to be reaped.

=back


=head3 C<< job_status(job_id, job_id, ...) >>

Obtains the current status for each job id. The value returned is an array
reference, which contains a hash reference per job_id.

		my $jobs_states = $client->job_status($job_a, $job_b);

		for my $state ($jobs_states) {
			print 'Job ' . $state->{id} . ' is ' . $state->{status} . "\n";
		}

=item job_id,... Job ids of the chirp jobs to request an status update.

=back


=head3 C<< job_wait(waiting_time, job_id=>id) >>

Waits waiting_time seconds for the job_id to terminate. Return value is the
same as job_status. If the call timesout, an empty string is returned. If
job_id is missing, C<<job_wait>> waits for any of the user's job.

=item waiting_time maximum number of seconds to wait for a job to finish.
=item job_id id of the job to wait.

=back

1;


=cut
