#!/usr/bin/env perl

use v5.8.8;

use strict;
use warnings;

use Data::Dumper;

use Chirp::Client;

sub make_job {
	my ($job_number) = @_;

	return {
		executable => './my_script',
		arguments  => ['my_script', "$job_number"],
		files      => [
		{
			task_path => 'my_script',
			serv_path => '/my_script.sh',
			type    => 'INPUT',
			binding => 'LINK'
		},

		{ task_path => 'my.output',
			serv_path => "/my.$job_number.output",
			type      => 'OUTPUT' }
		] };
}

die "Usage: $0 HOST:PORT" if @ARGV < 1;

my ($hostport) = @ARGV;

my $client;
eval { $client = Chirp::Client->new(hostport       => $hostport,
									authentication => ['unix'],
									timeout        => 15,
									debug          => 1) };
die "Could not connect to server: $@" if $@;

print 'Chirp server sees me as ' . $client->identity . "\n";

my @jobs = map { make_job $_ } (1..10);

$client->put('my_script.sh', destination => '/my_script.sh');

my $jobs_running = 0;
for my $job (@jobs) {
	my $job_id = $client->job_create($job);
	$client->job_commit($job_id);
	$jobs_running++;
}

while($jobs_running) {
	my $job_states;
	$job_states = $client->job_wait(2);

	for my $job_state (@{$job_states}) {
		print 'job ' . $job_state->{id} . ' is ' . lc($job_state->{status}) . "\n";

		my $status = $job_state->{status};

		$client->job_reap( $job_state->{id} ) if $status eq 'FINISHED';
		$client->job_kill( $job_state->{id} ) if $status eq 'ERRORED';

		$jobs_running--;
	}
}


exit 0;

__END__
