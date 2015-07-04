#!/usr/bin/env perl

use v5.8.8;

use strict;
use warnings;

use Chirp::Client;

sub write_some_file {
	my $filename = shift;
	$filename  ||= 'bar.txt';

	my $message = qq(One makeflow to rule them all, One catalog to find them,
One workqueue to bring them all and in the darkness bind them
In the land of condor where the Shadows lie.
);

	open my $file, '>', $filename || die("Could not open for writing: $filename.\n");
	print { $file } $message;
	close $file;
}


die "Usage: $0 HOST:PORT TICKET" if @ARGV < 2;

my ($hostport, $ticket) = @ARGV;

write_some_file();

my $client;
eval { $client = Chirp::Client->new(hostport       => $hostport,
									authentication => ['ticket'],
									tickets        => [$ticket],
									timeout        => 15,
									debug          => 1) };
die "Could not connect to server: $@" if $@;

print 'Chirp server sees me as ' . $client->identity . "\n";

eval {
		print "ACL on / is @{[$client->listacl('/')]}" . "\n";
		print "Contents of / are @{[$client->ls('/')]}" . "\n";
};
die "Could not access '/': $@" if $@;

eval {
	$client->put('bar.txt');
	$client->get('/bar.txt', destination => 'foo.txt')
};
die "Could not transfer file: $@" if $@;

eval {
	my $s = $client->stat('/bar.txt');
	print $s->path . ": size:" . $s->size . "\n";
};
die "Could not stat file: $@" if $@;

eval {
	print 'checksum of bar.txt is: ';
	print $client->hash('/bar.txt', algorithm => 'sha1') . "\n";
};
die "Could not compute hash of file: $@" if $@;


eval {
	$client->rm('/bar.txt');
};
die "Could not remove file: $@" if $@;

unlink 'bar.txt', 'foo.txt';

exit 0;

__END__
