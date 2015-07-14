######################################################################
## Copyright (C) 2015- The University of Notre Dame
## This software is distributed under the GNU General Public License.
##    See the file COPYING for details.
#######################################################################
#
#### See module documentation at the end of this file.
#

package Chirp::Stat;

use v5.8.8;
use strict;
use warnings;

use CChirp;

use Carp qw(croak);

use overload
	'""' => sub { my $self = shift;  return sprintf("[%s uid:%d gid:%d size:%d]", $self->path, $self->uid, $self->gid, $self->size) }
;


sub Chirp::Stat::__new {
	# path=>, cstat=>
	my ($class, $path, $cinfo) = @_;

	croak('A path should be given.')             unless $path;
	croak('struct stat from C should be given.') unless $cinfo;

	return bless {
		__path  => $path,
		__cstat => $cinfo},
	$class;
}

sub path {
	my $self = shift;
	return $self->{__path};
}

sub device {
	my $self = shift;
	return $self->{__cstat}->{cst_dev};
}

sub inode {
	my $self = shift;
	return $self->{__cstat}->{cst_ino};
}

sub mode {
	my $self = shift;
	return $self->{__cstat}->{cst_mode};
}

sub nlink {
	my $self = shift;
	return $self->{__cstat}->{cst_nlink};
}

sub uid {
	my $self = shift;
	return $self->{__cstat}->{cst_uid};
}

sub gid {
	my $self = shift;
	return $self->{__cstat}->{cst_gid};
}

sub rdev {
	my $self = shift;
	return $self->{__cstat}->{cst_rdev};
}

sub size {
	my $self = shift;
	return $self->{__cstat}->{cst_size};
}

sub block_size {
	my $self = shift;
	return $self->{__cstat}->{cst_blksize};
}

sub blocks {
	my $self = shift;
	return $self->{__cstat}->{cst_blocks};
}

sub atime {
	my $self = shift;
	return $self->{__cstat}->{cst_atime};
}

sub mtime {
	my $self = shift;
	return $self->{__cstat}->{cst_mtime};
}

sub ctime {
	my $self = shift;
	return $self->{__cstat}->{cst_ctime};
}

1;

__END__

=head1 NAME

Chirp::Stat - Perl Chirp file stat information, much like Unix C<< stat >>
structure.

=head1 SYNOPSIS

The objects and methods provided by this package correspond to the native C API
in chirp_types.h. This module is automatically loaded with C<< Chirp::Client >>.

Chirp::Stat objects are not be created directly, but instead are the result of
calling Chirp::Client::stat and Chirp::Client::ls.

head1 EXAMPLE

		use Chirp::Client;
		my $client = Chip::Client->new(localhost => 'localhost:9000');
		my $s = $client->stat('/myfile.txt');
		print $s->size, "\n";


=head1 METHODS

=head2 Chirp::Stat

=head3 C<< path >>

The target path.


=head3 C<< device >>

ID of device containing file.

=head3 C<< inode >>

inode number


=head3 C<< mode >>

file mode permissions


=head3 C<< nlinks >>

number of hard links


=head3 C<< uid >>

user ID of owner


=head3 C<< gid >>

group ID of owner


=head3 C<< rdev >>

device ID if special file


=head3 C<< size >>

total size, in bytes


=head3 C<< blksize >>

block size for file system I/O


=head3 C<< blocks >>

number of 512B blocks allocated


=head3 C<< atime >>

number of seconds since epoch since last access


=head3 C<< mtime >>

number of seconds since epoch since last modification


=head3 C<< ctime >>

number of seconds since epoch since last status change

=cut
