#!/usr/bin/env perl

use Mojo::Base -strict, -signatures;
use Mojo::JSON qw(decode_json);
use Mojo::File;
use Mojo::Collection;
use constant CHUNK_SIZE => 1000;

use DDP;

sub read_data($fn, $target_city) {
  die "Need File and City" if !$fn or !$target_city;
  my $fh = Mojo::File->new($fn)->open('<') or die "Couldn't open $fn";
  my @data_for_city;

  LOOP:
  while(1) {

    NEXT_CHUNCK:
    for (1..CHUNK_SIZE) {
      my $line = <$fh>;
      last LOOP unless defined $line;
      my $data = parse($line);

      if ($data->{properties}{city} ne $target_city){
        <$fh> for 1..CHUNK_SIZE; # Just discard the next CHUNK
        last NEXT_CHUNCK;
      }
      #next unless $data->{properties}{city} ne $target_city;
      push @data_for_city, my $d = {line => $line, nu => $. };
      p $d;
    }
  }
  close $fh;
  return Mojo::Collection->new(@data_for_city);
}

sub parse($line){
  my $register = decode_json($line);
  $register->{line_nu} = $.;
  #p $register;
  return $register;
}

my $city = "Ubatuba";
my $file = "./source.geojson";
my $results = read_data($file, $city);
my $rfile = Mojo::File->new("./results_for_$city.geojson");
#p $results;
$rfile->spew($results->map(sub { $_->{line} })->join("\n")) if $results->size > 0;
