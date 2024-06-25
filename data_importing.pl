#!/usr/bin/env perl

=pod

Scripts for importing data from OpenAddress project. Set of scripts: parse_sp_address.pl
and data_importing.pl.

Main steps explained below:

=over 4

=item Step 1: Chunk the main data file ( source.geojson )

Our data source file has 5Gb size and contains the geometric data of each
address (unfortunately, with lots of errors and missing values) in a GeoJson
format. Each line one data entry. The script that deals in chunking it is
**parse_sp_address.pl**

=item Step 2: Import the chunk file generated

In this step our script make the data importation in bulk mode, without any
constraint to make a fast import to the database.

=item Step 3: Adjust the data to the constraints

In this step we use database scripts to make sure we do not have any duplicates and
to assure certain constraint and the create of indices for searching.

=back 4

data_importing.pl makes Step 2 and 3, and parse_sp_address.pl makes the first Step.

=cut

package main;
use Mojo::Base -strict, -signatures;
use Mojo::JSON qw(decode_json encode_json);
use Mojo::File;
use Mojo::Collection qw(c);
use Mojo::Pg;
use Syntax::Keyword::Try;
use SQL::Abstract::Plugin::InsertMulti;
use DDP;

my $dsn= "postgresql:///?service=gis";
my $pg = Mojo::Pg->new($dsn);

$pg->on(connection => sub ($pg, $dbh) {
    $pg->options(
      {
        AutoCommit => 0,
        AutoInactiveDestroy => 1,
        PrintError => 1,
        PrintWarn => 1,
        RaiseError => 1,
      }
    );
  }
);

sub set_database { $pg->migrations->from_data->migrate; }
sub reset_database {
  $pg->migrations->name('after_bulk')->from_data('main', 'after_bulk_insertion')->migrate(0);
  $pg->migrations->name('migrations')->from_data->migrate(0)->migrate; 
}

sub read_json_data($file) {
  die "Need a file" unless -e $file && -r $file;
  my $data = c(split /\n/, Mojo::File->new($file)->slurp)->map(sub {decode_json $_});
  return $data;
}

sub show_progress($data, $initial) {
  my $msg = sprintf "%.2f %% processed\n", 100 - ($data->size / $initial)*100;
  print $msg;
}

# save multiple input
sub save_using_copy($data, $table) {
  my $csv_file = './file.csv';
  my $initial = $data->size;

  while ($data->size > 0 ) {
    show_progress($data, $initial);
    my $bulk_data = bulk_text($data, 70000);
    my $dbh = $pg->db->dbh;
    try {
      $dbh->do("COPY ubatuba.addresses(id, geom, properties) FROM STDIN");
      $bulk_data->each( 
        sub {
          my $row = join"\t", $_->{id}, $_->{geom}, $_->{properties};
          $dbh->pg_putcopydata($row . "\n");
        }
      );
      $dbh->pg_putcopyend();
    }catch ($e) {
      warn "Error saving addresses: $e";
    }
  }
}

sub bulk_text($data, $length = 1000)
{
  my @partition = splice @$data, 0, $length;
  return c(@partition)->map( 
    sub { 
      { 
        id          => $_->{properties}{hash},
        geom        => encode_json($_->{geometry}),
        properties  => encode_json($_->{properties}),
      } 
    } 
  );
}

sub adjust_data {
  my $migrations = $pg->migrations->name('after_bulk')->from_data('main', 'after_bulk_insertion');
  $migrations->migrate;
}

sub main 
{
  reset_database;
  my $file ='./uniq_results_for_ubatuba.geojson';
  my $data = read_json_data($file);
  save_using_copy($data, 'ubatuba.addresses');
  adjust_data;
}

main;

__DATA__
@@migrations
-- 1 up
BEGIN;
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE SCHEMA IF NOT EXISTS ubatuba;
  CREATE TABLE ubatuba.addresses (
    id TEXT,
    properties TEXT,
    geom TEXT
  );
COMMIT;

-- 1 down
BEGIN;
  DROP SCHEMA IF EXISTS ubatuba CASCADE;
COMMIT;

@@after_bulk_insertion
-- 1 up
-- This makes the address with uniq ids
BEGIN;
  CREATE TABLE ubatuba.addresses_temp (LIKE ubatuba.addresses);
  INSERT INTO ubatuba.addresses_temp(id, properties, geom)
    SELECT DISTINCT ON (id) id, properties, geom
    FROM ubatuba.addresses;

  DROP TABLE ubatuba.addresses;

  ALTER TABLE ubatuba.addresses_temp RENAME TO addresses;                 
COMMIT;

-- 1 down
BEGIN;
SELECT 1; --NOOP
COMMIT;

-- 2 up
-- this makes the addresses to have primary key and geometry types
BEGIN;
  ALTER TABLE ubatuba.addresses ADD CONSTRAINT addresses_pk PRIMARY KEY (id);
  ALTER TABLE ubatuba.addresses ADD COLUMN geom_temp geometry(Point, 4326);
  UPDATE ubatuba.addresses 
    SET geom_temp = CASE
      WHEN geom = 'null' THEN NULL
      ELSE ST_GeomFromGeoJSON(geom)
    END;
  ALTER TABLE ubatuba.addresses DROP COLUMN geom;
  ALTER TABLE ubatuba.addresses RENAME COLUMN geom_temp TO geom;
COMMIT;

-- 2 down
BEGIN;
SELECT 1; --NOOP
COMMIT;

-- 3 up
BEGIN;
  ALTER TABLE ubatuba.addresses ADD COLUMN properties_temp jsonb;
  UPDATE ubatuba.addresses SET properties_temp = properties::jsonb;
  ALTER TABLE ubatuba.addresses DROP COLUMN properties;
  ALTER TABLE ubatuba.addresses RENAME COLUMN properties_temp TO properties;
COMMIT;

-- 3 down
BEGIN;
SELECT 1; --NOOP
COMMIT;
