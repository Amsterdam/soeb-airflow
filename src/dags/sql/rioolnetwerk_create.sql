BEGIN;
DROP TABLE IF EXISTS rioolnetwerk_new CASCADE;

CREATE TABLE rioolnetwerk_new
(
  id                  SERIAL PRIMARY KEY,
  cs_external_id      character varying(64) NOT NULL UNIQUE,
  wkb_geometry        geometry(Geometry,28992),
  street              character varying(150),
  housenumber         character varying(6),
  housnumberext       character varying(6),
  postalcode          character varying(6),
  district            character varying(40),
  countryiso          character varying(3),
  region              character varying(40),
  city                character varying(40),
  last_update         timestamp with time zone default current_timestamp,
  last_status_update  timestamp with time zone default current_timestamp,
  charging_cap_max    real
);

CREATE INDEX ON rioolnetwerk_new USING gist (wkb_geometry);
COMMIT;