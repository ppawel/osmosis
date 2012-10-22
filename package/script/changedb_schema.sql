-- Database creation script for the change db schema.

-- Drop all tables if they exist.
DROP TABLE IF EXISTS changes;
DROP TABLE IF EXISTS changesets;

-- Create a table for changes.
CREATE TABLE changes (
    id bigserial NOT NULL,
    user_id int NOT NULL,
    version int NOT NULL,
    changeset_id bigint NOT NULL,
    tstamp timestamp without time zone NOT NULL,
    action character varying(10) NOT NULL,
    element_type character varying(10) NOT NULL,
    element_id bigint NOT NULL,
    old_tags hstore,
    new_tags hstore,
    geom geometry
);

-- Create a table for changesets.
CREATE TABLE changesets (
  id bigserial NOT NULL,
  user_id bigint NOT NULL,
  created_at timestamp without time zone NOT NULL,
  closed_at timestamp without time zone NOT NULL,
  num_changes integer NOT NULL DEFAULT 0,
  tags hstore
);

ALTER TABLE ONLY changes ADD CONSTRAINT pk_changes PRIMARY KEY (id);
ALTER TABLE ONLY changesets ADD CONSTRAINT pk_changesets PRIMARY KEY (id);
