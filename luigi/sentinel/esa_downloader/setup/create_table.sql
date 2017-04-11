-- Table: sentinel

-- DROP TABLE sentinel;

CREATE TABLE sentinel
(
  uniqueid uuid NOT NULL,
  polygonindex integer NOT NULL,
  title character varying(500) NOT NULL,
  ingestiondate date NOT NULL,
  beginposition character varying(500),
  endposition character varying(500),
  orbitdirection character varying(500),
  producttype character varying(500),
  orbitno integer,
  relorbitno integer,
  platform character varying(500),
  footprint geometry(Polygon,4326),
  centroid geometry(Point,4326),
  location character varying,
  CONSTRAINT sentinel_pkey PRIMARY KEY (uniqueid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE sentinel
  OWNER TO admin;

-- Index: idx_sentinel_centroid

-- DROP INDEX idx_sentinel_centroid;

CREATE INDEX idx_sentinel_centroid
  ON sentinel
  USING gist
  (centroid);

-- Index: idx_sentinel_footprint

-- DROP INDEX idx_sentinel_footprint;

CREATE INDEX idx_sentinel_footprint
  ON sentinel
  USING gist
  (footprint);

-- Index: idx_sentinel_ingestiondate

-- DROP INDEX idx_sentinel_ingestiondate;

CREATE INDEX idx_sentinel_ingestiondate
  ON sentinel
  USING btree
  (ingestiondate);

