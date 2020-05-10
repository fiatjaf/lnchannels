CREATE OR REPLACE FUNCTION abbr (id text) RETURNS text AS $$
  SELECT substring(id from 1 for 3) || '…' || substring(id from char_length(id) - 3);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION inter (x jsonb, y jsonb) RETURNS text AS $$
  SELECT xr.value
  FROM jsonb_array_elements_text(x) AS xr
  INNER JOIN jsonb_array_elements_text(y) AS yr
          ON xr.value = yr.value
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION diff (x jsonb, y jsonb) RETURNS text AS $$
  SELECT (x - (SELECT array_agg(value) FROM jsonb_array_elements_text(y)))->>0
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION matches (x jsonb, y jsonb) RETURNS boolean AS $$
  SELECT x ?| (SELECT array_agg(value) FROM jsonb_array_elements_text(y))
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION index (arr jsonb, item text) RETURNS int AS $$
  SELECT CASE WHEN arr->>0 = item THEN 0 ELSE 1 END
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION node_race()
RETURNS TABLE (
  alias text,
  color text,
  block int,
  nchannels numeric
) AS $$
  WITH topnodes AS (
    SELECT pubkey, alias, color FROM nodes
    ORDER BY (closedchannels + openchannels) DESC
    LIMIT 40
  ), topnodeschannels AS (
    SELECT
      pubkey, alias, color,
      (open->>'block')::int AS open_block,
      (close->>'block')::int AS close_block
    FROM topnodes
    LEFT JOIN channels ON channels.nodes ? pubkey
  ), blocks (block) AS (
    SELECT generate_series(578600, (SELECT last_block FROM last_block), 1000)
  ), keyframes AS (
    SELECT pubkey, alias, color,
      (blocks.block / 1000) AS block,
      (count(open_block) - count(close_block)) AS var
    FROM blocks
    INNER JOIN topnodeschannels ON open_block <= blocks.block
      AND ( CASE
          WHEN blocks.block = 578600 THEN true
          ELSE open_block >= blocks.block - 1000
        END )
    GROUP BY pubkey, alias, color, (blocks.block / 1000)
  )
  SELECT
    alias,
    color,
    block * 1000 AS block,
    sum(var) OVER (PARTITION BY pubkey ORDER BY block) AS nchannels
  FROM keyframes
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION hierarchy(nodepubkey text, depth int) RETURNS jsonb AS $$
  DECLARE
    name text;
  BEGIN
    SELECT alias INTO name FROM nodes WHERE pubkey = nodepubkey;
    RETURN jsonb_build_object('name', name,
                              'children', get_children(nodepubkey, depth));
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_children(nodepubkey text, depth int) RETURNS jsonb
AS $$
  DECLARE
    child RECORD;
    children jsonb[];
    node jsonb;
  BEGIN
    children := ARRAY[]::jsonb[];

    FOR child IN
      SELECT
        (nodes - nodepubkey)->>0 AS id,
        nodes.alias AS name
      FROM channels
      INNER JOIN nodes ON nodes.pubkey = (channels.nodes - nodepubkey)->>0
      WHERE nodes ? nodepubkey
    LOOP
      node := jsonb_build_object('name', coalesce(child.name, abbr(child.id)));

      IF depth > 1 THEN
        node = jsonb_set(node, '{children}', get_children(child.id, depth - 1), true);
      END IF;

      children = array_append(children, node);
    END LOOP;

    RETURN to_jsonb(children);
  END;
$$ LANGUAGE plpgsql;

select hierarchy('02bed1812d3824f7cc4ccd38da5d66a29fcfec146fe95e26cd2e0d3f930d653a8d', 3);
