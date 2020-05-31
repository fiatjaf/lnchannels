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

CREATE OR REPLACE FUNCTION node_policy_ranges(amount_msat int) RETURNS TABLE (
  cap numeric(13),
  fee_min numeric(13),
  fee_max numeric(13)
) AS $$
  SELECT
    nodes.pubkey,
    capacity,
    min(fee),
    max(fee)
  FROM nodes
  INNER JOIN channels ON channels.nodes ? nodes.pubkey
                     AND channels.close->>'block' IS NULL
  INNER JOIN (
    SELECT short_channel_id,
      (base_fee_millisatoshi + fee_per_millionth * 1000000 / 1000000)::numeric(13) AS fee
    FROM policies
  ) AS p ON channels.short_channel_id = p.short_channel_id
  WHERE openchannels > 0
  GROUP BY nodes.pubkey, nodes.capacity;
$$ LANGUAGE SQL STABLE;
