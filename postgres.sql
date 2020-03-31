CREATE TABLE IF NOT EXISTS channels (
  short_channel_id text PRIMARY KEY,
  open_block integer,
  open_time timestamp,
  open_transaction text,
  open_fee integer,
  address text,
  close_block integer,
  close_time timestamp,
  close_transaction text,
  close_fee integer,
  close_type text,
  close_htlc_count integer,
  close_balance_a integer,
  close_balance_b integer,
  node0 text NOT NULL,
  node1 text NOT NULL,
  satoshis integer,
  last_seen timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS index_scid ON channels(short_channel_id);
CREATE INDEX IF NOT EXISTS index_node0 ON channels(node0);
CREATE INDEX IF NOT EXISTS index_node1 ON channels(node1);
CREATE INDEX IF NOT EXISTS index_opentx ON channels(open_transaction);
CREATE INDEX IF NOT EXISTS index_closetx ON channels(close_transaction);
CREATE INDEX IF NOT EXISTS index_address ON channels(address);
GRANT SELECT ON channels TO web_anon;

CREATE TABLE IF NOT EXISTS nodealiases (
  pubkey text NOT NULL,
  alias text NOT NULL,
  first_seen timestamp NOT NULL,
  last_seen timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS index_pubkey ON nodealiases(pubkey);
GRANT SELECT ON nodealiases TO web_anon;

CREATE TABLE IF NOT EXISTS policies (
  short_channel_id text NOT NULL,
  direction integer NOT NULL, -- 1 means from node0 to node1 and vice-versa
  base_fee_millisatoshi numeric(13) NOT NULL,
  fee_per_millionth numeric(13) NOT NULL,
  delay integer NOT NULL,
  update_time timestamp NOT NULL
);
GRANT SELECT ON policies TO web_anon;

CREATE MATERIALIZED VIEW nodes AS
  WITH nodealias AS (
    SELECT
      pubkey,
      (SELECT alias FROM nodealiases AS n WHERE nodealiases.pubkey = n.pubkey ORDER BY last_seen DESC LIMIT 1) AS alias
    FROM nodealiases
    GROUP BY pubkey
  ), open AS (
    SELECT pubkey, count(*) AS openchannels, sum(satoshis) AS capacity FROM (
      SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
    )x WHERE close_block IS NULL GROUP BY pubkey
  ), agg AS (
    SELECT pubkey,
      min(open_block) AS oldestchannel,
      count(close_block) AS closedchannels,
      avg(CASE WHEN close_block IS NOT NULL
        THEN close_block
        ELSE (SELECT open_block FROM channels ORDER BY open_block DESC LIMIT 1)
      END - open_block) AS avg_duration,
      avg(open_fee) AS avg_open_fee,
      avg(close_fee) AS avg_close_fee
    FROM (
      SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
    )z GROUP BY pubkey
  )
  SELECT
    agg.pubkey AS pubkey,
    coalesce(nodealias.alias, '') AS alias,
    agg.oldestchannel AS oldestchannel,
    coalesce(open.openchannels, 0) AS openchannels,
    agg.closedchannels AS closedchannels,
    coalesce(open.capacity, 0) AS capacity,
    agg.avg_duration AS avg_duration,
    agg.avg_open_fee AS avg_open_fee,
    agg.avg_close_fee AS avg_close_fee
  FROM agg
  LEFT JOIN nodealias ON agg.pubkey = nodealias.pubkey
  LEFT JOIN open ON agg.pubkey = open.pubkey;
CREATE INDEX IF NOT EXISTS index_node ON nodes(pubkey);
GRANT SELECT ON nodes TO web_anon;

CREATE MATERIALIZED VIEW globalstats AS
  WITH last_block AS (
    SELECT max(b) AS last_block
    FROM (
        SELECT max(open_block) AS b FROM channels
      UNION ALL
        SELECT max(close_block) AS b FROM channels
    )x
  ), channels AS (
    SELECT
      max(CASE
        WHEN close_block IS NOT NULL THEN close_block
        ELSE (SELECT last_block FROM last_block)
      END - open_block) AS max_duration,
      max(open_fee) AS max_open_fee,
      max(close_fee) AS max_close_fee,
      max(satoshis) AS max_satoshis
    FROM channels
  ), nodes AS (
    SELECT
      max(capacity) AS max_capacity,
      max(openchannels) AS max_openchannels,
      max(closedchannels) AS max_closedchannels,
      max(openchannels + closedchannels) AS max_allchannels,
      max(closedchannels / CASE WHEN openchannels > 0 THEN openchannels ELSE 0.0001 END) AS max_close_rate,
      max(avg_duration) AS max_average_duration,
      max(avg_open_fee) AS max_average_open_fee,
      max(avg_close_fee) AS max_average_close_fee
    FROM nodes
  )
  SELECT
    (SELECT last_block FROM last_block) AS last_block,
    channels.max_duration       AS max_channel_duration,
    channels.max_open_fee       AS max_channel_open_fee,
    channels.max_close_fee      AS max_channel_close_fee,
    channels.max_satoshis       AS max_channel_satoshis,
    nodes.max_capacity          AS max_node_capacity,
    nodes.max_openchannels      AS max_node_openchannels,
    nodes.max_closedchannels    AS max_node_closedchannels,
    nodes.max_allchannels       AS max_node_allchannels,
    nodes.max_close_rate        AS max_node_close_rate,
    nodes.max_average_duration  AS max_node_average_duration,
    nodes.max_average_open_fee  AS max_node_average_open_fee,
    nodes.max_average_close_fee AS max_node_average_close_fee
  FROM channels, nodes;
GRANT SELECT ON globalstats TO web_anon;

CREATE MATERIALIZED VIEW closetypes AS
  WITH base AS (
    SELECT (close_block/1000)*1000 AS blockgroup,
      CASE WHEN close_type = 'force' AND close_balance_b = 0 AND close_htlc_count = 0
        THEN 'force_unused'
        ELSE close_type
      END AS t,
      count(close_type) AS c,
      sum(satoshis) AS s
    FROM channels
    WHERE close_type IS NOT NULL
    GROUP BY blockgroup, t
  )
  SELECT
    blockgroup,
    coalesce((SELECT to_jsonb(x) FROM (SELECT c, s FROM base WHERE base.blockgroup = b.blockgroup AND t = 'unknown')x), '{"c": 0, "s": 0}'::jsonb) AS unknown,
    coalesce((SELECT to_jsonb(x) FROM (SELECT c, s FROM base WHERE base.blockgroup = b.blockgroup AND t = 'unused')x), '{"c": 0, "s": 0}'::jsonb) AS unused,
    coalesce((SELECT to_jsonb(x) FROM (SELECT c, s FROM base WHERE base.blockgroup = b.blockgroup AND t = 'mutual')x), '{"c": 0, "s": 0}'::jsonb) AS mutual,
    coalesce((SELECT to_jsonb(x) FROM (SELECT c, s FROM base WHERE base.blockgroup = b.blockgroup AND t = 'force')x), '{"c": 0, "s": 0}'::jsonb) AS force,
    coalesce((SELECT to_jsonb(x) FROM (SELECT c, s FROM base WHERE base.blockgroup = b.blockgroup AND t = 'force_unused')x), '{"c": 0, "s": 0}'::jsonb) AS force_unused,
    coalesce((SELECT to_jsonb(x) FROM (SELECT c, s FROM base WHERE base.blockgroup = b.blockgroup AND t = 'penalty')x), '{"c": 0, "s": 0}'::jsonb) AS penalty
  FROM base AS b
  WHERE blockgroup IS NOT NULL
  GROUP BY blockgroup
  ORDER BY blockgroup;
GRANT SELECT ON closetypes TO web_anon;

CREATE OR REPLACE FUNCTION home_chart(since_block integer)
RETURNS TABLE (
  blockgroup int,
  opened numeric,
  closed numeric,
  cap_change numeric(13),
  fee numeric,
  htlcs numeric
) AS $$
  SELECT blockgroup,
    sum(opened) AS opened,
    sum(closed) AS closed,
    sum(cap_change) AS cap_change,
    sum(fee) AS fee_total,
    sum(htlcs) AS htlcs
  FROM (
      -- initial aggregates
      SELECT (($1/100)-1)*100 AS blockgroup,
        count(*) AS opened,
        0 AS closed,
        sum(satoshis) AS cap_change,
        sum(open_fee) + sum(close_fee) AS fee,
        0 AS htlcs
      FROM channels
      WHERE open_block < $1
      GROUP BY (($1/100)-1)*100
    UNION ALL
      -- ongoing opens
      SELECT (open_block/100)*100 AS blockgroup,
        count(open_block) AS opened,
        0 AS closed,
        sum(satoshis) AS cap_change,
        sum(open_fee) AS fee,
        0 AS htlcs
      FROM channels
      WHERE open_block >= $1
      GROUP BY open_block/100
    UNION ALL
      -- ongoing closes
      SELECT (close_block/100)*100 AS blockgroup,
        0 AS opened,
        count(close_block) AS closed,
        -sum(satoshis) AS cap_change,
        sum(close_fee) AS fee,
        sum(close_htlc_count) AS htlcs
      FROM channels
      WHERE close_block IS NOT NULL AND close_block >= $1
      GROUP BY close_block/100
  ) AS main
  GROUP BY blockgroup
  ORDER BY blockgroup
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION longest_living_channels(last_block int)
RETURNS TABLE (
  short_channel_id text,
  open_block int,
  close_block int,
  duration int,
  closed bool,
  id0 text,
  name0 text,
  id1 text,
  name1 text,
  satoshis int
) AS $$
  SELECT
    short_channel_id,
    open_block,
    close_block,
    close_block - open_block AS duration,
    closed,
    node0 AS id0,
    coalesce((SELECT alias FROM nodes WHERE pubkey = node0), '') AS name0,
    node1 AS id1,
    coalesce((SELECT alias FROM nodes WHERE pubkey = node1), '') AS name1,
    satoshis
  FROM (
    SELECT short_channel_id,
      open_block,
      CASE
        WHEN close_block IS NOT NULL THEN close_block
        ELSE $1
      END AS close_block,
      (close_block IS NOT NULL) AS closed,
      node0, node1, satoshis
    FROM channels
  )x ORDER BY duration DESC LIMIT 50
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION node_channels (pubkey text)
RETURNS TABLE (
  short_channel_id text,
  peer_id text,
  peer_name text,
  peer_size bigint,
  open_block int,
  open_fee int,
  close_block int,
  close_fee int,
  satoshis int,
  outgoing_fee_per_millionth numeric(13),
  outgoing_base_fee_millisatoshi numeric(13),
  outgoing_delay int,
  incoming_base_fee_millisatoshi numeric(13),
  incoming_fee_per_millionth numeric(13),
  incoming_delay int,
  close_type text,
  close_htlc_count int
) AS $$
  SELECT
    channels.short_channel_id,
    CASE WHEN node0 = $1 THEN node1 ELSE node0 END AS peer_id,
    coalesce((SELECT alias FROM nodes WHERE pubkey = (CASE WHEN node0 = $1 THEN node1 ELSE node0 END)), '') AS peer_name,
    coalesce((SELECT capacity FROM nodes WHERE pubkey = (CASE WHEN node0 = $1 THEN node1 ELSE node0 END)), 0) AS peer_size,
    open_block,
    open_fee,
    close_block,
    close_fee,
    satoshis,
    split_part(p_out.fee_per_millionth, '~', 2)::numeric(13) AS outgoing_fee_per_millionth,
    split_part(p_out.base_fee_millisatoshi, '~', 2)::numeric(13) AS outgoing_base_fee_millisatoshi,
    split_part(p_out.delay, '~', 2)::int AS outgoing_delay,
    split_part(p_in.base_fee_millisatoshi, '~', 2)::numeric(13) AS incoming_base_fee_millisatoshi,
    split_part(p_in.fee_per_millionth, '~', 2)::numeric(13) AS incoming_fee_per_millionth,
    split_part(p_in.delay, '~', 2)::int AS incoming_delay,
    close_type,
    close_htlc_count
  FROM channels
  LEFT OUTER JOIN (
    SELECT
      short_channel_id,
      direction,
      max(update_time || '~' || base_fee_millisatoshi) AS base_fee_millisatoshi,
      max(update_time || '~' || fee_per_millionth) AS fee_per_millionth,
      max(update_time || '~' || delay) AS delay
    FROM policies
    GROUP BY short_channel_id, direction
  ) AS p_out
     ON p_out.short_channel_id = channels.short_channel_id
    AND p_out.direction = CASE WHEN node0 = $1 THEN 1 ELSE 0 END
  LEFT OUTER JOIN (
    SELECT
      short_channel_id,
      direction,
      max(update_time || '~' || base_fee_millisatoshi) AS base_fee_millisatoshi,
      max(update_time || '~' || fee_per_millionth) AS fee_per_millionth,
      max(update_time || '~' || delay) AS delay
    FROM policies
    GROUP BY short_channel_id, direction
  ) AS p_in
     ON p_in.short_channel_id = channels.short_channel_id
    AND p_in.direction = CASE WHEN node0 = $1 THEN 0 ELSE 1 END
  WHERE node0 = $1 OR node1 = $1
  ORDER BY open_block DESC
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION channel_data (short_channel_id text)
RETURNS TABLE (
  open_block int, open_fee int, open_transaction text, open_time timestamp,
  close_block int, close_fee int, close_transaction text, close_time timestamp,
  address text, node0 text, node1 text, satoshis int,
  short_channel_id text, node0name text, node1name text,
  close_type text, close_htlc_count int, close_balance_a int, close_balance_b int
) AS $$
SELECT
  open_block, open_fee, open_transaction, open_time,
  close_block, close_fee, close_transaction, close_time,
  address, node0, node1, satoshis,
  short_channel_id, coalesce(n0.alias, ''), coalesce(n1.alias, ''),
  close_type, close_htlc_count, close_balance_a, close_balance_b
FROM channels
LEFT OUTER JOIN nodes AS n0 ON n0.pubkey = node0
LEFT OUTER JOIN nodes AS n1 ON n1.pubkey = node1
WHERE short_channel_id = $1
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION search(query text)
RETURNS TABLE (
  url text,
  kind text,
  label text,
  closed bool
) AS $$
  WITH q AS (
    SELECT lower($1) AS query
  )
  SELECT DISTINCT ON (url) url, kind, label, closed FROM
  (
    SELECT
      'channel' AS kind,
      short_channel_id || ' (' || satoshis || ' sat)' AS label,
      '/channel/' || short_channel_id AS url,
      close_block IS NOT NULL AS closed
    FROM channels
    WHERE short_channel_id >= (SELECT query FROM q) and short_channel_id < (SELECT query FROM q) || 'Z' OR open_transaction = (SELECT query FROM q) OR close_transaction = (SELECT query FROM q) OR address = (SELECT query FROM q)
  UNION ALL
    SELECT
      'node' AS kind,
      alias || ' (' || openchannels || ' channels)' AS label,
      '/node/' || pubkey AS url,
      false AS closed
    FROM nodes
    WHERE pubkey >= (SELECT query FROM q) AND pubkey < (SELECT query FROM q) || 'Z'
  UNION ALL
    SELECT
      'node' AS kind,
      alias || ' (' || openchannels || ' channels)' AS label,
      '/node/' || nodes.pubkey AS url,
      false AS closed
    FROM nodes
    INNER JOIN
        ( SELECT pubkey
          FROM nodealiases
          WHERE lower(alias) LIKE '%' || (SELECT query FROM q) || '%'
        ) AS n
        ON nodes.pubkey = n.pubkey
  )x
$$ LANGUAGE SQL STABLE;
