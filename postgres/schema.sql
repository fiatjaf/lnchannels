CREATE TABLE IF NOT EXISTS channels (
  short_channel_id text PRIMARY KEY,
  nodes jsonb NOT NULL,

  -- node identification
  a int,
  b int,
  funder int,

  -- labeled sides
  closer text,

  open jsonb NOT NULL DEFAULT '{
    "block": null,
    "txid": null,
    "time": null,
    "fee": null,
    "address": null
  }',

  close jsonb NOT NULL DEFAULT '{
    "block": null,
    "txid": null,
    "time": null,
    "fee": null,
    "type": null,
    "balance": {
      "a": 0,
      "b": 0
    },
    "htlcs": []
  }',

  -- data to perform chain analysis with
  txs jsonb NOT NULL DEFAULT '{
    "a": [],
    "b": [],
    "funding": []
  }',

  satoshis integer NOT NULL,
  last_update timestamp NOT NULL
);

CREATE INDEX IF NOT EXISTS index_scid ON channels(short_channel_id);
CREATE INDEX IF NOT EXISTS index_nodes ON channels USING gin (nodes);
CREATE INDEX IF NOT EXISTS index_open ON channels USING gin (open);
CREATE INDEX IF NOT EXISTS index_close ON channels USING gin (close);
CREATE INDEX IF NOT EXISTS index_txs ON channels USING gin (txs);
GRANT SELECT ON channels TO web_anon;

-- channel age function that works both for closed and open channels
CREATE OR REPLACE FUNCTION age (c channels) RETURNS bigint AS $$
  SELECT
    CASE
      WHEN c.close->>'block' IS NULL THEN -- open
        (SELECT last_block FROM last_block) - (c.open->>'block')::bigint
      ELSE -- closed
        (c.close->>'block')::bigint - (c.open->>'block')::bigint
    END
$$ LANGUAGE SQL STABLE;

-- assess how much a channel closure was bad
CREATE OR REPLACE FUNCTION crash (c channels) RETURNS bigint AS $$
  SELECT
    CASE
      WHEN c.close->>'type' = 'penalty' THEN
        (c.close->'balance'->>c.closer)::int / 5000
      WHEN c.close->>'type' = 'force' THEN
        10
        + (SELECT
             sum(CASE WHEN value->>'offerer' = c.closer THEN 16 ELSE 8 END)
           FROM jsonb_array_elements(c.close->'htlcs'))
        + (CASE WHEN (c.close->'balance'->>'b')::int = 0 THEN 7 ELSE 0 END)
        + (144 * 5
          / ((c.close->>'block')::int - (c.open->>'block')::int))
      ELSE 0
    END
$$ LANGUAGE SQL STABLE;

-- translate short_channel_id into an integer and into a hex string
CREATE FUNCTION scid_int (c channels) RETURNS bigint AS $$
  SELECT
    ((split_part(c.short_channel_id, 'x', 1)::bigint & 'xffffff'::bit(24)::bigint) << 40)
  | ((split_part(c.short_channel_id, 'x', 2)::bigint & 'xffffff'::bit(24)::bigint) << 16)
  | ((split_part(c.short_channel_id, 'x', 3)::bigint & 'xffff'::bit(24)::bigint))
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION scid_hex (c channels) RETURNS text AS $$
  SELECT to_hex(c.scid_int)
$$ LANGUAGE SQL IMMUTABLE;

select short_channel_id, channels.scid_int from channels limit 10;

CREATE TABLE IF NOT EXISTS nodealiases (
  pubkey text NOT NULL,
  alias text NOT NULL,
  color text,
  first_seen timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS index_pubkey ON nodealiases(pubkey);
GRANT SELECT ON nodealiases TO web_anon;

CREATE TABLE IF NOT EXISTS features (
  pubkey text NOT NULL,
  features text NOT NULL,
  first_seen timestamp NOT NULL
);
GRANT SELECT ON features TO web_anon;

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
  WITH pubkeys AS (
    SELECT DISTINCT pubkey FROM (
        SELECT nodes->>0 AS pubkey FROM channels
      UNION ALL
        SELECT nodes->>1 AS pubkey FROM channels
    )x
  ), nodealias AS (
    SELECT DISTINCT ON (pubkey)
      p.pubkey,
      coalesce(n.alias, '') AS alias,
      n.color AS color
    FROM pubkeys AS p
    LEFT OUTER JOIN nodealiases AS n ON n.pubkey = p.pubkey
    ORDER BY p.pubkey, n.first_seen DESC
  ), open AS (
    SELECT pubkey, count(*) AS openchannels, sum(satoshis) AS capacity FROM (
        SELECT nodes->>0 AS pubkey, * FROM channels
      UNION ALL
        SELECT nodes->>1 AS pubkey, * FROM channels
    )x WHERE close->>'block' IS NULL GROUP BY pubkey
  ), agg AS (
    SELECT pubkey,
      min((open->>'block')::int) AS oldestchannel,
      count(close->>'block') AS closedchannels,
      avg(CASE WHEN close->>'block' IS NOT NULL
        THEN (close->>'block')::int
        ELSE (SELECT (open->>'block')::int FROM channels
              ORDER BY (open->>'block')::int DESC LIMIT 1)
      END - (open->>'block')::int) AS avg_duration,
      avg((open->>'fee')::int) AS avg_open_fee,
      avg((close->>'fee')::int) AS avg_close_fee,
      jsonb_build_object(
        'mutual', count(CASE WHEN close->>'type' = 'mutual' THEN true ELSE NULL END),
        'penalty', count(CASE WHEN close->>'type' = 'penalty' THEN true ELSE NULL END),
        'force', count(CASE WHEN close->>'type' = 'force' THEN true ELSE NULL END)
      ) AS close_types
    FROM (
      SELECT nodes->>0 AS pubkey, * FROM channels
        UNION ALL
      SELECT nodes->>1 AS pubkey, * FROM channels
    )z GROUP BY pubkey
  )
  SELECT
    agg.pubkey AS pubkey,
    nodealias.alias AS alias,
    nodealias.color AS color,
    (SELECT implementation FROM implementations WHERE pubkey = agg.pubkey) AS software,
    agg.oldestchannel AS oldestchannel,
    coalesce(open.openchannels, 0) AS openchannels,
    agg.closedchannels AS closedchannels,
    coalesce(open.capacity, 0) AS capacity,
    agg.avg_duration AS avg_duration,
    agg.avg_open_fee AS avg_open_fee,
    agg.avg_close_fee AS avg_close_fee,
    agg.close_types AS close_types
  FROM agg
  LEFT JOIN nodealias ON agg.pubkey = nodealias.pubkey
  LEFT JOIN open ON agg.pubkey = open.pubkey;
CREATE INDEX IF NOT EXISTS index_node ON nodes(pubkey);
GRANT SELECT ON nodes TO web_anon;

CREATE MATERIALIZED VIEW last_block AS
  SELECT max(b) AS last_block
  FROM (
      SELECT max((open->>'block')::int) AS b FROM channels
    UNION ALL
      SELECT max((close->>'block')::int) AS b FROM channels
  )x;
GRANT SELECT on last_block TO web_anon;

CREATE MATERIALIZED VIEW globalstats AS
  WITH channels AS (
    SELECT
      max(CASE WHEN close->>'block' IS NULL
        THEN (close->>'block')::int
        ELSE (SELECT last_block FROM last_block)
      END - (open->>'block')::int) AS max_duration,
      max((open->>'fee')::int) AS max_open_fee,
      max((close->>'fee')::int) AS max_close_fee,
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
    SELECT
      ((close->>'block')::int / 1000) * 1000 AS blockgroup,
      close->>'type' AS typ,
      (close->'balance'->>'b')::int > 0 AS used,
      jsonb_array_length(close->'htlcs') > 0 AS inflight,
      count(*) AS c,
      sum(satoshis) AS s
    FROM channels
    WHERE close->>'block' IS NOT NULL
    GROUP BY blockgroup, typ, used, inflight
  )
  SELECT
    blockgroup,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'unknown' OR typ IS NULL)x), '{"c": 0, "s": 0}'::jsonb) AS unknown,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'mutual' AND NOT used)x), '{"c": 0, "s": 0}'::jsonb) AS mutual_unused,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'mutual' AND used)x), '{"c": 0, "s": 0}'::jsonb) AS mutual,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'force' AND inflight)x), '{"c": 0, "s": 0}'::jsonb) AS force_inflight,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'force' AND used AND NOT inflight)x), '{"c": 0, "s": 0}'::jsonb) AS force,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'force' AND NOT used AND NOT inflight)x), '{"c": 0, "s": 0}'::jsonb) AS force_unused,
    coalesce((SELECT to_jsonb(x) FROM (SELECT coalesce(sum(c), 0) AS c, coalesce(sum(s), 0) AS s FROM base WHERE base.blockgroup = b.blockgroup AND typ = 'penalty')x), '{"c": 0, "s": 0}'::jsonb) AS penalty
  FROM base AS b
  WHERE blockgroup IS NOT NULL
  GROUP BY blockgroup
  ORDER BY blockgroup;
GRANT SELECT ON closetypes TO web_anon;

CREATE MATERIALIZED VIEW implementations AS
  WITH daemon (name, version, featurebits) AS (
    VALUES
      ('c-lightning', '0.6', '88'),
      ('c-lightning', '0.6.1', '8a'),
      ('c-lightning', '0.6.2', '8a'),
      ('c-lightning', '0.6.3', '88'),
      ('c-lightning', '0.7.0', '8a'),
      ('c-lightning', '0.7.1', 'aa'),
      ('c-lightning', '0.7.2.1', 'aa'),
      ('c-lightning', '0.7.3', '28a2'),
      ('c-lightning', '0.8.0', '02aaa2'),
      ('c-lightning', '0.8.1', '02aaa2'),
      ('c-lightning', '0.8.2-keysend', '8000000002aaa2'),
      ('eclair', '0.3.1', '8a'),
      ('eclair', '0.3.2', '0a8a'),
      ('eclair', '0.3.3', '0a8a'),
      ('eclair', '0.3.3-mpp', '028a8a'),
      ('eclair', 'acinq_node', '0a8a8a'),
      ('eclair', 'guess', '0200'),
      ('eclair', '0.3.4', '0a8a'),
      ('eclair', '0.3.4-wumbo', '080a8a'),
      ('eclair', '0.3.4-mpp', '028a8a'),
      ('eclair', '0.4', '0a8a'),
      ('eclair', '0.4-wumbo', '080a8a'),
      ('eclair', '0.4-mpp', '028a8a'),
      ('eclair', '0.4-mpp-wumbo', '0a8a8a'),
      ('lnd', '0.4.1', '08'),
      ('lnd', '0.4.2', '08'),
      ('lnd', '0.5', '82'),
      ('lnd', '0.5.2', '82'),
      ('lnd', '0.6', '81'),
      ('lnd', '0.6.1', '81'),
      ('lnd', '0.7.1', '81'),
      ('lnd', '0.8.0', '2281'),
      ('lnd', '0.8.1', '2281'),
      ('lnd', '0.8.2', '2281'),
      ('lnd', '0.9.0', '02a2a1'),
      ('lnd', '0.9.1', '02a2a1'),
      ('lnd', '0.9.2', '02a2a1'),
      ('lnd', '0.10.0', '02a2a1'),
      ('lnd', 'probable', '0a00'),
      ('lnd', 'guess', '2200')
  ), agg AS (
    SELECT pubkey, array_agg(daemon.name) AS impl
    FROM features
    INNER JOIN daemon ON features = featurebits
    GROUP BY pubkey
  ), counts AS (
    SELECT pubkey,
      (SELECT count(*) FROM unnest(impl) AS v WHERE v = 'eclair') AS eclair,
      (SELECT count(*) FROM unnest(impl) AS v WHERE v = 'lnd') AS lnd,
      (SELECT count(*) FROM unnest(impl) AS v WHERE v = 'c-lightning') AS clightning
    FROM agg
  )
  SELECT pubkey, CASE
    WHEN eclair > clightning AND eclair > lnd THEN 'eclair'
    WHEN clightning > lnd THEN 'c-lightning'
    WHEN lnd > 0 THEN 'lnd'
  END AS implementation
  FROM counts;
GRANT SELECT ON implementations TO web_anon;

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
      SELECT ((since_block/100)-1)*100 AS blockgroup,
        count(*) AS opened,
        0 AS closed,
        sum(satoshis) AS cap_change,
        sum((open->>'fee')::int) + sum((close->>'fee')::int) AS fee,
        0 AS htlcs
      FROM channels
      WHERE (open->>'block')::int < since_block
      GROUP BY ((since_block/100)-1)*100
    UNION ALL
      -- ongoing opens
      SELECT ((open->>'block')::int/100)*100 AS blockgroup,
        count(open->>'block') AS opened,
        0 AS closed,
        sum(satoshis) AS cap_change,
        sum((open->>'fee')::int) AS fee,
        0 AS htlcs
      FROM channels
      WHERE (open->>'block')::int >= since_block
      GROUP BY (open->>'block')::int/100
    UNION ALL
      -- ongoing closes
      SELECT ((close->>'block')::int/100)*100 AS blockgroup,
        0 AS opened,
        count(close->>'block') AS closed,
        -sum(satoshis) AS cap_change,
        sum((close->>'fee')::int) AS fee,
        sum(jsonb_array_length(close->'htlcs')) AS htlcs
      FROM channels
      WHERE close->>'block' IS NOT NULL AND (close->>'block')::int >= since_block
      GROUP BY (close->>'block')::int/100
  ) AS main
  GROUP BY blockgroup
  ORDER BY blockgroup
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION node_channels (nodepubkey text)
RETURNS TABLE (
  short_channel_id text,
  peer jsonb,
  open jsonb,
  close jsonb,
  satoshis int,
  outpol jsonb,
  inpol jsonb,
  funded boolean, -- funded by us?
  closed boolean, -- force-closed by us?
  letter text -- we are 'a' or 'b'?
) AS $$
  SELECT
    channels.short_channel_id,
    jsonb_build_object(
      'id', peer.pubkey,
      'name', peer.alias,
      'color', peer.color,
      'size', peer.capacity
    ) AS peer,
    open,
    close,
    satoshis,
    jsonb_build_object(
      'base',
        split_part(p_out.base_fee_millisatoshi, '~', 2)::numeric(13),
      'rate',
        split_part(p_out.fee_per_millionth, '~', 2)::numeric(13),
      'delay',
        split_part(p_out.delay, '~', 2)::int
    ) AS out,
    jsonb_build_object(
      'base',
        split_part(p_in.base_fee_millisatoshi, '~', 2)::numeric(13),
      'rate',
        split_part(p_in.fee_per_millionth, '~', 2)::numeric(13),
      'delay',
        split_part(p_in.delay, '~', 2)::int
    ) AS in,
    (nodes->>funder != peer.pubkey) AS funded,
    CASE
      WHEN closer IS NOT NULL AND closer = 'a' THEN
        nodes->>a != peer.pubkey
      WHEN closer IS NOT NULL AND closer = 'b' THEN
        nodes->>b != peer.pubkey
    END AS closed,
    CASE
      WHEN peer.pubkey = nodes->>a THEN 'b'
      WHEN peer.pubkey = nodes->>b THEN 'a'
    END AS letter
  FROM channels
  LEFT OUTER JOIN nodes AS peer ON peer.pubkey = (nodes - nodepubkey)->>0
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
    AND p_out.direction = CASE WHEN nodes->>0 = nodepubkey THEN 1 ELSE 0 END
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
    AND p_in.direction = CASE WHEN nodes->>0 = nodepubkey THEN 0 ELSE 1 END
  WHERE nodes ? nodepubkey
  ORDER BY (open->>'block')::int DESC
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
      close->>'block' IS NOT NULL AS closed
    FROM channels
    WHERE (
          short_channel_id >= (SELECT query FROM q)
      AND short_channel_id < (SELECT query FROM q) || 'Z'
    )
      OR open->>'txid' = (SELECT query FROM q)
      OR close->>'txid' = (SELECT query FROM q)
      OR open->>'address' = (SELECT query FROM q)
      OR channels.scid_int::text = (SELECT query FROM q)
      OR channels.scid_hex = (SELECT query FROM q)
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
