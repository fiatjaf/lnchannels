interval = 300
global_start = 580000
global_end = 631001


def chain_analysis(db):
    # prepare functions we're going to need
    db.execute(
        """
CREATE FUNCTION pg_temp.inter (x jsonb, y jsonb) RETURNS text AS $$
  SELECT xr.value
  FROM jsonb_array_elements_text(x) AS xr
  INNER JOIN jsonb_array_elements_text(y) AS yr
          ON xr.value = yr.value
$$ LANGUAGE SQL;

CREATE FUNCTION pg_temp.diff (x jsonb, y jsonb) RETURNS text AS $$
  SELECT (x - (SELECT array_agg(value) FROM jsonb_array_elements_text(y)))->>0
$$ LANGUAGE SQL;

CREATE FUNCTION pg_temp.matches (x jsonb, y jsonb) RETURNS boolean AS $$
  SELECT x ?| (SELECT array_agg(value) FROM jsonb_array_elements_text(y))
$$ LANGUAGE SQL;
    """
    )

    db.execute(
        """
SELECT short_channel_id
FROM channels
WHERE onchain->'close'->>'block' IS NOT NULL
  AND (
      onchain->>'a' IS NULL
   OR onchain->>'funder' IS NULL
  )
ORDER BY short_channel_id
    """
    )
    for (scid,) in db.fetchall():
        run_for_channel(db, scid)


def run_for_channel(db, scid):
    print("  running for", scid)

    db.execute(
        """
WITH matching AS (
  SELECT x.short_channel_id AS x_scid, x.nodes AS x_nodes, x.onchain AS x_onchain,
         y.short_channel_id AS y_scid, y.nodes AS y_nodes, y.onchain AS y_onchain
  FROM (SELECT * FROM channels WHERE short_channel_id = %s) AS x
  INNER JOIN channels AS y
     ON pg_temp.matches(x.nodes, y.nodes)
    AND NOT x.nodes = y.nodes
), partial_updates (scid, label, value, other) AS (
    SELECT x_scid, 'a', pg_temp.inter(x_nodes, y_nodes), pg_temp.diff(x_nodes, y_nodes)
    FROM matching
    WHERE x_onchain->'close'->>'type' != 'penalty'
      AND pg_temp.matches(x_onchain->'txs'->'a',
            y_onchain->'txs'->'a' || y_onchain->'txs'->'b' || y_onchain->'txs'->'funding'
          )
  UNION
    SELECT x_scid, 'a', pg_temp.inter(x_nodes, y_nodes), pg_temp.inter(x_nodes, y_nodes)
    FROM matching
    WHERE x_onchain->'close'->>'type' = 'penalty'
      AND (
          pg_temp.matches(x_onchain->'txs'->'a',
            y_onchain->'txs'->'a' || y_onchain->'txs'->'b' || y_onchain->'txs'->'funding'
          )
       OR pg_temp.matches(x_onchain->'txs'->'a',
            y_onchain->'txs'->'b' || y_onchain->'txs'->'b' || y_onchain->'txs'->'funding'
          )
      )
  UNION
    SELECT x_scid, 'funder', pg_temp.inter(x_nodes, y_nodes), NULL
    FROM matching
    WHERE pg_temp.matches(x_onchain->'txs'->'funding',
            y_onchain->'txs'->'a' || y_onchain->'txs'->'b' || y_onchain->'txs'->'funding'
          )
  UNION
    SELECT x_scid, 'a', x_onchain->>'a', (x_nodes - (x_onchain->>'a'))->>0
    FROM matching
    WHERE (x_onchain->'close'->'balance'->>'b')::int = 0
      AND x_onchain->>'a' IS NOT NULL
  UNION
    SELECT x_scid, 'funder', x_onchain->>'funder', (x_nodes - (x_onchain->>'funder'))->>0
    FROM matching
    WHERE (x_onchain->'close'->'balance'->>'b')::int = 0
      AND x_onchain->>'funder' IS NOT NULL
), updates (scid, label, value) AS (
    SELECT scid, label, value FROM partial_updates
  UNION ALL
    SELECT scid, ('["a", "b"]'::jsonb - label)->>0, other
    FROM partial_updates
    WHERE label = 'a' OR label = 'b'
)

SELECT updates.*
FROM updates
INNER JOIN channels ON channels.short_channel_id = updates.scid
WHERE channels.onchain->>(updates.label) IS NULL
    """,
        (scid,),
    )
    for scid, label, value in db.fetchall():
        print("    update", scid, label, "=", value)
        db.execute(
            """
UPDATE channels
SET onchain = jsonb_set(onchain, ARRAY[%s::text], to_jsonb(%s::text))
WHERE short_channel_id = %s
        """,
            (label, value, scid),
        )
