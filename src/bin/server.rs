#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate rocket_contrib;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
extern crate tera;

use rocket_contrib::json::JsonValue;
use rocket_contrib::serve::StaticFiles;
use rocket_contrib::templates::tera::Context;
use rocket_contrib::templates::Template;
use rusqlite::{params, Connection, NO_PARAMS};
use serde::Serialize;

mod errors {
    error_chain! {
      foreign_links {
        SQLite(rusqlite::Error);
      }
    }
}

use errors::*;

// first block we may say we have meaningful data from
static FIRST_BLOCK: i64 = 578600;

#[get("/")]
fn index() -> Result<Template> {
    let mut context = Context::new();
    let conn = Connection::open("channels.db")?;

    let stats = get_stats()?;
    context.insert("stats", &stats);
    context.insert("first_block", &FIRST_BLOCK);

    // channel variation chart
    let mut blocks: Vec<i64> = Vec::new();
    let mut openings: Vec<i64> = Vec::new();
    let mut closings: Vec<i64> = Vec::new();
    let mut total: Vec<i64> = Vec::new();
    let mut capacity: Vec<i64> = Vec::new();
    let mut fee_total: Vec<i64> = Vec::new();
    let mut outstanding_htlcs: Vec<i64> = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT blockgroup, sum(opened) AS opened, sum(closed) AS closed, sum(cap_change) AS cap_change, fee_total, htlcs
FROM (
    -- initial aggregates
    SELECT ((?1/100)-1)*100 AS blockgroup,
      count(*) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change,
      coalesce(open_fee, 0) + coalesce(close_fee, 0) AS fee_total,
      0 AS htlcs
    FROM channels
    WHERE open_block < ?1
  UNION ALL
    -- ongoing opens
    SELECT (open_block/100)*100 AS blockgroup,
      count(open_block) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change,
      coalesce(open_fee, 0) + coalesce(close_fee, 0) AS fee_total,
      0 AS htlcs
    FROM channels
    WHERE open_block >= ?1
    GROUP BY open_block/100
  UNION ALL
    -- ongoing closes
    SELECT (close_block/100)*100 AS blockgroup,
      0 AS opened,
      count(close_block) AS closed,
      -sum(satoshis) AS cap_change,
      coalesce(open_fee, 0) + coalesce(close_fee, 0) AS fee_total,
      coalesce(close_htlc_count, 0) AS htlcs
    FROM channels
    WHERE close_block IS NOT NULL AND close_block >= ?1
    GROUP BY close_block/100
) AS main
GROUP BY blockgroup
ORDER BY blockgroup
    "#,
    )?;
    let mut rows = q.query(params![FIRST_BLOCK])?;
    while let Some(row) = rows.next()? {
        blocks.push(row.get(0)?);
        let opens: i64 = row.get(1)?;
        let closes: i64 = row.get(2)?;
        openings.push(opens);
        closings.push(closes);
        let current_total = *match total.last() {
            Some(curr) => curr,
            None => &0i64,
        } + opens
            - closes;
        total.push(current_total);

        let cap_change_sat: i64 = row.get(3)?;
        let cap_change = cap_change_sat / 100000000;
        let current_cap = *match capacity.last() {
            Some(curr) => curr,
            None => &0i64,
        } + cap_change;
        capacity.push(current_cap);

        let fee: i64 = row.get(4)?;
        fee_total.push(fee);

        let htlcs: i64 = row.get(5)?;
        outstanding_htlcs.push(htlcs);
    }
    context.insert("blocks", &blocks[1..]);
    context.insert("openings", &openings[1..]);
    context.insert("closings", &closings[1..]);
    context.insert("total", &total[1..]);
    context.insert("capacity", &capacity[1..]);
    context.insert("fee_total", &fee_total[1..]);
    context.insert("outstanding_htlcs", &outstanding_htlcs[1..]);

    // close types
    let mut closeblocks: Vec<i64> = Vec::new();
    let mut unknown: Vec<i64> = Vec::new();
    let mut unused: Vec<i64> = Vec::new();
    let mut mutual: Vec<i64> = Vec::new();
    let mut force: Vec<i64> = Vec::new();
    let mut force_unused: Vec<i64> = Vec::new();
    let mut penalty: Vec<i64> = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT * FROM (
  SELECT
    blockgroup, unknown, unused, mutual, force, force_unused, penalty
  FROM closetypes
  WHERE blockgroup >= ?1
  ORDER BY blockgroup DESC
  LIMIT -1 OFFSET 1
)p
ORDER BY blockgroup ASC
    "#,
    )?;
    let mut rows = q.query(params![FIRST_BLOCK])?;
    while let Some(row) = rows.next()? {
        closeblocks.push(row.get(0)?);
        unknown.push(row.get(1)?);
        unused.push(row.get(2)?);
        mutual.push(row.get(3)?);
        force.push(row.get(4)?);
        force_unused.push(row.get(5)?);
        penalty.push(row.get(6)?);
    }
    context.insert("closeblocks", &closeblocks);
    context.insert("unknown", &unknown);
    context.insert("unused", &unused);
    context.insert("mutual", &mutual);
    context.insert("force", &force);
    context.insert("force_unused", &force_unused);
    context.insert("penalty", &penalty);

    // longest-living channels
    let mut longestliving = Vec::new();
    let mut q = conn.prepare(
        r#"
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
      ELSE ?1
    END AS close_block,
    (close_block IS NOT NULL) AS closed,
    node0, node1, satoshis
  FROM channels
)x ORDER BY duration DESC LIMIT 50
    "#,
    )?;
    let mut rows = q.query(params![stats.last_block])?;
    while let Some(row) = rows.next()? {
        let channel = ChannelEntry {
            short_channel_id: row.get(0)?,
            open_block: row.get(1)?,
            close_block: row.get(2)?,
            duration: row.get(3)?,
            closed: row.get(4)?,
            id0: row.get(5)?,
            name0: row.get(6)?,
            id1: row.get(7)?,
            name1: row.get(8)?,
            satoshis: row.get(9)?,
        };
        longestliving.push(channel);
    }
    context.insert("longestliving", &longestliving);

    // all nodes with aggregate data
    let mut allnodes = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT
  pubkey,
  coalesce(alias, ''),
  openchannels,
  closedchannels,
  avg_duration,
  avg_open_fee,
  avg_close_fee,
  oldestchannel,
  capacity
FROM nodes
WHERE openchannels > 0
ORDER BY openchannels DESC
    "#,
    )?;
    let mut rows = q.query(NO_PARAMS)?;
    while let Some(row) = rows.next()? {
        allnodes.push(row_to_node_aggregate(row)?);
    }
    context.insert("allnodes", &allnodes);

    Ok(Template::render("index", &context))
}

#[get("/node/<nodeid>")]
fn show_node(nodeid: String) -> Result<Template> {
    let mut context = Context::new();
    let conn = Connection::open("channels.db")?;

    let stats = get_stats()?;
    context.insert("stats", &stats);
    context.insert("first_block", &FIRST_BLOCK);

    let pubkey = nodeid.to_lowercase();

    let node: NodeAggregate = match conn.query_row_and_then(
        r#"
SELECT
  pubkey,
  coalesce(alias, ''),
  openchannels,
  closedchannels,
  avg_duration,
  avg_open_fee,
  avg_close_fee,
  oldestchannel,
  capacity
FROM nodes WHERE pubkey = ?1
        "#,
        params![pubkey],
        row_to_node_aggregate,
    ) {
        Ok(node) => node,
        Err(_) => NodeAggregate {
            id: pubkey.clone(),
            ..Default::default()
        },
    };
    context.insert("node", &node);

    let mut aliases = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT first_seen, alias
FROM nodealiases
WHERE pubkey = ?1
ORDER BY first_seen DESC
    "#,
    )?;
    let mut rows = q.query(params![pubkey])?;
    while let Some(row) = rows.next()? {
        let alias = NodeAlias {
            first_seen: row.get(0)?,
            alias: row.get(1)?,
        };
        aliases.push(alias);
    }
    context.insert("aliases", &aliases);

    let mut channels = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT
  channels.short_channel_id,
  CASE WHEN node0 = ?1 THEN node1 ELSE node0 END AS peer_id,
  coalesce((SELECT alias FROM nodes WHERE pubkey = (CASE WHEN node0 = ?1 THEN node1 ELSE node0 END)), '') AS peer_name,
  coalesce((SELECT capacity FROM nodes WHERE pubkey = (CASE WHEN node0 = ?1 THEN node1 ELSE node0 END)), 0) AS peer_size,
  open_block, open_fee,
  close_block, close_fee,
  satoshis,
  policy_out.base_fee_millisatoshi AS outgoing_base_fee_millisatoshi,
  policy_out.fee_per_millionth AS outgoing_fee_per_millionth,
  policy_out.delay AS outgoing_delay,
  policy_in.base_fee_millisatoshi AS incoming_base_fee_millisatoshi,
  policy_in.fee_per_millionth AS incoming_fee_per_millionth,
  policy_in.delay AS incoming_delay,
  close_type,
  close_htlc_count
FROM channels
LEFT OUTER JOIN (
  SELECT * FROM policies
  GROUP BY short_channel_id, direction
  ORDER BY short_channel_id, direction, update_time DESC
)
  AS policy_out ON policy_out.short_channel_id = channels.short_channel_id
               AND policy_out.direction = CASE WHEN node0 = ?1 THEN 1 ELSE 0 END
LEFT OUTER JOIN (
  SELECT * FROM policies
  GROUP BY short_channel_id, direction
  ORDER BY short_channel_id, direction, update_time DESC
)
  AS policy_in ON policy_in.short_channel_id = channels.short_channel_id
              AND policy_in.direction = CASE WHEN node0 = ?1 THEN 0 ELSE 1 END
WHERE node0 = ?1 OR node1 = ?1
ORDER BY open_block DESC
    "#,
    )?;
    let mut rows = q.query(params![pubkey])?;
    while let Some(row) = rows.next()? {
        let channel = NodeChannel {
            short_channel_id: row.get(0)?,
            peer_id: row.get(1)?,
            peer_name: row.get(2)?,
            peer_size: row.get(3)?,
            open_block: row.get(4).unwrap_or(0),
            open_fee: row.get(5).unwrap_or(0),
            close_block: row.get(6).unwrap_or(0),
            close_fee: row.get(7).unwrap_or(0),
            satoshis: row.get(8)?,
            outgoing_base_fee_millisatoshi: row.get(9).unwrap_or(0),
            outgoing_fee_per_millionth: row.get(10).unwrap_or(0),
            outgoing_delay: row.get(11).unwrap_or(0),
            incoming_base_fee_millisatoshi: row.get(12).unwrap_or(0),
            incoming_fee_per_millionth: row.get(13).unwrap_or(0),
            incoming_delay: row.get(14).unwrap_or(0),
            close_type: row.get(15).unwrap_or("".to_string()),
            close_htlc_count: row.get(16).unwrap_or(0),
        };
        channels.push(channel);
    }
    context.insert("channels", &channels);

    // a canonical node name
    if aliases.len() > 0 {
        context.insert("nodename", &aliases[0].alias);
    } else {
        let nodename = abbreviate(pubkey);
        context.insert("nodename", &nodename);
    }

    Ok(Template::render("node", &context))
}

#[get("/channel/<short_channel_id>")]
fn show_channel(short_channel_id: String) -> Result<Template> {
    let mut context = Context::new();
    let conn = Connection::open("channels.db")?;

    let stats = get_stats()?;
    context.insert("stats", &stats);
    context.insert("FIRST_BLOCK", &FIRST_BLOCK);

    let channel = conn.query_row(
        r#"
SELECT
    open_block, open_fee, open_transaction, open_time, 
    close_block, close_fee, close_transaction, close_time,
    address, node0, node1, satoshis,
    short_channel_id, coalesce(n0.alias, ''), coalesce(n1.alias, ''),
    close_type, close_htlc_count, close_balance_a, close_balance_b
FROM channels
LEFT OUTER JOIN nodes AS n0 ON n0.pubkey = node0
LEFT OUTER JOIN nodes AS n1 ON n1.pubkey = node1
WHERE short_channel_id = ?1
        "#,
        params![short_channel_id],
        |row| {
            Ok(FullChannel {
                open_block: row.get(0)?,
                open_fee: row.get(1)?,
                open_transaction: row.get(2)?,
                open_time: row.get(3)?,
                close_block: row.get(4).unwrap_or(0),
                close_fee: row.get(5).unwrap_or(0),
                close_transaction: row.get(6).unwrap_or(String::from("")),
                close_time: row.get(7).unwrap_or(0),
                address: row.get(8)?,
                node0: row.get(9)?,
                node1: row.get(10)?,
                satoshis: row.get(11)?,
                short_channel_id: row.get(12)?,
                node0name: row.get(13)?,
                node1name: row.get(14)?,
                close_type: row.get(15).unwrap_or(String::from("")),
                close_htlc_count: row.get(16).unwrap_or(0),
                close_balance_a: row.get(17).unwrap_or(0),
                close_balance_b: row.get(18).unwrap_or(0),
            })
        },
    )?;
    context.insert("channel", &channel);

    let mut upwardpolicies = Vec::new();
    let mut downwardpolicies = Vec::new();
    let mut query = conn.prepare(
        r#"
SELECT direction, base_fee_millisatoshi, fee_per_millionth, delay, update_time
FROM policies
WHERE short_channel_id = ?1
        "#,
    )?;
    let mut rows = query.query(params![short_channel_id])?;
    while let Some(row) = rows.next()? {
        let policy = ChannelPolicy {
            base_fee_millisatoshi: row.get(1)?,
            fee_per_millionth: row.get(2)?,
            delay: row.get(3)?,
            update_time: row.get(4)?,
        };

        let direction: i64 = row.get(0)?;
        if direction == 1 {
            upwardpolicies.push(policy);
        } else {
            downwardpolicies.push(policy);
        }
    }

    context.insert("upwardpolicies", &upwardpolicies);
    context.insert("downwardpolicies", &downwardpolicies);

    Ok(Template::render("channel", &context))
}

#[get("/search?<q>")]
fn search(q: String) -> Result<JsonValue> {
    let conn = Connection::open("channels.db")?;
    let mut results: Vec<SearchResult> = Vec::new();

    let search: String = q.trim().to_string();

    // search node alias
    let mut query = conn.prepare(
        r#"
  SELECT
    'channel' AS kind,
    short_channel_id || ' (' || satoshis || ' sat)' AS label,
    '/channel/' || short_channel_id AS url,
    close_block IS NOT NULL AS closed,
    0
  FROM channels WHERE short_channel_id >= ?1 and short_channel_id < ?1 || '{'
UNION ALL
  SELECT
    'node' AS kind,
    alias || ' (' || openchannels || ' channels)' AS label,
    '/node/' || pubkey AS url,
    false AS closed,
    0
  FROM nodes WHERE pubkey >= ?1 AND pubkey < ?1 || '{'
UNION ALL
  SELECT                                                                                                                   'node ' AS kind,                                                                                                       alias || ' (' || openchannels || ' channels)' AS label,
    '/node/' || pubkey AS url,
    false AS closed,
    capacity
  FROM (
    SELECT *, nodes.openchannels AS openchannels, nodes.capacity AS capacity
    FROM (SELECT pubkey, alias FROM nodealiases WHERE alias LIKE '%' || ?1 || '%') AS namesearch
    INNER JOIN
      nodes ON nodes.pubkey = namesearch.pubkey
  )
  ORDER BY capacity DESC
    "#,
    )?;
    let mut rows = query.query(params![search])?;
    while let Some(row) = rows.next()? {
        results.push(SearchResult {
            kind: row.get(0)?,
            label: row.get(1)?,
            url: row.get(2)?,
            closed: row.get(3)?,
        });
    }
    Ok(json!({ "results": results }))
}

#[derive(Serialize)]
struct SearchResult {
    kind: String,
    label: String,
    url: String,
    closed: bool,
}

#[derive(Serialize)]
struct FullChannel {
    short_channel_id: String,
    open_block: i64,
    open_fee: i64,
    open_time: i64,
    open_transaction: String,
    close_block: i64,
    close_fee: i64,
    close_time: i64,
    close_transaction: String,
    address: String,
    node0: String,
    node0name: String,
    node1: String,
    node1name: String,
    satoshis: i64,
    close_type: String,
    close_htlc_count: i64,
    close_balance_a: i64,
    close_balance_b: i64,
}

#[derive(Serialize)]
struct ChannelEntry {
    satoshis: i64,
    id0: String,
    name0: String,
    id1: String,
    name1: String,
    short_channel_id: String,
    open_block: i64,
    close_block: i64,
    duration: i64,
    closed: bool,
}

#[derive(Serialize)]
struct ChannelPolicy {
    base_fee_millisatoshi: i64,
    fee_per_millionth: i64,
    delay: i64,
    update_time: i64,
}

#[derive(Serialize)]
struct NodeAlias {
    alias: String,
    first_seen: String,
}

#[derive(Serialize)]
struct NodeChannel {
    peer_id: String,
    peer_name: String,
    peer_size: i64,
    short_channel_id: String,
    open_block: i64,
    open_fee: i64,
    close_block: i64,
    close_fee: i64,
    satoshis: i64,
    outgoing_base_fee_millisatoshi: i64,
    outgoing_fee_per_millionth: i64,
    outgoing_delay: i64,
    incoming_base_fee_millisatoshi: i64,
    incoming_fee_per_millionth: i64,
    incoming_delay: i64,
    close_type: String,
    close_htlc_count: i64,
}

#[derive(Serialize, Default)]
struct NodeAggregate {
    id: String,
    name: String,
    nopen: i64,
    nclosed: i64,
    avgduration: f64,
    avgopenfee: f64,
    avgclosefee: f64,
    oldest: i64,
    cap: i64,
}

#[derive(Serialize)]
struct GlobalStats {
    last_block: i64,
    max_channel_duration: i64,
    max_channel_open_fee: i64,
    max_channel_close_fee: i64,
    max_channel_satoshis: i64,
    max_node_capacity: i64,
    max_node_openchannels: i64,
    max_node_closedchannels: i64,
    max_node_allchannels: i64,
    max_node_close_rate: f64,
    max_node_average_duration: f64,
    max_node_average_open_fee: f64,
    max_node_average_close_fee: f64,
}

fn main() {
    rocket::ignite()
        .attach(Template::custom(|engines| {
            engines.tera.register_filter(
                "abbr",
                move |val: tera::Value, _args| -> tera::Result<tera::Value> {
                    match val.clone() {
                        tera::Value::String(v) => Ok(tera::Value::String(abbreviate(v))),
                        _ => Ok(val),
                    }
                },
            );
        }))
        .mount("/", routes![index, show_channel, show_node, search])
        .mount("/static", StaticFiles::from("static"))
        .launch();
}

fn abbreviate(long: String) -> String {
    let last = long.len() - 4;
    format!("{}…{}", &long[..4], &long[last..])
}

fn row_to_node_aggregate(row: &rusqlite::Row) -> Result<NodeAggregate> {
    let nodeagg = NodeAggregate {
        id: row.get(0)?,
        name: row.get(1)?,
        nopen: row.get(2)?,
        nclosed: row.get(3)?,
        avgduration: row.get(4)?,
        avgopenfee: row.get(5)?,
        avgclosefee: row.get(6).unwrap_or(0f64),
        oldest: row.get(7)?,
        cap: row.get(8)?,
    };
    Ok(nodeagg)
}

fn get_stats() -> Result<GlobalStats> {
    let conn = Connection::open("channels.db")?;
    let globalstats = conn.query_row("SELECT * FROM globalstats", NO_PARAMS, |row| {
        Ok(GlobalStats {
            last_block: row.get(0)?,
            max_channel_duration: row.get(1)?,
            max_channel_open_fee: row.get(2)?,
            max_channel_close_fee: row.get(3)?,
            max_channel_satoshis: row.get(4)?,
            max_node_capacity: row.get(5)?,
            max_node_openchannels: row.get(6)?,
            max_node_closedchannels: row.get(7)?,
            max_node_allchannels: row.get(8)?,
            max_node_close_rate: row.get(9)?,
            max_node_average_duration: row.get(10)?,
            max_node_average_open_fee: row.get(11)?,
            max_node_average_close_fee: row.get(12)?,
        })
    })?;
    Ok(globalstats)
}
