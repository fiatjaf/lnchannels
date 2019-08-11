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

#[get("/")]
fn index() -> Result<Template> {
    let mut context = Context::new();
    let conn = Connection::open("channels.db")?;

    // first block we may say we have meaningful data from
    let first_block: i64 = 578600;

    // last block we have data from
    let last_block: i64 = conn.query_row(
        "SELECT open_block FROM channels ORDER BY open_block DESC LIMIT 1",
        NO_PARAMS,
        |row| row.get(0),
    )?;

    println!("{}---{}", first_block, last_block);

    // channel variation chart
    let mut blocks: Vec<i64> = Vec::new();
    let mut openings: Vec<i64> = Vec::new();
    let mut closings: Vec<i64> = Vec::new();
    let mut total: Vec<i64> = Vec::new();
    let mut capacity: Vec<i64> = Vec::new();
    let mut fee_total: Vec<i64> = Vec::new();
    let mut fee_average: Vec<f64> = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT blockgroup, sum(opened) AS opened, sum(closed) AS closed, sum(cap_change) AS cap_change, fee_total
FROM (
    -- initial aggregates
    SELECT ((?1/100)-1)*100 AS blockgroup,
      count(*) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change,
      coalesce(open_fee, 0) + coalesce(close_fee, 0) AS fee_total
    FROM channels
    WHERE open_block < ?1
  UNION ALL
    -- ongoing opens
    SELECT (open_block/100)*100 AS blockgroup,
      count(open_block) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change,
      coalesce(open_fee, 0) + coalesce(close_fee, 0) AS fee_total
    FROM channels
    WHERE open_block >= ?1
    GROUP BY open_block/100
  UNION ALL
    -- ongoing closes
    SELECT (close_block/100)*100 AS blockgroup,
      0 AS opened,
      count(close_block) AS closed,
      -sum(satoshis) AS cap_change,
      coalesce(open_fee, 0) + coalesce(close_fee, 0) AS fee_total
    FROM channels
    WHERE close_block IS NOT NULL AND close_block >= ?1
    GROUP BY open_block/100
) AS main
GROUP BY blockgroup
ORDER BY blockgroup
    "#,
    )?;
    let mut rows = q.query(params![first_block])?;
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
        fee_average.push(fee as f64 / current_total as f64);
    }
    context.insert("blocks", &blocks[1..]);
    context.insert("openings", &openings[1..]);
    context.insert("closings", &closings[1..]);
    context.insert("total", &total[1..]);
    context.insert("capacity", &capacity[1..]);
    context.insert("fee_total", &fee_total[1..]);
    context.insert("fee_average", &fee_average[1..]);

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
  node0 AS id1,
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
    let mut rows = q.query(params![last_block])?;
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

    let pubkey = nodeid.to_lowercase();

    let node: NodeAggregate = conn.query_row_and_then(
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
FROM nodes WHERE pubkey = ?1"#,
        params![pubkey],
        row_to_node_aggregate,
    )?;
    context.insert("node", &node);

    let mut aliases = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT last_seen, alias
FROM nodealiases
WHERE pubkey = ?1
ORDER BY first_seen DESC
    "#,
    )?;
    let mut rows = q.query(params![pubkey])?;
    while let Some(row) = rows.next()? {
        let alias = NodeAlias {
            last_seen: row.get(0)?,
            alias: row.get(1)?,
        };
        aliases.push(alias);
    }
    context.insert("aliases", &aliases);

    let mut channels = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT
  short_channel_id,
  CASE WHEN node0 = ?1 THEN node1 ELSE node0 END AS peer_id,
  coalesce((SELECT alias FROM nodes WHERE pubkey = (CASE WHEN node0 = ?1 THEN node1 ELSE node0 END)), '') AS peer_name,
  coalesce((SELECT capacity FROM nodes WHERE pubkey = (CASE WHEN node0 = ?1 THEN node1 ELSE node0 END)), 0) AS peer_size,
  open_block, open_fee,
  close_block, close_fee,
  satoshis
FROM channels
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

    let channel = conn.query_row(
        r#"
        SELECT
            open_block, open_fee, open_transaction, open_time, 
            close_block, close_fee, close_transaction, close_time,
            address, node0, node1, satoshis,
            short_channel_id, coalesce(n0.alias, ''), coalesce(n1.alias, '')
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
            })
        },
    )?;
    context.insert("channel", &channel);

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
struct NodeAlias {
    alias: String,
    last_seen: String,
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
}

#[derive(Serialize)]
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
