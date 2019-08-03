#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate error_chain;
extern crate rocket_contrib;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;

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
    let mut blocks: Vec<i32> = Vec::new();
    let mut openings: Vec<i32> = Vec::new();
    let mut closings: Vec<i32> = Vec::new();
    let mut total: Vec<i32> = Vec::new();
    let mut capacity: Vec<i64> = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT blockgroup, sum(opened) AS opened, sum(closed) AS closed, sum(cap_change) AS cap_change
FROM (
    -- initial aggregates
    SELECT ((?1/100)-1)*100 AS blockgroup,
      count(*) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change
    FROM channels
    WHERE open_block < ?1
  UNION ALL
    -- ongoing opens
    SELECT (open_block/100)*100 AS blockgroup,
      count(open_block) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change
    FROM channels
    WHERE open_block >= ?1
    GROUP BY open_block/100
  UNION ALL
    -- ongoing closes
    SELECT (close_block/100)*100 AS blockgroup,
      0 AS opened,
      count(close_block) AS closed,
      -sum(satoshis) AS cap_change
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
        let opens: i32 = row.get(1)?;
        let closes: i32 = row.get(2)?;
        openings.push(opens);
        closings.push(closes);
        total.push(
            *match total.last() {
                Some(curr) => curr,
                None => &0i32,
            } + opens
                - closes,
        );
        let cap_change_sat: i64 = row.get(3)?;
        let cap_change = cap_change_sat / 100000000;
        capacity.push(
            *match capacity.last() {
                Some(curr) => curr,
                None => &0i64,
            } + cap_change,
        );
    }
    context.insert("blocks", &blocks[1..]);
    context.insert("openings", &openings[1..]);
    context.insert("closings", &closings[1..]);
    context.insert("total", &total[1..]);
    context.insert("capacity", &capacity[1..]);

    // longest-living channels
    let mut longestliving = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT short_channel_id, open_block, close_block, close_block - open_block AS duration, closed FROM (
  SELECT short_channel_id,
    open_block,
    CASE
      WHEN close_block IS NOT NULL THEN close_block
      ELSE ?1
    END AS close_block,
    (close_block IS NOT NULL) AS closed
  FROM channels
)x ORDER BY duration DESC LIMIT 10
    "#,
    )?;
    let mut rows = q.query(params![last_block])?;
    while let Some(row) = rows.next()? {
        let channel = CompressedChannel {
            short_channel_id: row.get(0)?,
            open_block: row.get(1)?,
            close_block: row.get(2)?,
            duration: row.get(3)?,
            closed: row.get(4)?,
        };
        longestliving.push(channel);
    }
    context.insert("longestliving", &longestliving);

    // nodes that open and close more channels
    let mut mostactivity = Vec::new();
    let mut q = conn.prepare(
        r#"
SELECT
  id,
  historical_total,
  historical_total - closed_already AS open_now,
  avg_duration
FROM (
  SELECT
    id,
    count(*) AS historical_total,
    count(close_block) AS closed_already,
    avg(CASE
      WHEN close_block IS NOT NULL THEN close_block
      ELSE ?1
    END - open_block) AS avg_duration
  FROM (
    SELECT node0 AS id, *
    FROM channels
  UNION ALL
    SELECT node1 AS id, *
    FROM channels
  )x
  GROUP BY id
)y
GROUP BY id
ORDER BY avg_duration DESC
LIMIT 10
    "#,
    )?;
    let mut rows = q.query(params![last_block])?;
    while let Some(row) = rows.next()? {
        let node = NodeActivity {
            id: row.get(0)?,
            historical_total: row.get(1)?,
            open_now: row.get(2)?,
            avg_duration: row.get(3)?,
        };
        mostactivity.push(node);
    }
    context.insert("mostactivity", &mostactivity);

    Ok(Template::render("index", &context))
}

#[get("/node/<pubkey>")]
fn show_node(pubkey: String) -> Result<Template> {
    let mut context = Context::new();
    let conn = Connection::open("channels.db")?;

    context.insert("node", &pubkey);

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
  CASE WHEN node0 = ?1 THEN node1 ELSE node0 END AS peer,
  open_block, open_time,
  close_block, close_time,
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
            peer: row.get(1)?,
            open_block: row.get(2).unwrap_or(0),
            open_time: row.get(3).unwrap_or(0),
            close_block: row.get(4).unwrap_or(0),
            close_time: row.get(5).unwrap_or(0),
            satoshis: row.get(6)?,
        };
        channels.push(channel);
    }
    context.insert("channels", &channels);

    Ok(Template::render("node", &context))
}

#[get("/channel/<short_channel_id>")]
fn show_channel(short_channel_id: String) -> Result<Template> {
    let mut context = Context::new();
    let conn = Connection::open("channels.db")?;

    let channel = conn.query_row(
        "SELECT
            open_block, open_fee, open_transaction, open_time, 
            close_block, close_fee, close_transaction, close_time,
            address, node0, node1, satoshis, last_seen,
            short_channel_id
        FROM channels WHERE short_channel_id = ?1",
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
                // last_seen: row.get(12)?,
                short_channel_id: row.get(13)?,
            })
        },
    )?;
    context.insert("channel", &channel);

    Ok(Template::render("channel", &context))
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
    node1: String,
    satoshis: i64,
    // last_seen: i64,
}

#[derive(Serialize)]
struct CompressedChannel {
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
    peer: String,
    short_channel_id: String,
    open_block: i64,
    open_time: i64,
    close_block: i64,
    close_time: i64,
    satoshis: i64,
}

#[derive(Serialize)]
struct NodeActivity {
    id: String,
    open_now: i64,
    historical_total: i64,
    avg_duration: f64,
}

fn main() {
    rocket::ignite()
        .attach(Template::fairing())
        .mount("/", routes![index, show_channel, show_node])
        .mount("/static", StaticFiles::from("static"))
        .launch();
}
