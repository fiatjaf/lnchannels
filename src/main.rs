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
    SELECT ((?1/100)-1)*100 AS blockgroup,
      count(*) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change
    FROM channels
    WHERE open_block < ?1
  UNION ALL
    SELECT (open_block/100)*100 AS blockgroup,
      count(open_block) AS opened,
      0 AS closed,
      sum(satoshis) AS cap_change
    FROM channels
    WHERE open_block >= ?1
    GROUP BY open_block/100
  UNION ALL
    SELECT (close_block/100)*100 AS blockgroup,
      0 AS opened,
      count(close_block) AS closed,
      -sum(satoshis) AS cap_change
    FROM channels
    WHERE close_block IS NOT NULL
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
SELECT short_channel_id, open_block, close_block, close_block - open_block AS duration FROM (
  SELECT short_channel_id,
    open_block,
    CASE
      WHEN close_block IS NOT NULL THEN close_block
      ELSE ?1
    END AS close_block
  FROM channels
) ORDER BY duration DESC LIMIT 100
    "#,
    )?;
    let mut rows = q.query(params![last_block])?;
    while let Some(row) = rows.next()? {
        let channel = Channel {
            short_channel_id: row.get(0)?,
            open_block: row.get(1)?,
            close_block: row.get(2)?,
            duration: row.get(3)?,
        };
        longestliving.push(channel);
    }
    context.insert("longestliving", &longestliving);

    // nodes that open and close more channels

    Ok(Template::render("index", &context))
}

#[derive(Serialize)]
struct Channel {
    #[serde(rename = "s")]
    short_channel_id: String,
    #[serde(rename = "o")]
    open_block: i64,
    #[serde(rename = "c")]
    close_block: i64,
    #[serde(rename = "d")]
    duration: i64,
}

fn main() {
    rocket::ignite()
        .attach(Template::fairing())
        .mount("/", routes![index])
        .mount("/static", StaticFiles::from("static"))
        .launch();
}
