extern crate reqwest;
extern crate rusqlite;
extern crate serde;

use rusqlite::types::ToSql;
use rusqlite::{Connection, NO_PARAMS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::result::Result;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    println!("creating sqlite database");
    let conn = Connection::open("channels.db")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS channels (
            short_channel_id TEXT NOT NULL,
            source TEXT NOT NULL,
            destination TEXT NOT NULL,
            satoshis INTEGER,

            UNIQUE(short_channel_id, source, destination)
        )",
        NO_PARAMS,
    )?;

    let channels = getchannels()?;
    println!("inserting {} channels", channels.len());
    for channel in channels.iter() {
        conn.execute(
            "INSERT INTO channels VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(short_channel_id, source, destination) DO NOTHING",
            &[
                &channel.short_channel_id as &ToSql,
                &channel.source as &ToSql,
                &channel.destination as &ToSql,
                &channel.satoshis as &ToSql,
            ],
        )?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct ListChannels {
    channels: Vec<Channel>,
}

#[derive(Serialize, Deserialize)]
struct Channel {
    source: String,
    destination: String,
    short_channel_id: String,
    satoshis: i64,
}

fn getchannels() -> Result<Vec<Channel>, Box<dyn Error>> {
    let mut spark_url = env::var("SPARK_URL").expect("Spark URL not set.");
    let spark_token = env::var("SPARK_TOKEN").expect("Spark token not set.");

    println!("fetching channels from {}", &spark_url);

    spark_url.push_str("/rpc");
    let client = reqwest::Client::builder()
        .gzip(true)
        .timeout(Duration::from_secs(120))
        .build()?;
    let mut call = HashMap::new();
    call.insert("method", "listchannels");

    let mut w = client
        .post(&spark_url)
        .header("X-Access", spark_token)
        .body(r#"{"method": "listchannels"}"#)
        .send()?;

    let listchannels: ListChannels = w.json()?;

    Ok(listchannels.channels)
}
