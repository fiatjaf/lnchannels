#![recursion_limit = "1024"]

extern crate jsonrpc;
extern crate reqwest;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate error_chain;

use rusqlite::types::ToSql;
use rusqlite::{Connection, NO_PARAMS};
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::env;
use std::time::Duration;

mod errors {
    error_chain! {}
}

mod bitcoin {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Block {
        pub tx: Vec<String>,
    }

    #[derive(Deserialize)]
    pub struct DecodedTransaction {
        pub vout: Vec<Output>,
    }

    #[derive(Deserialize)]
    pub struct Output {
        #[serde(rename(deserialize = "scriptPubKey"))]
        pub script_pub_key: ScriptPubKey,
    }

    #[derive(Deserialize)]
    pub struct ScriptPubKey {
        pub addresses: Vec<String>,
    }
}

mod lightning {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct ListChannels {
        pub channels: Vec<Channel>,
    }

    #[derive(Deserialize)]
    pub struct Channel {
        pub source: String,
        pub destination: String,
        pub short_channel_id: String,
        pub satoshis: i64,
    }
}

use bitcoin::*;
use errors::*;
use lightning::*;

quick_main! {run}

fn run() -> Result<()> {
    println!("creating sqlite database");
    let conn = Connection::open("channels.db").chain_err(|| "failed to open database")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS channels (
            short_channel_id TEXT PRIMARY KEY,

            open_block INTEGER NOT NULL,
            open_transaction TEXT NOT NULL,
            address TEXT NOT NULL,
            close_block INTEGER,
            close_transaction TEXT,

            node0 TEXT NOT NULL,
            node1 TEXT NOT NULL,

            satoshis INTEGER,
            last_seen DATETIME NOT NULL
        )",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create table")?;

    let channels = getchannels()?;
    println!("inserting {} channels", channels.len());

    let mut i = 0;
    for channel in channels.iter() {
        let (node0, node1) = if channel.source < channel.destination {
            (&channel.source, &channel.destination)
        } else {
            (&channel.destination, &channel.source)
        };

        let blockchaindata = getchannelblockchaindata(channel)?;

        conn.execute(
            "INSERT INTO channels
                (short_channel_id, open_block, open_transaction, address, node0, node1, satoshis, last_seen)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, datetime('now'))
            ON CONFLICT (short_channel_id) DO UPDATE SET last_seen = excluded.last_seen",
            &[
                &channel.short_channel_id as &dyn ToSql,
                &blockchaindata.block as &dyn ToSql,
                &blockchaindata.transaction as &dyn ToSql,
                &blockchaindata.address as &dyn ToSql,
                node0 as &dyn ToSql,
                node1  as &dyn ToSql,
                &channel.satoshis as &dyn ToSql,
            ],
        )
        .chain_err(|| "failed to insert")?;

        println!(
            "{}: inserted {}-[{}]-{} at block {}",
            &i, node0, &channel.satoshis, node1, &blockchaindata.block
        );
        i += 1;
    }

    Ok(())
}

fn getchannels() -> Result<Vec<Channel>> {
    let mut spark_url = env::var("SPARK_URL").chain_err(|| "failed to read SPARK_URL")?;
    let spark_token = env::var("SPARK_TOKEN").chain_err(|| "failed to read SPARK_TOKEN")?;

    println!("fetching channels from {}", &spark_url);

    spark_url.push_str("/rpc");
    let client = reqwest::Client::builder()
        .gzip(true)
        .timeout(Duration::from_secs(120))
        .build()
        .chain_err(|| "failed to make spark client")?;
    let mut call = HashMap::new();
    call.insert("method", "listchannels");

    let mut w = client
        .post(&spark_url)
        .header("X-Access", spark_token)
        .body(r#"{"method": "listchannels"}"#)
        .send()
        .chain_err(|| "listchannels call failed")?;

    let listchannels: ListChannels = w.json().chain_err(|| "failed to decode listchannels")?;

    Ok(listchannels.channels)
}

struct ChannelBitcoinData {
    block: i64,
    transaction: String,
    address: String,
}

fn getchannelblockchaindata(channel: &Channel) -> Result<ChannelBitcoinData> {
    let bitcoind_url = env::var("BITCOIN_URL").chain_err(|| "failed to read BITCOIN_URL")?;
    let bitcoind_user = env::var("BITCOIN_USER").chain_err(|| "failed to read BITCOIN_USER")?;
    let bitcoind_password =
        env::var("BITCOIN_PASSWORD").chain_err(|| "failed to read BITCOIN_PASSWORD")?;

    println!(
        "fetching channel data from bitcoind for {}",
        &channel.short_channel_id
    );

    let client =
        jsonrpc::client::Client::new(bitcoind_url, Some(bitcoind_user), Some(bitcoind_password));

    let scid_parts: Vec<&str> = channel.short_channel_id.split("x").collect();
    let blockheight: i64 = scid_parts[0].parse().chain_err(|| "failed to parse scid")?;
    let txindex: usize = scid_parts[1].parse().chain_err(|| "failed to parse scid")?;
    let txoutputindex: usize = scid_parts[2].parse().chain_err(|| "failed to parse scid")?;

    let blockhash: String = client
        .do_rpc("getblockhash", &[Value::Number(Number::from(blockheight))])
        .chain_err(|| "failed to getblockhash")?;

    let block: Block = client
        .do_rpc("getblock", &[Value::String(blockhash)])
        .chain_err(|| "failed to getblock")?;

    let transactionid = &block.tx[txindex];

    let rawtransaction: String = client
        .do_rpc("getrawtransaction", &[Value::String(transactionid.clone())])
        .chain_err(|| "failed to getrawtransaction")?;

    let transaction: DecodedTransaction = client
        .do_rpc("decoderawtransaction", &[Value::String(rawtransaction)])
        .chain_err(|| "failed to decoderawtransaction")?;

    let address = &transaction.vout[txoutputindex].script_pub_key.addresses[0];

    Ok(ChannelBitcoinData {
        block: blockheight,
        transaction: transactionid.to_string(),
        address: address.to_string(),
    })
}
