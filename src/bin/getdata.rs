#![recursion_limit = "1024"]

extern crate docopt;
extern crate jsonrpc;
extern crate reqwest;
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate error_chain;

use docopt::Docopt;
use rusqlite::types::ToSql;
use rusqlite::{Connection, NO_PARAMS};
use serde::Deserialize;
use serde_json::{Number, Value};
use std::collections::HashMap;
use std::env;
use std::time::Duration;

use bitcoin::*;
use errors::*;
use lightning::*;
use smartbit::*;

const USAGE: &'static str = "
getdata

Usage:
  getdata [--justcheckcloses]

Options:
  --justcheckcloses     Skips fetching all lightning channels and upserting them (developer usage only).
";

#[derive(Deserialize)]
struct Args {
    flag_justcheckcloses: bool,
}

quick_main! {run}

fn run() -> Result<()> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    println!("creating sqlite database");
    let conn = Connection::open("channels.db").chain_err(|| "failed to open database")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS channels (
            short_channel_id TEXT PRIMARY KEY,

            open_block INTEGER,
            open_transaction TEXT,
            address TEXT,
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

    let mut i = 0;

    if !args.flag_justcheckcloses {
        let channels = getchannels()?;
        println!("inserting {} channels", channels.len());

        for channel in channels.iter() {
            let (node0, node1) = if channel.source < channel.destination {
                (&channel.source, &channel.destination)
            } else {
                (&channel.destination, &channel.source)
            };

            conn.execute(
                "INSERT INTO channels
                (short_channel_id, node0, node1, satoshis, last_seen)
            VALUES (?1, ?2, ?3, ?4, datetime('now'))
            ON CONFLICT (short_channel_id) DO UPDATE SET last_seen = excluded.last_seen",
                &[
                    &channel.short_channel_id as &dyn ToSql,
                    node0 as &dyn ToSql,
                    node1 as &dyn ToSql,
                    &channel.satoshis as &dyn ToSql,
                ],
            )
            .chain_err(|| "failed to insert")?;

            println!(
                "  {}: inserted {}-[{}]-{} at {}",
                &i, node0, &channel.satoshis, node1, &channel.short_channel_id
            );
            i += 1;
        }

        println!("getting blockchain data");
        i = 0;
        let mut q =
            conn.prepare("SELECT short_channel_id FROM channels WHERE open_block IS NULL")?;
        let mut rows = q.query(NO_PARAMS)?;
        while let Some(row) = rows.next()? {
            let short_channel_id: String = row.get(0)?;

            let blockchaindata = getchannelblockchaindata(&short_channel_id)?;
            conn.execute(
                "UPDATE channels SET open_block = ?2, open_transaction = ?3, address = ?4
            WHERE short_channel_id = ?1",
                &[
                    &short_channel_id as &dyn ToSql,
                    &blockchaindata.block as &dyn ToSql,
                    &blockchaindata.transaction as &dyn ToSql,
                    &blockchaindata.address as &dyn ToSql,
                ],
            )
            .chain_err(|| "failed to update with blockchain data")?;

            println!(
                "  {}: updated with blockchain data: {} {} {}",
                &i, &blockchaindata.block, &blockchaindata.transaction, &blockchaindata.address
            );
            i += 1;
        }
    }

    println!("inspecting channels that may have been closed");
    i = 0;
    let mut q = conn.prepare(
        "SELECT short_channel_id, address FROM channels
        WHERE close_block IS NULL and last_seen < datetime('now', '-1 day')",
    )?;
    let mut rows = q.query(NO_PARAMS)?;
    while let Some(row) = rows.next()? {
        let short_channel_id: String = row.get(0)?;
        let address: String = row.get(1)?;
        println!(
            "  {}: checking {}, address {}",
            i, short_channel_id, address
        );

        match getchannelclosedata(address)? {
            None => println!("  {}: still open.", i),
            Some(closedata) => {
                conn.execute(
                    "UPDATE channels SET close_block = ?2, close_transaction = ?3
                    WHERE short_channel_id = ?1",
                    &[
                        &short_channel_id as &dyn ToSql,
                        &closedata.block as &dyn ToSql,
                        &closedata.transaction as &dyn ToSql,
                    ],
                )
                .chain_err(|| "failed to update with blockchain data")?;

                println!("  {}: was closed!", i);
            }
        };
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

fn getchannelblockchaindata(short_channel_id: &String) -> Result<ChannelBitcoinData> {
    let bitcoind_url = env::var("BITCOIN_URL").chain_err(|| "failed to read BITCOIN_URL")?;
    let bitcoind_user = env::var("BITCOIN_USER").chain_err(|| "failed to read BITCOIN_USER")?;
    let bitcoind_password =
        env::var("BITCOIN_PASSWORD").chain_err(|| "failed to read BITCOIN_PASSWORD")?;

    let client =
        jsonrpc::client::Client::new(bitcoind_url, Some(bitcoind_user), Some(bitcoind_password));

    let scid_parts: Vec<&str> = short_channel_id.split("x").collect();
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

struct ChannelCloseData {
    block: i64,
    transaction: String,
}

fn getchannelclosedata(address: String) -> Result<Option<ChannelCloseData>> {
    let mut w = reqwest::get(
        format!(
            "https://api.smartbit.com.au/v1/blockchain/address/{}",
            address
        )
        .as_str(),
    )
    .chain_err(|| "smartbit address call failed")?;

    let response: SmartbitResponse = w
        .json()
        .chain_err(|| "failed to decode smartbit response")?;

    if response.address.transactions.len() == 2 {
        Ok(Some(ChannelCloseData {
            block: response.address.transactions[1].block,
            transaction: response.address.transactions[1].txid.clone(),
        }))
    } else {
        Ok(None)
    }
}

mod errors {
    error_chain! {
      foreign_links {
        SQLite(rusqlite::Error);
      }
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

mod smartbit {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct SmartbitResponse {
        pub address: Address,
    }

    #[derive(Deserialize)]
    pub struct Address {
        pub transactions: Vec<Transaction>,
    }

    #[derive(Deserialize)]
    pub struct Transaction {
        pub txid: String,
        pub block: i64,
    }
}
