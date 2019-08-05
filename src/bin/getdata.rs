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
use std::env;
use std::time::Duration;

use crate::bitcoin::*;
use crate::errors::*;
use crate::lightning::*;
use crate::smartbit::*;

const USAGE: &'static str = "
getdata

Usage:
  getdata [--skiplistchannels] [--justcheckcloses] [--justenrich] [--justupdatealiases]

Options:
  --skiplistchannels      Skips the `listchannels` part and (re-)inserting all channels in the database.
  --justupdatealiases     Skips everything but updating node aliases.
  --justcheckcloses       Skips everything but checking channels closes (developer usage only).
  --justenrich            Skips everything but fetching time and fee from transactions (developer usage only).
";

#[derive(Deserialize)]
struct Args {
    flag_skiplistchannels: bool,
    flag_justupdatealiases: bool,
    flag_justcheckcloses: bool,
    flag_justenrich: bool,
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
            open_time DATETIME,
            open_transaction TEXT,
            open_fee INTEGER,

            address TEXT,

            close_block INTEGER,
            close_time DATETIME,
            close_transaction TEXT,
            close_fee INTEGER,

            node0 TEXT NOT NULL,
            node1 TEXT NOT NULL,

            satoshis INTEGER,
            last_seen DATETIME NOT NULL
        )",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create table")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS nodealiases (
            pubkey TEXT NOT NULL,
            alias TEXT NOT NULL,
            first_seen DATETIME NOT NULL,
            last_seen DATETIME NOT NULL
        )",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create table")?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS index_scid ON channels(short_channel_id)",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create index")?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS index_node0 ON channels(node0)",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create index")?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS index_node1 ON channels(node1)",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create index")?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS index_pubkey ON channels(pubkey)",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create index")?;

    let mut i = 0;
    if !args.flag_justcheckcloses
        && !args.flag_justenrich
        && !args.flag_justupdatealiases
        && !args.flag_skiplistchannels
    {
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
    }

    if !args.flag_justcheckcloses && !args.flag_justenrich {
        let nodes = getnodes()?;
        println!("inserting {} aliases", nodes.len());

        i = 0;
        for node in nodes.iter() {
            // query current name for node
            let last_seen: String = conn
                .query_row_and_then(
                    "SELECT last_seen FROM (
                        SELECT last_seen, pubkey, alias
                        FROM nodealiases
                        WHERE pubkey = ?1
                        ORDER BY last_seen DESC
                        LIMIT 1
                    ) WHERE alias = ?2
                    UNION ALL SELECT ''",
                    &[&node.nodeid as &dyn ToSql, &node.alias as &dyn ToSql],
                    |row| row.get(0),
                )
                .chain_err(|| "failed to query existing alias")?;

            // if the current name is different, insert
            if last_seen == "" {
                conn.execute(
                    "INSERT INTO nodealiases (pubkey, alias, first_seen, last_seen)
                    VALUES (?1, ?2, datetime('now'), datetime('now'))",
                    &[&node.nodeid as &dyn ToSql, &node.alias as &dyn ToSql],
                )
                .chain_err(|| "failed to insert")?;

                println!("  {}: inserted {} {}", &i, &node.nodeid, &node.alias);
            } else {
                // otherwise update last_seen
                conn.execute(
                    "UPDATE nodealiases
                    SET last_seen = datetime('now')
                    WHERE last_seen = ?1 AND pubkey = ?2",
                    &[&last_seen as &dyn ToSql, &node.nodeid as &dyn ToSql],
                )
                .chain_err(|| "failed to update")?;

                println!("  {}: updated {} {}", &i, &node.nodeid, &node.alias);
            };

            i += 1;
        }
    }

    if !args.flag_justcheckcloses && !args.flag_justenrich && !args.flag_justupdatealiases {
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

    if !args.flag_justenrich && !args.flag_justupdatealiases {
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

                    println!("  {}: was closed on block {}", i, &closedata.block);
                }
            };
            i += 1;
        }
    }

    if !args.flag_justcheckcloses && !args.flag_justupdatealiases {
        println!("adding more transaction data to closed channels");
        i = 0;

        let mut q = conn.prepare(
            "SELECT short_channel_id, open_block, open_transaction FROM channels
            WHERE open_block IS NOT NULL and open_time IS NULL",
        )?;
        let mut rows = q.query(NO_PARAMS)?;
        while let Some(row) = rows.next()? {
            let short_channel_id: String = row.get(0)?;
            let open_block: i64 = row.get(1)?;
            let open_transaction: String = row.get(2)?;

            println!("  {}: enriching channel {}", i, short_channel_id);
            let open_data = gettransactionblockchaindata(open_block, &open_transaction)?;
            conn.execute(
                "UPDATE channels
                SET open_time = ?2, open_fee = ?3
                WHERE short_channel_id = ?1",
                &[
                    &short_channel_id as &dyn ToSql,
                    &open_data.time as &dyn ToSql,
                    &open_data.fee as &dyn ToSql,
                ],
            )
            .chain_err(|| "failed to enrich channel data")?;
            println!(
                "  {}: {} enriched with {}/{}",
                i, short_channel_id, open_data.time, open_data.fee
            );
            i += 1;
        }

        let mut q = conn.prepare(
            "SELECT short_channel_id, close_block, close_transaction FROM channels
            WHERE close_block IS NOT NULL and close_time IS NULL",
        )?;
        let mut rows = q.query(NO_PARAMS)?;
        while let Some(row) = rows.next()? {
            let short_channel_id: String = row.get(0)?;
            let close_block: i64 = row.get(1)?;
            let close_transaction: String = row.get(2)?;

            println!("  {}: enriching channel {}", i, short_channel_id);
            let close_data = gettransactionblockchaindata(close_block, &close_transaction)?;
            conn.execute(
                "UPDATE channels
                SET close_time = ?2, close_fee = ?3
                WHERE short_channel_id = ?1",
                &[
                    &short_channel_id as &dyn ToSql,
                    &close_data.time as &dyn ToSql,
                    &close_data.fee as &dyn ToSql,
                ],
            )
            .chain_err(|| "failed to enrich channel data")?;
            println!(
                "  {}: {} enriched with {}/{}",
                i, short_channel_id, close_data.time, close_data.fee
            );
            i += 1;
        }
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

    let mut w = client
        .post(&spark_url)
        .header("X-Access", spark_token)
        .body(r#"{"method": "listchannels"}"#)
        .send()
        .chain_err(|| "listchannels call failed")?;

    let listchannels: ListChannels = w.json().chain_err(|| "failed to decode listchannels")?;

    Ok(listchannels.channels)
}

fn getnodes() -> Result<Vec<Node>> {
    let mut spark_url = env::var("SPARK_URL").chain_err(|| "failed to read SPARK_URL")?;
    let spark_token = env::var("SPARK_TOKEN").chain_err(|| "failed to read SPARK_TOKEN")?;

    println!("fetching nodes from {}", &spark_url);

    spark_url.push_str("/rpc");
    let client = reqwest::Client::builder()
        .gzip(true)
        .timeout(Duration::from_secs(120))
        .build()
        .chain_err(|| "failed to make spark client")?;

    let mut w = client
        .post(&spark_url)
        .header("X-Access", spark_token)
        .body(r#"{"method": "listnodes"}"#)
        .send()
        .chain_err(|| "listnodes call failed")?;

    let listnodes: ListNodes = w.json().chain_err(|| "failed to decode listnodes")?;

    Ok(listnodes.nodes)
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

    let block = readblock(&client, blockheight)?;
    let transactionid = &block.tx[txindex];
    let transaction = readtransaction(&client, transactionid)?;

    let address = &transaction.vout[txoutputindex].script_pub_key.addresses[0];

    Ok(ChannelBitcoinData {
        block: blockheight,
        transaction: transactionid.to_string(),
        address: address.to_string(),
    })
}

struct TransactionData {
    time: i64,
    fee: i64,
}

fn gettransactionblockchaindata(blockheight: i64, txid: &String) -> Result<TransactionData> {
    let bitcoind_url = env::var("BITCOIN_URL").chain_err(|| "failed to read BITCOIN_URL")?;
    let bitcoind_user = env::var("BITCOIN_USER").chain_err(|| "failed to read BITCOIN_USER")?;
    let bitcoind_password =
        env::var("BITCOIN_PASSWORD").chain_err(|| "failed to read BITCOIN_PASSWORD")?;

    let client =
        jsonrpc::client::Client::new(bitcoind_url, Some(bitcoind_user), Some(bitcoind_password));

    let block = readblock(&client, blockheight)?;

    let transaction = readtransaction(&client, txid)?;
    let sats_out: i64 = transaction.vout.iter().fold(0, |acc, out| acc + out.sat);
    let mut sats_in = 0;
    for input in transaction.vin.iter() {
        let input_transaction = readtransaction(&client, &input.txid)?;
        let index = input.vout as usize;
        sats_in += input_transaction.vout[index].sat;
    }

    Ok(TransactionData {
        time: block.time,
        fee: sats_in - sats_out,
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

    if response.address.transactions.len() == 2 && response.address.transactions[1].block.is_some()
    {
        let transactions = response.address.transactions;
        let closetx = if transactions[0].block > transactions[1].block {
            &transactions[0]
        } else {
            &transactions[1]
        };

        Ok(Some(ChannelCloseData {
            block: closetx.block.unwrap(),
            transaction: closetx.txid.clone(),
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

    #[derive(Deserialize)]
    pub struct ListNodes {
        pub nodes: Vec<Node>,
    }

    #[derive(Deserialize)]
    pub struct Node {
        pub nodeid: String,
        #[serde(default)]
        pub alias: String,
    }
}

mod bitcoin {
    use crate::errors::*;
    use serde::{Deserialize, Deserializer};
    use serde_json::{Number, Value};
    use std::result::Result as StdResult;

    #[derive(Deserialize)]
    pub struct Block {
        pub tx: Vec<String>,
        pub time: i64,
    }

    #[derive(Deserialize)]
    pub struct DecodedTransaction {
        pub vout: Vec<Output>,
        pub vin: Vec<Input>,
    }

    #[derive(Deserialize)]
    pub struct Input {
        pub txid: String,
        pub vout: i64,
    }

    #[derive(Deserialize)]
    pub struct Output {
        #[serde(rename(deserialize = "scriptPubKey"))]
        pub script_pub_key: ScriptPubKey,
        #[serde(rename(deserialize = "value"), deserialize_with = "btctosat")]
        pub sat: i64,
    }

    #[derive(Deserialize)]
    pub struct ScriptPubKey {
        #[serde(default)]
        pub addresses: Vec<String>,
    }

    pub fn readtransaction(
        client: &jsonrpc::client::Client,
        txid: &String,
    ) -> Result<DecodedTransaction> {
        let rawtransaction: String = client
            .do_rpc("getrawtransaction", &[Value::String(txid.clone())])
            .chain_err(|| "failed to getrawtransaction")?;

        let transaction = client
            .do_rpc("decoderawtransaction", &[Value::String(rawtransaction)])
            .chain_err(|| "failed to decoderawtransaction")?;

        Ok(transaction)
    }

    pub fn readblock(client: &jsonrpc::client::Client, blockheight: i64) -> Result<Block> {
        let blockhash: String = client
            .do_rpc("getblockhash", &[Value::Number(Number::from(blockheight))])
            .chain_err(|| "failed to getblockhash")?;

        let block: Block = client
            .do_rpc("getblock", &[Value::String(blockhash)])
            .chain_err(|| "failed to getblock")?;

        Ok(block)
    }

    fn btctosat<'de, D>(amount: D) -> StdResult<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let btc: f64 = Deserialize::deserialize(amount)?;
        Ok((btc * 100_000_000f64) as i64)
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
        pub block: Option<i64>,
    }
}
