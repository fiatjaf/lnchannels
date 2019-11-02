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
use rusqlite::OptionalExtension;
use rusqlite::{Connection, NO_PARAMS};
use serde::Deserialize;
use std::env;
use std::time::Duration;

use crate::bitcoin::*;
use crate::errors::*;
use crate::esplora::*;
use crate::lightning::*;

const USAGE: &'static str = "
getdata

Usage:
  getdata [--all] [--skipgossip] [--listchannels] [--listnodes] [--checkcloses] [--enrich] [--materialize]

Options:
  --listchannels    fetches channels from the lightning network and save them
  --listnodes       fetches nodes from the lightning network and save their aliases
  --checkcloses     uses a third-party API to check if channels are closed
  --enrich          uses a local blockchain to enrich the channel transactions data
  --materialize     reset the materialized views
  --all             do all steps (default)
  --skipgossip      do all steps except fetch channels or nodes
";

#[derive(Deserialize)]
struct Args {
    flag_listchannels: bool,
    flag_listnodes: bool,
    flag_checkcloses: bool,
    flag_enrich: bool,
    flag_materialize: bool,
    flag_all: bool,
    flag_skipgossip: bool,
}

struct StepsToRun {
    listchannels: bool,
    listnodes: bool,
    checkcloses: bool,
    enrich: bool,
    materialize: bool,
}

quick_main! {run}

fn run() -> Result<()> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let mut all = args.flag_all;
    if !all
        && !args.flag_skipgossip
        && !args.flag_listchannels
        && !args.flag_listnodes
        && !args.flag_checkcloses
        && !args.flag_enrich
        && !args.flag_materialize
    {
        all = true;
    }

    let mut run = StepsToRun {
        listchannels: args.flag_all || args.flag_listchannels,
        listnodes: args.flag_all || args.flag_listnodes,
        checkcloses: args.flag_all || args.flag_checkcloses,
        enrich: args.flag_all || args.flag_enrich,
        materialize: args.flag_all || args.flag_materialize,
    };

    if !all && args.flag_skipgossip {
        run.listchannels = false;
        run.listnodes = false;
        run.checkcloses = true;
        run.enrich = true;
        run.materialize = true;
    }

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

            close_type TEXT,
            close_htlc_count INTEGER,
            close_balance_a INTEGER, -- we don't know if node0 is A or B
            close_balance_b INTEGER, -- and vice-versa

            node0 TEXT NOT NULL,
            node1 TEXT NOT NULL,

            satoshis INTEGER,
            last_update INTEGER,
            last_seen DATETIME NOT NULL
        )",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create table channels")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS nodealiases (
            pubkey TEXT NOT NULL,
            alias TEXT NOT NULL,
            first_seen DATETIME NOT NULL,
            last_seen DATETIME NOT NULL
        )",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create table nodealiases")?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS policies (
            short_channel_id TEXT NOT NULL,
            direction INTEGER NOT NULL, -- 1 means from node0 to node1 and vice-versa

            base_fee_millisatoshi INTEGER NOT NULL,
            fee_per_millionth INTEGER NOT NULL,
            delay INTEGER NOT NULL,
            
            update_time INTEGER NOT NULL
        )",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create table policies")?;

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
        "CREATE INDEX IF NOT EXISTS index_pubkey ON nodealiases(pubkey)",
        NO_PARAMS,
    )
    .chain_err(|| "failed to create index")?;

    let mut i = 0;
    if run.listchannels {
        let channels = getchannels()?;
        println!("inserting {} channels", channels.len());

        let mut last_inserted = "";

        for channel in channels.iter() {
            if last_inserted != channel.short_channel_id {
                i += 1;

                // insert into channel table
                let (node0, node1) = if channel.source < channel.destination {
                    (&channel.source, &channel.destination)
                } else {
                    (&channel.destination, &channel.source)
                };

                conn.execute(
                    "INSERT INTO channels
                        (short_channel_id, node0, node1, satoshis, last_update, last_seen)
                    VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
                    ON CONFLICT (short_channel_id)
                        DO UPDATE SET last_seen = excluded.last_seen, last_update = excluded.last_update",
                &[
                    &channel.short_channel_id as &dyn ToSql,
                    node0 as &dyn ToSql,
                    node1 as &dyn ToSql,
                    &channel.satoshis as &dyn ToSql,
                    &channel.last_update as &dyn ToSql,
                ],
            )
            .chain_err(|| "failed to insert")?;
                println!(
                    "  {}: inserted {}-[{}]-{} at {}",
                    &i, node0, &channel.satoshis, node1, &channel.short_channel_id
                );

                last_inserted = &channel.short_channel_id;
            }

            // now update channel policy
            let towards = if channel.source < channel.destination {
                1
            } else {
                0
            };

            let isfeepolicyuptodate: Option<bool> = conn
                .query_row(
                    "SELECT
                      CASE WHEN base_fee_millisatoshi = ?3 AND fee_per_millionth = ?4 AND delay = ?5
                        THEN 1
                        ELSE 0
                      END
                    FROM policies
                    WHERE short_channel_id = ?1 AND direction = ?2
                    ORDER BY update_time DESC
                    LIMIT 1
                ",
                    &[
                        &channel.short_channel_id as &dyn ToSql,
                        &towards as &dyn ToSql,
                        &channel.base_fee_millisatoshi as &dyn ToSql,
                        &channel.fee_per_millionth as &dyn ToSql,
                        &channel.delay as &dyn ToSql,
                    ],
                    |row| row.get(0).map(|integervalue: i64| integervalue == 1),
                )
                .optional()
                .chain_err(|| "failed to prequery policy")?;

            match isfeepolicyuptodate {
                Some(true) => (), /* current policy is equal to the last stored */
                _ => {
                    /* otherwise -- either Some(false) or None -- insert the current policy */
                    conn.execute(
                        "INSERT INTO policies
                            (short_channel_id, direction,
                             base_fee_millisatoshi, fee_per_millionth, delay,
                             update_time)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    ",
                        &[
                            &channel.short_channel_id as &dyn ToSql,
                            &towards as &dyn ToSql,
                            &channel.base_fee_millisatoshi as &dyn ToSql,
                            &channel.fee_per_millionth as &dyn ToSql,
                            &channel.delay as &dyn ToSql,
                            &channel.last_update as &dyn ToSql,
                        ],
                    )
                    .chain_err(|| "failed to add policy")?;

                    println!(
                        "    {} {}: updated policy ({})->({})",
                        &i, &channel.short_channel_id, &channel.source, &channel.destination
                    );
                }
            }
        }
    }

    if run.listnodes {
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

    if run.enrich {
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

    if run.checkcloses {
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

            match getchannelclosedata(address) {
                Ok(None) => println!("  {}: still open.", i),
                Ok(Some(closedata)) => {
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
                Err(err) => {
                    println!("  {}: failed to check: {}", i, &err);
                }
            };
            i += 1;
        }
    }

    if run.enrich {
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

        i = 0;
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

        i = 0;
        let mut q = conn.prepare(
            "SELECT short_channel_id, close_transaction FROM channels
            WHERE close_transaction IS NOT NULL and close_type IS NULL",
        )?;
        let mut rows = q.query(NO_PARAMS)?;
        while let Some(row) = rows.next()? {
            let short_channel_id: String = row.get(0)?;
            let close_transaction: String = row.get(1)?;

            println!("  {}: enriching channel {}", i, short_channel_id);
            let close_type_data = getclosetypedata(&close_transaction)?;
            conn.execute(
                "UPDATE channels
                SET close_type = ?2,
                close_balance_a = ?3,
                close_balance_b = ?4,
                close_htlc_count = ?5
                WHERE short_channel_id = ?1",
                &[
                    &short_channel_id as &dyn ToSql,
                    &close_type_data.typ as &dyn ToSql,
                    &close_type_data.balance_a as &dyn ToSql,
                    &close_type_data.balance_b as &dyn ToSql,
                    &close_type_data.htlcs as &dyn ToSql,
                ],
            )
            .chain_err(|| "failed to enrich channel data")?;
            println!(
                "  {}: {} enriched with type {}, balance {}/{}, {} htlcs",
                i,
                short_channel_id,
                close_type_data.typ,
                close_type_data.balance_a,
                close_type_data.balance_b,
                close_type_data.htlcs,
            );
            i += 1;
        }
    }

    // create materialized views
    if run.materialize {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS nodes (
            pubkey TEXT PRIMARY KEY,
            oldestchannel INTEGER NOT NULL,
            openchannels INTEGER NOT NULL,
            closedchannels INTEGER NOT NULL,
            capacity INTEGER NOT NULL,
            avg_duration INTEGER NOT NULL,
            avg_open_fee INTEGER NOT NULL,
            avg_close_fee INTEGER,
            alias TEXT
        )",
            NO_PARAMS,
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS globalstats (
            last_block INTEGER NOT NULL,
            max_channel_duration INTEGER NOT NULL,
            max_channel_open_fee INTEGER NOT NULL,
            max_channel_close_fee INTEGER NOT NULL,
            max_channel_satoshis INTEGER NOT NULL,
            max_node_capacity INTEGER NOT NULL,
            max_node_openchannels INTEGER NOT NULL,
            max_node_closedchannels INTEGER NOT NULL,
            max_node_allchannels INTEGER NOT NULL,
            max_node_close_rate INTEGER NOT NULL,
            max_node_average_duration INTEGER NOT NULL,
            max_node_average_open_fee INTEGER NOT NULL,
            max_node_average_close_fee INTEGER NOT NULL
        )",
            NO_PARAMS,
        )?;

        conn.execute("DELETE FROM nodes", NO_PARAMS)?;
        conn.execute("DELETE FROM globalstats", NO_PARAMS)?;

        conn.execute(
        r#"
        INSERT INTO nodes
          (pubkey, alias, oldestchannel,
            openchannels, closedchannels, capacity,
            avg_duration, avg_open_fee, avg_close_fee)
        SELECT
          n.pubkey, n.alias, agg.oldestchannel,
            open.openchannels, agg.closedchannels, open.capacity,
            agg.avg_duration, agg.avg_open_fee, agg.avg_close_fee
        FROM nodealiases AS n
        INNER JOIN (
          SELECT pubkey, count(*) AS openchannels, sum(satoshis) AS capacity FROM (
            SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
          ) WHERE close_block IS NULL GROUP BY pubkey
        ) AS open ON open.pubkey = n.pubkey
        INNER JOIN (
          SELECT pubkey,
            min(open_block) AS oldestchannel,
            count(close_block) AS closedchannels,
            avg(CASE WHEN close_block IS NOT NULL
              THEN close_block
              ELSE (SELECT open_block FROM channels ORDER BY open_block DESC LIMIT 1)
            END - open_block) AS avg_duration,
            avg(open_fee) AS avg_open_fee,
            avg(close_fee) AS avg_close_fee
          FROM (
            SELECT node0 AS pubkey, * FROM channels UNION ALL SELECT node1 AS pubkey, * FROM channels
          ) GROUP BY pubkey
        ) AS agg ON agg.pubkey = n.pubkey
        GROUP BY n.pubkey
        ORDER BY n.last_seen
        "#,
        NO_PARAMS,
    )?;

        conn.execute(
            r#"
        WITH last_block AS (
          SELECT max(b) AS last_block
          FROM (
              SELECT max(open_block) AS b FROM channels
            UNION ALL
              SELECT max(close_block) AS b FROM channels
          )
        )

        INSERT INTO globalstats
        SELECT
          (SELECT last_block FROM last_block), -- last_block
          channels.max_duration      , -- max_channel_duration
          channels.max_open_fee      , -- max_channel_open_fee
          channels.max_close_fee     , -- max_channel_close_fee
          channels.max_satoshis      , -- max_channel_satoshis
          nodes.max_capacity         , -- max_node_capacity
          nodes.max_openchannels     , -- max_node_openchannels
          nodes.max_closedchannels   , -- max_node_closedchannels
          nodes.max_allchannels      , -- max_node_allchannels
          nodes.max_close_rate       , -- max_node_close_rate
          nodes.max_average_duration , -- max_node_average_duration
          nodes.max_average_open_fee , -- max_node_average_open_fee
          nodes.max_average_close_fee  -- max_node_average_close_fee
        FROM (
          SELECT
            max(CASE
              WHEN close_block IS NOT NULL THEN close_block
              ELSE (SELECT last_block FROM last_block)
            END - open_block) AS max_duration,
            max(open_fee) AS max_open_fee,
            max(close_fee) AS max_close_fee,
            max(satoshis) AS max_satoshis
          FROM channels
        ) AS channels
        JOIN (
          SELECT
            max(capacity) AS max_capacity,
            max(openchannels) AS max_openchannels,
            max(closedchannels) AS max_closedchannels,
            max(openchannels + closedchannels) AS max_allchannels,
            max(closedchannels / openchannels) AS max_close_rate,
            max(avg_duration) AS max_average_duration,
            max(avg_open_fee) AS max_average_open_fee,
            max(avg_close_fee) AS max_average_close_fee
          FROM nodes
        ) AS nodes
        "#,
            NO_PARAMS,
        )?;
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
        .danger_accept_invalid_certs(true)
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
        .danger_accept_invalid_certs(true)
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

struct ChannelCloseTypeData {
    typ: String,
    balance_a: i64,
    balance_b: i64,
    htlcs: i64,
}

fn getclosetypedata(txid: &String) -> Result<ChannelCloseTypeData> {
    let bitcoind_url = env::var("BITCOIN_URL").chain_err(|| "failed to read BITCOIN_URL")?;
    let bitcoind_user = env::var("BITCOIN_USER").chain_err(|| "failed to read BITCOIN_USER")?;
    let bitcoind_password =
        env::var("BITCOIN_PASSWORD").chain_err(|| "failed to read BITCOIN_PASSWORD")?;

    let client =
        jsonrpc::client::Client::new(bitcoind_url, Some(bitcoind_user), Some(bitcoind_password));

    // defaults
    let mut detail = ChannelCloseTypeData {
        typ: "unknown".to_string(),
        balance_a: 0,
        balance_b: 0,
        htlcs: 0,
    };

    // try to determine what happened when the channel was closed by inspecting the closing transaction
    // and the ones after it.
    let transaction = readtransaction(&client, txid)?;
    let mut outs: Vec<&str> = Vec::new();

    // label each output of the closing transaction (we'll have to look at the transactions that spend them)
    for o in transaction.vout.iter() {
        if o.script_pub_key.typ == "witness_v0_keyhash" {
            outs.push("pubkey");
            continue;
        } else {
            let address = &o.script_pub_key.addresses[0];
            let mut w = reqwest::get(
                format!("https://blockstream.info/api/address/{}/txs", address).as_str(),
            )
            .chain_err(|| "esplora address call failed")?;

            let transactions: Vec<EsploraTx> =
                w.json().chain_err(|| "failed to decode esplora response")?;

            let mut witness = Vec::new();

            // find the followup transaction and the witness data we need to determine the type of the previous
            for followuptx in transactions.iter() {
                // inspect the script from the witness data of the followup transaction to determine this.
                let next = readtransaction(&client, &followuptx.txid)?;

                // find the output we're interested in if the followup transaction uses many
                let mut found_yet = false;
                for input in next.vin.iter() {
                    if input.txid == transaction.txid && input.vout == o.n {
                        witness = input.txinwitness.clone();
                        found_yet = true;
                        break;
                    }
                }

                if found_yet {
                    break;
                }
            }

            if witness.len() == 0 {
                // didn't find a witness.
                // transaction wasn't spent (and also isn't a pubkey). very odd.
                // however, we'll assume it's an htlc if there are more than 2 outputs
                if transaction.vout.len() > 2 {
                    outs.push("htlc");
                } else {
                    outs.push("unknown");
                }
                continue;
            }

            let script = decodescript(&client, &witness[witness.len() - 1])?;
            if script.contains("OP_HASH160") {
                outs.push("htlc");
            } else if script.contains("OP_CHECKSEQUENCEVERIFY") {
                if witness[witness.len() - 2] == "01" {
                    outs.push("penalty");
                } else {
                    outs.push("balance");
                }
            }
        }
    }

    // now that we have labels for all outputs we use a simple (maybe wrong?) heuristic
    // to determine what happened.
    if outs.len() == 1 && outs[0] == "pubkey" {
        detail.typ = "unused".to_string();
        detail.balance_a = transaction.vout[0].sat;
    } else if outs.len() == 2 && outs[0] == "pubkey" && outs[1] == "pubkey" {
        detail.typ = "mutual".to_string();
        detail.balance_a = transaction.vout[0].sat;
        detail.balance_b = transaction.vout[1].sat;
    } else {
        let mut i = 0;
        for out in outs.iter() {
            if *out == "htlc" {
                detail.htlcs += 1;
                continue;
            }

            if *out == "penalty" {
                detail.typ = "penalty".to_string();
                if detail.balance_a == 0 {
                    detail.balance_a = transaction.vout[i].sat;
                } else if detail.balance_b == 0 {
                    detail.balance_b = transaction.vout[i].sat;
                } else {
                    panic!("3 balances!")
                }
                continue;
            }

            if *out == "balance" || *out == "pubkey" {
                detail.typ = "force".to_string();

                if detail.balance_a == 0 {
                    detail.balance_a = transaction.vout[i].sat;
                } else if detail.balance_b == 0 {
                    detail.balance_b = transaction.vout[i].sat;
                } else {
                    // this should never happen, but theoretically it's possible for both peers to agree
                    // to spend the funding transaction to multiple pubkeys or whatever
                    detail.typ = "unknown".to_string();
                }
            }

            i += 1;
        }
    }

    Ok(detail)
}

struct ChannelCloseData {
    block: i64,
    transaction: String,
}

fn getchannelclosedata(address: String) -> Result<Option<ChannelCloseData>> {
    let mut w =
        reqwest::get(format!("https://blockstream.info/api/address/{}/txs", address).as_str())
            .chain_err(|| "esplora address call failed")?;

    let transactions: Vec<EsploraTx> =
        w.json().chain_err(|| "failed to decode esplora response")?;

    if transactions.len() == 2 && transactions[0].status.confirmed {
        Ok(Some(ChannelCloseData {
            block: transactions[0].status.block_height.clone(),
            transaction: transactions[0].txid.clone(),
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
        pub last_update: i64,
        pub base_fee_millisatoshi: i64,
        pub fee_per_millionth: i64,
        pub delay: i64,
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
        pub txid: String,
        pub vout: Vec<Output>,
        pub vin: Vec<Input>,
    }

    #[derive(Deserialize)]
    struct SegwitScript {
        pub asm: String,
    }

    #[derive(Deserialize)]
    pub struct Input {
        pub txid: String,
        pub vout: i64,
        pub txinwitness: Vec<String>,
    }

    #[derive(Deserialize)]
    pub struct Output {
        pub n: i64,
        #[serde(rename(deserialize = "scriptPubKey"))]
        pub script_pub_key: ScriptPubKey,
        #[serde(rename(deserialize = "value"), deserialize_with = "btctosat")]
        pub sat: i64,
    }

    #[derive(Deserialize)]
    pub struct ScriptPubKey {
        #[serde(default)]
        pub addresses: Vec<String>,
        #[serde(rename(deserialize = "type"))]
        pub typ: String,
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

    pub fn decodescript(client: &jsonrpc::client::Client, script: &String) -> Result<String> {
        let data: SegwitScript = client
            .do_rpc("decodescript", &[Value::String(script.clone())])
            .chain_err(|| "failed to decodescript")?;

        Ok(data.asm)
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

mod esplora {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct EsploraTx {
        pub txid: String,
        pub status: EsploraTxStatus,
    }

    #[derive(Deserialize)]
    pub struct EsploraInput {
        pub prevout: EsploraPrevout,
    }

    #[derive(Deserialize)]
    pub struct EsploraPrevout {
        pub scriptpubkey_address: String,
    }

    #[derive(Deserialize)]
    pub struct EsploraTxStatus {
        pub confirmed: bool,
        pub block_height: i64,
    }
}
