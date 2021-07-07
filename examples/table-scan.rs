use anyhow::{anyhow, Result};
use scylla::{
    prepared_statement::PreparedStatement, IntoTypedRows, QueryResult, Session, SessionBuilder,
};
use std::{ops::RangeInclusive, sync::Arc};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Config {
    /// Uri of a live Scylla instance
    #[structopt(short, long, default_value = "127.0.0.1:9042")]
    uri: String,

    /// Shows token function's ranges used in query WHERE clauses
    #[structopt(short, long)]
    show_parallel_ranges: bool,

    keyspace_name: String,
    table_name: String,
    partition_key_name: String,

    /// Uses synchronous_scan if <= 1
    concurrency: usize,
}

async fn synchronous_scan(session: Arc<Session>, config: &Config) -> Result<usize> {
    let query_str = format!(
        "SELECT COUNT(*) FROM {}.{} BYPASS CACHE USING TIMEOUT 5h",
        config.keyspace_name, config.table_name
    );

    let res = session.query(query_str, &[]).await?;
    extract_count(res)
}

async fn parallel_scan(session: Arc<Session>, config: &Config) -> Result<usize> {
    let prepare_str = format!(
        "SELECT COUNT(*) FROM {ks}.{t} WHERE token({pk}) >= ? AND token({pk}) <= ? BYPASS CACHE USING TIMEOUT 5h",
        ks = config.keyspace_name,
        t = config.table_name,
        pk = config.partition_key_name,
    );

    let select_stmt = session.prepare(prepare_str).await?;

    let ranges = TokenFunctionSubRanges::new(config.concurrency);
    if config.show_parallel_ranges {
        println!("ranges: {:x?}", ranges.clone().collect::<Vec<_>>());
    }

    let futures =
        ranges.map(|range| execute_and_count(session.clone(), select_stmt.clone(), range));
    let handles = futures.map(tokio::spawn).collect::<Vec<_>>(); // collect to spawn all tasks

    let mut sum = 0;
    for handle in handles {
        let result = handle.await.expect("The query sending task panicked");
        match result {
            Err(err) => return Err(err),
            Ok(res) => sum += res,
        }
    }

    Ok(sum)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_args();

    println!("{:?}", config);
    let session: Session = SessionBuilder::new()
        .known_node(config.uri.clone())
        .build()
        .await?;
    let session = Arc::new(session);

    if config.concurrency > 1 {
        let number_of_rows = parallel_scan(session.clone(), &config).await?;
        println!("row count computed by parallel_scan: {}", number_of_rows);
    } else {
        let number_of_rows = synchronous_scan(session.clone(), &config).await?;
        println!("row count computed by synchronous_scan: {}", number_of_rows);
    }

    Ok(())
}

// extract count information from query result
fn extract_count(res: QueryResult) -> Result<usize> {
    if let Some(rows) = res.rows {
        if let Some(row) = rows.into_typed::<(i64,)>().next() {
            let (count,) = row?;
            return Ok(count as usize);
        }
    }

    Err(anyhow!("Empty query result"))
}

#[derive(Clone)]
struct TokenFunctionSubRanges {
    start: i64,
    minimal_len_of_each_range: u64,
    ranges_to_grow: u64,
}

impl TokenFunctionSubRanges {
    fn new(count: usize) -> TokenFunctionSubRanges {
        Self {
            start: i64::MIN + 1, // -(2^63-1)
            minimal_len_of_each_range: u64::MAX / count as u64,
            ranges_to_grow: u64::MAX % count as u64,
        }
    }
}

impl Iterator for TokenFunctionSubRanges {
    type Item = RangeInclusive<i64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start == i64::MAX {
            return None;
        }

        let range_len = if self.ranges_to_grow > 0 {
            self.ranges_to_grow -= 1;
            self.minimal_len_of_each_range + 1
        } else {
            self.minimal_len_of_each_range
        };

        let range = RangeInclusive::new(self.start, self.start + (range_len - 1) as i64);
        if *range.end() == i64::MAX {
            self.start = i64::MAX;
        } else {
            // range_len can be 1 greater than i64::MAX, so it cannot be casted to i64
            self.start += (range_len - 1) as i64;
            self.start += 1;
        }

        Some(range)
    }
}

async fn execute_and_count(
    session: Arc<Session>,
    select_stmt: PreparedStatement,
    range: RangeInclusive<i64>,
) -> Result<usize> {
    let res = session
        .execute(&select_stmt, (*range.start(), *range.end()))
        .await?;
    extract_count(res)
}
