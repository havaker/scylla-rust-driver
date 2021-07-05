use anyhow::Result;
use futures::stream::StreamExt;
use scylla::{Session, SessionBuilder};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Config {
    #[structopt(short, long, default_value = "127.0.0.1:9042")]
    uri: String,

    keyspace_name: String,
    table_name: String,

    concurrency: usize,
}

async fn naive_attempt(session: &Session, config: &Config) -> Result<usize> {
    let query_str = format!(
        "SELECT * FROM {}.{}",
        config.keyspace_name, config.table_name
    );

    let mut received_row_count = 0;

    let mut row_stream = session.query_iter(query_str, &[]).await?;
    while let Some(row_res) = row_stream.next().await {
        if let Err(err) = row_res {
            return Err(err.into());
        }
        received_row_count += 1;
    }

    Ok(received_row_count)
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_args();

    println!("{:?}", config);
    let session: Session = SessionBuilder::new()
        .known_node(config.uri.clone())
        .build()
        .await?;

    let number_of_rows = naive_attempt(&session, &config).await?;
    println!("number_of_rows = {}", number_of_rows);
    Ok(())
}
