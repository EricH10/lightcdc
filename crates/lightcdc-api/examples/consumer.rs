//! Demonstrates seeking, subscribing, printing, and acknowledging through gRPC.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use lightcdc_api::proto::{
    AckRequest, ChangeEvent, Operation, SeekPosition, SeekRequest, SubscribeRequest,
    light_cdc_client::LightCdcClient,
};
use tokio_postgres::NoTls;

/// Parses options for the example streaming consumer.
#[derive(Debug, Parser)]
#[command(about = "Print events from a lightcdc gRPC stream and ack them")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    #[arg(long, default_value = "orders")]
    stream: String,

    #[arg(long, default_value = "example-printer")]
    consumer: String,

    #[arg(long, default_value_t = 0)]
    limit: u32,

    #[arg(long, value_enum)]
    seek: Option<SeekStart>,

    #[arg(long)]
    seed_sql: Option<PathBuf>,

    #[arg(
        long,
        default_value = "postgres://lightcdc:lightcdc@localhost:5432/lightcdc"
    )]
    database_url: String,
}

/// Selects where the consumer offset should start before subscribing.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum SeekStart {
    Earliest,
    Latest,
}

/// Connects to lightcdc, optionally runs seed SQL, prints events, and acks them.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut client = LightCdcClient::connect(args.endpoint.clone())
        .await
        .with_context(|| format!("connect to {}", args.endpoint))?;
    let mut ack_client = client.clone();

    if let Some(seek) = args.seek {
        let position = match seek {
            SeekStart::Earliest => SeekPosition::Earliest,
            SeekStart::Latest => SeekPosition::Latest,
        };
        let response = client
            .seek(SeekRequest {
                stream: args.stream.clone(),
                consumer: args.consumer.clone(),
                position: position as i32,
                sequence: 0,
            })
            .await
            .context("seek consumer offset")?
            .into_inner();

        eprintln!(
            "consumer {} on stream {} positioned at offset {}",
            args.consumer, args.stream, response.offset
        );
    }

    let mut events = client
        .subscribe(SubscribeRequest {
            stream: args.stream.clone(),
            consumer: args.consumer.clone(),
            limit: args.limit,
        })
        .await
        .context("subscribe to stream")?
        .into_inner();

    if let Some(seed_sql) = &args.seed_sql {
        run_seed_sql(&args.database_url, seed_sql).await?;
    }

    while let Some(event) = events.message().await.context("receive stream event")? {
        print_event(&event)?;
        ack_client
            .ack(AckRequest {
                stream: args.stream.clone(),
                consumer: args.consumer.clone(),
                sequence: event.sequence,
            })
            .await
            .with_context(|| format!("ack sequence {}", event.sequence))?;
    }

    Ok(())
}

/// Runs a SQL file against PostgreSQL to create demo changes.
async fn run_seed_sql(database_url: &str, path: &PathBuf) -> Result<()> {
    let sql = std::fs::read_to_string(path)
        .with_context(|| format!("read seed SQL from {}", path.display()))?;
    let (client, connection) = tokio_postgres::connect(database_url, NoTls)
        .await
        .with_context(|| format!("connect to postgres at {database_url}"))?;

    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {error}");
        }
    });

    client
        .batch_execute(&sql)
        .await
        .with_context(|| format!("run seed SQL from {}", path.display()))?;

    connection_task.abort();
    eprintln!("ran seed SQL from {}", path.display());
    Ok(())
}

/// Prints one streamed event in a compact human-readable format.
fn print_event(event: &ChangeEvent) -> Result<()> {
    let source = event
        .source
        .as_ref()
        .map(|source| format!("{}:{}@{}", source.database, source.slot, source.lsn))
        .unwrap_or_else(|| "unknown-source".to_owned());

    println!(
        "#{sequence} {operation} {schema}.{table} {source}",
        sequence = event.sequence,
        operation = operation_name(event.operation),
        schema = event.schema,
        table = event.table,
        source = source
    );

    if let Some(key) = format_payload(&event.key)? {
        println!("  key: {key}");
    }
    if let Some(before) = format_payload(&event.before)? {
        println!("  before: {before}");
    }
    if let Some(after) = format_payload(&event.after)? {
        println!("  after: {after}");
    }

    Ok(())
}

/// Formats optional event payload bytes as JSON when possible.
fn format_payload(payload: &Option<Vec<u8>>) -> Result<Option<String>> {
    let Some(payload) = payload else {
        return Ok(None);
    };

    match serde_json::from_slice::<serde_json::Value>(payload) {
        Ok(value) => Ok(Some(serde_json::to_string(&value)?)),
        Err(_) => Ok(Some(String::from_utf8_lossy(payload).into_owned())),
    }
}

/// Converts a protobuf operation enum value into display text.
fn operation_name(operation: i32) -> &'static str {
    match Operation::try_from(operation).unwrap_or(Operation::Unspecified) {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
        Operation::Truncate => "truncate",
        Operation::Unspecified => "unspecified",
    }
}
