//! Differential JSON-RPC test harness.
//!
//! Sends identical JSON-RPC requests from a corpus file to TWO endpoints
//! (a geth URL and a reth URL) and compares the responses after normalization,
//! reporting per-case IDENTICAL/DIFFERS plus a summary.
//!
//! This validates that our custom reth is byte-identical to custom geth for the
//! methods `eth_estimateGasBundle` and `eth_calls`.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about = "Differential EVM RPC test harness", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Compare responses from two endpoints for a corpus of requests.
    Diff(DiffArgs),
}

#[derive(Parser, Debug)]
struct DiffArgs {
    /// geth HTTP endpoint URL
    #[arg(long)]
    geth_url: String,

    /// reth HTTP endpoint URL
    #[arg(long)]
    reth_url: String,

    /// Path to the JSON corpus file
    #[arg(long)]
    corpus: String,

    /// Optional block number hex string (e.g. "0x185b6ee") to pin state-dependent
    /// requests to a common block so results are comparable.
    #[arg(long)]
    state_block: Option<String>,

    /// Verbose output (prints the request sent and raw responses)
    #[arg(long)]
    verbose: bool,

    /// Per-request timeout in seconds
    #[arg(long, default_value_t = 30)]
    timeout: u64,
}

/// A single corpus entry.
#[derive(Debug, Deserialize)]
struct CorpusRequest {
    name: String,
    method: String,
    params: Value,
}

/// Top-level corpus file shape.
#[derive(Debug, Deserialize)]
struct Corpus {
    requests: Vec<CorpusRequest>,
}

/// The JSON-RPC request wire shape (mirrors `RpcRequest` in `main.rs`).
#[derive(Debug, Serialize)]
struct RpcRequest {
    jsonrpc: String,
    method: String,
    params: Value,
    id: u64,
}

/// Recursively remove any object keys with a name in `keys`, anywhere in `v`.
fn strip_keys(v: &mut Value, keys: &[&str]) {
    match v {
        Value::Object(map) => {
            for k in keys {
                map.remove(*k);
            }
            for (_k, child) in map.iter_mut() {
                strip_keys(child, keys);
            }
        }
        Value::Array(arr) => {
            for child in arr.iter_mut() {
                strip_keys(child, keys);
            }
        }
        _ => {}
    }
}

/// Parse a hex string like "0x185b6ee" into a u64.
fn parse_hex_u64(s: &str) -> Result<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(trimmed, 16).with_context(|| format!("invalid hex number: {s}"))
}

/// Rewrite a corpus request's params to pin it to a common state block.
///
/// - `eth_estimateGasBundle`: set params[0].stateBlockNumber = state_block and
///   params[0].blockNumber = (state_block + 1) as hex, only for fields that exist.
/// - `eth_calls`: set the 2nd positional param (params[1]) — the block tag — to
///   state_block.
///
/// Implemented defensively: only touches fields/positions that are present.
fn rewrite_state_block(method: &str, params: &mut Value, state_block: &str) -> Result<()> {
    match method {
        "eth_estimateGasBundle" => {
            if let Some(obj) = params.get_mut(0).and_then(Value::as_object_mut) {
                if obj.contains_key("stateBlockNumber") {
                    obj.insert("stateBlockNumber".to_string(), json!(state_block));
                }
                if obj.contains_key("blockNumber") {
                    let next = parse_hex_u64(state_block)? + 1;
                    obj.insert("blockNumber".to_string(), json!(format!("0x{next:x}")));
                }
            }
        }
        "eth_calls" => {
            if let Some(arr) = params.as_array_mut()
                && arr.len() >= 2
            {
                arr[1] = json!(state_block);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Send a single JSON-RPC request and return the full response as a `Value`.
async fn send_request(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: &Value,
) -> Result<Value> {
    let request = RpcRequest {
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params: params.clone(),
        id: 1,
    };

    let response = client
        .post(url)
        .json(&request)
        .send()
        .await
        .context("failed to send RPC request")?;

    let body: Value = response
        .json()
        .await
        .context("failed to parse RPC response as JSON")?;

    Ok(body)
}

/// Core comparison: strip non-deterministic keys from both responses, then
/// compare. `serde_json::Value` objects are `BTreeMap`-backed (no
/// `preserve_order` feature), so `==` ignores object key order while remaining
/// order-sensitive for arrays — exactly what we want.
fn responses_match(geth: &mut Value, reth: &mut Value) -> bool {
    const STRIP: &[&str] = &["transactionHash", "blockHash"];
    strip_keys(geth, STRIP);
    strip_keys(reth, STRIP);
    geth == reth
}

async fn run_diff(args: DiffArgs) -> Result<i32> {
    let corpus_raw = std::fs::read_to_string(&args.corpus)
        .with_context(|| format!("failed to read corpus file: {}", args.corpus))?;
    let corpus: Corpus = serde_json::from_str(&corpus_raw)
        .with_context(|| format!("failed to parse corpus file: {}", args.corpus))?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()
        .context("failed to build HTTP client")?;

    let total = corpus.requests.len();
    let mut identical = 0usize;
    let mut differ = 0usize;

    for req in &corpus.requests {
        let mut params = req.params.clone();
        if let Some(sb) = &args.state_block {
            rewrite_state_block(&req.method, &mut params, sb)
                .with_context(|| format!("state-block rewrite failed for '{}'", req.name))?;
        }

        if args.verbose {
            println!(
                "\n=== {} ({}) ===\nrequest params: {}",
                req.name,
                req.method,
                serde_json::to_string_pretty(&params).unwrap_or_default()
            );
        }

        let (geth_res, reth_res) = tokio::join!(
            send_request(&client, &args.geth_url, &req.method, &params),
            send_request(&client, &args.reth_url, &req.method, &params),
        );

        match (geth_res, reth_res) {
            (Ok(mut geth), Ok(mut reth)) => {
                if args.verbose {
                    println!("geth raw: {geth}");
                    println!("reth raw: {reth}");
                }
                if responses_match(&mut geth, &mut reth) {
                    identical += 1;
                    println!("{:40} IDENTICAL \u{2713}", req.name);
                } else {
                    differ += 1;
                    println!("{:40} DIFFERS \u{2717}", req.name);
                    println!(
                        "  --- geth (normalized) ---\n{}",
                        serde_json::to_string_pretty(&geth).unwrap_or_default()
                    );
                    println!(
                        "  --- reth (normalized) ---\n{}",
                        serde_json::to_string_pretty(&reth).unwrap_or_default()
                    );
                }
            }
            (geth_res, reth_res) => {
                differ += 1;
                println!("{:40} FAILURE \u{2717} (transport error)", req.name);
                if let Err(e) = &geth_res {
                    println!("  geth error: {e:#}");
                }
                if let Err(e) = &reth_res {
                    println!("  reth error: {e:#}");
                }
            }
        }
    }

    println!("\n{identical} identical, {differ} differ (of {total})");

    Ok(if differ == 0 { 0 } else { 1 })
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let code = match cli.command {
        Command::Diff(args) => run_diff(args).await?,
    };
    std::process::exit(code);
}
