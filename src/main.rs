// Cargo.toml dependencies:
// [dependencies]
// tokio = { version = "1", features = ["full"] }
// serde = { version = "1", features = ["derive"] }
// serde_json = "1"
// reqwest = { version = "0.11", features = ["json"] }
// anyhow = "1"
// hex = "0.4"
// clap = { version = "4", features = ["derive"] }

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about = "Multi-Chain EVM RPC Test Harness", long_about = None)]
struct Args {
    /// RPC URL to test against
    #[arg(short, long)]
    url: String,

    /// Chain type: optimism, arbitrum, ethereum, or auto-detect
    #[arg(short, long, default_value = "auto")]
    chain: String,

    /// Timeout in seconds for each request (default: 30)
    #[arg(short, long, default_value_t = 30)]
    timeout: u64,

    /// Run only specific test by name (optional)
    #[arg(short = 'f', long)]
    filter: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Skip chain-specific tests
    #[arg(long)]
    skip_chain_specific: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum ChainType {
    Optimism,
    Arbitrum,
    Ethereum,
    Unknown,
}

impl ChainType {
    fn from_chain_id(chain_id: u64) -> Self {
        match chain_id {
            1 => ChainType::Ethereum,
            10 | 11155420 => ChainType::Optimism,
            42161 | 421614 => ChainType::Arbitrum,
            _ => ChainType::Unknown,
        }
    }

    fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "optimism" | "op" => ChainType::Optimism,
            "arbitrum" | "arb" => ChainType::Arbitrum,
            "ethereum" | "eth" | "geth" => ChainType::Ethereum,
            _ => ChainType::Unknown,
        }
    }

    fn name(&self) -> &str {
        match self {
            ChainType::Optimism => "Optimism",
            ChainType::Arbitrum => "Arbitrum",
            ChainType::Ethereum => "Ethereum",
            ChainType::Unknown => "Unknown",
        }
    }

    fn sample_contract(&self) -> &str {
        match self {
            ChainType::Optimism => "0x4200000000000000000000000000000000000006", // OP token
            ChainType::Arbitrum => "0x912CE59144191C1204E64559FE8253a0e49E6548", // ARB token
            ChainType::Ethereum => "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
            ChainType::Unknown => "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC as fallback
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcRequest {
    jsonrpc: String,
    method: String,
    params: serde_json::Value,
    id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcResponse<T> {
    jsonrpc: String,
    id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

struct RpcClient {
    url: String,
    client: reqwest::Client,
    request_id: std::sync::atomic::AtomicU64,
    verbose: bool,
    chain_type: ChainType,
}

impl RpcClient {
    fn new(url: String, timeout_secs: u64, verbose: bool, chain_type: ChainType) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            url,
            client,
            request_id: std::sync::atomic::AtomicU64::new(1),
            verbose,
            chain_type,
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let id = self
            .request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id,
        };

        if self.verbose {
            println!("  → Request: {}", serde_json::to_string(&request)?);
        }

        let response = self
            .client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .context("Failed to send RPC request")?;

        let rpc_response: RpcResponse<T> = response
            .json()
            .await
            .context("Failed to parse RPC response")?;

        if self.verbose {
            println!("  ← Response ID: {}", rpc_response.id);
        }

        if let Some(error) = rpc_response.error {
            anyhow::bail!("RPC error: {} (code: {})", error.message, error.code);
        }

        rpc_response
            .result
            .context("RPC response missing result field")
    }
}

struct TestResults {
    passed: Vec<String>,
    failed: Vec<(String, String)>,
}

impl TestResults {
    fn new() -> Self {
        Self {
            passed: Vec::new(),
            failed: Vec::new(),
        }
    }

    fn add_pass(&mut self, test_name: String) {
        self.passed.push(test_name);
    }

    fn add_fail(&mut self, test_name: String, error: String) {
        self.failed.push((test_name, error));
    }

    fn print_summary(&self) {
        println!("\n{}", "=".repeat(80));
        println!("Test Summary");
        println!("{}\n", "=".repeat(80));
        println!("✓ Passed: {}", self.passed.len());
        println!("✗ Failed: {}", self.failed.len());
        println!("Total: {}\n", self.passed.len() + self.failed.len());

        if !self.failed.is_empty() {
            println!("Failed tests:");
            for (name, error) in &self.failed {
                println!("  ✗ {}: {}", name, error);
            }
        }
    }
}

async fn run_test<F, Fut>(
    results: &mut TestResults,
    test_name: &str,
    filter: &Option<String>,
    optional: bool,
    test_fn: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    if let Some(f) = filter {
        if !test_name.to_lowercase().contains(&f.to_lowercase()) {
            return;
        }
    }

    let label = if optional {
        format!("{} (optional)", test_name)
    } else {
        test_name.to_string()
    };

    print!("Running test: {}... ", label);
    match test_fn().await {
        Ok(_) => {
            println!("✓ PASS");
            results.add_pass(test_name.to_string());
        }
        Err(e) => {
            let err_str = e.to_string();
            // Check if it's a "method not available" error for optional tests
            if optional && (err_str.contains("does not exist") || err_str.contains("not available") || err_str.contains("-32601")) {
                println!("⊘ SKIP (method not available on this node)");
                // Don't count as failure for optional tests
            } else {
                println!("✗ FAIL");
                println!("  Error: {:#}", e);
                results.add_fail(test_name.to_string(), e.to_string());
            }
        }
    }
}

// Core EVM RPC Tests (work on all chains)

async fn test_eth_chain_id(client: &RpcClient) -> Result<()> {
    let chain_id: String = client.call("eth_chainId", json!([])).await?;
    let id = u64::from_str_radix(chain_id.trim_start_matches("0x"), 16)?;
    
    let detected_chain = ChainType::from_chain_id(id);
    println!("  Chain ID: {} ({} - {})", chain_id, id, detected_chain.name());
    Ok(())
}

async fn test_eth_block_number(client: &RpcClient) -> Result<()> {
    let block_number: String = client.call("eth_blockNumber", json!([])).await?;
    let num = u64::from_str_radix(block_number.trim_start_matches("0x"), 16)?;
    println!("  Current block: {} ({})", block_number, num);
    
    if num == 0 {
        anyhow::bail!("Block number should be greater than 0");
    }
    Ok(())
}

async fn test_eth_gas_price(client: &RpcClient) -> Result<()> {
    let gas_price: String = client.call("eth_gasPrice", json!([])).await?;
    let price = u64::from_str_radix(gas_price.trim_start_matches("0x"), 16)?;
    println!("  Gas price: {} wei ({} gwei)", gas_price, price / 1_000_000_000);
    Ok(())
}

async fn test_eth_get_balance(client: &RpcClient) -> Result<()> {
    let zero_address = "0x0000000000000000000000000000000000000000";
    let balance: String = client
        .call("eth_getBalance", json!([zero_address, "latest"]))
        .await?;
    println!("  Balance of zero address: {}", balance);
    
    if !balance.starts_with("0x") {
        anyhow::bail!("Balance should start with 0x");
    }
    Ok(())
}

async fn test_eth_get_transaction_count(client: &RpcClient) -> Result<()> {
    let sample_address = client.chain_type.sample_contract();
    let count: String = client
        .call("eth_getTransactionCount", json!([sample_address, "latest"]))
        .await?;
    
    let nonce = u64::from_str_radix(count.trim_start_matches("0x"), 16)?;
    println!("  Transaction count: {} ({})", count, nonce);
    Ok(())
}

async fn test_eth_get_block_by_number(client: &RpcClient) -> Result<()> {
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", false]))
        .await?;
    
    let number = block["number"]
        .as_str()
        .context("Block should have number field")?;
    let num = u64::from_str_radix(number.trim_start_matches("0x"), 16)?;
    println!("  Block number: {} ({})", number, num);
    
    if block.get("hash").is_none() {
        anyhow::bail!("Block should have hash field");
    }
    Ok(())
}

async fn test_eth_get_block_by_hash(client: &RpcClient) -> Result<()> {
    // First get latest block to get its hash
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", false]))
        .await?;
    
    let hash = block["hash"]
        .as_str()
        .context("Block should have hash field")?;
    
    // Now fetch by hash
    let block_by_hash: serde_json::Value = client
        .call("eth_getBlockByHash", json!([hash, false]))
        .await?;
    
    let number = block_by_hash["number"]
        .as_str()
        .context("Block should have number field")?;
    println!("  Block by hash {}: {}", hash, number);
    Ok(())
}

async fn test_eth_get_transaction_by_block_hash_and_index(client: &RpcClient) -> Result<()> {
    // Get a recent block with transactions
    let mut found_tx = false;
    
    // Try last few blocks to find one with transactions
    for i in 0..10 {
        let block_tag = if i == 0 {
            "latest".to_string()
        } else {
            let latest: String = client.call("eth_blockNumber", json!([])).await?;
            let latest_num = u64::from_str_radix(latest.trim_start_matches("0x"), 16)?;
            format!("0x{:x}", latest_num.saturating_sub(i))
        };
        
        let block: serde_json::Value = client
            .call("eth_getBlockByNumber", json!([block_tag, false]))
            .await?;
        
        let hash = block["hash"]
            .as_str()
            .context("Block should have hash")?;
        
        let txs = block["transactions"]
            .as_array()
            .context("Block should have transactions array")?;
        
        if !txs.is_empty() {
            // Get first transaction by block hash and index
            let tx: serde_json::Value = client
                .call("eth_getTransactionByBlockHashAndIndex", json!([hash, "0x0"]))
                .await?;
            
            if let Some(tx_hash) = tx.get("hash") {
                println!("  Transaction at index 0: {}", tx_hash);
                found_tx = true;
                break;
            }
        }
    }
    
    if !found_tx {
        println!("  No transactions found in recent blocks (this is OK for test nets)");
    }
    
    Ok(())
}

async fn test_eth_get_block_with_txs(client: &RpcClient) -> Result<()> {
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", true]))
        .await?;
    
    let txs = block["transactions"]
        .as_array()
        .context("Block should have transactions array")?;
    println!("  Latest block has {} transactions", txs.len());
    Ok(())
}

async fn test_eth_call(client: &RpcClient) -> Result<()> {
    let contract = client.chain_type.sample_contract();
    let call_data = json!({
        "to": contract,
        "data": "0x18160ddd" // totalSupply()
    });
    
    let result: String = client
        .call("eth_call", json!([call_data, "latest"]))
        .await?;
    println!("  Call result: {}", result);
    
    if !result.starts_with("0x") {
        anyhow::bail!("Call result should start with 0x");
    }
    Ok(())
}

async fn test_eth_estimate_gas(client: &RpcClient) -> Result<()> {
    let tx = json!({
        "from": "0x0000000000000000000000000000000000000001",
        "to": "0x0000000000000000000000000000000000000002",
        "value": "0x1"
    });
    
    let estimate: String = client.call("eth_estimateGas", json!([tx])).await?;
    let gas = u64::from_str_radix(estimate.trim_start_matches("0x"), 16)?;
    println!("  Estimated gas: {} ({})", estimate, gas);
    Ok(())
}

async fn test_eth_fee_history(client: &RpcClient) -> Result<()> {
    let history: serde_json::Value = client
        .call("eth_feeHistory", json!([4, "latest", [25, 50, 75]]))
        .await?;
    
    if history.get("baseFeePerGas").is_none() {
        anyhow::bail!("Fee history should have baseFeePerGas");
    }
    
    let base_fees = history["baseFeePerGas"].as_array().unwrap();
    if let Some(latest_fee) = base_fees.last() {
        if let Some(fee_str) = latest_fee.as_str() {
            let fee = u64::from_str_radix(fee_str.trim_start_matches("0x"), 16)?;
            println!("  Latest base fee: {} wei ({} gwei)", fee_str, fee / 1_000_000_000);
        }
    }
    Ok(())
}

async fn test_eth_get_logs(client: &RpcClient) -> Result<()> {
    let contract = client.chain_type.sample_contract();
    let filter = json!({
        "fromBlock": "latest",
        "toBlock": "latest",
        "address": contract
    });
    
    let logs: serde_json::Value = client.call("eth_getLogs", json!([filter])).await?;
    let log_array = logs.as_array().context("Logs should be an array")?;
    println!("  Found {} logs in latest block", log_array.len());
    Ok(())
}

async fn test_net_version(client: &RpcClient) -> Result<()> {
    let version: String = client.call("net_version", json!([])).await?;
    println!("  Network version: {}", version);
    Ok(())
}

async fn test_web3_client_version(client: &RpcClient) -> Result<()> {
    let version: String = client.call("web3_clientVersion", json!([])).await?;
    println!("  Client version: {}", version);
    Ok(())
}

// Chain-specific tests

async fn test_optimism_output_at_block(client: &RpcClient) -> Result<()> {
    let output: serde_json::Value = client
        .call("optimism_outputAtBlock", json!(["latest"]))
        .await?;
    println!("  Output root available: {}", output.get("outputRoot").is_some());
    Ok(())
}

async fn test_arbitrum_get_l1_confirmations(client: &RpcClient) -> Result<()> {
    // Get latest block first
    let block_num: String = client.call("eth_blockNumber", json!([])).await?;
    
    let confirmations: serde_json::Value = client
        .call("arb_getL1Confirmations", json!([block_num]))
        .await?;
    println!("  L1 confirmations: {}", confirmations);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Multi-Chain EVM RPC Test Harness v{}", env!("CARGO_PKG_VERSION"));
    println!("RPC URL: {}", args.url);
    println!("Timeout: {}s", args.timeout);

    // Detect or set chain type
    let mut chain_type = if args.chain == "auto" {
        ChainType::Unknown
    } else {
        ChainType::from_string(&args.chain)
    };

    // Auto-detect chain if needed
    if chain_type == ChainType::Unknown {
        println!("Auto-detecting chain type...");
        let temp_client = RpcClient::new(args.url.clone(), args.timeout, false, ChainType::Unknown);
        if let Ok(chain_id) = temp_client.call::<String>("eth_chainId", json!([])).await {
            if let Ok(id) = u64::from_str_radix(chain_id.trim_start_matches("0x"), 16) {
                chain_type = ChainType::from_chain_id(id);
            }
        }
    }

    println!("Chain type: {}\n", chain_type.name());

    if let Some(ref f) = args.filter {
        println!("Filter: Only running tests matching '{}'\n", f);
    }

    let client = RpcClient::new(args.url, args.timeout, args.verbose, chain_type.clone());
    let mut results = TestResults::new();

    println!("=== Core EVM RPC Methods ===\n");
    
    run_test(&mut results, "eth_chainId", &args.filter, false, || {
        test_eth_chain_id(&client)
    }).await;
    
    run_test(&mut results, "eth_blockNumber", &args.filter, false, || {
        test_eth_block_number(&client)
    }).await;
    
    run_test(&mut results, "eth_gasPrice", &args.filter, false, || {
        test_eth_gas_price(&client)
    }).await;
    
    run_test(&mut results, "eth_getBalance", &args.filter, false, || {
        test_eth_get_balance(&client)
    }).await;
    
    run_test(&mut results, "eth_getTransactionCount", &args.filter, false, || {
        test_eth_get_transaction_count(&client)
    }).await;
    
    run_test(&mut results, "eth_getBlockByNumber (no txs)", &args.filter, false, || {
        test_eth_get_block_by_number(&client)
    }).await;
    
    run_test(&mut results, "eth_getBlockByHash", &args.filter, false, || {
        test_eth_get_block_by_hash(&client)
    }).await;
    
    run_test(&mut results, "eth_getBlockByNumber (with txs)", &args.filter, false, || {
        test_eth_get_block_with_txs(&client)
    }).await;
    
    run_test(&mut results, "eth_getTransactionByBlockHashAndIndex", &args.filter, false, || {
        test_eth_get_transaction_by_block_hash_and_index(&client)
    }).await;
    
    run_test(&mut results, "eth_call", &args.filter, false, || {
        test_eth_call(&client)
    }).await;
    
    run_test(&mut results, "eth_estimateGas", &args.filter, false, || {
        test_eth_estimate_gas(&client)
    }).await;
    
    run_test(&mut results, "eth_feeHistory", &args.filter, false, || {
        test_eth_fee_history(&client)
    }).await;
    
    run_test(&mut results, "eth_getLogs", &args.filter, false, || {
        test_eth_get_logs(&client)
    }).await;
    
    run_test(&mut results, "net_version", &args.filter, false, || {
        test_net_version(&client)
    }).await;
    
    run_test(&mut results, "web3_clientVersion", &args.filter, false, || {
        test_web3_client_version(&client)
    }).await;

    // Chain-specific tests
    if !args.skip_chain_specific {
        match client.chain_type {
            ChainType::Optimism => {
                println!("\n=== Optimism-Specific Methods ===\n");
                run_test(&mut results, "optimism_outputAtBlock", &args.filter, true, || {
                    test_optimism_output_at_block(&client)
                }).await;
            }
            ChainType::Arbitrum => {
                println!("\n=== Arbitrum-Specific Methods ===\n");
                run_test(&mut results, "arb_getL1Confirmations", &args.filter, true, || {
                    test_arbitrum_get_l1_confirmations(&client)
                }).await;
            }
            ChainType::Ethereum => {
                println!("\n=== Ethereum Mainnet (no chain-specific methods) ===\n");
            }
            ChainType::Unknown => {
                println!("\n=== Unknown chain - skipping chain-specific tests ===\n");
            }
        }
    }

    results.print_summary();

    if !results.failed.is_empty() {
        std::process::exit(1);
    }

    Ok(())
}