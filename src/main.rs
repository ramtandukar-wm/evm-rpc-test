use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};

#[derive(Parser, Debug)]
#[command(author, version, about = "Multi-Chain EVM RPC Test Harness", long_about = None)]
struct Args {
    /// RPC URL to test against (http://, https://, ws://, or wss://)
    #[arg(short, long)]
    url: String,

    /// Chain type: optimism, arbitrum, ethereum, bsc, or auto-detect
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

    /// Test WebSocket subscriptions (requires ws:// or wss:// URL)
    #[arg(long)]
    test_subscriptions: bool,

    /// Duration to listen for subscription events in seconds (default: 5)
    #[arg(long, default_value_t = 5)]
    subscription_duration: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum ChainType {
    Optimism,
    Arbitrum,
    Ethereum,
    BSC,
    Base,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
enum NodeType {
    Geth,
    Reth,
    OpGeth,
    OpReth,
    Nitro,
    BSCGeth,
    Unknown,
}

impl NodeType {
    fn from_client_version(version: &str) -> Self {
        let version_lower = version.to_lowercase();
        if version_lower.contains("geth") {
            if version_lower.contains("optimism") {
                NodeType::OpGeth
            } else if version_lower.contains("bsc") || version_lower.contains("binance") {
                NodeType::BSCGeth
            } else {
                NodeType::Geth
            }
        } else if version_lower.contains("reth") {
            if version_lower.contains("optimism") || version_lower.contains("op-reth") {
                NodeType::OpReth
            } else {
                NodeType::Reth
            }
        } else if version_lower.contains("nitro") {
            NodeType::Nitro
        } else {
            NodeType::Unknown
        }
    }

    fn name(&self) -> &str {
        match self {
            NodeType::Geth => "Geth",
            NodeType::Reth => "Reth",
            NodeType::OpGeth => "op-geth",
            NodeType::OpReth => "op-reth",
            NodeType::Nitro => "Nitro",
            NodeType::BSCGeth => "BSC-Geth",
            NodeType::Unknown => "Unknown",
        }
    }

    fn supports_debug_namespace(&self) -> bool {
        matches!(self, NodeType::Geth | NodeType::OpGeth | NodeType::BSCGeth)
    }

    fn supports_trace_namespace(&self) -> bool {
        matches!(self, NodeType::Reth | NodeType::OpReth | NodeType::Geth | NodeType::OpGeth | NodeType::BSCGeth)
    }
}

impl ChainType {
    fn from_chain_id(chain_id: u64) -> Self {
        match chain_id {
            1 => ChainType::Ethereum,
            10 | 11155420 => ChainType::Optimism,
            42161 | 421614 => ChainType::Arbitrum,
            8453 | 84532 => ChainType::Base,
            56 | 97 => ChainType::BSC,
            _ => ChainType::Unknown,
        }
    }

    fn from_string(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "optimism" | "op" => ChainType::Optimism,
            "arbitrum" | "arb" => ChainType::Arbitrum,
            "ethereum" | "eth" | "geth" => ChainType::Ethereum,
            "bsc" | "binance" | "bnb" => ChainType::BSC,
            _ => ChainType::Unknown,
        }
    }

    fn name(&self) -> &str {
        match self {
            ChainType::Optimism => "Optimism",
            ChainType::Arbitrum => "Arbitrum",
            ChainType::Ethereum => "Ethereum",
            ChainType::BSC => "BNB Smart Chain (BSC)",
            ChainType::Base => "Base",
            ChainType::Unknown => "Unknown",
        }
    }

    fn sample_contract(&self) -> &str {
        match self {
            ChainType::Optimism => "0x4200000000000000000000000000000000000006", // OP token
            ChainType::Arbitrum => "0x912CE59144191C1204E64559FE8253a0e49E6548", // ARB token
            ChainType::Ethereum => "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", // USDC
            ChainType::BSC => "0x55d398326f99059fF775485246999027E3197955", // USDT on BSC
            ChainType::Base => "0x4200000000000000000000000000000000000006", // WETH on Base (system contract)
            ChainType::Unknown => "0x4200000000000000000000000000000000000006", // System contract as fallback
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
    node_type: NodeType,
    is_websocket: bool,
}

impl RpcClient {
    fn new(url: String, timeout_secs: u64, verbose: bool, chain_type: ChainType, node_type: NodeType) -> Self {
        let is_websocket = url.starts_with("ws://") || url.starts_with("wss://");
        
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
            node_type,
            is_websocket,
        }
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        if self.is_websocket {
            self.call_websocket(method, params).await
        } else {
            self.call_http(method, params).await
        }
    }

    async fn call_http<T: for<'de> Deserialize<'de>>(
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
            println!("  → HTTP Request: {}", serde_json::to_string(&request)?);
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

    async fn call_websocket<T: for<'de> Deserialize<'de>>(
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
            println!("  → WS Request: {}", serde_json::to_string(&request)?);
        }

        let (ws_stream, _) = connect_async(&self.url)
            .await
            .context("Failed to connect to WebSocket")?;

        let (mut write, mut read) = ws_stream.split();

        let msg = Message::Text(serde_json::to_string(&request)?);
        write.send(msg).await.context("Failed to send WebSocket message")?;

        let response = read
            .next()
            .await
            .context("No response from WebSocket")??;

        let response_text = response.to_text()?;
        
        if self.verbose {
            println!("  ← WS Response: {}", response_text);
        }

        let rpc_response: RpcResponse<T> = serde_json::from_str(response_text)
            .context("Failed to parse WebSocket response")?;

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

// WebSocket subscription tests
// WebSocket subscription test functions

async fn test_subscribe_new_heads(client: &RpcClient, duration_secs: u64) -> Result<()> {
    if !client.is_websocket {
        println!("  Skipped (requires WebSocket connection)");
        return Ok(());
    }

    let (ws_stream, _) = connect_async(&client.url).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newHeads"]
    });

    write.send(Message::Text(serde_json::to_string(&subscribe_request)?)).await?;

    let response = read.next().await.context("No subscription response")??;
    let sub_response: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    let sub_id = sub_response["result"].as_str().context("No subscription ID")?;
    
    println!("  Subscription ID: {}", sub_id);

    let mut event_count = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(duration_secs));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let notification: serde_json::Value = serde_json::from_str(&text)?;
                        if notification.get("method").and_then(|v| v.as_str()) == Some("eth_subscription") {
                            event_count += 1;
                            if let Some(block_num) = notification["params"]["result"]["number"].as_str() {
                                println!("  Received new head: {}", block_num);
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            _ = &mut timeout => {
                println!("  Listening timeout reached");
                break;
            }
        }
    }

    let unsubscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_unsubscribe",
        "params": [sub_id]
    });

    write.send(Message::Text(serde_json::to_string(&unsubscribe_request)?)).await?;
    println!("  Total events received: {}", event_count);
    Ok(())
}

async fn test_subscribe_logs(client: &RpcClient, duration_secs: u64) -> Result<()> {
    if !client.is_websocket {
        println!("  Skipped (requires WebSocket connection)");
        return Ok(());
    }

    let (ws_stream, _) = connect_async(&client.url).await?;
    let (mut write, mut read) = ws_stream.split();

    let contract = client.chain_type.sample_contract();
    let subscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["logs", {"address": contract}]
    });

    write.send(Message::Text(serde_json::to_string(&subscribe_request)?)).await?;

    let response = read.next().await.context("No subscription response")??;
    let sub_response: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    let sub_id = sub_response["result"].as_str().context("No subscription ID")?;
    
    println!("  Subscription ID: {}", sub_id);

    let mut event_count = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(duration_secs));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let notification: serde_json::Value = serde_json::from_str(&text)?;
                        if notification.get("method").and_then(|v| v.as_str()) == Some("eth_subscription") {
                            event_count += 1;
                            println!("  Received log event");
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            _ = &mut timeout => {
                println!("  Listening timeout reached");
                break;
            }
        }
    }

    let unsubscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_unsubscribe",
        "params": [sub_id]
    });

    write.send(Message::Text(serde_json::to_string(&unsubscribe_request)?)).await?;
    println!("  Total log events received: {}", event_count);
    Ok(())
}

async fn test_subscribe_pending_transactions(client: &RpcClient, duration_secs: u64) -> Result<()> {
    if !client.is_websocket {
        println!("  Skipped (requires WebSocket connection)");
        return Ok(());
    }

    let (ws_stream, _) = connect_async(&client.url).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["newPendingTransactions"]
    });

    write.send(Message::Text(serde_json::to_string(&subscribe_request)?)).await?;

    let response = read.next().await.context("No subscription response")??;
    let sub_response: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    let sub_id = sub_response["result"].as_str().context("No subscription ID")?;
    
    println!("  Subscription ID: {}", sub_id);

    let mut event_count = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(duration_secs));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let notification: serde_json::Value = serde_json::from_str(&text)?;
                        if notification.get("method").and_then(|v| v.as_str()) == Some("eth_subscription") {
                            event_count += 1;
                            if let Some(tx_hash) = notification["params"]["result"].as_str() {
                                println!("  Received pending tx: {}", tx_hash);
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            _ = &mut timeout => {
                println!("  Listening timeout reached");
                break;
            }
        }
    }

    let unsubscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_unsubscribe",
        "params": [sub_id]
    });

    write.send(Message::Text(serde_json::to_string(&unsubscribe_request)?)).await?;
    println!("  Total pending transactions received: {}", event_count);
    Ok(())
}

async fn test_subscribe_syncing(client: &RpcClient, duration_secs: u64) -> Result<()> {
    if !client.is_websocket {
        println!("  Skipped (requires WebSocket connection)");
        return Ok(());
    }

    let (ws_stream, _) = connect_async(&client.url).await?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["syncing"]
    });

    write.send(Message::Text(serde_json::to_string(&subscribe_request)?)).await?;

    let response = read.next().await.context("No subscription response")??;
    let sub_response: serde_json::Value = serde_json::from_str(response.to_text()?)?;
    let sub_id = sub_response["result"].as_str().context("No subscription ID")?;
    
    println!("  Subscription ID: {}", sub_id);

    let mut event_count = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(duration_secs));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let notification: serde_json::Value = serde_json::from_str(&text)?;
                        if notification.get("method").and_then(|v| v.as_str()) == Some("eth_subscription") {
                            event_count += 1;
                            println!("  Received syncing status update");
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                }
            }
            _ = &mut timeout => {
                println!("  Listening timeout reached");
                break;
            }
        }
    }

    let unsubscribe_request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_unsubscribe",
        "params": [sub_id]
    });

    write.send(Message::Text(serde_json::to_string(&unsubscribe_request)?)).await?;
    println!("  Total syncing events received: {}", event_count);
    Ok(())
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
    // Simple eth_call to get ETH balance (always works)
    let call_data = json!({
        "to": "0x0000000000000000000000000000000000000000",
        "data": "0x"
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

async fn test_eth_get_code(client: &RpcClient) -> Result<()> {
    let contract = client.chain_type.sample_contract();
    let code: String = client
        .call("eth_getCode", json!([contract, "latest"]))
        .await?;
    
    if code.len() > 2 {
        println!("  Contract code length: {} bytes", (code.len() - 2) / 2);
    } else {
        println!("  No code at address (EOA or empty)");
    }
    Ok(())
}

async fn test_eth_get_transaction_receipt(client: &RpcClient) -> Result<()> {
    // Get a recent transaction
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", true]))
        .await?;
    
    let txs = block["transactions"].as_array().context("No transactions array")?;
    
    if let Some(tx) = txs.first() {
        if let Some(hash) = tx["hash"].as_str() {
            let receipt: serde_json::Value = client
                .call("eth_getTransactionReceipt", json!([hash]))
                .await?;
            
            if let Some(status) = receipt.get("status") {
                println!("  Receipt status: {}", status);
            }
            if let Some(gas_used) = receipt.get("gasUsed") {
                println!("  Gas used: {}", gas_used);
            }
            return Ok(());
        }
    }
    
    println!("  No transactions in latest block to get receipt");
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

// Geth-specific tests

async fn test_eth_estimate_gas_bundle(client: &RpcClient) -> Result<()> {
    // Create a simple bundle with one transaction
    let tx_args = json!({
        "from": "0x0000000000000000000000000000000000000001",
        "to": "0x0000000000000000000000000000000000000002",
        "value": "0x1",
        "gas": "0x5208"
    });
    
    let bundle_args = json!({
        "txs": [tx_args],
        "blockNumber": "0x1",
        "stateBlockNumber": "latest"
    });
    
    let result: serde_json::Value = client
        .call("eth_estimateGasBundle", json!([bundle_args]))
        .await?;
    
    if let Some(results) = result.get("results").and_then(|v| v.as_array()) {
        println!("  Estimated gas for {} transactions in bundle", results.len());
    }
    Ok(())
}

async fn test_eth_call_bundle(client: &RpcClient) -> Result<()> {
    // Get current block number
    let block_num: String = client.call("eth_blockNumber", json!([])).await?;
    
    // Create a simple bundle for testing
    // Note: In production, this would require properly signed RLP-encoded transactions
    let bundle_args = json!({
        "txs": [], // Empty array for test - real usage requires signed tx bytes
        "blockNumber": block_num,
        "stateBlockNumber": "latest"
    });
    
    let result: serde_json::Value = client
        .call("eth_callBundle", json!([bundle_args]))
        .await?;
    
    if result.get("results").is_some() {
        println!("  Bundle simulation completed");
    }
    if let Some(gas_used) = result.get("totalGasUsed") {
        println!("  Total gas used: {}", gas_used);
    }
    Ok(())
}

async fn test_eth_simulate_v1(client: &RpcClient) -> Result<()> {
    // eth_simulateV1 is for simulating block execution
    let block_state_call = json!({
        "blockStateCalls": [{
            "blockOverrides": {
                "number": "0x1"
            },
            "calls": [{
                "from": "0x0000000000000000000000000000000000000001",
                "to": "0x0000000000000000000000000000000000000002",
                "value": "0x1"
            }]
        }]
    });
    
    let result: serde_json::Value = client
        .call("eth_simulateV1", json!([block_state_call, "latest"]))
        .await?;
    
    if let Some(blocks) = result.as_array() {
        println!("  Simulated {} blocks", blocks.len());
    }
    Ok(())
}

async fn test_eth_calls(client: &RpcClient) -> Result<()> {
    // eth_calls executes multiple calls in sequence on the same state
    let contract = client.chain_type.sample_contract();
    
    let calls = json!([
        {
            "to": contract,
            "data": "0x18160ddd" // totalSupply()
        },
        {
            "to": contract,
            "data": "0x70a082310000000000000000000000000000000000000000000000000000000000000000" // balanceOf(0x0)
        }
    ]);
    
    let results: serde_json::Value = client
        .call("eth_calls", json!([calls, "latest"]))
        .await?;
    
    if let Some(arr) = results.as_array() {
        println!("  Executed {} calls in sequence", arr.len());
    }
    Ok(())
}

async fn test_eth_get_transaction_by_hash(client: &RpcClient) -> Result<()> {
    // Get a recent transaction hash
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", true]))
        .await?;
    
    let txs = block["transactions"].as_array().context("No transactions array")?;
    
    if let Some(tx) = txs.first() {
        if let Some(hash) = tx["hash"].as_str() {
            let tx_detail: serde_json::Value = client
                .call("eth_getTransactionByHash", json!([hash]))
                .await?;
            
            if let Some(from) = tx_detail.get("from") {
                println!("  Transaction from: {}", from);
            }
            return Ok(());
        }
    }
    
    println!("  No transactions found in latest block");
    Ok(())
}

async fn test_eth_get_block_receipts(client: &RpcClient) -> Result<()> {
    let receipts: serde_json::Value = client
        .call("eth_getBlockReceipts", json!(["latest"]))
        .await?;
    
    if let Some(arr) = receipts.as_array() {
        println!("  Found {} receipts in latest block", arr.len());
    }
    Ok(())
}

async fn test_debug_trace_transaction(client: &RpcClient) -> Result<()> {
    // Get a recent transaction to trace
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", true]))
        .await?;
    
    let txs = block["transactions"].as_array().context("No transactions array")?;
    
    if let Some(tx) = txs.first() {
        if let Some(hash) = tx["hash"].as_str() {
            let trace: serde_json::Value = client
                .call("debug_traceTransaction", json!([hash, {"tracer": "callTracer"}]))
                .await?;
            
            println!("  Trace type: {}", trace.get("type").and_then(|v| v.as_str()).unwrap_or("unknown"));
            return Ok(());
        }
    }
    
    println!("  No transactions found to trace");
    Ok(())
}

async fn test_debug_trace_block_by_number(client: &RpcClient) -> Result<()> {
    let trace: serde_json::Value = client
        .call("debug_traceBlockByNumber", json!(["latest", {"tracer": "callTracer"}]))
        .await?;
    
    let traces = trace.as_array().context("Trace should be an array")?;
    println!("  Traced {} transactions in latest block", traces.len());
    Ok(())
}

async fn test_txpool_status(client: &RpcClient) -> Result<()> {
    let status: serde_json::Value = client
        .call("txpool_status", json!([]))
        .await?;
    
    println!("  Pending: {}, Queued: {}", 
        status.get("pending").and_then(|v| v.as_str()).unwrap_or("0"),
        status.get("queued").and_then(|v| v.as_str()).unwrap_or("0")
    );
    Ok(())
}

async fn test_txpool_content(client: &RpcClient) -> Result<()> {
    let content: serde_json::Value = client
        .call("txpool_content", json!([]))
        .await?;
    
    let pending = content.get("pending").and_then(|v| v.as_object()).map(|o| o.len()).unwrap_or(0);
    let queued = content.get("queued").and_then(|v| v.as_object()).map(|o| o.len()).unwrap_or(0);
    
    println!("  Pending accounts: {}, Queued accounts: {}", pending, queued);
    Ok(())
}

async fn test_admin_node_info(client: &RpcClient) -> Result<()> {
    let info: serde_json::Value = client
        .call("admin_nodeInfo", json!([]))
        .await?;
    
    if let Some(name) = info.get("name").and_then(|v| v.as_str()) {
        println!("  Node name: {}", name);
    }
    if let Some(enode) = info.get("enode").and_then(|v| v.as_str()) {
        println!("  Enode: {}...", &enode[..50.min(enode.len())]);
    }
    Ok(())
}

async fn test_admin_peers(client: &RpcClient) -> Result<()> {
    let peers: serde_json::Value = client
        .call("admin_peers", json!([]))
        .await?;
    
    let peer_list = peers.as_array().context("Peers should be an array")?;
    println!("  Connected peers: {}", peer_list.len());
    Ok(())
}

// Reth-specific tests

async fn test_eth_call_many(client: &RpcClient) -> Result<()> {
    // Get current block for blockOverride
    let block_num: String = client.call("eth_blockNumber", json!([])).await?;
    
    // Base/Reth eth_callMany format
    let params = json!([
        [
            {
                "transactions": [
                    {
                        "to": "0x4200000000000000000000000000000000000006",
                        "data": "0x18160ddd"  // totalSupply()
                    }
                ],
                "blockOverride": {
                    "blockNumber": block_num
                }
            }
        ],
        {},
        {}
    ]);
    
    let results: serde_json::Value = client
        .call("eth_callMany", json!(params))
        .await?;
    
    println!("  Executed eth_callMany successfully");
    Ok(())
}

async fn test_trace_transaction(client: &RpcClient) -> Result<()> {
    // Get a recent transaction
    let block: serde_json::Value = client
        .call("eth_getBlockByNumber", json!(["latest", true]))
        .await?;
    
    let txs = block["transactions"].as_array().context("No transactions array")?;
    
    if let Some(tx) = txs.first() {
        if let Some(hash) = tx["hash"].as_str() {
            let trace: serde_json::Value = client
                .call("trace_transaction", json!([hash]))
                .await?;
            
            let traces = trace.as_array().context("Trace should be an array")?;
            println!("  Found {} traces for transaction", traces.len());
            return Ok(());
        }
    }
    
    println!("  No transactions found to trace");
    Ok(())
}

async fn test_trace_block(client: &RpcClient) -> Result<()> {
    let trace: serde_json::Value = client
        .call("trace_block", json!(["latest"]))
        .await?;
    
    let traces = trace.as_array().context("Trace should be an array")?;
    println!("  Found {} traces in latest block", traces.len());
    Ok(())
}

// BSC-specific tests

async fn test_parlia_get_snapshot(client: &RpcClient) -> Result<()> {
    // Get validator snapshot for Parlia consensus
    let snapshot: serde_json::Value = client
        .call("parlia_getSnapshot", json!(["latest"]))
        .await?;
    
    if let Some(validators) = snapshot.get("validators") {
        if let Some(arr) = validators.as_array() {
            println!("  Validator count: {}", arr.len());
        }
    }
    Ok(())
}

async fn test_parlia_get_validators(client: &RpcClient) -> Result<()> {
    // Get current validators
    let block_num: String = client.call("eth_blockNumber", json!([])).await?;
    
    let validators: serde_json::Value = client
        .call("parlia_getValidators", json!([block_num]))
        .await?;
    
    if let Some(arr) = validators.as_array() {
        println!("  Active validators: {}", arr.len());
    }
    Ok(())
}

async fn test_eth_get_finalized_header(client: &RpcClient) -> Result<()> {
    // BSC has fast finality, can query finalized header
    let header: serde_json::Value = client
        .call("eth_getFinalizedHeader", json!([]))
        .await?;
    
    if let Some(number) = header.get("number") {
        println!("  Finalized block: {}", number);
    }
    Ok(())
}

async fn test_eth_get_finalized_block(client: &RpcClient) -> Result<()> {
    // Get finalized block with transactions
    let block: serde_json::Value = client
        .call("eth_getFinalizedBlock", json!([false]))
        .await?;
    
    if let Some(number) = block.get("number") {
        println!("  Finalized block number: {}", number);
    }
    if let Some(txs) = block.get("transactions").and_then(|v| v.as_array()) {
        println!("  Transactions: {}", txs.len());
    }
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
    println!("Connection type: {}", if args.url.starts_with("ws") { "WebSocket" } else { "HTTP" });
    println!("Timeout: {}s", args.timeout);

    // Convert ws:// to http:// for initial detection calls
    let http_url = if args.url.starts_with("ws://") {
        args.url.replace("ws://", "http://")
    } else if args.url.starts_with("wss://") {
        args.url.replace("wss://", "https://")
    } else {
        args.url.clone()
    };

    // Detect or set chain type
    let mut chain_type = if args.chain == "auto" {
        ChainType::Unknown
    } else {
        ChainType::from_string(&args.chain)
    };

    let mut node_type = NodeType::Unknown;

    // Auto-detect chain and node type (use HTTP for detection)
    let temp_client = RpcClient::new(http_url.clone(), args.timeout, false, ChainType::Unknown, NodeType::Unknown);
    
    // Get chain ID
    if chain_type == ChainType::Unknown {
        println!("Auto-detecting chain type...");
        if let Ok(chain_id) = temp_client.call::<String>("eth_chainId", json!([])).await {
            if let Ok(id) = u64::from_str_radix(chain_id.trim_start_matches("0x"), 16) {
                chain_type = ChainType::from_chain_id(id);
            }
        }
    }

    // Get client version to detect node type
    // Get client version to detect node type
    println!("Detecting node type...");
    if let Ok(version) = temp_client.call::<String>("web3_clientVersion", json!([])).await {
        node_type = NodeType::from_client_version(&version);
        println!("Client version: {}", version);
    } else {
        // Fallback: detect from URL if web3_clientVersion fails
        println!("web3_clientVersion not available, detecting from URL...");
        if args.url.contains("reth") || args.url.contains("RETH") {
            node_type = NodeType::Reth;
            println!("Detected Reth from URL");
        } else if args.url.contains("geth") || args.url.contains("GETH") {
            node_type = NodeType::Geth;
            println!("Detected Geth from URL");
        } else if args.url.contains("optimism") || args.url.contains("unichain") || args.url.contains("ink") {
            node_type = NodeType::OpGeth; // Assume op-geth
            println!("Detected op-geth from URL");
        } else if args.url.contains("bsc") || args.url.contains("binance") {
            node_type = NodeType::BSCGeth;
            println!("Detected BSC-Geth from URL");
        }
        else {
            println!("Could not detect node type");
        }
    }

    println!("Chain type: {}", chain_type.name());
    println!("Node type: {}\n", node_type.name());

    if let Some(ref f) = args.filter {
        println!("Filter: Only running tests matching '{}'\n", f);
    }

    let client = RpcClient::new(args.url, args.timeout, args.verbose, chain_type.clone(), node_type.clone());
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
    
    run_test(&mut results, "eth_getCode", &args.filter, false, || {
        test_eth_get_code(&client)
    }).await;
    
    run_test(&mut results, "eth_getTransactionReceipt", &args.filter, false, || {
        test_eth_get_transaction_receipt(&client)
    }).await;
    
    run_test(&mut results, "eth_getTransactionByHash", &args.filter, false, || {
        test_eth_get_transaction_by_hash(&client)
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
                println!("\n=== Ethereum Mainnet ===\n");
            }
            ChainType::BSC => {
                println!("\n=== BSC-Specific Methods ===\n");
                
                // Parlia consensus methods
                run_test(&mut results, "parlia_getSnapshot", &args.filter, true, || {
                    test_parlia_get_snapshot(&client)
                }).await;
                
                run_test(&mut results, "parlia_getValidators", &args.filter, true, || {
                    test_parlia_get_validators(&client)
                }).await;
                
                // Fast finality methods
                run_test(&mut results, "eth_getFinalizedHeader", &args.filter, true, || {
                    test_eth_get_finalized_header(&client)
                }).await;
                
                run_test(&mut results, "eth_getFinalizedBlock", &args.filter, true, || {
                    test_eth_get_finalized_block(&client)
                }).await;
            }
            ChainType::Base => {
                println!("\n=== Base (no chain-specific methods) ===\n");
                run_test(&mut results, "optimism_outputAtBlock", &args.filter, true, || {
                    test_optimism_output_at_block(&client)
                }).await;
            }
            ChainType::Unknown => {
                println!("\n=== Unknown chain - skipping chain-specific tests ===\n");
            }
        }

        // Node-specific tests
        match client.node_type {
            NodeType::Geth => {
                println!("\n=== Geth-Specific Methods ===\n");
                
                // Flashbots/MEV bundle methods (69.3% + 9.9% = 79.2% of traffic!)
                run_test(&mut results, "eth_estimateGasBundle", &args.filter, true, || {
                    test_eth_estimate_gas_bundle(&client)
                }).await;
                
                run_test(&mut results, "eth_callBundle", &args.filter, true, || {
                    test_eth_call_bundle(&client)
                }).await;
                
                // Simulation methods (4.6% of traffic)
                run_test(&mut results, "eth_simulateV1", &args.filter, true, || {
                    test_eth_simulate_v1(&client)
                }).await;
                
                // Batch call method (0.0% but implemented)
                run_test(&mut results, "eth_calls", &args.filter, true, || {
                    test_eth_calls(&client)
                }).await;
                
                // Block receipts (0.0% but available)
                run_test(&mut results, "eth_getBlockReceipts", &args.filter, true, || {
                    test_eth_get_block_receipts(&client)
                }).await;
                
                // Debug namespace
                if client.node_type.supports_debug_namespace() {
                    run_test(&mut results, "debug_traceTransaction", &args.filter, true, || {
                        test_debug_trace_transaction(&client)
                    }).await;
                    
                    run_test(&mut results, "debug_traceBlockByNumber", &args.filter, true, || {
                        test_debug_trace_block_by_number(&client)
                    }).await;
                }
                
                // Txpool namespace
                run_test(&mut results, "txpool_status", &args.filter, true, || {
                    test_txpool_status(&client)
                }).await;
                
                run_test(&mut results, "txpool_content", &args.filter, true, || {
                    test_txpool_content(&client)
                }).await;
                
                // Admin namespace (may not be exposed on public nodes)
                run_test(&mut results, "admin_nodeInfo", &args.filter, true, || {
                    test_admin_node_info(&client)
                }).await;
                
                run_test(&mut results, "admin_peers", &args.filter, true, || {
                    test_admin_peers(&client)
                }).await;
            }
            NodeType::OpGeth => {
                println!("\n=== op-geth Methods (Standard Geth APIs only) ===\n");
                
                // Only standard Geth methods, no custom bundle/simulation APIs
                if client.node_type.supports_debug_namespace() {
                    run_test(&mut results, "debug_traceTransaction", &args.filter, true, || {
                        test_debug_trace_transaction(&client)
                    }).await;
                    
                    run_test(&mut results, "debug_traceBlockByNumber", &args.filter, true, || {
                        test_debug_trace_block_by_number(&client)
                    }).await;
                }
                
                run_test(&mut results, "txpool_status", &args.filter, true, || {
                    test_txpool_status(&client)
                }).await;
                
                run_test(&mut results, "txpool_content", &args.filter, true, || {
                    test_txpool_content(&client)
                }).await;
                
                run_test(&mut results, "admin_nodeInfo", &args.filter, true, || {
                    test_admin_node_info(&client)
                }).await;
                
                run_test(&mut results, "admin_peers", &args.filter, true, || {
                    test_admin_peers(&client)
                }).await;
            }
            NodeType::BSCGeth => {
                println!("\n=== BSC-Geth Methods (Standard Geth APIs only) ===\n");
                
                // Only standard Geth methods, no custom bundle/simulation APIs
                if client.node_type.supports_debug_namespace() {
                    run_test(&mut results, "debug_traceTransaction", &args.filter, true, || {
                        test_debug_trace_transaction(&client)
                    }).await;
                    
                    run_test(&mut results, "debug_traceBlockByNumber", &args.filter, true, || {
                        test_debug_trace_block_by_number(&client)
                    }).await;
                }
                
                run_test(&mut results, "txpool_status", &args.filter, true, || {
                    test_txpool_status(&client)
                }).await;
                
                run_test(&mut results, "txpool_content", &args.filter, true, || {
                    test_txpool_content(&client)
                }).await;
                
                run_test(&mut results, "admin_nodeInfo", &args.filter, true, || {
                    test_admin_node_info(&client)
                }).await;
                
                run_test(&mut results, "admin_peers", &args.filter, true, || {
                    test_admin_peers(&client)
                }).await;
            }
            NodeType::Reth | NodeType::OpReth => {
                println!("\n=== Reth-Specific Methods ===\n");
                
                // eth_callMany (8.6% of Reth traffic)
                run_test(&mut results, "eth_callMany", &args.filter, true, || {
                    test_eth_call_many(&client)
                }).await;
                
                // Trace namespace
                if client.node_type.supports_trace_namespace() {
                    run_test(&mut results, "trace_transaction", &args.filter, true, || {
                        test_trace_transaction(&client)
                    }).await;
                    
                    run_test(&mut results, "trace_block", &args.filter, true, || {
                        test_trace_block(&client)
                    }).await;
                }
            }
            NodeType::Nitro => {
                println!("\n=== Arbitrum Nitro Node ===\n");
                // Nitro-specific methods could be added here
            }
            NodeType::Unknown => {
                println!("\n=== Unknown node type - skipping node-specific tests ===\n");
            }
        }
    }

    // WebSocket subscription tests
    if args.test_subscriptions {
        if client.is_websocket {
            println!("\n=== WebSocket Subscription Tests ===\n");
            println!("Listening duration: {}s per subscription\n", args.subscription_duration);
            
            run_test(&mut results, "eth_subscribe (newHeads)", &args.filter, false, || {
                test_subscribe_new_heads(&client, args.subscription_duration)
            }).await;
            
            run_test(&mut results, "eth_subscribe (logs)", &args.filter, false, || {
                test_subscribe_logs(&client, args.subscription_duration)
            }).await;
            
            run_test(&mut results, "eth_subscribe (newPendingTransactions)", &args.filter, false, || {
                test_subscribe_pending_transactions(&client, args.subscription_duration)
            }).await;
            
            run_test(&mut results, "eth_subscribe (syncing)", &args.filter, false, || {
                test_subscribe_syncing(&client, args.subscription_duration)
            }).await;
        } else {
            println!("\n=== WebSocket Subscription Tests ===\n");
            println!("⚠ Skipped: WebSocket URL required (use ws:// or wss://)\n");
        }
    }

    results.print_summary();

    if !results.failed.is_empty() {
        std::process::exit(1);
    }

    Ok(())
}