#!/bin/bash

target/release/evm-rpc-test --url http://reth-2-tokyo.rsync-builder.xyz:8545
target/release/evm-rpc-test --url ws://reth-2-tokyo.rsync-builder.xyz:8546 --test-subscriptions

target/release/evm-rpc-test --url http://arbitrum-7-ohio.rsync-builder.xyz:8547
target/release/evm-rpc-test --url ws://arbitrum-7-ohio.rsync-builder.xyz:8548 --test-subscriptions

target/release/evm-rpc-test --url http://base-7-nvirginia.rsync-builder.xyz:8545
target/release/evm-rpc-test --url ws://base-7-nvirginia.rsync-builder.xyz:8546 --test-subscriptions
