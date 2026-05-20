//!  benchmarks — pure CPU / allocations, no network.
//!
//! These benchmarks target every hot path identified in the transport layer:
//!
//!   1. Request body construction  (`provider.rs` -> `send()`)
//!      a. Current:  `to_value` -> `Value` -> `to_string`   (2 allocs)
//!      b. Candidate: direct `to_string` on params         (1 alloc)
//!
//!   2. Response parsing           (`provider.rs` -> `send()`)
//!      a. Current:  `from_str::<Value>` -> `get("result").cloned()` -> `from_value`
//!      b. Candidate: `from_str` with `RawValue` borrow    (zero clone)
//!
//!   3. Error path                 (`provider.rs` -> `parse_rpc_error()`)
//!      a. Current:  `from_value(error.clone())`
//!      b. Candidate: `from_str` on the raw error slice
//!
//!   4. PubSub envelope            (`pubsub_provider.rs` -> `send_request()`)
//!      a. Current:  `json!()` macro -> `Value` -> `to_string`
//!      b. Candidate: `write!` into a pre-allocated `String`
//!
//!   5. Notification dispatch      (`pubsub_provider.rs` -> reader task)
//!      a. Current:  parse to `Value`, clone `result` subtree, channel -> re-parse to `T`
//!      b. Candidate: store raw `Box<str>`, parse once to `T` on poll
//!
//! Run with:
//!   cargo bench --bench transport
//!
//! (Native only — these measure allocator and serde work independent of WASM runtime.)

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use serde::de::DeserializeOwned;
use serde_json::{Value, json, value::RawValue};
use std::hint::black_box;

// Fixture data — representative payloads from a real surfpool instance

/// A typical `getSlot` response (~60 bytes).
const RESPONSE_GET_SLOT: &str =
    r#"{"jsonrpc":"2.0","result":203847,"id":1}"#;

/// A typical `getLatestBlockhash` response (~160 bytes).
const RESPONSE_GET_BLOCKHASH: &str = r#"{
    "jsonrpc":"2.0",
    "result":{
        "context":{"apiVersion":"2.2.0","slot":203847},
        "value":{
            "blockhash":"EkSnNWid2cvwEVnVx9aBqawnmiCNiDgp3gUdkDPTKN1N",
            "lastValidBlockHeight":333000
        }
    },
    "id":1
}"#;

/// A large `getAccountInfo` response (~900 bytes, base64 data).
const RESPONSE_GET_ACCOUNT_INFO: &str = r#"{
    "jsonrpc":"2.0",
    "result":{
        "context":{"apiVersion":"2.2.0","slot":203847},
        "value":{
            "data":["AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==","base64"],
            "executable":false,
            "lamports":1000000000,
            "owner":"NativeLoader1111111111111111111111111111111",
            "rentEpoch":0,
            "space":512
        }
    },
    "id":1
}"#;

/// A JSON-RPC error response.
const RESPONSE_ERROR: &str = r#"{
    "jsonrpc":"2.0",
    "error":{
        "code":-32002,
        "message":"Transaction simulation failed: Error processing Instruction 0: custom program error: 0x1",
        "data":{"accounts":null,"err":{"InstructionError":[0,{"Custom":1}]},"innerInstructions":null,"logs":[],"returnData":null,"unitsConsumed":0}
    },
    "id":1
}"#;

/// A PubSub slot notification frame.
const WS_SLOT_NOTIFICATION: &str = r#"{
    "jsonrpc":"2.0",
    "method":"slotNotification",
    "params":{
        "result":{"parent":203846,"root":203820,"slot":203847},
        "subscription":1
    }
}"#;

/// A PubSub account notification frame (~1 KB).
const WS_ACCOUNT_NOTIFICATION: &str = r#"{
    "jsonrpc":"2.0",
    "method":"accountNotification",
    "params":{
        "result":{
            "context":{"slot":203847},
            "value":{
                "data":["AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==","base64"],
                "executable":false,
                "lamports":1000000000,
                "owner":"11111111111111111111111111111111",
                "rentEpoch":0,
                "space":512
            }
        },
        "subscription":2
    }
}"#;

// 1. Request body construction

/// Current approach: `serde_json::to_value(params)` then `build_request_json(...).to_string()`.
/// This allocates a `Value` first, then serializes it again to a `String`.
fn build_request_body_current(method: &str, params: impl serde::Serialize) -> String {
    // Mirrors exactly what HttpProvider::send() does today.
    let params_value = serde_json::to_value(params).unwrap();
    // build_request_json is private, so we replicate its output shape here.
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1u64,
        "method": method,
        "params": params_value,
    });
    body.to_string()
}

/// Candidate approach: serialize params directly into the envelope string.
/// One allocation for the final `String`, no intermediate `Value`.
fn build_request_body_candidate(method: &str, params: impl serde::Serialize) -> String {
    let params_str = serde_json::to_string(&params).unwrap();
    format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{params_str}}}"#
    )
}

fn bench_request_body(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_body");

    // Simple params: getSlot with no config
    let simple_params: (Option<()>,) = (None,);
    let simple_bytes = serde_json::to_string(&simple_params).unwrap().len();
    group.throughput(Throughput::Bytes(simple_bytes as u64));

    group.bench_function("simple_params/current", |b| {
        b.iter(|| build_request_body_current("getSlot", black_box(&simple_params)))
    });
    group.bench_function("simple_params/candidate", |b| {
        b.iter(|| build_request_body_candidate("getSlot", black_box(&simple_params)))
    });

    // Realistic params: getAccountInfo with commitment config
    let complex_params = (
        "11111111111111111111111111111111",
        json!({"commitment": "confirmed", "encoding": "base64"}),
    );
    let complex_bytes = serde_json::to_string(&complex_params).unwrap().len();
    group.throughput(Throughput::Bytes(complex_bytes as u64));

    group.bench_function("complex_params/current", |b| {
        b.iter(|| build_request_body_current("getAccountInfo", black_box(&complex_params)))
    });
    group.bench_function("complex_params/candidate", |b| {
        b.iter(|| build_request_body_candidate("getAccountInfo", black_box(&complex_params)))
    });

    group.finish();
}

// 2. Response parsing

/// Current: parse full JSON to `Value`, get "result", clone it, `from_value`.
fn parse_response_current<T: DeserializeOwned>(text: &str) -> T {
    let v: Value = serde_json::from_str(text).unwrap();
    serde_json::from_value(v.get("result").unwrap().clone()).unwrap()
}

/// Candidate: borrow `result` as `RawValue` — zero clone, one parse to `T`.
fn parse_response_candidate<T: DeserializeOwned>(text: &str) -> T {
    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        result: &'a RawValue,
    }
    let env: Envelope<'_> = serde_json::from_str(text).unwrap();
    serde_json::from_str(env.result.get()).unwrap()
}

fn bench_response_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("response_parsing");

    // Small response: u64 slot number
    group.throughput(Throughput::Bytes(RESPONSE_GET_SLOT.len() as u64));
    group.bench_function("get_slot/current", |b| {
        b.iter(|| parse_response_current::<u64>(black_box(RESPONSE_GET_SLOT)))
    });
    group.bench_function("get_slot/candidate", |b| {
        b.iter(|| parse_response_candidate::<u64>(black_box(RESPONSE_GET_SLOT)))
    });

    // Medium response: blockhash struct
    group.throughput(Throughput::Bytes(RESPONSE_GET_BLOCKHASH.len() as u64));
    group.bench_function("get_blockhash/current", |b| {
        b.iter(|| parse_response_current::<Value>(black_box(RESPONSE_GET_BLOCKHASH)))
    });
    group.bench_function("get_blockhash/candidate", |b| {
        b.iter(|| parse_response_candidate::<Value>(black_box(RESPONSE_GET_BLOCKHASH)))
    });

    // Large response: account info with base64 data
    group.throughput(Throughput::Bytes(RESPONSE_GET_ACCOUNT_INFO.len() as u64));
    group.bench_function("get_account_info/current", |b| {
        b.iter(|| parse_response_current::<Value>(black_box(RESPONSE_GET_ACCOUNT_INFO)))
    });
    group.bench_function("get_account_info/candidate", |b| {
        b.iter(|| parse_response_candidate::<Value>(black_box(RESPONSE_GET_ACCOUNT_INFO)))
    });

    group.finish();
}

// 3. Error path — `parse_rpc_error`

/// Current: clones the entire `&Value` subtree before `from_value`.
fn parse_error_current(text: &str) {
    let v: Value = serde_json::from_str(text).unwrap();
    let error = v.get("error").unwrap();
    // Mirrors parse_rpc_error exactly.
    let _: Value = serde_json::from_value(error.clone()).unwrap();
}

/// Candidate: borrow the raw error slice, deserialize by reference.
fn parse_error_candidate(text: &str) {
    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        error: &'a RawValue,
    }
    #[derive(serde::Deserialize)]
    struct JsonRpcError<'a> {
        code: i64,
        message: &'a str,
        data: Option<&'a RawValue>,
    }
    let env: Envelope<'_> = serde_json::from_str(text).unwrap();
    let err: JsonRpcError<'_> = serde_json::from_str(env.error.get()).unwrap();
    black_box((err.code, err.message, err.data.map(RawValue::get)));
}

fn bench_error_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("error_parsing");
    group.throughput(Throughput::Bytes(RESPONSE_ERROR.len() as u64));

    group.bench_function("current", |b| {
        b.iter(|| parse_error_current(black_box(RESPONSE_ERROR)))
    });
    group.bench_function("candidate", |b| {
        b.iter(|| parse_error_candidate(black_box(RESPONSE_ERROR)))
    });

    group.finish();
}

// 4. PubSub request envelope construction

/// Current: `json!()` macro builds a `Value` map, then `.to_string()`.
fn pubsub_envelope_current(method: &str, params: impl serde::Serialize, id: u64) -> String {
    let params_value = serde_json::to_value(params).unwrap();
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params_value,
    })
    .to_string()
}

/// Candidate: format directly into a pre-allocated `String`.
fn pubsub_envelope_candidate(method: &str, params: impl serde::Serialize, id: u64) -> String {
    let params_str = serde_json::to_string(&params).unwrap();
    // Reserve capacity for the final string upfront.
    let mut out = String::with_capacity(40 + method.len() + params_str.len() + 20);
    use std::fmt::Write;
    write!(
        out,
        r#"{{"jsonrpc":"2.0","id":{id},"method":"{method}","params":{params_str}}}"#
    )
    .unwrap();
    out
}

fn bench_pubsub_envelope(c: &mut Criterion) {
    let mut group = c.benchmark_group("pubsub_envelope");

    // slotSubscribe — empty params array
    let slot_params: [(); 0] = [];
    group.bench_function("slot_subscribe/current", |b| {
        b.iter(|| pubsub_envelope_current("slotSubscribe", black_box(&slot_params), black_box(1)))
    });
    group.bench_function("slot_subscribe/candidate", |b| {
        b.iter(|| {
            pubsub_envelope_candidate("slotSubscribe", black_box(&slot_params), black_box(1))
        })
    });

    // accountSubscribe — pubkey + config
    let account_params = (
        "11111111111111111111111111111111",
        json!({"commitment": "confirmed", "encoding": "base64"}),
    );
    group.bench_function("account_subscribe/current", |b| {
        b.iter(|| {
            pubsub_envelope_current("accountSubscribe", black_box(&account_params), black_box(2))
        })
    });
    group.bench_function("account_subscribe/candidate", |b| {
        b.iter(|| {
            pubsub_envelope_candidate("accountSubscribe", black_box(&account_params), black_box(2))
        })
    });

    group.finish();
}

// 5. PubSub notification dispatch

/// Current: parse the full frame to `Value`, clone `result`, send through channel.
/// On the consumer side, `from_value` is called again.
/// We simulate both halves here.
fn notification_dispatch_current(frame: &str) -> Value {
    // Producer half: parse entire frame, clone result subtree.
    let v: Value = serde_json::from_str(frame).unwrap();
    let result = v
        .get("params")
        .and_then(|p| p.get("result"))
        .unwrap()
        .clone();

    // Consumer half: re-deserialize from the cloned Value.
    serde_json::from_value::<Value>(result).unwrap()
}

/// Candidate: store only the raw result JSON as `Box<str>`, deserialize once on poll.
fn notification_dispatch_candidate(frame: &str) -> Value {
    #[derive(serde::Deserialize)]
    struct Frame<'a> {
        #[serde(borrow)]
        params: Params<'a>,
    }
    #[derive(serde::Deserialize)]
    struct Params<'a> {
        #[serde(borrow)]
        result: &'a RawValue,
    }
    let parsed: Frame<'_> = serde_json::from_str(frame).unwrap();
    let raw: Box<str> = parsed.params.result.get().into();

    serde_json::from_str::<Value>(&raw).unwrap()
}

fn bench_notification_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("notification_dispatch");

    group.throughput(Throughput::Bytes(WS_SLOT_NOTIFICATION.len() as u64));
    group.bench_function("slot/current", |b| {
        b.iter(|| notification_dispatch_current(black_box(WS_SLOT_NOTIFICATION)))
    });
    group.bench_function("slot/candidate", |b| {
        b.iter(|| notification_dispatch_candidate(black_box(WS_SLOT_NOTIFICATION)))
    });

    group.throughput(Throughput::Bytes(WS_ACCOUNT_NOTIFICATION.len() as u64));
    group.bench_function("account/current", |b| {
        b.iter(|| notification_dispatch_current(black_box(WS_ACCOUNT_NOTIFICATION)))
    });
    group.bench_function("account/candidate", |b| {
        b.iter(|| notification_dispatch_candidate(black_box(WS_ACCOUNT_NOTIFICATION)))
    });

    group.finish();
}

// 6. End-to-end send() path — full pipeline, current vs candidate

fn send_pipeline_current<T: DeserializeOwned>(
    method: &str,
    params: impl serde::Serialize,
    response: &str,
) -> T {
    let _body = build_request_body_current(method, params);
    parse_response_current(response)
}

fn send_pipeline_candidate<T: DeserializeOwned>(
    method: &str,
    params: impl serde::Serialize,
    response: &str,
) -> T {
    let _body = build_request_body_candidate(method, params);
    parse_response_candidate(response)
}

fn bench_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_pipeline");

    let slot_params: (Option<()>,) = (None,);
    group.bench_function("get_slot/current", |b| {
        b.iter(|| {
            send_pipeline_current::<u64>(
                "getSlot",
                black_box(&slot_params),
                black_box(RESPONSE_GET_SLOT),
            )
        })
    });
    group.bench_function("get_slot/candidate", |b| {
        b.iter(|| {
            send_pipeline_candidate::<u64>(
                "getSlot",
                black_box(&slot_params),
                black_box(RESPONSE_GET_SLOT),
            )
        })
    });

    let account_params = ("11111111111111111111111111111111", json!({"encoding": "base64"}));
    group.bench_function("get_account_info/current", |b| {
        b.iter(|| {
            send_pipeline_current::<Value>(
                "getAccountInfo",
                black_box(&account_params),
                black_box(RESPONSE_GET_ACCOUNT_INFO),
            )
        })
    });
    group.bench_function("get_account_info/candidate", |b| {
        b.iter(|| {
            send_pipeline_candidate::<Value>(
                "getAccountInfo",
                black_box(&account_params),
                black_box(RESPONSE_GET_ACCOUNT_INFO),
            )
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_request_body,
    bench_response_parsing,
    bench_error_parsing,
    bench_pubsub_envelope,
    bench_notification_dispatch,
    bench_full_pipeline,
);
criterion_main!(benches);
