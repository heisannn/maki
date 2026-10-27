use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use flume::Sender;
use futures_lite::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use httparse::{EMPTY_HEADER, Request as ParsedRequest};
use isahc::{HttpClient, Request};
use serde_json::{Value, json};
use smol::net::{TcpListener, TcpStream};
use tracing::debug;

use crate::providers::anthropic::shared::EventParser;
use crate::providers::{Timeouts, http_client, next_sse_line};
use crate::{AgentError, ContentBlock, ProviderEvent, StreamResponse};

use super::config_error;

const UPSTREAM: &str = "https://api.anthropic.com/v1/messages?beta=true";
const MAX_HEADERS: usize = 64;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const READ_BUFFER_BYTES: usize = 8192;
const NONCE_BYTES: usize = 32;
const BAD_REQUEST: &str = "invalid Claude Code relay request";
const INCOMPLETE_STREAM: &str =
    "Claude Code upstream stream ended before message_stop and stop_reason";
const CONSUMED: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
const NOT_FOUND: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
const SSE_RESPONSE: &str = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n";
const STRIPPED_HEADERS: &[&str] = &[
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "proxy-authorization",
    "proxy-connection",
    "accept-encoding",
    "anthropic-beta",
];

pub(super) struct Relay {
    listener: TcpListener,
    path: String,
    client: HttpClient,
    timeout: Duration,
    upstream: String,
}

impl Relay {
    pub(super) async fn bind(timeouts: Timeouts) -> Result<Self, AgentError> {
        let mut nonce = [0; NONCE_BYTES];
        getrandom::fill(&mut nonce)
            .map_err(|error| config_error(format!("relay nonce: {error}")))?;
        Ok(Self {
            listener: TcpListener::bind(("127.0.0.1", 0)).await?,
            path: format!("/turn/{}/v1/messages", URL_SAFE_NO_PAD.encode(nonce)),
            client: http_client(timeouts),
            timeout: timeouts.stream,
            upstream: UPSTREAM.to_owned(),
        })
    }

    pub(super) fn base_url(&self) -> Result<String, AgentError> {
        let prefix = self
            .path
            .strip_suffix("/v1/messages")
            .ok_or_else(|| config_error(BAD_REQUEST))?;
        Ok(format!("http://{}{prefix}", self.listener.local_addr()?))
    }

    pub(super) async fn serve(
        self,
        body: Value,
        betas: Vec<&'static str>,
        event_tx: Sender<ProviderEvent>,
        started: Arc<AtomicBool>,
        response_tx: Sender<Result<StreamResponse, AgentError>>,
    ) {
        loop {
            let operation = async {
                let (mut socket, headers, native) = futures_lite::future::or(
                    async {
                        let (mut socket, _) = self.listener.accept().await?;
                        let (headers, native) = read_request(&mut socket, &self.path).await?;
                        Ok::<_, AgentError>((socket, headers, native))
                    },
                    async {
                        smol::Timer::after(self.timeout).await;
                        Err(AgentError::Timeout {
                            secs: self.timeout.as_secs(),
                        })
                    },
                )
                .await?;
                let Some(native) = native else {
                    socket.write_all(NOT_FOUND).await?;
                    return Ok(());
                };
                if started.swap(true, Ordering::AcqRel) {
                    socket.write_all(CONSUMED).await?;
                    return Ok(());
                }
                let result = self
                    .forward(
                        &mut socket,
                        headers,
                        overlay_request(native, &body)?,
                        &betas,
                        &event_tx,
                    )
                    .await;
                let _ = response_tx.send_async(result).await;
                Ok::<_, AgentError>(())
            };
            let result = operation.await;
            if let Err(error) = result {
                let _ = response_tx.send_async(Err(error)).await;
                return;
            }
        }
    }

    async fn forward(
        &self,
        socket: &mut TcpStream,
        headers: Vec<(String, String)>,
        body: Value,
        betas: &[&str],
        event_tx: &Sender<ProviderEvent>,
    ) -> Result<StreamResponse, AgentError> {
        let mut request = Request::post(&self.upstream);
        let mut combined_betas = Vec::new();
        for (name, value) in &headers {
            if name.eq_ignore_ascii_case("anthropic-beta") {
                combined_betas.extend(value.split(',').map(str::trim));
            } else if !STRIPPED_HEADERS.contains(&name.as_str()) {
                request = request.header(name, value);
            }
        }
        combined_betas.extend_from_slice(betas);
        request = request.header("accept-encoding", "identity");
        if !combined_betas.is_empty() {
            request = request.header("anthropic-beta", combined_betas.join(","));
        }
        let mut timing = StreamTiming::new();
        let response = self
            .client
            .send_async(request.body(serde_json::to_vec(&body)?)?)
            .await?;
        let headers_at = timing.sent.elapsed();
        if !response.status().is_success() {
            return Err(AgentError::from_response(response).await);
        }
        let mut response_head = SSE_RESPONSE.to_owned();
        for (name, value) in response.headers() {
            if (name.as_str().starts_with("anthropic-") || name == "request-id")
                && let Ok(value) = value.to_str()
            {
                response_head.push_str(&format!("{name}: {value}\r\n"));
            }
        }
        response_head.push_str("\r\n");
        socket.write_all(response_head.as_bytes()).await?;
        let mut lines = BufReader::new(response.into_body()).lines();
        let mut parser = EventParser::new();
        let mut deadline = Instant::now() + self.timeout;
        let mut complete = false;
        while let Some(line) = next_sse_line(&mut lines, &mut deadline, self.timeout).await? {
            // The canonical response belongs to maki even if the CLI rejects an
            // unfamiliar tool name after receiving it.
            let chunk = format!("{line}\n");
            let framed = format!("{:x}\r\n{chunk}\r\n", chunk.len());
            let _ = socket.write_all(framed.as_bytes()).await;
            if let Some(data) = line.strip_prefix("data:") {
                let event: Value = serde_json::from_str(data.trim_start())?;
                timing.observe(&event);
                let kind = event["type"]
                    .as_str()
                    .ok_or_else(|| config_error(INCOMPLETE_STREAM))?;
                if parser
                    .process(kind, data.trim_start(), event_tx)
                    .await?
                    .is_break()
                {
                    complete = true;
                    break;
                }
            }
        }
        let _ = socket.write_all(b"1\r\n\n\r\n0\r\n\r\n").await;
        let response = parser.finish();
        debug!(
            model = body["model"].as_str().unwrap_or_default(),
            headers_ms = millis(headers_at),
            first_block_ms = timing.first_block.map(millis),
            thinking_ms = millis(timing.thinking),
            thinking_blocks = timing.thinking_blocks,
            first_output_ms = timing.first_output.map(millis),
            total_ms = millis(timing.sent.elapsed()),
            output_tokens = response.usage.output,
            stop_reason = ?response.stop_reason,
            "Claude Code generation timing"
        );
        if !complete || response.stop_reason.is_none() {
            return Err(config_error(INCOMPLETE_STREAM));
        }
        for block in &response.message.content {
            if let ContentBlock::ToolUse { name, .. } = block
                && !body["tools"].as_array().is_some_and(|tools| {
                    tools
                        .iter()
                        .any(|tool| tool["name"].as_str() == Some(name.as_str()))
                })
            {
                return Err(config_error(format!(
                    "Claude Code returned an unknown Maki tool: {name}"
                )));
            }
        }
        Ok(response)
    }
}

/// Where an upstream generation spends its time. Thinking is measured from a
/// thinking block's start to its stop, so it excludes the wait before the
/// first block and the visible output after it.
struct StreamTiming {
    sent: Instant,
    first_block: Option<Duration>,
    thinking: Duration,
    thinking_blocks: u32,
    open_thinking: Option<Instant>,
    first_output: Option<Duration>,
}

impl StreamTiming {
    fn new() -> Self {
        Self {
            sent: Instant::now(),
            first_block: None,
            thinking: Duration::ZERO,
            thinking_blocks: 0,
            open_thinking: None,
            first_output: None,
        }
    }

    fn observe(&mut self, event: &Value) {
        match event["type"].as_str() {
            Some("content_block_start") => {
                let elapsed = self.sent.elapsed();
                self.first_block.get_or_insert(elapsed);
                if matches!(
                    event["content_block"]["type"].as_str(),
                    Some("thinking" | "redacted_thinking")
                ) {
                    self.thinking_blocks += 1;
                    self.open_thinking = Some(Instant::now());
                } else {
                    self.first_output.get_or_insert(elapsed);
                }
            }
            Some("content_block_stop") => {
                if let Some(started) = self.open_thinking.take() {
                    self.thinking += started.elapsed();
                }
            }
            _ => {}
        }
    }
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn overlay_request(mut native: Value, body: &Value) -> Result<Value, AgentError> {
    if !native.is_object() {
        return Err(config_error(BAD_REQUEST));
    }
    debug!(system = %native["system"], "Claude Code native system prompt");
    for field in [
        "thinking",
        "output_config",
        "temperature",
        "top_p",
        "tool_choice",
        "context_management",
    ] {
        if let Some(object) = native.as_object_mut() {
            object.remove(field);
        }
    }
    if let (Some(native), Some(body)) = (native.as_object_mut(), body.as_object()) {
        native.extend(
            body.iter()
                .filter(|(key, _)| key.as_str() != "system")
                .map(|(key, value)| (key.clone(), value.clone())),
        );
    }
    native["stream"] = Value::Bool(true);
    // Keep the CLI's TTL. The API rejects a longer TTL after a shorter one, so
    // Maki's tool and message breakpoints adopt it too.
    let cache_control = native["system"]
        .as_array()
        .and_then(|system| {
            system
                .iter()
                .rev()
                .find_map(|block| block.get("cache_control").cloned())
        })
        .unwrap_or_else(|| json!({"type":"ephemeral"}));
    if let Some(system) = native["system"].as_array_mut() {
        // Reserve the other three Anthropic cache breakpoints for Maki's
        // tools and most recent messages.
        for block in system.iter_mut() {
            if let Some(block) = block.as_object_mut() {
                block.remove("cache_control");
            }
        }
        if let Some(last) = system.last_mut().and_then(Value::as_object_mut) {
            last.insert("cache_control".to_owned(), cache_control.clone());
        }
    }
    let tools = native["tools"].as_array_mut().into_iter().flatten();
    retag_breakpoints(tools, &cache_control);
    let blocks = native["messages"]
        .as_array_mut()
        .into_iter()
        .flatten()
        .filter_map(|message| message["content"].as_array_mut())
        .flatten();
    retag_breakpoints(blocks, &cache_control);
    Ok(native)
}

fn retag_breakpoints<'a>(items: impl Iterator<Item = &'a mut Value>, cache_control: &Value) {
    for existing in items.filter_map(|item| item.get_mut("cache_control")) {
        *existing = cache_control.clone();
    }
}

async fn read_request(
    socket: &mut TcpStream,
    expected_path: &str,
) -> Result<(Vec<(String, String)>, Option<Value>), AgentError> {
    let mut bytes = Vec::new();
    let mut buffer = [0; READ_BUFFER_BYTES];
    let (header_len, body_len, headers) = loop {
        let mut headers = [EMPTY_HEADER; MAX_HEADERS];
        let mut request = ParsedRequest::new(&mut headers);
        if let httparse::Status::Complete(header_len) = request
            .parse(&bytes)
            .map_err(|_| config_error(BAD_REQUEST))?
        {
            if request.method != Some("POST")
                || request.path.and_then(|path| path.split('?').next()) != Some(expected_path)
                || request
                    .headers
                    .iter()
                    .any(|header| header.name.eq_ignore_ascii_case("origin"))
            {
                return Ok((Vec::new(), None));
            }
            if request
                .headers
                .iter()
                .any(|header| header.name.eq_ignore_ascii_case("transfer-encoding"))
            {
                return Err(config_error(BAD_REQUEST));
            }
            let lengths: Vec<usize> = request
                .headers
                .iter()
                .filter(|header| header.name.eq_ignore_ascii_case("content-length"))
                .map(|header| {
                    str::from_utf8(header.value)
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .ok_or_else(|| config_error(BAD_REQUEST))
                })
                .collect::<Result<_, _>>()?;
            let [body_len] = lengths.as_slice() else {
                return Err(config_error(BAD_REQUEST));
            };
            if *body_len > MAX_BODY_BYTES || header_len > MAX_HEADER_BYTES {
                return Err(config_error(BAD_REQUEST));
            }
            let headers = request
                .headers
                .iter()
                .map(|header| {
                    Ok((
                        header.name.to_ascii_lowercase(),
                        str::from_utf8(header.value)
                            .map_err(|_| config_error(BAD_REQUEST))?
                            .to_owned(),
                    ))
                })
                .collect::<Result<Vec<_>, AgentError>>()?;
            break (header_len, *body_len, headers);
        }
        if bytes.len() >= MAX_HEADER_BYTES {
            return Err(config_error(BAD_REQUEST));
        }
        let count = socket.read(&mut buffer).await?;
        if count == 0 {
            return Err(config_error(BAD_REQUEST));
        }
        bytes.extend_from_slice(&buffer[..count]);
    };
    while bytes.len() < header_len + body_len {
        let remaining = (header_len + body_len - bytes.len()).min(buffer.len());
        let count = socket.read(&mut buffer[..remaining]).await?;
        if count == 0 {
            return Err(config_error(BAD_REQUEST));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    let body = serde_json::from_slice(&bytes[header_len..header_len + body_len])?;
    Ok((headers, Some(body)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use futures_lite::future::try_zip;
    use futures_lite::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use isahc::{AsyncReadResponseExt, Request};
    use serde_json::{Value, json};
    use smol::net::{TcpListener, TcpStream};
    use test_case::test_case;

    use super::{
        BAD_REQUEST, INCOMPLETE_STREAM, Relay, StreamTiming, overlay_request, read_request,
    };
    use crate::providers::{Timeouts, http_client};
    use crate::test_support::{Canned, serve};
    use crate::{ContentBlock, StopReason};

    const STREAM: &str = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":11,\"output_tokens\":0}}}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"Reasoning\"}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"signed-prefix\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_fixture\",\"name\":\"read\",\"input\":{}}}\n\n",
        "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\\\"fixture.rs\\\"}\"}}\n\n",
        "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\n",
        "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );
    const TEXT_ONLY: &str = concat!(
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    );
    const TRUNCATED: &str = "data: {\"type\":\"message_start\",\"message\":{}}\n\n";
    const AUTH: &str = "Bearer fixture-oauth";
    const SIGNATURE: &str = "signed-prefix";
    const BETA: &str = "fixture-beta";
    const RATE_HEADER: &str = "anthropic-ratelimit-unified-overage-status";
    const RATE_STATUS: &str = "allowed";
    const RESPONSE_HEADERS: &[(&str, &str)] = &[
        ("content-type", "text/event-stream"),
        (RATE_HEADER, RATE_STATUS),
    ];
    const IO_TIMEOUT: Duration = Duration::from_secs(5);

    fn native_body() -> Value {
        json!({
            "model":"claude-sonnet-5", "system":[
                {"type":"text","text":"Native prefix","cache_control":{"type":"ephemeral"}},
                {"type":"text","text":"Maki system","cache_control":{"type":"ephemeral"}}
            ],
            "messages":[{"role":"user","content":"CLI reminder"}],
            "metadata":{"user_id":"fixture"}, "thinking":{"type":"adaptive"},
            "temperature":1, "tools":[], "max_tokens":1,
        })
    }

    fn maki_body() -> Value {
        json!({
            "system":[], "messages":[{"role":"user","content":"Maki history"}],
            "tools":[{"name":"read","input_schema":{"type":"object"}}],
            "max_tokens":1024,
        })
    }

    #[test_case(0 ; "initial_turn")]
    #[test_case(2 ; "tool_round_trip")]
    fn canonical_history_is_append_only(extra_messages: usize) {
        let first = overlay_request(native_body(), &maki_body()).unwrap();
        let mut body = maki_body();
        for _ in 0..extra_messages {
            body["messages"]
                .as_array_mut()
                .unwrap()
                .push(json!({"role":"user","content":"Appended"}));
        }
        let next = overlay_request(native_body(), &body).unwrap();
        assert_eq!(first["system"], next["system"]);
        assert_eq!(first["tools"], next["tools"]);
        assert_eq!(first["messages"][0], next["messages"][0]);
        assert_eq!(
            next["messages"].as_array().unwrap().len(),
            1 + extra_messages
        );
        assert!(next.get("thinking").is_none());
        assert!(next.get("temperature").is_none());
        assert_eq!(next["max_tokens"], body["max_tokens"]);
        assert_eq!(next["metadata"], native_body()["metadata"]);
        let system = next["system"].as_array().unwrap();
        assert_eq!(
            system
                .iter()
                .filter(|block| block.get("cache_control").is_some())
                .count(),
            1
        );
        assert!(system.last().unwrap().get("cache_control").is_some());
    }

    #[test]
    fn breakpoints_adopt_the_cli_ttl() {
        let one_hour = json!({"type":"ephemeral","ttl":"1h"});
        let mut native = native_body();
        native["system"][1]["cache_control"] = one_hour.clone();
        let body = json!({
            "tools":[
                {"name":"read","input_schema":{"type":"object"}},
                {"name":"edit","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral"}}
            ],
            "messages":[{"role":"user","content":[
                {"type":"text","text":"Maki history","cache_control":{"type":"ephemeral"}}
            ]}],
        });
        let request = overlay_request(native, &body).unwrap();
        assert_eq!(request["system"][1]["cache_control"], one_hour);
        assert!(request["tools"][0].get("cache_control").is_none());
        assert_eq!(request["tools"][1]["cache_control"], one_hour);
        assert_eq!(
            request["messages"][0]["content"][0]["cache_control"],
            one_hour
        );
    }

    #[test_case(STREAM, 1 ; "thinking_then_tool_use")]
    #[test_case(TEXT_ONLY, 0 ; "text_without_thinking")]
    fn timing_separates_thinking_from_output(stream: &str, thinking_blocks: u32) {
        let mut timing = StreamTiming::new();
        for data in stream
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
        {
            timing.observe(&serde_json::from_str(data).unwrap());
        }
        assert_eq!(timing.thinking_blocks, thinking_blocks);
        assert!(timing.open_thinking.is_none());
        assert!(timing.first_block.unwrap() <= timing.first_output.unwrap());
    }

    #[test_case(STREAM, None ; "signed_thinking_and_tools")]
    #[test_case(TRUNCATED, Some(INCOMPLETE_STREAM) ; "truncated_stream_fails")]
    fn relay_forwards_one_generation(stream: &'static str, error: Option<&str>) {
        smol::block_on(async {
            let (upstream, requests) = serve(Box::leak(
                vec![Canned {
                    status: 200,
                    headers: RESPONSE_HEADERS,
                    body: stream,
                    path: None,
                }]
                .into_boxed_slice(),
            ));
            let timeouts = Timeouts {
                stream: IO_TIMEOUT,
                ..Timeouts::default()
            };
            let mut relay = Relay::bind(timeouts).await.unwrap();
            relay.upstream = format!("{upstream}/messages");
            let url = format!("{}/v1/messages?beta=true", relay.base_url().unwrap());
            let (event_tx, _events) = flume::unbounded();
            let started = Arc::new(AtomicBool::new(false));
            let (response_tx, response_rx) = flume::bounded(1);
            let task = smol::spawn(relay.serve(
                maki_body(),
                vec![BETA],
                event_tx,
                Arc::clone(&started),
                response_tx,
            ));
            let client = http_client(timeouts);
            let request = || {
                Request::post(&url)
                    .header("authorization", AUTH)
                    .header("content-type", "application/json")
                    .body(native_body().to_string())
                    .unwrap()
            };
            let mut reply = client.send_async(request()).await.unwrap();
            assert_eq!(reply.headers()[RATE_HEADER], RATE_STATUS);
            let _ = reply.text().await;
            let response = response_rx.recv_async().await.unwrap();
            if let Some(error) = error {
                assert_eq!(response.unwrap_err().to_string(), error);
            } else {
                let response = response.unwrap();
                assert_eq!(response.stop_reason, Some(StopReason::ToolUse));
                assert_eq!(response.usage.input, 11);
                assert_eq!(response.usage.output, 7);
                assert!(
                    matches!(&response.message.content[0], ContentBlock::Thinking { signature: Some(signature), .. } if signature == SIGNATURE)
                );
                assert!(
                    matches!(&response.message.content[1], ContentBlock::ToolUse { name, input, .. } if name == "read" && input == &json!({"path":"fixture.rs"}))
                );
            }
            let second = client.send_async(request()).await.unwrap();
            assert_eq!(second.status(), 400);
            assert!(started.load(Ordering::Acquire));
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].authorization(), AUTH);
            assert_eq!(requests[0].headers["anthropic-beta"], BETA);
            assert_eq!(requests[0].json()["messages"], maki_body()["messages"]);
            drop(task);
        });
    }

    #[test_case("Content-Length: 67108865\r\n", "{}" ; "oversized_body")]
    #[test_case("Content-Length: 2\r\nContent-Length: 2\r\n", "{}" ; "duplicate_length")]
    #[test_case("Transfer-Encoding: chunked\r\n", "0\r\n\r\n" ; "chunked_request")]
    fn invalid_http_framing_is_rejected(headers: &str, body: &str) {
        smol::block_on(async {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let send = async {
                let mut socket = TcpStream::connect(address).await?;
                socket
                    .write_all(format!("POST /fixture HTTP/1.1\r\n{headers}\r\n{body}").as_bytes())
                    .await
            };
            let receive = async {
                let (mut socket, _) = listener.accept().await?;
                let error = read_request(&mut socket, "/fixture").await.unwrap_err();
                assert_eq!(error.to_string(), BAD_REQUEST);
                Ok(())
            };
            try_zip(send, receive).await.unwrap();
        });
    }

    #[test_case("/wrong", "" ; "wrong_nonce")]
    #[test_case("/fixture", "Origin: http://example.com\r\n" ; "browser_origin")]
    fn foreign_requests_do_not_consume_turn(path: &str, header: &str) {
        smol::block_on(async {
            let timeouts = Timeouts {
                stream: IO_TIMEOUT,
                ..Timeouts::default()
            };
            let relay = Relay::bind(timeouts).await.unwrap();
            let address = relay.listener.local_addr().unwrap();
            let started = Arc::new(AtomicBool::new(false));
            let (event_tx, _) = flume::unbounded();
            let (response_tx, _) = flume::bounded(1);
            let task = smol::spawn(relay.serve(
                maki_body(),
                vec![],
                event_tx,
                Arc::clone(&started),
                response_tx,
            ));
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket
                .write_all(
                    format!("POST {path} HTTP/1.1\r\n{header}Content-Length: 2\r\n\r\n{{}}")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let mut reply = String::new();
            socket.read_to_string(&mut reply).await.unwrap();
            assert!(reply.starts_with("HTTP/1.1 404"));
            assert!(!started.load(Ordering::Acquire));
            drop(task);
        });
    }

    #[test_case(": ping\n\n" ; "comment_heartbeat")]
    #[test_case("event: ping\ndata: {\"type\":\"ping\"}\n\n" ; "event_heartbeat")]
    fn cancelling_relay_disconnects_upstream(heartbeat: &'static str) {
        smol::block_on(async {
            let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let upstream_address = upstream.local_addr().unwrap();
            let upstream_task = smol::spawn(async move {
                let (mut socket, _) = upstream.accept().await.unwrap();
                read_request(&mut socket, "/messages").await.unwrap();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{heartbeat}").as_bytes()).await.unwrap();
                let mut byte = [0];
                socket.read(&mut byte).await.unwrap()
            });
            let timeouts = Timeouts {
                stream: IO_TIMEOUT,
                ..Timeouts::default()
            };
            let mut relay = Relay::bind(timeouts).await.unwrap();
            relay.upstream = format!("http://{upstream_address}/messages");
            let address = relay.listener.local_addr().unwrap();
            let path = relay.path.clone();
            let (event_tx, _events) = flume::unbounded();
            let (response_tx, _responses) = flume::bounded(1);
            let task = smol::spawn(relay.serve(
                maki_body(),
                vec![],
                event_tx,
                Arc::new(AtomicBool::new(false)),
                response_tx,
            ));
            let mut socket = TcpStream::connect(address).await.unwrap();
            let body = native_body().to_string();
            socket
                .write_all(
                    format!(
                        "POST {path} HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut reader = BufReader::new(socket);
            let mut line = String::new();
            loop {
                assert!(
                    reader.read_line(&mut line).await.unwrap() > 0,
                    "relay closed before sending response headers"
                );
                if line == "\r\n" {
                    break;
                }
                line.clear();
            }
            task.cancel().await;
            let disconnected = futures_lite::future::or(upstream_task, async {
                smol::Timer::after(IO_TIMEOUT).await;
                panic!("cancelled relay kept the upstream connection open");
            })
            .await;
            assert_eq!(disconnected, 0);
        });
    }
}
