use crate::error::ReplayError;
use crate::sink::{Sink, SinkRecord};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

pub const DEFAULT_IGGY_USERNAME: &str = "iggy";
pub const DEFAULT_IGGY_PASSWORD: &str = "iggy";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Normalize a user-supplied Iggy HTTP endpoint into a base URL without a trailing slash.
pub fn normalize_iggy_url(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

/// Split an Iggy destination into `(stream, topic)`.
///
/// Accepts `stream/topic` (e.g. `orders/intake`). When no `/` is present, defaults the
/// stream name to `"default"` and uses the entire destination as the topic name.
pub fn parse_stream_topic(destination: &str) -> Result<(String, String), ReplayError> {
    let trimmed = destination.trim();
    if trimmed.is_empty() {
        return Err(ReplayError::Config(
            "Iggy destination must not be empty (expected '<stream>/<topic>' or '<topic>')".into(),
        ));
    }
    if let Some((stream, topic)) = trimmed.split_once('/') {
        let stream = stream.trim();
        let topic = topic.trim();
        if stream.is_empty() || topic.is_empty() {
            return Err(ReplayError::Config(format!(
                "Invalid Iggy destination '{destination}': expected '<stream>/<topic>'"
            )));
        }
        Ok((stream.to_string(), topic.to_string()))
    } else {
        Ok(("default".to_string(), trimmed.to_string()))
    }
}

#[derive(Serialize)]
struct LoginRequest<'a> {
    username: &'a str,
    password: &'a str,
}

#[derive(Deserialize)]
struct LoginResponse {
    access_token: AccessToken,
}

#[derive(Deserialize)]
struct AccessToken {
    token: String,
}

#[derive(Serialize)]
struct CreateStreamRequest<'a> {
    name: &'a str,
}

#[derive(Serialize)]
struct CreateTopicRequest<'a> {
    name: &'a str,
    partitions_count: u32,
}

#[derive(Serialize)]
struct SendMessagesRequest<'a> {
    partitioning: Partitioning<'a>,
    messages: [IggyMessage; 1],
}

#[derive(Serialize)]
struct Partitioning<'a> {
    kind: &'a str,
    value: String,
}

#[derive(Serialize)]
struct IggyMessage {
    id: u128,
    payload: String,
}

/// Native Apache Iggy HTTP sink.
pub struct IggySink {
    client: Client,
    base_url: String,
    stream: String,
    topic: String,
    username: String,
    password: String,
    token: RwLock<String>,
}

impl IggySink {
    pub async fn connect(
        url: &str,
        destination: &str,
        username: String,
        password: String,
    ) -> Result<Self, ReplayError> {
        let base_url = normalize_iggy_url(url);
        let (stream, topic) = parse_stream_topic(destination)?;

        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| ReplayError::Sink(format!("Failed to build Iggy HTTP client: {e}")))?;

        info!(
            "Connecting to Apache Iggy HTTP endpoint at {} (destination: {}/{})",
            base_url, stream, topic
        );

        let token = Self::login(&client, &base_url, &username, &password).await?;

        let sink = Self {
            client,
            base_url,
            stream,
            topic,
            username,
            password,
            token: RwLock::new(token),
        };

        sink.ensure_stream_and_topic().await?;
        Ok(sink)
    }

    async fn login(
        client: &Client,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, ReplayError> {
        let endpoint = format!("{base_url}/users/login");
        let resp = client
            .post(&endpoint)
            .json(&LoginRequest { username, password })
            .send()
            .await
            .map_err(|e| {
                ReplayError::Sink(format!("Failed to reach Apache Iggy at {base_url}: {e}"))
            })?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ReplayError::Sink(format!(
                "Apache Iggy login failed ({status}): {body}"
            )));
        }

        let parsed: LoginResponse = resp
            .json()
            .await
            .map_err(|e| ReplayError::Sink(format!("Invalid Iggy login response: {e}")))?;
        Ok(parsed.access_token.token)
    }

    async fn ensure_stream_and_topic(&self) -> Result<(), ReplayError> {
        let token = self.token.read().await.clone();
        let topic_url = format!(
            "{}/streams/{}/topics/{}",
            self.base_url, self.stream, self.topic
        );
        let check = self
            .client
            .get(&topic_url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                ReplayError::Sink(format!(
                    "Failed to verify Iggy topic {}/{}: {e}",
                    self.stream, self.topic
                ))
            })?;

        if check.status().is_success() {
            debug!(
                "Verified existing Iggy destination {}/{}",
                self.stream, self.topic
            );
            return Ok(());
        }

        // Provision stream idempotently (400 stream_name_already_exists is fine)
        let stream_resp = self
            .client
            .post(format!("{}/streams", self.base_url))
            .bearer_auth(&token)
            .json(&CreateStreamRequest { name: &self.stream })
            .send()
            .await
            .map_err(|e| ReplayError::Sink(format!("Failed to create Iggy stream: {e}")))?;
        if !stream_resp.status().is_success() && stream_resp.status() != StatusCode::BAD_REQUEST {
            let status = stream_resp.status();
            let body = stream_resp.text().await.unwrap_or_default();
            warn!("Unexpected status creating Iggy stream '{}': {status} {body}", self.stream);
        }

        // Provision topic idempotently
        let topic_resp = self
            .client
            .post(format!("{}/streams/{}/topics", self.base_url, self.stream))
            .bearer_auth(&token)
            .json(&CreateTopicRequest {
                name: &self.topic,
                partitions_count: 1,
            })
            .send()
            .await
            .map_err(|e| ReplayError::Sink(format!("Failed to create Iggy topic: {e}")))?;
        if !topic_resp.status().is_success() && topic_resp.status() != StatusCode::BAD_REQUEST {
            let status = topic_resp.status();
            let body = topic_resp.text().await.unwrap_or_default();
            return Err(ReplayError::Sink(format!(
                "Failed to provision Iggy topic {}/{} ({status}): {body}",
                self.stream, self.topic
            )));
        }

        info!(
            "Provisioned Apache Iggy destination {}/{}",
            self.stream, self.topic
        );
        Ok(())
    }

    async fn publish_once(
        &self,
        stream: &str,
        topic: &str,
        key: Option<&str>,
        payload: &[u8],
        token: &str,
    ) -> Result<StatusCode, ReplayError> {
        let partitioning = match key {
            Some(k) if !k.is_empty() => Partitioning {
                kind: "messages_key",
                value: BASE64.encode(k.as_bytes()),
            },
            _ => Partitioning {
                kind: "balanced",
                value: String::new(),
            },
        };

        let body = SendMessagesRequest {
            partitioning,
            messages: [IggyMessage {
                id: 0,
                payload: BASE64.encode(payload),
            }],
        };

        let url = format!("{}/streams/{}/topics/{}/messages", self.base_url, stream, topic);
        let resp = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| ReplayError::Sink(format!("Failed to send message to Iggy: {e}")))?;

        let status = resp.status();
        if status.is_success() || status == StatusCode::UNAUTHORIZED {
            return Ok(status);
        }

        let err_body = resp.text().await.unwrap_or_default();
        Err(ReplayError::Sink(format!(
            "Apache Iggy rejected message to {stream}/{topic} ({status}): {err_body}"
        )))
    }
}

#[async_trait]
impl Sink for IggySink {
    async fn send(
        &self,
        destination: &str,
        key: Option<&str>,
        payload: &[u8],
    ) -> Result<(), ReplayError> {
        let (stream, topic) = if destination.is_empty() {
            (self.stream.clone(), self.topic.clone())
        } else {
            parse_stream_topic(destination)?
        };

        let token = self.token.read().await.clone();
        let status = self
            .publish_once(&stream, &topic, key, payload, &token)
            .await?;

        if status == StatusCode::UNAUTHORIZED {
            // Token expired during a long-running replay loop; refresh once and retry.
            let new_token =
                Self::login(&self.client, &self.base_url, &self.username, &self.password).await?;
            *self.token.write().await = new_token.clone();
            let retry_status = self
                .publish_once(&stream, &topic, key, payload, &new_token)
                .await?;
            if !retry_status.is_success() {
                return Err(ReplayError::Sink(format!(
                    "Apache Iggy message publish failed after token refresh ({retry_status})"
                )));
            }
        }

        Ok(())
    }

    async fn send_record(
        &self,
        destination: &str,
        record: &SinkRecord<'_>,
    ) -> Result<(), ReplayError> {
        self.send(destination, record.key, record.payload).await
    }
}

pub async fn create_iggy_sink(url: &str, destination: &str) -> Result<Box<dyn Sink>, ReplayError> {
    let username = std::env::var("IGGY_USERNAME").unwrap_or_else(|_| DEFAULT_IGGY_USERNAME.into());
    let password = std::env::var("IGGY_PASSWORD").unwrap_or_else(|_| DEFAULT_IGGY_PASSWORD.into());
    let sink = IggySink::connect(url, destination, username, password).await?;
    Ok(Box::new(sink))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn normalize_url_adds_http_and_strips_slash() {
        assert_eq!(normalize_iggy_url("127.0.0.1:3000/"), "http://127.0.0.1:3000");
        assert_eq!(
            normalize_iggy_url("http://iggy-broker:3000/"),
            "http://iggy-broker:3000"
        );
        assert_eq!(
            normalize_iggy_url("https://iggy.local:3000"),
            "https://iggy.local:3000"
        );
    }

    #[test]
    fn parse_stream_topic_handles_slash_and_default() {
        assert_eq!(
            parse_stream_topic("orders/intake").unwrap(),
            ("orders".to_string(), "intake".to_string())
        );
        assert_eq!(
            parse_stream_topic("events").unwrap(),
            ("default".to_string(), "events".to_string())
        );
        assert!(parse_stream_topic("").is_err());
        assert!(parse_stream_topic("orders/").is_err());
        assert!(parse_stream_topic("/intake").is_err());
    }

    #[tokio::test]
    async fn iggy_sink_authenticates_and_publishes_keyed_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let published = Arc::new(AtomicUsize::new(0));
        let published_bg = published.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let first_line = req.lines().next().unwrap_or("");

                let (status, body) = if first_line.starts_with("POST /users/login") {
                    (
                        "200 OK",
                        r#"{"user_id":0,"access_token":{"token":"test-jwt"}}"#,
                    )
                } else if first_line.starts_with("GET /streams/orders/topics/intake") {
                    ("200 OK", r#"{"id":0,"name":"intake"}"#)
                } else if first_line.starts_with("POST /streams/orders/topics/intake/messages") {
                    assert!(req.contains("Authorization: Bearer test-jwt") || req.contains("authorization: Bearer test-jwt"));
                    assert!(req.contains(r#""kind":"messages_key""#));
                    published_bg.fetch_add(1, Ordering::SeqCst);
                    ("201 Created", r#"{"confirmations":[]}"#)
                } else {
                    ("404 Not Found", r#"{"error":"not_found"}"#)
                };

                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            }
        });

        let sink = IggySink::connect(
            &format!("http://{addr}"),
            "orders/intake",
            "iggy".into(),
            "iggy".into(),
        )
        .await
        .unwrap();

        sink.send("orders/intake", Some("order-42"), br#"{"order_id":"order-42"}"#)
            .await
            .unwrap();

        assert_eq!(published.load(Ordering::SeqCst), 1);
    }
}
