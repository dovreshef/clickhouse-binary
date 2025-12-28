//! Shared integration test helpers.

use std::io::Read;

use clickhouse_binary::{Row, RowBinaryFormat, RowBinaryReader, RowBinaryWriter, Schema};
use rand::{Rng, distr::Alphanumeric, rng};
use serde_json::Value as JsonValue;
use ureq::{Agent, Body, Error as UreqError, config::Config, http::Response as HttpResponse};

type Response = HttpResponse<Body>;

/// Helper wrapping a `ClickHouse` HTTP endpoint for integration tests.
pub struct ClickhouseServer {
    client: Agent,
    dsn: String,
}

impl ClickhouseServer {
    /// Connects using the `CLICKHOUSE_DSN` environment variable.
    ///
    /// # Panics
    ///
    /// Panics when the environment variable is missing, which typically means
    /// the test harness was not configured via `validate.sh`.
    #[must_use]
    pub fn connect() -> Self {
        let dsn = std::env::var("CLICKHOUSE_DSN")
            .expect("CLICKHOUSE_DSN env var must be defined (see validate.sh)");
        let config = Config::builder().http_status_as_error(false).build();
        let client = Agent::new_with_config(config);
        Self { client, dsn }
    }

    /// Executes any SQL statement.
    ///
    /// # Panics
    ///
    /// Panics when the HTTP request fails or the response status is not
    /// successful.
    pub fn exec(&self, sql: &str) {
        Self::expect_success(self.send_query(sql, None, None), "SQL failed");
    }

    /// Attempts to execute SQL and returns whether it succeeded.
    pub fn try_exec(&self, sql: &str) -> bool {
        match self.send_query(sql, None, None) {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Executes SQL with `ClickHouse` settings passed as URL parameters.
    pub fn exec_with_settings(&self, sql: &str, settings: &str) {
        Self::expect_success(self.send_query(sql, None, Some(settings)), "SQL failed");
    }

    /// Streams a `RowBinary` payload into an INSERT statement.
    pub fn insert_rowbinary(
        &self,
        sql: &str,
        format: RowBinaryFormat,
        schema: &Schema,
        rows: &[Row],
    ) {
        let mut writer = RowBinaryWriter::new(Vec::new(), format, schema.clone());
        writer.write_rows(rows).unwrap();
        let payload = writer.into_inner();
        self.insert_payload(sql, &payload);
    }

    /// Sends a raw payload attached to an SQL statement.
    pub fn insert_payload(&self, sql: &str, payload: &[u8]) {
        Self::expect_success(self.send_query(sql, Some(payload), None), "insert failed");
    }

    /// Fetches rows as JSON for assertions.
    ///
    /// # Panics
    ///
    /// Panics when the HTTP request fails or returns a non-successful response.
    #[must_use]
    pub fn fetch_json(&self, sql: &str) -> Vec<JsonValue> {
        let query = format!("{sql} FORMAT JSONEachRow");
        let response = Self::expect_success(self.send_query(&query, None, None), "select failed");
        response_body(response)
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// Fetches the raw `RowBinary` payload.
    pub fn fetch_rowbinary(&self, sql: &str, format: RowBinaryFormat) -> Vec<u8> {
        let query = format!("{sql} FORMAT {format}");
        let response = Self::expect_success(
            self.send_query(&query, None, None),
            "select rowbinary failed",
        );
        response_bytes(response)
    }

    fn send_query(
        &self,
        sql: &str,
        payload: Option<&[u8]>,
        query_settings: Option<&str>,
    ) -> Result<Response, Box<UreqError>> {
        let mut body = Vec::with_capacity(sql.len() + 1 + payload.map_or(0, <[u8]>::len));
        body.extend_from_slice(sql.as_bytes());
        body.push(b'\n');
        if let Some(block) = payload {
            body.extend_from_slice(block);
        }
        let mut url = self.dsn.clone();
        if let Some(settings) = query_settings {
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str(settings);
        }
        self.send_raw(&body, &url)
    }

    fn send_raw(&self, body: &[u8], url: &str) -> Result<Response, Box<UreqError>> {
        self.client
            .post(url)
            .header("Content-Type", "application/octet-stream")
            .send(body)
            .map_err(Box::new)
    }

    fn expect_success(result: Result<Response, Box<UreqError>>, context: &str) -> Response {
        match result {
            Ok(response) => {
                if response.status().is_success() {
                    response
                } else {
                    let body = response_body(response);
                    panic!("{context}: {body}");
                }
            }
            Err(err) => panic!("{context}: {err}"),
        }
    }
}

fn response_body(mut response: Response) -> String {
    let mut buf = String::new();
    if let Err(err) = response.body_mut().as_reader().read_to_string(&mut buf) {
        panic!("failed to read ClickHouse response: {err}");
    }
    buf
}

fn response_bytes(mut response: Response) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Err(err) = response.body_mut().as_reader().read_to_end(&mut buf) {
        panic!("failed to read ClickHouse response: {err}");
    }
    buf
}

/// Decodes all rows from a `RowBinary` payload.
pub fn decode_rows(payload: &[u8], format: RowBinaryFormat, schema: &Schema) -> Vec<Row> {
    let mut reader = RowBinaryReader::with_schema(payload, format, schema.clone());
    let mut rows = Vec::new();
    while let Some(row) = reader.read_row().unwrap() {
        rows.push(row);
    }
    rows
}

/// Generates a unique table name for isolation. Incorporates the current test
/// name (when available) to avoid collisions between concurrent tests.
pub fn unique_table(prefix: &str) -> String {
    let mut components = Vec::new();
    if let Some(base) = sanitize_identifier(prefix) {
        components.push(base);
    }
    if let Some(test) = current_test_identifier() {
        components.push(test);
    }
    if components.is_empty() {
        components.push("tbl".to_string());
    }
    let base = components.join("_");
    let suffix: String = rng()
        .sample_iter(Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    format!("{base}_{suffix}")
}

fn current_test_identifier() -> Option<String> {
    std::thread::current().name().and_then(sanitize_identifier)
}

fn sanitize_identifier(input: &str) -> Option<String> {
    if input.is_empty() {
        return None;
    }
    let mut sanitized = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    let sanitized = sanitized.trim_matches('_').to_string();
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized)
    }
}
