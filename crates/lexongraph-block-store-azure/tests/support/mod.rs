// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonGraph contributors
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use lexongraph_block::BlockHash;
use lexongraph_block_store_azure::AzureBlobBlockStore;

pub struct MockAzureServer {
    inner: Arc<MockAzureServerInner>,
}

impl Clone for MockAzureServer {
    fn clone(&self) -> Self {
        self.inner.external_handles.fetch_add(1, Ordering::AcqRel);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for MockAzureServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockAzureServer")
            .field("base_url", &self.inner.base_url)
            .finish()
    }
}

struct MockAzureServerInner {
    container_name: String,
    state: Mutex<MockState>,
    shutdown: AtomicBool,
    external_handles: AtomicUsize,
    thread: Mutex<Option<JoinHandle<()>>>,
    base_url: String,
}

#[derive(Default)]
struct MockState {
    blobs: HashMap<String, Vec<u8>>,
    extra_list_names: Vec<String>,
    request_log: Vec<RecordedRequest>,
    deny_put: bool,
    disconnect_put_attempts: usize,
    disconnect_get_attempts: usize,
    disconnect_list_attempts: usize,
    drop_put_attempts: usize,
    malformed_listing: bool,
    list_error: Option<u16>,
    blob_status_overrides: HashMap<String, u16>,
}

#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub target: String,
}

impl MockAzureServer {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let container_name = "container".to_string();
        let base_url = format!("http://{addr}/{container_name}?sv=test&sig=fake");
        let inner = Arc::new(MockAzureServerInner {
            container_name: container_name.clone(),
            state: Mutex::new(MockState::default()),
            shutdown: AtomicBool::new(false),
            external_handles: AtomicUsize::new(1),
            thread: Mutex::new(None),
            base_url,
        });

        let server = Self {
            inner: Arc::clone(&inner),
        };
        let thread_inner = Arc::clone(&inner);
        let handle = thread::spawn(move || run_server(listener, thread_inner));
        *server.inner.thread.lock().unwrap() = Some(handle);
        server
    }

    pub fn store(&self) -> AzureBlobBlockStore {
        AzureBlobBlockStore::new(&self.inner.base_url).unwrap()
    }

    pub fn sas_url(&self) -> String {
        self.inner.base_url.clone()
    }

    pub fn blob_name(&self, block_id: &BlockHash) -> String {
        let hex = block_id.to_string();
        format!("{}/{}/{}.cbor", &hex[..2], &hex[2..4], hex)
    }

    pub fn insert_blob(&self, blob_name: impl Into<String>, bytes: Vec<u8>) {
        self.inner
            .state
            .lock()
            .unwrap()
            .blobs
            .insert(blob_name.into(), bytes);
    }

    pub fn blob_bytes(&self, blob_name: &str) -> Option<Vec<u8>> {
        self.inner
            .state
            .lock()
            .unwrap()
            .blobs
            .get(blob_name)
            .cloned()
    }

    pub fn set_deny_put(&self, deny: bool) {
        self.inner.state.lock().unwrap().deny_put = deny;
    }

    pub fn set_disconnect_put_attempts(&self, attempts: usize) {
        self.inner.state.lock().unwrap().disconnect_put_attempts = attempts;
    }

    pub fn set_disconnect_get_attempts(&self, attempts: usize) {
        self.inner.state.lock().unwrap().disconnect_get_attempts = attempts;
    }

    pub fn set_disconnect_list_attempts(&self, attempts: usize) {
        self.inner.state.lock().unwrap().disconnect_list_attempts = attempts;
    }

    pub fn set_drop_put_attempts(&self, attempts: usize) {
        self.inner.state.lock().unwrap().drop_put_attempts = attempts;
    }

    pub fn set_blob_status(&self, blob_name: impl Into<String>, status: u16) {
        self.inner
            .state
            .lock()
            .unwrap()
            .blob_status_overrides
            .insert(blob_name.into(), status);
    }

    pub fn set_list_error(&self, status: u16) {
        self.inner.state.lock().unwrap().list_error = Some(status);
    }

    pub fn set_malformed_listing(&self, malformed: bool) {
        self.inner.state.lock().unwrap().malformed_listing = malformed;
    }

    pub fn add_extra_list_name(&self, name: impl Into<String>) {
        self.inner
            .state
            .lock()
            .unwrap()
            .extra_list_names
            .push(name.into());
    }

    pub fn recorded_requests(&self) -> Vec<RecordedRequest> {
        self.inner.state.lock().unwrap().request_log.clone()
    }
}

impl Drop for MockAzureServer {
    fn drop(&mut self) {
        if self.inner.external_handles.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        self.inner.shutdown.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.inner.base_url_host_port());
        if let Some(handle) = self.inner.thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl MockAzureServerInner {
    fn base_url_host_port(&self) -> String {
        self.base_url
            .trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap()
            .to_string()
    }
}

pub fn collect_block_ids(
    iter: lexongraph_block_store::BlockIdIterator<'_>,
) -> Result<HashSet<BlockHash>, lexongraph_block_store::BlockStoreError> {
    iter.collect()
}

fn run_server(listener: TcpListener, inner: Arc<MockAzureServerInner>) {
    while !inner.shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _)) => {
                let connection_inner = Arc::clone(&inner);
                thread::spawn(move || handle_connection(stream, &connection_inner));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(_) => break,
        }
    }
}

fn handle_connection(stream: TcpStream, inner: &Arc<MockAzureServerInner>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
        return;
    }

    let request_line = request_line.trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() || line == "\r\n" || line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }

    let mut body = vec![0_u8; content_length];
    if reader.read_exact(&mut body).is_err() {
        return;
    }

    inner
        .state
        .lock()
        .unwrap()
        .request_log
        .push(RecordedRequest {
            method: method.clone(),
            target: target.clone(),
        });

    let (path, query) = split_target(&target);
    let is_list = path == format!("/{}", inner.container_name)
        && query.contains("restype=container")
        && query.contains("comp=list");
    if is_list {
        respond_to_list(stream, inner);
        return;
    }

    let blob_prefix = format!("/{}/", inner.container_name);
    if !path.starts_with(&blob_prefix) {
        write_response(stream, 404, &[]);
        return;
    }
    let blob_name = path[blob_prefix.len()..].to_string();

    match method.as_str() {
        "GET" => respond_to_get(stream, inner, &blob_name),
        "HEAD" => respond_to_head(stream, inner, &blob_name),
        "PUT" => respond_to_put(stream, inner, &blob_name, body),
        _ => write_response(stream, 405, &[]),
    }
}

fn respond_to_get(stream: TcpStream, inner: &Arc<MockAzureServerInner>, blob_name: &str) {
    let mut state = inner.state.lock().unwrap();
    if state.disconnect_get_attempts > 0 {
        state.disconnect_get_attempts -= 1;
        drop(state);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }
    if let Some(status) = state.blob_status_overrides.get(blob_name) {
        let request_id = mock_request_id("GET", blob_name);
        let headers = [("x-ms-request-id", request_id.as_str())];
        write_response_with_headers(stream, *status, &headers, &[]);
        return;
    }
    match state.blobs.get(blob_name) {
        Some(bytes) => {
            let request_id = mock_request_id("GET", blob_name);
            let etag = mock_etag(blob_name);
            let last_modified = mock_last_modified();
            let content_length = bytes.len().to_string();
            let headers = [
                ("x-ms-request-id", request_id.as_str()),
                ("etag", etag.as_str()),
                ("last-modified", last_modified),
                ("content-length", content_length.as_str()),
            ];
            write_response_with_headers(stream, 200, &headers, bytes)
        }
        None => {
            let request_id = mock_request_id("GET", blob_name);
            let headers = [
                ("x-ms-request-id", request_id.as_str()),
                ("x-ms-error-code", "BlobNotFound"),
            ];
            write_response_with_headers(stream, 404, &headers, &[])
        }
    }
}

fn respond_to_head(stream: TcpStream, inner: &Arc<MockAzureServerInner>, blob_name: &str) {
    let state = inner.state.lock().unwrap();
    if let Some(status) = state.blob_status_overrides.get(blob_name) {
        let request_id = mock_request_id("HEAD", blob_name);
        let headers = [("x-ms-request-id", request_id.as_str())];
        write_response_with_headers(stream, *status, &headers, &[]);
        return;
    }
    match state.blobs.get(blob_name) {
        Some(bytes) => {
            let request_id = mock_request_id("HEAD", blob_name);
            let etag = mock_etag(blob_name);
            let last_modified = mock_last_modified();
            let content_length = bytes.len().to_string();
            let headers = [
                ("x-ms-request-id", request_id.as_str()),
                ("etag", etag.as_str()),
                ("last-modified", last_modified),
                ("content-length", content_length.as_str()),
            ];
            write_response_with_headers(stream, 200, &headers, &[])
        }
        None => {
            let request_id = mock_request_id("HEAD", blob_name);
            let headers = [
                ("x-ms-request-id", request_id.as_str()),
                ("x-ms-error-code", "BlobNotFound"),
            ];
            write_response_with_headers(stream, 404, &headers, &[])
        }
    }
}

fn respond_to_put(
    stream: TcpStream,
    inner: &Arc<MockAzureServerInner>,
    blob_name: &str,
    body: Vec<u8>,
) {
    let mut state = inner.state.lock().unwrap();
    if state.deny_put {
        let request_id = mock_request_id("PUT", blob_name);
        let headers = [("x-ms-request-id", request_id.as_str())];
        write_response_with_headers(stream, 403, &headers, &[]);
        return;
    }
    if state.drop_put_attempts > 0 {
        state.drop_put_attempts -= 1;
        drop(state);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }
    if state.disconnect_put_attempts > 0 {
        state.disconnect_put_attempts -= 1;
        if !state.blobs.contains_key(blob_name) {
            state.blobs.insert(blob_name.to_string(), body);
        }
        drop(state);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }
    if state.blobs.contains_key(blob_name) {
        let request_id = mock_request_id("PUT", blob_name);
        let headers = [("x-ms-request-id", request_id.as_str())];
        write_response_with_headers(stream, 412, &headers, &[]);
        return;
    }
    state.blobs.insert(blob_name.to_string(), body);
    let request_id = mock_request_id("PUT", blob_name);
    let etag = mock_etag(blob_name);
    let last_modified = mock_last_modified();
    let headers = [
        ("x-ms-request-id", request_id.as_str()),
        ("etag", etag.as_str()),
        ("last-modified", last_modified),
    ];
    write_response_with_headers(stream, 201, &headers, &[]);
}

fn respond_to_list(stream: TcpStream, inner: &Arc<MockAzureServerInner>) {
    let mut state = inner.state.lock().unwrap();
    if state.disconnect_list_attempts > 0 {
        state.disconnect_list_attempts -= 1;
        drop(state);
        let _ = stream.shutdown(Shutdown::Both);
        return;
    }
    if let Some(status) = state.list_error {
        let request_id = mock_request_id("LIST", "container");
        let headers = [("x-ms-request-id", request_id.as_str())];
        write_response_with_headers(stream, status, &headers, &[]);
        return;
    }
    if state.malformed_listing {
        let request_id = mock_request_id("LIST", "container");
        let headers = [("x-ms-request-id", request_id.as_str())];
        write_response_with_headers(stream, 200, &headers, b"<not-xml");
        return;
    }

    let mut names = state.blobs.keys().cloned().collect::<Vec<_>>();
    names.extend(state.extra_list_names.iter().cloned());
    names.sort();
    let body = render_listing(&names);
    let request_id = mock_request_id("LIST", "container");
    let headers = [("x-ms-request-id", request_id.as_str())];
    write_response_with_headers(stream, 200, &headers, body.as_bytes());
}

fn render_listing(names: &[String]) -> String {
    let blobs = names
        .iter()
        .map(|name| format!("<Blob><Name>{}</Name></Blob>", xml_escape(name)))
        .collect::<String>();
    format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults><Blobs>{blobs}</Blobs><NextMarker></NextMarker></EnumerationResults>"
    )
}

fn xml_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn split_target(target: &str) -> (&str, &str) {
    match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    }
}

fn mock_request_id(method: &str, target: &str) -> String {
    format!("mock-{method}-{}", target.replace('/', "_"))
}

fn mock_etag(blob_name: &str) -> String {
    format!("\"etag-{}\"", blob_name.replace('/', "-"))
}

fn mock_last_modified() -> &'static str {
    "Thu, 02 Jul 2026 00:00:00 GMT"
}

fn write_response(stream: TcpStream, status: u16, body: &[u8]) {
    write_response_with_headers(stream, status, &[], body);
}

fn write_response_with_headers(
    mut stream: TcpStream,
    status: u16,
    headers: &[(&str, &str)],
    body: &[u8],
) {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        412 => "Precondition Failed",
        500 => "Internal Server Error",
        _ => "Mock",
    };
    let has_content_length = headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
    let mut header = format!("HTTP/1.1 {status} {reason}\r\n");
    if !has_content_length {
        header.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    header.push_str("Connection: close\r\n");
    for (name, value) in headers {
        header.push_str(name);
        header.push_str(": ");
        header.push_str(value);
        header.push_str("\r\n");
    }
    header.push_str("\r\n");
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}
