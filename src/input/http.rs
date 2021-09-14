use crate::input::compression::{DecompressingReader, ParBzDecoder};
use crate::input::DataInput;
use bzip2::read::BzDecoder;
use reqwest::blocking::Response;
use reqwest::header::{self, HeaderMap, HeaderValue};
use std::io::{self, Read};
use thiserror::Error;

pub const USER_AGENT: &str = "AKSO geo-db (+https://akso.org)";
const MAX_OPEN_TRIES: usize = 32;
const OPEN_RETRY_INTERVAL_SECS: u64 = 8;

/// Reentrant HTTP data input. If interrupted, will attempt to re-establish connection and seek
/// to the appropriate location.
pub struct HttpDataInput<B> {
    src_url: String,
    state: Option<HttpDataInputState<B>>,
}

pub type HttpBz2DataInput = HttpDataInput<BzDecoder<Response>>;
#[allow(dead_code)]
pub type HttpParBz2DataInput = HttpDataInput<ParBzDecoder<Response>>;

/// HttpDataInput state. Exists during download.
struct HttpDataInputState<B> {
    read: B,

    /// The etag of the data.
    /// Wikidata supplies etags, so we use this to check that we are still downloading the
    /// same file if the connection was interrupted.
    etag: String,

    // content length
    len: Option<u64>,
}

impl<B> HttpDataInput<B>
where
    B: DecompressingReader<Response>,
{
    pub fn new(src_url: String) -> Self {
        HttpDataInput {
            src_url,
            state: None,
        }
    }

    pub fn open(&mut self) -> Result<(), HttpError> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(USER_AGENT)
            .build()?;

        debug!("opening new connection");

        let mut headers = HeaderMap::new();
        if let Some(state) = &self.state {
            debug!(
                "setting HTTP range header because we already read some data (cursor: {})",
                state.read.total_in()
            );
            headers.append(
                header::RANGE,
                HeaderValue::from_str(&format!("bytes={}-", state.read.total_in()))
                    .expect("failed to create range header"),
            );
        }

        let mut response = client.get(&self.src_url).headers(headers).send()?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text()?;
            return Err(HttpError::Status(status, body));
        }

        let etag = response
            .headers()
            .get(header::ETAG)
            .map(|s| s.to_str().unwrap_or(""))
            .unwrap_or("");
        let res_offset = if let Some(state) = &self.state {
            // we sent a partial request, so we need to check the etag & range

            if state.etag != etag {
                return Err(HttpError::EtagMismatch);
            }

            let mut res_offset: u64 = 0;
            if let Some(content_range) = response.headers().get(header::CONTENT_RANGE) {
                let content_range = content_range
                    .to_str()
                    .map_err(|_| HttpError::UnexpectedContentRange)?;
                let mut parts = content_range.split(" ");
                if parts.next() != Some("bytes") {
                    return Err(HttpError::UnexpectedContentRange);
                }
                if let Some(range) = parts.next() {
                    // remove /size part and split by -
                    let mut range = range.split("/").next().unwrap().split("-");
                    res_offset = range
                        .next()
                        .expect("first item should exist")
                        .parse()
                        .map_err(|_| HttpError::UnexpectedContentRange)?;
                    debug!("parsed content-range to get start offset {}", res_offset);
                } else {
                    return Err(HttpError::UnexpectedContentRange);
                }
            }

            if res_offset > state.read.total_in() {
                return Err(HttpError::ContentRangeTooSmall);
            }
            res_offset
        } else {
            0
        };

        if let Some(state) = &mut self.state {
            // seek until offset matches
            let diff = (state.read.total_in() - res_offset) as usize;
            if diff > 0 {
                let mut buf = [0; 1024];
                for _ in 0..(diff / 1024) {
                    response.read_exact(&mut buf)?;
                }
                let remaining = diff - (diff / 1024) * 1024;
                if remaining > 0 {
                    response.read_exact(&mut buf[..remaining])?;
                }

                debug!(
                    "response seeked from offset {} to offset {}",
                    res_offset,
                    state.read.total_in()
                );
            }

            *state.read.inner_mut() = response;
        } else {
            let etag = etag.to_string();
            let len = response.content_length();
            // no state exists; create
            self.state = Some(HttpDataInputState {
                read: B::new(response),
                etag,
                len,
            });
        }

        Ok(())
    }

    fn try_open(&mut self) -> Result<(), HttpError> {
        let mut try_count = 1;
        loop {
            let is_last_try = try_count == MAX_OPEN_TRIES;

            match self.open() {
                Ok(()) => break Ok(()),
                Err(err) => {
                    if is_last_try {
                        break Err(err);
                    } else {
                        std::thread::sleep(std::time::Duration::from_secs(
                            OPEN_RETRY_INTERVAL_SECS,
                        ));
                        try_count += 1;
                        debug!(
                            "retrying connection because it failed (try {}/{}): {}",
                            try_count, MAX_OPEN_TRIES, err
                        );
                    }
                }
            }
        }
    }

    fn read_raw(&mut self, buf: &mut [u8]) -> Result<usize, HttpError> {
        match &mut self.state {
            Some(state) => Ok(state.read.read(buf)?),
            None => return Err(HttpError::NoConnection),
        }
    }
}

impl<B> DataInput for HttpDataInput<B>
where
    B: DecompressingReader<Response>,
{
    type Error = HttpError;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, HttpError> {
        match self.read_raw(buf) {
            Ok(bytes) => Ok(bytes),
            Err(err) => match err.retry_policy() {
                RetryPolicy::Retry => {
                    debug!("retrying read because of interrupt error: {}", err);
                    self.read(buf)
                }
                RetryPolicy::Reopen => {
                    debug!("reopening connection because of error: {}", err);
                    self.try_open()?;
                    self.read(buf)
                }
                RetryPolicy::Fail => Err(err),
            },
        }
    }

    fn bytes_read(&self) -> u64 {
        self.state.as_ref().map(|s| s.read.total_in()).unwrap_or(0)
    }

    fn content_length(&self) -> Option<u64> {
        self.state.as_ref().map_or(None, |s| s.len)
    }
}

enum RetryPolicy {
    Fail,
    Reopen,
    Retry,
}

#[derive(Debug, Error)]
pub enum HttpError {
    #[error("no connection exists")]
    NoConnection,
    #[error("connection was reopened but the etag no longer matches")]
    EtagMismatch,
    #[error("server returned unexpected status code {0} ({1:?})")]
    Status(reqwest::StatusCode, String),
    #[error("server returned unexpected content range header")]
    UnexpectedContentRange,
    #[error("response content range is too small")]
    ContentRangeTooSmall,
    #[error("request error: {0}")]
    Req(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

impl HttpError {
    fn retry_policy(&self) -> RetryPolicy {
        match self {
            HttpError::NoConnection => RetryPolicy::Reopen,
            HttpError::Io(err) => match err.kind() {
                io::ErrorKind::Interrupted => RetryPolicy::Retry,
                io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::TimedOut
                | io::ErrorKind::UnexpectedEof => RetryPolicy::Reopen,
                io::ErrorKind::Other => match err.get_ref() {
                    Some(inner) => match inner.downcast_ref::<reqwest::Error>() {
                        Some(err) => {
                            if err.is_timeout()
                                || err.is_connect()
                                || err.is_decode()
                                || err.is_body()
                                || err.is_request()
                            {
                                RetryPolicy::Reopen
                            } else {
                                RetryPolicy::Fail
                            }
                        }
                        None => RetryPolicy::Fail,
                    },
                    None => RetryPolicy::Fail,
                },
                _ => RetryPolicy::Fail,
            },
            _ => RetryPolicy::Fail,
        }
    }
}
