use std::collections::VecDeque;
use std::mem;
use std::string::FromUtf8Error;

use thiserror::Error;

mod compression;
pub mod file;
pub mod http;

pub trait DataInput {
    type Error;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
    fn bytes_read(&self) -> u64;
    fn content_length(&self) -> Option<u64>;
}

pub struct InputLineIter<I> {
    pub input: I,
    line_buf: Vec<u8>,
    pending_lines: VecDeque<String>,
}

const ESTIMATED_LINE_SIZE: usize = 65536;

impl<I> InputLineIter<I>
where
    I: DataInput,
{
    pub fn new(input: I) -> Self {
        InputLineIter {
            input,
            line_buf: Vec::with_capacity(ESTIMATED_LINE_SIZE),
            pending_lines: VecDeque::new(),
        }
    }

    fn push_line(&mut self) -> Result<(), LineIterError<I::Error>> {
        self.pending_lines.push_back(
            String::from_utf8(mem::replace(
                &mut self.line_buf,
                Vec::with_capacity(ESTIMATED_LINE_SIZE),
            ))
            .map_err(LineIterError::Utf8)?,
        );
        Ok(())
    }

    pub fn next(&mut self) -> Result<String, LineIterError<I::Error>> {
        let mut buf = [0; 1024];

        while self.pending_lines.is_empty() {
            let bytes_read = self.input.read(&mut buf)?;

            let mut cursor = 0;
            for i in 0..bytes_read {
                let byte = buf[i];
                if byte == b'\n' {
                    self.line_buf.extend_from_slice(&buf[cursor..i]);
                    self.push_line()?;
                    cursor = i + 1;
                }
            }
            if cursor < bytes_read {
                self.line_buf.extend_from_slice(&buf[cursor..bytes_read]);
            }
            if bytes_read == 0 {
                // EOF
                if !self.line_buf.is_empty() {
                    self.push_line()?;
                } else {
                    // the end of the end
                    return Err(LineIterError::Eof);
                }
            }
        }

        Ok(self.pending_lines.pop_front().unwrap())
    }
}

#[derive(Debug, Error)]
pub enum LineIterError<I> {
    #[error("eof")]
    Eof,
    #[error("{0}")]
    Input(#[from] I),
    #[error("utf8 error: {0}")]
    Utf8(FromUtf8Error),
}
