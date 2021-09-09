use bzip2::read::BzDecoder;
use bzip2_rs::decoder::ParallelDecoder;
use bzip2_rs::RayonThreadPool;
use std::io::{self, Read};

pub trait DecompressingReader<R>: Read {
    fn new(r: R) -> Self;
    fn inner(&self) -> &R;
    fn inner_mut(&mut self) -> &mut R;
    fn total_in(&self) -> u64;
}

impl<R> DecompressingReader<R> for BzDecoder<R>
where
    R: Read,
{
    fn new(r: R) -> Self {
        BzDecoder::new(r)
    }
    fn inner(&self) -> &R {
        self.get_ref()
    }
    fn inner_mut(&mut self) -> &mut R {
        self.get_mut()
    }
    fn total_in(&self) -> u64 {
        self.total_in()
    }
}

/// Higher throughput Bzip2 decoder but it seems to occasionally decode things incorrectly?
pub struct ParBzDecoder<R> {
    decoder: ParallelDecoder<RayonThreadPool>,
    inner: R,
    total_in: u64,
}
impl<R> DecompressingReader<R> for ParBzDecoder<R>
where
    R: Read,
{
    fn new(r: R) -> Self {
        Self {
            decoder: ParallelDecoder::new(RayonThreadPool, 99999999999),
            inner: r,
            total_in: 0,
        }
    }
    fn inner(&self) -> &R {
        &self.inner
    }
    fn inner_mut(&mut self) -> &mut R {
        &mut self.inner
    }
    fn total_in(&self) -> u64 {
        self.total_in
    }
}
impl<R> Read for ParBzDecoder<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_zero = false;
        let mut read_buf = [0; 16384];

        loop {
            match self.decoder.read(buf)? {
                bzip2_rs::decoder::ReadState::NeedsWrite(space) => {
                    let read = self.inner.read(&mut read_buf[..space.min(16384)])?;
                    if read_zero {
                        // FIXME: what is this exactly?
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected EOF while reading compressed data",
                        ));
                    }
                    self.total_in += read as u64;
                    read_zero = read == 0;
                    self.decoder.write(&read_buf[..read])?;
                }
                bzip2_rs::decoder::ReadState::Read(n) => return Ok(n),
                bzip2_rs::decoder::ReadState::Eof => return Ok(0),
            }
        }
    }
}
