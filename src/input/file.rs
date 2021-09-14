use crate::input::compression::{DecompressingReader, ParBzDecoder};
use crate::input::DataInput;
use bzip2::read::BzDecoder;
use std::{fs, io};

pub struct FileInput<B> {
    read: B,
    size: u64,
}

#[allow(dead_code)]
pub type Bz2FileInput = FileInput<BzDecoder<fs::File>>;
#[allow(dead_code)]
pub type ParBz2FileInput = FileInput<ParBzDecoder<fs::File>>;

impl<B> FileInput<B>
where
    B: DecompressingReader<fs::File>,
{
    #[allow(dead_code)]
    pub fn new(file: fs::File) -> Self {
        let size = file.metadata().unwrap().len();

        FileInput {
            read: B::new(file),
            size,
        }
    }
}

impl<B> DataInput for FileInput<B>
where
    B: DecompressingReader<fs::File>,
{
    type Error = io::Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.read.read(buf)
    }

    fn bytes_read(&self) -> u64 {
        self.read.total_in()
    }

    fn content_length(&self) -> Option<u64> {
        Some(self.size)
    }
}
