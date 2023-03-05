use std::io;

use bincode;
use bytes::{Buf, BytesMut};
use serde::{de::Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::errors::to_invalid_data_error;

const HEADER_SIZE: usize = 4;

fn parse_frame<'a, T>(buffer: &'a mut BytesMut, data_size: usize) -> io::Result<(T, usize)>
where
    T: Deserialize<'a>,
{
    let value = bincode::deserialize(&buffer[HEADER_SIZE..HEADER_SIZE + data_size])
        .map_err(to_invalid_data_error)?;
    Ok((value, HEADER_SIZE + data_size))
}

pub struct FrameReader {
    buffer: BytesMut,
    next_advance: usize,
    data_limit: Option<usize>,
}

impl FrameReader {
    pub fn new(data_limit: Option<usize>) -> Self {
        Self {
            data_limit,
            buffer: BytesMut::with_capacity(1024),
            next_advance: 0,
        }
    }

    fn check_data_limit(&self, data_size: usize) -> io::Result<()> {
        match self.data_limit {
            Some(limit) if data_size > limit => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "The data packet is too large {} bytes vs max {} allowed",
                    data_size, limit
                ),
            )),
            _ => Ok(()),
        }
    }

    fn is_full_frame_available(&self) -> io::Result<Option<usize>> {
        if self.buffer.remaining() >= HEADER_SIZE {
            let size_buf: [u8; HEADER_SIZE] = (&self.buffer[..HEADER_SIZE])
                .try_into()
                .expect("The header is checked to be of the right size");
            let data_size = u32::from_le_bytes(size_buf) as usize;
            self.check_data_limit(data_size)?;
            if self.buffer.remaining() - HEADER_SIZE >= data_size {
                Ok(Some(data_size))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn next<'a, T>(&'a mut self, socket: &mut TcpStream) -> io::Result<Option<T>>
    where
        T: Deserialize<'a>,
    {
        self.buffer.advance(self.next_advance);
        self.next_advance = 0;
        loop {
            if let Some(data_size) = self.is_full_frame_available()? {
                let (frame, next_advance) = parse_frame(&mut self.buffer, data_size)?;
                self.next_advance = next_advance;
                return Ok(Some(frame));
            } else if socket.read_buf(&mut self.buffer).await? == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Connection reset by peer",
                    ))
                };
            }
        }
    }
}

pub async fn send_frame<T: Serialize>(socket: &mut TcpStream, value: &T) -> io::Result<()> {
    let buf = bincode::serialize(&value).map_err(to_invalid_data_error)?;
    let block_size = (buf.len() as u32).to_le_bytes();
    socket.write_all(&block_size).await?;
    socket.write_all(&buf).await
}
