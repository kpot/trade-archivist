//! Since the vast majority of errors in this crate are IO-related, we don't really
//! need any fancy libraries like [thiserror](https://docs.rs/thiserror/latest/thiserror/index.html)
//! or [anyhow](https://docs.rs/anyhow/latest/anyhow/.
//! Thus we need a few coverters into `std::io::Error` in some cases.

use std::io;

pub(crate) fn to_invalid_data_error<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub(crate) fn wrap_lock_error<E>(error: E) -> io::Error
where
    E: std::fmt::Debug,
{
    io::Error::new(
        io::ErrorKind::Other,
        format!("Unable to acquire the lock: {:#?}", error),
    )
}
