//! A thread-safe rotating file with customizable rotation behavior.
//!
//! ## Example
//!
//! ```
//! use rotating_file::RotatingFile;
//!
//! let root_dir = "./target/tmp";
//! let s = "The quick brown fox jumps over the lazy dog";
//! let _ = std::fs::remove_dir_all(root_dir);
//!
//! // rotated by 1 kilobyte, compressed with gzip
//! let rotating_file = RotatingFile::new(root_dir, Some(1), None, None, None, None, None);
//! for _ in 0..24 {
//!     rotating_file.writeln(s).unwrap();
//! }
//! rotating_file.close();
//!
//! assert_eq!(2, std::fs::read_dir(root_dir).unwrap().count());
//! std::fs::remove_dir_all(root_dir).unwrap();
//! ```
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{ffi::OsString, fs, io::Error, sync::Mutex};

use chrono::{DateTime, NaiveDateTime, Utc};
use flate2::write::GzEncoder;
use log::*;

#[derive(Copy, Clone)]
pub enum Compression {
    GZip,
    Zip,
}

struct CurrentContext {
    file: BufWriter<fs::File>,
    file_path: OsString,
    timestamp: u64,
    total_written: usize,
}

/// A thread-safe rotating file with customizable rotation behavior.
pub struct RotatingFile {
    /// Root directory
    root_dir: String,
    /// Max size(in kilobytes) of the file after which it will rotate, 0 means unlimited
    size: usize,
    /// How often(in seconds) to rotate, 0 means unlimited
    interval: u64,
    /// Compression method, default to None
    compression: Option<Compression>,

    /// Format as used in chrono <https://docs.rs/chrono/latest/chrono/format/strftime/>, default to `%Y-%m-%d-%H-%M-%S`
    date_format: String,
    /// File name prefix, default to empty
    prefix: String,
    /// File name suffix, default to `.log`
    suffix: String,

    // current context
    context: Mutex<CurrentContext>,
    // compression threads
    handles: Mutex<Vec<JoinHandle<Result<(), Error>>>>,
}

unsafe impl Send for RotatingFile {}
unsafe impl Sync for RotatingFile {}

impl RotatingFile {
    /// Creates a new RotatingFile.
    ///
    /// ## Arguments
    ///
    /// - `root_dir` The directory to store files.
    /// - `size` Max size(in kilobytes) of the file after which it will rotate,
    /// `None` and `0` mean unlimited.
    /// - `interval` How often(in seconds) to rotate, 0 means unlimited.
    /// - `compression` Available values are `GZip` and `Zip`, default to `None`
    /// - `date_format` uses the syntax from chrono
    /// <https://docs.rs/chrono/latest/chrono/format/strftime/>, default to `%Y-%m-%d-%H-%M-%S`
    /// - `prefix` File name prefix, default to empty
    /// - `suffix` File name suffix, default to `.log`
    pub fn new(
        root_dir: &str,
        size: Option<usize>,
        interval: Option<u64>,
        compression: Option<Compression>,
        date_format: Option<String>,
        prefix: Option<String>,
        suffix: Option<String>,
    ) -> Self {
        if let Err(e) = std::fs::create_dir_all(root_dir) {
            error!("{}", e);
        }

        let interval = interval.unwrap_or(0);

        let date_format = date_format.unwrap_or_else(|| "%Y-%m-%d-%H-%M-%S".to_string());
        let prefix = prefix.unwrap_or("".to_string());
        let suffix = suffix.unwrap_or(".log".to_string());

        let context = Self::create_context(
            interval,
            root_dir,
            date_format.as_str(),
            prefix.as_str(),
            suffix.as_str(),
        );

        RotatingFile {
            root_dir: root_dir.to_string(),
            size: size.unwrap_or(0),
            interval,
            compression,
            date_format,
            prefix,
            suffix,
            context: Mutex::new(context),
            handles: Mutex::new(Vec::new()),
        }
    }

    pub fn writeln(&self, s: &str) -> Result<(), Error> {
        let mut guard = self.context.lock().unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if (self.size > 0 && guard.total_written + s.len() + 1 >= self.size * 1024)
            || (self.interval > 0 && now >= (guard.timestamp + self.interval))
        {
            guard.file.flush()?;
            let old_file = guard.file_path.clone();

            // reset context
            *guard = Self::create_context(
                self.interval,
                self.root_dir.as_str(),
                self.date_format.as_str(),
                self.prefix.as_str(),
                self.suffix.as_str(),
            );

            // compress in a background thread
            if let Some(c) = self.compression {
                let handle = std::thread::spawn(move || Self::compress(old_file, c));
                self.handles.lock().unwrap().push(handle);
            }
        }

        if let Err(e) = writeln!(&mut guard.file, "{}", s) {
            error!(
                "Failed to write to file {}: {}",
                guard.file_path.to_str().unwrap(),
                e
            );
        } else {
            guard.total_written += s.len() + 1;
        }

        Ok(())
    }

    pub fn close(&self) {
        // wait for compression threads
        let mut handles = self.handles.lock().unwrap();
        for handle in handles.drain(..) {
            if let Err(e) = handle.join().unwrap() {
                error!("{}", e);
            }
        }

        // let mut guard = self.context.lock().unwrap();
        if let Err(e) = self.context.lock().unwrap().file.flush() {
            error!("{}", e);
        }
    }

    fn create_context(
        interval: u64,
        root_dir: &str,
        date_format: &str,
        prefix: &str,
        suffix: &str,
    ) -> CurrentContext {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let timestamp = if interval > 0 {
            now / interval * interval
        } else {
            now
        };

        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp as i64, 0), Utc);
        let dt_str = dt.format(date_format).to_string();

        let mut file_name = format!("{}{}{}", prefix, dt_str, suffix);
        let mut index = 1;
        while Path::new(root_dir).join(file_name.as_str()).exists() {
            file_name = format!("{}{}-{}{}", prefix, dt_str, index, suffix);
            index += 1;
        }

        let file_path = Path::new(root_dir).join(file_name).into_os_string();

        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(file_path.as_os_str())
            .unwrap();

        CurrentContext {
            file: BufWriter::new(file),
            file_path,
            timestamp,
            total_written: 0,
        }
    }

    fn compress(file: OsString, compress: Compression) -> Result<(), Error> {
        let mut out_file_path = file.clone();
        match compress {
            Compression::GZip => out_file_path.push(".gz"),
            Compression::Zip => out_file_path.push(".zip"),
        }

        let out_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(out_file_path.as_os_str())?;

        let input_buf = fs::read(file.as_os_str())?;

        match compress {
            Compression::GZip => {
                let mut encoder = GzEncoder::new(out_file, flate2::Compression::new(9));
                encoder.write_all(&input_buf)?;
                encoder.flush()?;
            }
            Compression::Zip => {
                let file_name = Path::new(file.as_os_str())
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap();
                let mut zip = zip::ZipWriter::new(out_file);
                zip.start_file(file_name, zip::write::FileOptions::default())?;
                zip.write_all(&input_buf)?;
                zip.finish()?;
            }
        }

        fs::remove_file(file.as_os_str())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use lazy_static::lazy_static;
    use std::path::Path;
    use std::time::Duration;
    use std::time::SystemTime;

    const TEXT: &'static str = "The quick brown fox jumps over the lazy dog";

    #[test]
    fn rotate_by_size() {
        let root_dir = "./target/tmp1";
        let _ = std::fs::remove_dir_all(root_dir);
        let timestamp = current_timestamp_str();
        let rotating_file =
            super::RotatingFile::new(root_dir, Some(1), None, None, None, None, None);

        for _ in 0..23 {
            rotating_file.writeln(TEXT).unwrap();
        }

        rotating_file.close();

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log")
            .exists());
        assert!(!Path::new(root_dir)
            .join(timestamp.clone() + "-1.log")
            .exists());

        std::fs::remove_dir_all(root_dir).unwrap();

        let timestamp = current_timestamp_str();
        let rotating_file =
            super::RotatingFile::new(root_dir, Some(1), None, None, None, None, None);

        for _ in 0..24 {
            rotating_file.writeln(TEXT).unwrap();
        }

        rotating_file.close();

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log")
            .exists());
        assert!(Path::new(root_dir)
            .join(timestamp.clone() + "-1.log")
            .exists());
        assert_eq!(
            format!("{}\n", TEXT),
            std::fs::read_to_string(Path::new(root_dir).join(timestamp + "-1.log")).unwrap()
        );

        std::fs::remove_dir_all(root_dir).unwrap();
    }

    #[test]
    fn rotate_by_time() {
        let root_dir = "./target/tmp2";
        let _ = std::fs::remove_dir_all(root_dir);
        let rotating_file =
            super::RotatingFile::new(root_dir, None, Some(1), None, None, None, None);

        let timestamp1 = current_timestamp_str();
        rotating_file.writeln(TEXT).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let timestamp2 = current_timestamp_str();
        rotating_file.writeln(TEXT).unwrap();

        rotating_file.close();

        assert!(Path::new(root_dir).join(timestamp1 + ".log").exists());
        assert!(Path::new(root_dir).join(timestamp2 + ".log").exists());

        std::fs::remove_dir_all(root_dir).unwrap();
    }

    #[test]
    fn rotate_by_size_and_gzip() {
        let root_dir = "./target/tmp3";
        let _ = std::fs::remove_dir_all(root_dir);
        let timestamp = current_timestamp_str();
        let rotating_file = super::RotatingFile::new(
            root_dir,
            Some(1),
            None,
            Some(super::Compression::GZip),
            None,
            None,
            None,
        );

        for _ in 0..24 {
            rotating_file.writeln(TEXT).unwrap();
        }

        rotating_file.close();

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log.gz")
            .exists());
        assert!(Path::new(root_dir).join(timestamp + "-1.log").exists());

        std::fs::remove_dir_all(root_dir).unwrap();
    }

    #[test]
    fn rotate_by_size_and_zip() {
        let root_dir = "./target/tmp4";
        let _ = std::fs::remove_dir_all(root_dir);
        let timestamp = current_timestamp_str();
        let rotating_file = super::RotatingFile::new(
            root_dir,
            Some(1),
            None,
            Some(super::Compression::Zip),
            None,
            None,
            None,
        );

        for _ in 0..24 {
            rotating_file.writeln(TEXT).unwrap();
        }

        rotating_file.close();

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log.zip")
            .exists());
        assert!(Path::new(root_dir).join(timestamp + "-1.log").exists());

        std::fs::remove_dir_all(root_dir).unwrap();
    }

    #[test]
    fn rotate_by_time_and_gzip() {
        let root_dir = "./target/tmp5";
        let _ = std::fs::remove_dir_all(root_dir);
        let rotating_file = super::RotatingFile::new(
            root_dir,
            None,
            Some(1),
            Some(super::Compression::GZip),
            None,
            None,
            None,
        );

        let timestamp1 = current_timestamp_str();
        rotating_file.writeln(TEXT).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let timestamp2 = current_timestamp_str();
        rotating_file.writeln(TEXT).unwrap();

        rotating_file.close();

        assert!(Path::new(root_dir).join(timestamp1 + ".log.gz").exists());
        assert!(Path::new(root_dir).join(timestamp2 + ".log").exists());

        std::fs::remove_dir_all(root_dir).unwrap();
    }

    #[test]
    fn rotate_by_time_and_zip() {
        let root_dir = "./target/tmp6";
        let _ = std::fs::remove_dir_all(root_dir);
        let rotating_file = super::RotatingFile::new(
            root_dir,
            None,
            Some(1),
            Some(super::Compression::Zip),
            None,
            None,
            None,
        );

        let timestamp1 = current_timestamp_str();
        rotating_file.writeln(TEXT).unwrap();

        std::thread::sleep(Duration::from_secs(1));

        let timestamp2 = current_timestamp_str();
        rotating_file.writeln(TEXT).unwrap();

        rotating_file.close();

        assert!(Path::new(root_dir).join(timestamp1 + ".log.zip").exists());
        assert!(Path::new(root_dir).join(timestamp2 + ".log").exists());

        std::fs::remove_dir_all(root_dir).unwrap();
    }

    #[test]
    fn referred_in_two_threads() {
        lazy_static! {
            static ref ROOT_DIR: &'static str = "./target/tmp7";
            static ref ROTATING_FILE: super::RotatingFile = super::RotatingFile::new(
                *ROOT_DIR,
                Some(1),
                None,
                Some(super::Compression::GZip),
                None,
                None,
                None,
            );
        }
        let _ = std::fs::remove_dir_all(*ROOT_DIR);

        let timestamp = current_timestamp_str();
        let handle1 = std::thread::spawn(move || {
            for _ in 0..23 {
                ROTATING_FILE.writeln(TEXT).unwrap();
            }
        });

        let handle2 = std::thread::spawn(move || {
            for _ in 0..23 {
                ROTATING_FILE.writeln(TEXT).unwrap();
            }
        });

        // trigger the third file creation
        ROTATING_FILE.writeln(TEXT).unwrap();

        let _ = handle1.join();
        let _ = handle2.join();

        ROTATING_FILE.close();

        assert!(Path::new(*ROOT_DIR)
            .join(timestamp.clone() + ".log.gz")
            .exists());
        assert!(Path::new(*ROOT_DIR)
            .join(timestamp.clone() + "-1.log.gz")
            .exists());

        let third_file = Path::new(*ROOT_DIR).join(timestamp.clone() + "-2.log");
        assert!(third_file.exists());
        assert_eq!(
            TEXT.len() + 1,
            std::fs::metadata(third_file).unwrap().len() as usize
        );

        std::fs::remove_dir_all(*ROOT_DIR).unwrap();
    }

    fn current_timestamp_str() -> String {
        let dt: DateTime<Utc> = SystemTime::now().into();
        let dt_str = dt.format("%Y-%m-%d-%H-%M-%S").to_string();
        dt_str
    }
}
