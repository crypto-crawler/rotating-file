use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
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
}

impl RotatingFile {
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let timestamp = if interval > 0 {
            now / interval * interval
        } else {
            now
        };

        let date_format = date_format.unwrap_or("%Y-%m-%d-%H-%M-%S".to_string());
        let prefix = prefix.unwrap_or("".to_string());
        let suffix = suffix.unwrap_or(".log".to_string());

        let context = Self::create_context(
            timestamp,
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
        }
    }

    pub fn writeln(&self, s: &str) -> Result<(), Error> {
        let mut guard = self.context.lock().unwrap();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let next_timestamp = guard.timestamp + self.interval;

        if (self.size > 0 && guard.total_written + s.len() >= self.size * 1024)
            || (self.interval > 0 && now >= next_timestamp)
        {
            guard.file.flush()?;

            // compress in a background thread
            if let Some(c) = self.compression {
                let input_file = guard.file_path.clone();
                std::thread::spawn(move || Self::compress(input_file, c));
            }

            // reset context
            *guard = Self::create_context(
                next_timestamp,
                self.root_dir.as_str(),
                self.date_format.as_str(),
                self.prefix.as_str(),
                self.suffix.as_str(),
            );
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

    fn create_context(
        timestamp: u64,
        root_dir: &str,
        date_format: &str,
        prefix: &str,
        suffix: &str,
    ) -> CurrentContext {
        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(timestamp as i64, 0), Utc);
        let dt_str = dt.format(date_format).to_string();

        let mut file_name = format!("{}{}{}", prefix, dt_str, suffix);
        let mut index = 1;
        while Path::new(root_dir).join(file_name.as_str()).exists() {
            file_name = format!("{}{}-{}{}", prefix, dt_str, index, suffix);
            index += 1;
        }

        let file_path = Path::new(root_dir).join(file_name).into_os_string();
        println!("{:?}", file_path.as_os_str());

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
        println!("Compressing {:?}", file.as_os_str());
        let mut out_file_path = file.clone();
        match compress {
            Compression::GZip => out_file_path.push(".gz"),
            Compression::Zip => out_file_path.push(".zip"),
        }

        let out_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(out_file_path.as_os_str())?;

        let mut input_buf = fs::read(file.as_os_str())?;

        match compress {
            Compression::GZip => {
                let mut encoder = GzEncoder::new(out_file, flate2::Compression::new(9));
                encoder.write_all(&mut input_buf)?;
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
    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::path::Path;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEXT: &'static str = "The quick brown fox jumps over the lazy dog";

    #[test]
    fn rotate_by_size() {
        let root_dir = "./target/tmp1";
        let timestamp = current_timestamp_str();
        let rotating_file =
            super::RotatingFile::new(root_dir, Some(1), None, None, None, None, None);

        for _ in 0..(1024 / TEXT.len() + 1) {
            let _ = rotating_file.writeln(TEXT);
        }

        // // wait for the compression thread
        // std::thread::sleep(Duration::from_secs(1));

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log")
            .exists());
        assert!(Path::new(root_dir).join(timestamp + "-1.log").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn rotate_by_time() {
        let root_dir = "./target/tmp2";
        let rotating_file =
            super::RotatingFile::new(root_dir, None, Some(1), None, None, None, None);

        let timestamp1 = current_timestamp_str();
        let _ = rotating_file.writeln(TEXT);

        std::thread::sleep(Duration::from_secs(1));

        let timestamp2 = current_timestamp_str();
        let _ = rotating_file.writeln(TEXT);

        assert!(Path::new(root_dir).join(timestamp1 + ".log").exists());
        assert!(Path::new(root_dir).join(timestamp2 + ".log").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn rotate_by_size_and_gzip() {
        let root_dir = "./target/tmp3";
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

        for _ in 0..(1024 / TEXT.len() + 1) {
            let _ = rotating_file.writeln(TEXT);
        }

        // wait for the compression thread
        std::thread::sleep(Duration::from_secs(1));

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log.gz")
            .exists());
        assert!(Path::new(root_dir).join(timestamp + "-1.log").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn rotate_by_size_and_zip() {
        let root_dir = "./target/tmp4";
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

        for _ in 0..(1024 / TEXT.len() + 1) {
            let _ = rotating_file.writeln(TEXT);
        }

        // wait for the compression thread
        std::thread::sleep(Duration::from_secs(1));

        assert!(Path::new(root_dir)
            .join(timestamp.clone() + ".log.zip")
            .exists());
        assert!(Path::new(root_dir).join(timestamp + "-1.log").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn rotate_by_time_and_gzip() {
        let root_dir = "./target/tmp5";
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
        let _ = rotating_file.writeln(TEXT);

        std::thread::sleep(Duration::from_secs(1));

        let timestamp2 = current_timestamp_str();
        let _ = rotating_file.writeln(TEXT);

        // wait for the compression thread
        std::thread::sleep(Duration::from_secs(1));

        assert!(Path::new(root_dir).join(timestamp1 + ".log.gz").exists());
        assert!(Path::new(root_dir).join(timestamp2 + ".log").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }

    #[test]
    fn rotate_by_time_and_zip() {
        let root_dir = "./target/tmp6";
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
        let _ = rotating_file.writeln(TEXT);

        std::thread::sleep(Duration::from_secs(1));

        let timestamp2 = current_timestamp_str();
        let _ = rotating_file.writeln(TEXT);

        // wait for the compression thread
        std::thread::sleep(Duration::from_secs(1));

        assert!(Path::new(root_dir).join(timestamp1 + ".log.zip").exists());
        assert!(Path::new(root_dir).join(timestamp2 + ".log").exists());

        let _ = std::fs::remove_dir_all(root_dir);
    }

    fn current_timestamp_str() -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let dt = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(now as i64, 0), Utc);
        let dt_str = dt.format("%Y-%m-%d-%H-%M-%S").to_string();
        dt_str
    }
}
