# rotating-file

[![](https://img.shields.io/github/workflow/status/soulmachine/rotating-file/CI/main)](https://github.com/soulmachine/rotating-file/actions?query=branch%3Amain)
[![](https://img.shields.io/crates/v/rotating-file.svg)](https://crates.io/crates/rotating-file)
[![](https://docs.rs/rotating-file/badge.svg)](https://docs.rs/rotating-file)
==========
A thread-safe rotating file with customizable rotation behavior.

## Example

```rust
use chrono::{DateTime, Utc};
use std::path::Path;
use std::time::SystemTime;
use rotating_file::RotatingFile;

fn main() {
    let root_dir = "./target/tmp";
    let s = "The quick brown fox jumps over the lazy dog";
    let rotating_file = RotatingFile::new(root_dir, Some(1), None, None, None, None, None);

    let dt: DateTime::<Utc> = SystemTime::now().into();
    let timestamp = dt.format("%Y-%m-%d-%H-%M-%S").to_string();
    for _ in 0..23 {
        rotating_file.writeln(s).unwrap();
    }

    rotating_file.close();

    assert!(Path::new(root_dir).join(timestamp.clone() + ".log").exists());
    assert!(!Path::new(root_dir).join(timestamp.clone() + "-1.log").exists());
    std::fs::remove_dir_all(root_dir).unwrap();
}
```
