# rotating-file

[![](https://img.shields.io/github/workflow/status/soulmachine/rotating-file/CI/main)](https://github.com/soulmachine/rotating-file/actions?query=branch%3Amain)
[![](https://img.shields.io/crates/v/rotating-file.svg)](https://crates.io/crates/rotating-file)
[![](https://docs.rs/rotating-file/badge.svg)](https://docs.rs/rotating-file)
==========
A thread-safe rotating file with customizable rotation behavior.

## Example

```rust
use rotating_file::RotatingFile;

fn main() {
    let root_dir = "./target/tmp";
    let s = "The quick brown fox jumps over the lazy dog";

    // rotated by 1 kilobyte, compressed with gzip
    let rotating_file = RotatingFile::new(root_dir, Some(1), None, None, None, None, None);
    for _ in 0..24 {
        rotating_file.writeln(s).unwrap();
    }
    rotating_file.close();

    assert_eq!(2, std::fs::read_dir(root_dir).unwrap().count());
    std::fs::remove_dir_all(root_dir).unwrap();
}
```
