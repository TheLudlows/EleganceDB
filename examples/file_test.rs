use std::fs::File;
use std::io::{Read, Write};
use std::os::windows::fs::FileExt;

use tempfile::tempdir;

fn main() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");

    let mut file = File::options().write(true).read(true).create(true).truncate(true).open(&path).expect("[FileObject::create] create file fail");
    file.write_all("abc123".as_bytes())
        .expect("[FileObject::create] write file fail");
    file.flush().expect("[FileObject::create] flush file fail");

    // let mut file = File::open(&path).unwrap();
    // let r = file.seek(SeekFrom::Start(0)).expect("");
    // println!("seek {}", r);
    //reader.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = vec![0; 6];


    let len = file.metadata().expect("").len();
    println!("{}", len);
    println!("{:?}", file.metadata());

    //let r = file.read_exact(buf.as_mut_slice());
    file.seek_read(buf.as_mut_slice(), 3).unwrap();
    println!("{:?}", "abc123".as_bytes());
    println!("{:?}", buf);
}