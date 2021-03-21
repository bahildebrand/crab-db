use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

pub fn size_from_bytes(buf: Vec<u8>) -> usize {
    let mut cursor = Cursor::new(buf);
    cursor.read_i32::<LittleEndian>().unwrap() as usize
}
