use bytes::{Buf, BufMut, Bytes, BytesMut};

pub mod iterator;
mod tests;

#[derive(Default)]
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
    //crc:u32
}

impl Block {


    pub fn encode(&self) -> Bytes {
        let mut block = BytesMut::with_capacity(self.block_size());
        block.put(&self.data[..]);
        for off in &self.offsets {
            block.put_u16(*off);
        }
        block.put_u16(self.offsets.len() as u16);
        block.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        if data.len() <= 2 {
            return Self::default();
        }

        let num = (&data[data.len() - 2..data.len()]).get_u16() as usize;
        if data.len() <= 2 + num * 2 {
            return Self::default();
        }
        let data = &data[..data.len() - 2];
        let mut offs = Vec::with_capacity(num);
        let mut off_buf = &data[data.len() - num * 2..];
        let data_buf = &data[..data.len() - num * 2];
        while off_buf.has_remaining() {
            offs.push(off_buf.get_u16());
        }

        Self {
            data: data_buf.to_vec(),
            offsets: offs,
        }
    }


    fn block_size(&self) -> usize {
        let mut size = self.data.len();
        size += self.offsets.len() * 2;
        size += 2;
        size
    }


    pub fn new(data: Vec<u8>, offsets: Vec<u16>) -> Self {
        Self { data, offsets }
    }
}
