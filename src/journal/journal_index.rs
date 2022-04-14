use anyhow::{anyhow, Result};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Segment {
    pub offset: u64,
    pub length: u64,
    pub logic_offset: u64,
    pub version: u64,
    pub term: u64,
}

impl Segment {
    pub fn end_offset_in_chunk(&self) -> u64 {
        self.offset + self.length
    }
    pub fn end_offset_in_journal(&self) -> u64 {
        self.logic_offset + self.length
    }
}

pub struct JournalIndex {
    index: Vec<Segment>,
}

impl JournalIndex {
    pub fn insert(
        &mut self,
        offset_in_chunk: u64,
        data_len: u64,
        version: u64,
        term: u64,
        logic_offset: u64,
    ) {
        let mut i = 0;
        let mut new_i = Vec::new();

        while i < self.index.len() && self.index[i].end_offset_in_chunk() <= offset_in_chunk {
            new_i.push(self.index[i]);
            i += 1;
        }

        if i < self.index.len() && self.index[i].offset < offset_in_chunk {
            let mut s = self.index[i];
            s.length = offset_in_chunk - s.offset;
            new_i.push(s);
            i += 1;
        }

        while i < self.index.len()
            && self.index[i].end_offset_in_chunk() <= offset_in_chunk + data_len
        {
            i += 1;
        }

        let s = Segment {
            offset: offset_in_chunk,
            logic_offset,
            length: data_len,
            term,
            version,
        };
        new_i.push(s);

        if i < self.index.len() && self.index[i].offset < offset_in_chunk + data_len {
            let mut s = self.index[i];
            s.offset = offset_in_chunk + data_len;
            s.length = self.index[i].end_offset_in_chunk() - s.offset;
            new_i.push(s);
            i += 1;
        }

        while i < self.index.len() {
            new_i.push(self.index[i]);
            i += 1;
        }

        std::mem::swap(&mut new_i, &mut self.index)
    }

    pub fn search(&self, offset: u64, data_len: u64) -> Result<Vec<Segment>> {
        let mut ret = Vec::new();

        let mut i = 0;
        while i < self.index.len() && self.index[i].end_offset_in_chunk() <= offset {
            i += 1;
        }

        if i < self.index.len() && self.index[i].offset < offset {
            let mut s = self.index[i];
            s.length = s.length - (self.index[i].offset - offset);
            s.offset = offset;
            ret.push(s);
            i += 1;
        }

        while i < self.index.len() && self.index[i].end_offset_in_chunk() <= (offset + data_len) {
            ret.push(self.index[i]);
            i += 1;
        }

        if i < self.index.len() && (offset + data_len) < self.index[i].end_offset_in_chunk() {
            let mut s = self.index[i];
            s.length = offset + data_len - self.index[i].offset;
            ret.push(s);
            i += 1;
        }

        Ok(ret)
    }

    pub fn clear(&mut self, offset: u64, data_len: u64, version: u64) {
        let mut new_i = Vec::new();

        let mut i = 0;
        while i < self.index.len() && self.index[i].end_offset_in_chunk() <= offset {
            new_i.push(self.index[i]);
            i += 1;
        }

        if i < self.index.len() && offset < self.index[i].offset && self.index[i].version == version
        {
            let mut s = self.index[i];
            s.length = self.index[i].end_offset_in_chunk() - offset;
            new_i.push(s);
            i += 1;
        }

        while i < self.index.len() && self.index[i].end_offset_in_chunk() <= (offset + data_len) {
            if self.index[i].version != version {
                new_i.push(self.index[i]);
            }
            i += 1;
        }

        if i < self.index.len()
            && self.index[i].version == version
            && (offset + data_len) < self.index[i].end_offset_in_chunk()
        {
            let mut s = self.index[i];
            s.length = self.index[i].end_offset_in_chunk() - (offset + data_len);
            s.offset = offset + data_len;
            new_i.push(s);
            i += 1;
        }

        while i < self.index.len() {
            new_i.push(self.index[i]);
            i += 1;
        }

        std::mem::swap(&mut new_i, &mut self.index);
    }

    pub fn new() -> Self {
        Self { index: Vec::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_insert00() {
        let mut index = JournalIndex::new();

        index.insert(0, 10, 1, 1, 0);
        index.insert(0, 10, 2, 1, 0);
        index.insert(0, 10, 3, 1, 0);
        index.insert(0, 10, 4, 2, 0);

        let s = Segment {
            offset: 0,
            length: 10,
            logic_offset: 0,
            version: 4,
            term: 2,
        };
        assert_eq!(index.index.len(), 1);
        assert_eq!(index.index[0], s);
    }

    #[test]
    pub fn test_insert01() {
        let mut index = JournalIndex::new();

        index.insert(0, 10, 1, 1, 0);
        index.insert(10, 10, 2, 1, 0);
        index.insert(21, 4, 3, 1, 0);
        index.insert(30, 10, 4, 1, 0);

        let s0 = Segment {
            offset: 0,
            length: 10,
            logic_offset: 0,
            version: 1,
            term: 1,
        };
        let s1 = Segment {
            offset: 10,
            length: 10,
            logic_offset: 0,
            version: 2,
            term: 1,
        };
        let s2 = Segment {
            offset: 21,
            length: 4,
            logic_offset: 0,
            version: 3,
            term: 1,
        };
        let s3 = Segment {
            offset: 30,
            length: 10,
            logic_offset: 0,
            version: 4,
            term: 1,
        };

        assert_eq!(index.index.len(), 4);
        assert_eq!(index.index[0], s0);
        assert_eq!(index.index[1], s1);
        assert_eq!(index.index[2], s2);
        assert_eq!(index.index[3], s3);
    }

    #[test]
    pub fn test_insert02() {
        let mut index = JournalIndex::new();

        index.insert(0, 10, 1, 1, 0);
        index.insert(5, 10, 2, 1, 0);

        let s0 = Segment {
            offset: 0,
            length: 5,
            logic_offset: 0,
            version: 1,
            term: 1,
        };
        let s1 = Segment {
            offset: 5,
            length: 10,
            logic_offset: 0,
            version: 2,
            term: 1,
        };

        assert_eq!(index.index.len(), 2);
        assert_eq!(index.index[0], s0);
        assert_eq!(index.index[1], s1);
    }

    #[test]
    pub fn test_insert03() {
        let mut index = JournalIndex::new();

        index.insert(0, 10, 1, 1, 0);
        index.insert(5, 10, 2, 1, 0);
        index.insert(30, 10, 4, 1, 0);
        index.insert(4, 31, 4, 1, 0);

        let s0 = Segment {
            offset: 0,
            length: 4,
            logic_offset: 0,
            version: 1,
            term: 1,
        };

        let s1 = Segment {
            offset: 4,
            length: 31,
            logic_offset: 0,
            version: 4,
            term: 1,
        };

        let s2 = Segment {
            offset: 35,
            length: 5,
            logic_offset: 0,
            version: 4,
            term: 1,
        };
        assert_eq!(index.index.len(), 3);
        assert_eq!(index.index[0], s0);
        assert_eq!(index.index[1], s1);
        assert_eq!(index.index[2], s2);
    }
    #[test]
    pub fn test_clear() {}

    #[test]
    pub fn test_search() {}
}
