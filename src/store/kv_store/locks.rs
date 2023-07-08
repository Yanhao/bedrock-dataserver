use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;

#[derive(PartialEq, Eq, Debug)]
pub struct LockItem {
    pub start: u64,
    pub end: u64,
}

impl Ord for LockItem {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.eq(other) {
            return Ordering::Equal;
        }

        if let Some(o) = self.partial_cmp(other) {
            return o;
        }

        if self.start >= other.start && self.end <= other.end {
            return Ordering::Greater;
        }

        if self.start <= other.start && self.end >= other.end {
            return Ordering::Less;
        }

        if self.start <= other.start && self.end <= other.end {
            return Ordering::Less;
        }

        if self.start >= other.start && self.end >= other.end {
            return Ordering::Greater;
        }

        Ordering::Equal
    }
}

impl PartialOrd for LockItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.end <= other.start {
            return Some(std::cmp::Ordering::Less);
        }

        if self.start >= other.end {
            return Some(std::cmp::Ordering::Greater);
        }

        None
    }
}

// impl PartialEq for Item {}

// impl Eq for Item {}

pub struct RangeLock {
    data: BTreeSet<LockItem>,
}

impl RangeLock {
    pub fn new() -> RangeLock {
        return RangeLock {
            data: BTreeSet::<LockItem>::new(),
        };
    }

    pub fn lock_range(&mut self, start: u64, end: u64) {
        let item = LockItem { start, end };
        self.data.insert(item);
    }

    pub fn unlock_range(&mut self, start: u64, end: u64) {
        let item = LockItem { start, end };
        self.data.remove(&item);
    }

    pub fn lock_record(&mut self, position: u64) {
        let item = LockItem {
            start: position,
            end: position + 1,
        };

        self.data.insert(item);
    }

    pub fn lock_overlap(&self, start: u64, end: u64) -> Vec<LockItem> {
        let r = self
            .data
            .range(LockItem { start, end: start }..LockItem { start: end, end });

        let mut ret = Vec::<LockItem>::new();

        for i in r.into_iter() {
            ret.push(LockItem {
                start: i.start,
                end: i.end,
            });
        }

        ret
    }

    fn debug_info(&self) {
        println!("debug info:");
        for i in self.data.iter() {
            print!("Item: start {}, end: {}\n", i.start, i.end);
        }
    }
}

impl fmt::Display for RangeLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("debug info:\n");

        for i in self.data.iter() {
            s = s + &format!("Item: start {}, end: {}\n", i.start, i.end);
        }

        f.write_str(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_range_lock() {
        let mut range_lock = RangeLock::new();
        range_lock.lock_range(10, 20);
        // range_lock.lock_range(10, 20);
        range_lock.lock_range(10, 30);
        range_lock.lock_range(20, 30);
        range_lock.lock_record(25);
        range_lock.lock_record(10);

        let overlap = range_lock.lock_overlap(10, 10 + 1);

        for o in overlap.iter() {
            if o.start == o.end {
                println!("record lock: {}", o.start);
                continue;
            }
            println!("range lock: start: {}, end: {}", o.start, o.end);
        }

        assert_eq!(overlap[0], LockItem { start: 10, end: 30 });
        assert_eq!(overlap[1], LockItem { start: 10, end: 20 });
        assert_eq!(overlap[2], LockItem { start: 10, end: 11 });
    }
}
