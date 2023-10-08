use std::fmt;

#[repr(u32)]
pub enum Operation {
    Noop = 0,
    Set = 1,
    Del = 2,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Noop => "noop",
            Operation::Set => "set",
            Operation::Del => "del",
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for Operation {
    fn from(value: String) -> Self {
        if value == "noop" {
            Self::Noop
        } else if value == "set" {
            Self::Set
        } else if value == "del" {
            Self::Del
        } else {
            unreachable!()
        }
    }
}
