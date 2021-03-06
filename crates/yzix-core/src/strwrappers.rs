use serde::{Deserialize, Serialize};
use std::{convert, fmt};

macro_rules! make_strwrapper {
    ($name:ident ( $inp:ident ) || $errmsg:expr; { $($x:tt)* }) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new($inp: String) -> Option<Self> {
                $($x)*
            }
        }

        impl std::borrow::Borrow<String> for $name {
            #[inline(always)]
            fn borrow(&self) -> &String {
                &self.0
            }
        }

        impl std::borrow::Borrow<str> for $name {
            #[inline(always)]
            fn borrow(&self) -> &str {
                &*self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = &'static str;
            fn try_from(x: String) -> Result<Self, &'static str> {
                Self::new(x).ok_or($errmsg)
            }
        }

        impl From<$name> for String {
            #[inline(always)]
            fn from(x: $name) -> String {
                x.0
            }
        }

        impl convert::AsRef<str> for $name {
            #[inline(always)]
            fn as_ref(&self) -> &str {
                &*self.0
            }
        }

        impl convert::AsRef<[u8]> for $name {
            #[inline(always)]
            fn as_ref(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            #[inline(always)]
            fn deref(&self) -> &str {
                &*self.0
            }
        }

        impl fmt::Display for $name {
            #[inline(always)]
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&*self.0)
            }
        }
    }
}

make_strwrapper! { OutputName(outp) || "invalid output name"; {
    let is_illegal = |i: char| {
        !i.is_ascii_alphanumeric() && !matches!(i, '_' | '-' | '.')
    };
    if outp.is_empty() || outp.contains(is_illegal) {
        None
    } else {
        Some(Self(outp))
    }
}}

impl Default for OutputName {
    #[inline]
    fn default() -> Self {
        Self("out".to_string())
    }
}

pub(crate) fn is_default_output(o: &OutputName) -> bool {
    &*o.0 == "out"
}

#[cfg(test)]
mod tests {
    use super::{is_default_output, OutputName};

    #[test]
    fn default_output_valid() {
        let o = OutputName::default();
        assert!(is_default_output(&o));
        let _ = OutputName::new(o.to_string()).expect("roundtrip failed");
    }
}
