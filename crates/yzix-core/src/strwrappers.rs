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

        impl TryFrom<String> for $name {
            type Error = &'static str;
            fn try_from(x: String) -> Result<Self, &'static str> {
                Self::new(x).ok_or($errmsg)
            }
        }

        impl From<$name> for String {
            #[inline]
            fn from(x: $name) -> String {
                x.0
            }
        }

        impl convert::AsRef<str> for $name {
            #[inline]
            fn as_ref(&self) -> &str {
                &*self.0
            }
        }

        impl convert::AsRef<[u8]> for $name {
            #[inline]
            fn as_ref(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            #[inline]
            fn deref(&self) -> &str {
                &*self.0
            }
        }

        impl fmt::Display for $name {
            #[inline]
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&*self.0)
            }
        }
    }
}

make_strwrapper!{ InputName(inp) || "invalid input name"; {
    if inp.contains(|i: char| !i.is_ascii_alphanumeric() && !matches!(i, '_' | '-' | '.'))
        || inp.starts_with(|i: char| i.is_ascii_digit())
    {
        None
    } else {
        Some(Self(inp))
    }
}}

make_strwrapper!{StoreName(inp) || "invalid store name"; {
    if inp.contains(|i: char| !i.is_ascii_graphic() || i == '/') {
        None
    } else {
        Some(Self(inp))
    }
}}
