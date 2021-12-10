use crate::BuiltItem;
use yzix_core::store::{Dump, Hash as StoreHash};
use yzix_core::OutputError;

pub async fn mangle_result(
    url: yzix_core::Url,
    r: Result<reqwest::Response, reqwest::Error>,
    expect_hash: Option<StoreHash>,
) -> Result<BuiltItem, OutputError> {
    let r = r?;
    let rstat = r.status();

    if !rstat.is_success() {
        return Err(OutputError::FetchFailed {
            url: Some(url),
            status: Some(rstat.as_u16()),
            msg: rstat.to_string(),
        });
    }

    let content = r.bytes().await?;
    let content2 = content.as_ref().to_vec();
    let _ = content;
    let dump = Dump::Regular {
        contents: content2,
        executable: false,
    };

    let outhash = tokio::task::block_in_place(|| StoreHash::hash_complex(&dump));

    if let Some(expect_hash) = expect_hash {
        // verify hash
        if expect_hash != outhash {
            return Err(OutputError::HashMismatch {
                expected: expect_hash,
                got: outhash,
            });
        }
    }

    Ok(BuiltItem::with_single(
        None,
        Some(std::sync::Arc::new(dump)),
        outhash,
    ))
}
