#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use proptest::prelude::*;
use std::path::Path;
use tsdb_reader::chunks::decode_xor;
use tsdb_reader::Index;

fn put_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

proptest! {
    #[test]
    fn signed_varints_round_trip(value: i64) {
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, ((value as u64) << 1) ^ ((value >> 63) as u64));
        let (decoded, used) = tsdb_reader::varint::varint(&bytes, 0, Path::new("p")).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(used, bytes.len());
    }

    #[test]
    fn arbitrary_bytes_never_panic_in_xor_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode_xor(Path::new("fuzz"), &bytes);
    }

    #[test]
    fn arbitrary_bytes_never_panic_in_index_parser(
        bytes in proptest::collection::vec(any::<u8>(), 0..4096)
    ) {
        let _ = Index::parse(Path::new("fuzz"), &bytes);
    }
}
