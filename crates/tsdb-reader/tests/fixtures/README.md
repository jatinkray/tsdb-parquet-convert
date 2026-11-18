# TSDB test fixtures

`index_format_v1` is copied from the MIT-licensed `tsdb` crate v0.3.3
(`testdata/index_format_v1`), used to exercise the native index/chunk parser
against a real Prometheus block. It is test data only and is never written by
the converter.
`testblock` is copied from the same MIT-licensed crate (`testdata/testblock`):
a real 35,354-series block with five million samples, used for v2 index,
chunk decoding and source-scan integration tests.
