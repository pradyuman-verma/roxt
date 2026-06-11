# MCAP conformance fixtures

Vendored from [foxglove/mcap](https://github.com/foxglove/mcap)
(`tests/conformance/data/`, fetched from the `main` branch LFS storage),
MIT licensed. These are the upstream container-format conformance files;
roxt uses them to pin `McapSource` behaviour — message counts, schemaless
channels, attachment/metadata skipping — across `mcap` crate upgrades.

Variant suffixes encode which optional sections the file contains
(`ch` chunks, `chx` chunk indexes, `mx` message indexes, `pad` padding,
`rch`/`rsh` repeated channels/schemas, `st` statistics, `sum` summary).

To refresh, re-download from
`https://media.githubusercontent.com/media/foxglove/mcap/main/tests/conformance/data/<Group>/<file>`.
