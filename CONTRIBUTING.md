# Contributing

Thanks for helping improve botwright.

## Before opening a pull request

```sh
cargo fmt --check
cargo clippy --all-targets --features mempool,hypersync,bundles,telegram,metrics,http,test-util -- -D warnings
cargo clippy --all-targets --no-default-features --features evm-alloy2,signal,mempool,hypersync,bundles,telegram,metrics,http,test-util -- -D warnings
cargo test --features mempool,hypersync,bundles,telegram,metrics,http,test-util
```

The EVM integration tests in `tests/anvil/` need [`anvil`](https://getfoundry.sh) on `PATH` (or
its path in `ANVIL`). Without it they are skipped, unless `BOTWRIGHT_REQUIRE_ANVIL` is set:

```sh
BOTWRIGHT_REQUIRE_ANVIL=1 cargo test --test anvil
BOTWRIGHT_REQUIRE_ANVIL=1 cargo test --test anvil --no-default-features --features evm-alloy2
```

`evm-alloy1` and `evm-alloy2` are mutually exclusive, so never use `--all-features`.

## Guidelines

- Behaviour described in [docs/guarantees.md](docs/guarantees.md) is part of the API. Changes to it
  need a test and an entry in the changelog.
- Code written against `botwright::eth` must compile with both alloy backends.
- Add a line to the `Unreleased` section of [CHANGELOG.md](CHANGELOG.md).

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you shall be licensed under the Unlicense, the MIT license and the Apache License,
Version 2.0, without any additional terms or conditions.
