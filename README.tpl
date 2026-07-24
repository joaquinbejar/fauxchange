[![MIT License](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)
[![Crates.io](https://img.shields.io/crates/v/fauxchange.svg)](https://crates.io/crates/fauxchange)
[![Downloads](https://img.shields.io/crates/d/fauxchange.svg)](https://crates.io/crates/fauxchange)
[![Stars](https://img.shields.io/github/stars/joaquinbejar/fauxchange.svg)](https://github.com/joaquinbejar/fauxchange/stargazers)
[![Issues](https://img.shields.io/github/issues/joaquinbejar/fauxchange.svg)](https://github.com/joaquinbejar/fauxchange/issues)
[![PRs](https://img.shields.io/github/issues-pr/joaquinbejar/fauxchange.svg)](https://github.com/joaquinbejar/fauxchange/pulls)

[![Build Status](https://img.shields.io/github/actions/workflow/status/joaquinbejar/fauxchange/ci.yml)](https://github.com/joaquinbejar/fauxchange/actions)
[![Coverage](https://img.shields.io/codecov/c/github/joaquinbejar/fauxchange)](https://codecov.io/gh/joaquinbejar/fauxchange)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://docs.rs/fauxchange)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](https://www.rust-lang.org)



{{readme}}



## 🧩 The ecosystem

`fauxchange` is the venue that wires together four upstream crates by the same
author — it never reimplements matching, the option-chain hierarchy, FIX
framing, or options math:

| Crate | Role in fauxchange |
|-------|--------------------|
| [IronFix](https://github.com/joaquinbejar/IronFix) | FIX 4.4 framing + typed primitives (`FixCodec`, `MsgType`/`CompId`/`SeqNum`, tag-value `Decoder`/`Encoder`). The acceptor, session FSM, typed messages, and resend/gap-fill are the venue's own work on top. |
| [option-chain-orderbook](https://github.com/joaquinbejar/Option-Chain-OrderBook) | Hierarchical options books (`Underlying → Expiration → Strike`) and the `sequencer` feature (command/event/journal/replay). |
| [orderbook-rs](https://github.com/joaquinbejar/OrderBook-rs) | The lock-free matching engine underneath — fills, fees, and self-trade prevention. |
| [OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) | Options pricing, Greeks, `ExpirationDate`, and `WalkType` price walks. |

## 🛠 Makefile commands

This project ships a `Makefile` with the common developer tasks. A few of the
most useful:

### 🔧 Build & run

```sh
make build         # cargo build (debug, all features)
make release       # cargo build --release (zero warnings)
make run           # boot the venue locally (REST + WS on 127.0.0.1:8080)
make run-seeded    # boot with seeded instruments (BTC, ETH) for manual testing
```

### 🧪 Test & quality

```sh
make test              # unit + integration + doctest suite (mirrors CI `test`)
make test-conformance  # the tests/ conformance + e2e suite (golden/determinism/parity/rest/ws)
make fmt               # apply rustfmt
make lint              # clippy, all targets/features, warnings denied
make fix               # auto-apply compiler-suggested fixes
make doc               # missing-docs gate on every pub item
make pre-push          # the canonical ready-to-push gate (fmt + lint + test + readme + doc)
```

### 📦 Packaging & docs

```sh
make docker-smoke  # docker e2e smoke: compose up -> health -> order -> WS fill -> shutdown
make soak          # stability soak (flat memory, no sequence gaps, restart-from-journal)
make readme        # regenerate README.md from src/lib.rs docs via cargo-readme
make publish       # publish fauxchange to crates.io (confirm first)
```

### 📈 Coverage, benches & security

```sh
make coverage         # cargo tarpaulin — text + lcov summary
make bench-regression # reduced-sample hot-path benches + the bench-regression gate
make audit         # cargo audit — RustSec advisory scan
make deny          # cargo deny — license/ban/duplicate/source policy
```

## Contribution and Contact

Contributions are welcome. If you would like to contribute, please:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and ensure the project still builds and all tests pass (`make pre-push`).
4. Commit your changes and push your branch to your fork.
5. Open a pull request against `main`.

If you have any questions, issues, or feedback, please contact the maintainer:

### **Contact Information**
- **Author**: Joaquín Béjar García
- **Email**: jb@taunais.com
- **Telegram**: [@joaquin_bejar](https://t.me/joaquin_bejar)
- **Repository**: <https://github.com/joaquinbejar/fauxchange>
- **Documentation**: <https://docs.rs/fauxchange>

We appreciate your interest and look forward to your contributions!

**License**: MIT
