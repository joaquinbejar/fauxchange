# fauxchange

> 🚧 **Early development** — v0.0.1 reserves the crate name. APIs do not exist yet.

**fauxchange** (*faux* + *exchange*) is a local options exchange simulator for
testing trading systems — think **LocalStack for trading**.

## Planned features

- **Real matching**: full limit order book matching powered by
  [OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) and
  [Option-Chain-OrderBook](https://github.com/joaquinbejar/Option-Chain-OrderBook).
- **Standard APIs**: connect your system over FIX 4.4, WebSocket or REST —
  the same protocols real venues speak.
- **Historical replay**: feed recorded market data and test against realistic
  conditions, or generate synthetic chains with
  [OptionChain-Simulator](https://github.com/joaquinbejar/OptionChain-Simulator).
- **Configurable microstructure**: latency, fees, rate limits and market
  maker behavior.
- **One command**: `docker compose up` and you have an exchange.

## Ecosystem

Part of a family of Rust crates for options trading infrastructure:
[OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) ·
[OptionStratLib](https://github.com/joaquinbejar/OptionStratLib) ·
[IronCondor](https://github.com/joaquinbejar/IronCondor) ·
[ChainView](https://github.com/joaquinbejar/ChainView)

## Documentation

The full design documentation (PRD, roadmap, domain model, matching
architecture, protocol surfaces, market data/replay, microstructure,
deployment, and ADRs) is maintained locally during the design phase and will
be published with the first implementation release (v0.1.0). v0.0.1 reserves
the name and no implementation code exists yet.

## License

MIT — see [LICENSE](./LICENSE).

## Contact

Joaquin Bejar — jb@taunais.com
