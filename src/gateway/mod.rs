//! Transport layer: the three protocol front-ends — REST, WebSocket, and
//! FIX 4.4 — that translate wire formats into venue commands over the
//! same order path (`fauxchange::exchange`). A gateway translates; it
//! never decides.
//!
//! Governed by `docs/03-protocol-surfaces.md`.

pub mod fix;
pub mod rest;
pub mod ws;
