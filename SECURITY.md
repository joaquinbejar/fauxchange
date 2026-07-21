# Security Policy

`fauxchange` is an options-exchange **simulator**. It handles no real money,
no settlement, and no custody. But it is designed to be wired into
**production CI and integration infrastructure** as the venue-under-test: it
runs inside corporate networks next to real services, holds credential
material (JWT signing keys, Argon2id password hashes, `AUTH_BOOTSTRAP_SECRET`),
and processes untrusted protocol input on open ports. Its security posture is
treated as that of any internal service.

> **Status:** this policy has been in force since the first implementation
> commit. The version-support table below states the policy that governs
> from the `v1.0.0` release onward (the `v1.0` stability milestone —
> [ROADMAP.md §v1.0 stability commitment](docs/ROADMAP.md#v10--stability-commitment));
> before `v1.0.0`, only the latest published `0.x` minor carries any
> security-fix commitment, and no `0.x` release carries a long-term-support
> guarantee ([SEMVER.md](docs/SEMVER.md)). The engineering threat model
> behind this policy is [`docs/08-threat-model.md`](docs/08-threat-model.md);
> the owner decision that made security a first-class priority is
> [ADR-0008](docs/adr/0008-production-grade-performance-and-security.md).

## Supported versions

From `v1.0.0` onward, `fauxchange` follows [SEMVER.md](docs/SEMVER.md):
breaking changes land only in a major bump, so a security fix backports
cleanly onto the current major line.

| Version | Supported |
|---------|-----------|
| latest published `1.x` minor | Security fixes |
| `< latest 1.x` minor (still `1.x`) | Not supported — upgrade to the latest `1.x` minor |
| `0.x` (pre-`v1.0.0`) | Not supported — pre-stability; upgrade to `1.x` |

Only the **latest published minor** of the current major line receives
security fixes — there is no parallel-maintained older minor. A pre-`1.0`
(`0.x`) release carries no security-fix guarantee; report a vulnerability
against a `0.x` release through the same channel below and it is triaged
against the latest `1.x` (or, before `v1.0.0` ships, the latest `0.x`)
minor. When `fauxchange` ships a `2.0`, this table is revised the same way
the `1.x` row was.

## Reporting a vulnerability

**Do not open a public GitHub issue for a security vulnerability.**

Report privately through either channel:

- **GitHub Security Advisories** — "Report a vulnerability" on the
  [`joaquinbejar/fauxchange`](https://github.com/joaquinbejar/fauxchange)
  repository (Security tab), or
- **Email** — Joaquin Bejar, `jb@taunais.com`, with `fauxchange security` in
  the subject.

Please include: affected version/commit, a description of the issue, a
proof-of-concept or reproduction steps if you have them, and the impact you
believe it has.

### What to expect

- **Acknowledgement** of your report within a few business days.
- An initial **assessment and severity** as soon as it is triaged.
- **Coordinated disclosure**: we agree a disclosure timeline with you, fix the
  issue, publish an advisory crediting you (unless you prefer to remain
  anonymous), and release the fix on the supported minor.

## Scope

**In scope** — issues in `fauxchange`'s own code or shipped artifacts:

- Authentication or authorization bypass on any surface (REST / WebSocket /
  FIX), including FIX-logon → permission mapping and account revocation.
- Leakage of secrets (JWT signing key, `AUTH_BOOTSTRAP_SECRET`, Argon2id
  password hashes, `DATABASE_URL`) into logs, error responses, or FIX
  `Text (58)`.
- Denial-of-service that bypasses the venue's bounds (rate limits, bounded
  mailboxes, connection/subscription caps, message-size limits) to hang or
  OOM the process.
- Memory-safety or panic-on-untrusted-input defects (a malformed REST/WS/FIX
  frame or a hostile journal/replay bundle that panics or corrupts state).
- SQL injection or unsafe query construction in the optional persistence path.
- Supply-chain issues in `fauxchange`'s dependency declarations or container
  image.
- Container-hardening gaps (secrets baked into an image, dev keys valid in a
  published image, metrics exposed beyond loopback by default).

**Out of scope** (see [`docs/08-threat-model.md` §3](docs/08-threat-model.md#3-trust-boundaries-attackers-and-scope)):

- Anything requiring theft of real money / settlement / custody — no such
  assets exist.
- Network-level confidentiality / MITM on the local trust domain — TLS is out
  of scope for v1 and this is an **explicitly accepted risk**; deploy
  `fauxchange` only on a trusted network segment.
- Vulnerabilities in upstream dependencies themselves (report those upstream);
  we will update our pin once a fix is available.
- Findings that require physical access, a pre-compromised host, or
  micro-architectural side channels against a local test fixture.

## Our commitments

- **No `unsafe`.** `#![forbid(unsafe_code)]` at the crate root.
- **Supply-chain gates.** `cargo audit` + `cargo deny` run in CI from v0.1;
  dependencies are pinned to verified versions.
- **Untrusted-input hardening.** Every external input has a documented
  validation, resource ceiling, and typed error
  ([docs/08-threat-model.md §4](docs/08-threat-model.md#4-untrusted-input-hardening)).
- **Enforcement.** The `api-security-auditor` review agent audits every change
  to a gateway, auth, persistence, or migration surface before merge
  ([AGENTS.md](AGENTS.md)).

Thank you for helping keep `fauxchange` and the infrastructure it runs inside
safe.
