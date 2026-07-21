# Adversarial deserialiser corpus (#034)

Committed hostile / corrupt fixtures for the **journal / replay / seed-bundle
deserialiser** — the v0.3 **security gate** for the semi-trusted-operator (A-7)
decode surface
([docs/08 §4](../../docs/08-threat-model.md#4-untrusted-input-hardening),
[docs/08 §6](../../docs/08-threat-model.md#6-fuzzing-and-adversarial-testing),
[TESTING.md §14](../../docs/TESTING.md#14-security-testing)).

Each file is fed to the **real** deserialiser by `tests/adversarial.rs` and MUST
produce the **correct typed reject** (the specific `JournalError` / `ReplayError` /
`ConfigError` variant — never a blanket `is_err()`), with **no panic**, **no
unbounded allocation**, and **no partial apply**.

This corpus is committed as files so it also **seeds the coverage-guided
`cargo fuzz` targets** that land in v1.0 (#052); this issue is the fixture gate, the
fuzz harness is deliberately staged after it.

## Layout

| Dir | Surface | Deserialiser | Error type |
|-----|---------|--------------|------------|
| `journal/` | write-ahead record | `exchange::decode_journal_record` | `JournalError` |
| `bundle/`  | scenario bundle    | `simulation::ScenarioBundle::from_json` + `replay_bundle` | `ReplayError` |
| `seed/`    | seed manifest      | `config::SeedManifest::from_toml_str` | `ConfigError` |

## Classes covered

- oversized records (size ceiling; generated, not committed — a >2 MiB record /
  >64 MiB bundle blob is volume, not a shape seed);
- truncated records / bundles;
- field / tag injection (unknown field, unknown enum variant tag);
- duplicate fields;
- missing required fields;
- out-of-range economic fields (negative cents, overflow quantity);
- malformed symbols;
- hostile scenario-bundle manifest (wrong pinned version);
- newer / mismatched `schema_version` (bundle schema **and** journal envelope
  schema);
- a tampered stored event → `JournalCorruption { underlying, sequence }` (the
  integrity oracle), driven from **tampered bytes on disk**.

## Regenerating

The committed files are (re)generated from the real envelope types with:

```sh
UPDATE_CORPUS=1 cargo test --test adversarial
```

(mirroring the `UPDATE_GOLDEN` convention). The default run **reads the committed
files** and asserts the typed reject — it never regenerates, so a drift is a test
failure. Add a crash regression by committing the offending bytes here and asserting
its typed reject in `tests/adversarial.rs`.
