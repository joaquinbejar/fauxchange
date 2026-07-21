# Scenario seeds

A **scenario** makes the venue useful the moment it is up: it materialises a small
instrument set, opening prices, quoting personas, and an account registry. It is
config + REST calls, versioned here and reproducible
([docs/06 §7](../docs/06-deployment.md#7-seed-data-and-scenarios)).

`default.toml` is the default scenario. Load it with:

```sh
fauxchange --config seeds/default.toml
```

The compose one-shot `seed` service (#25) drives this same manifest.

## The seeding phase

The seed sections (`[accounts.*]`, `[instruments.*]`, `[market_maker.*]`) are
applied in a **bounded seeding phase**, in a **fixed manifest order**, *before* the
venue flips to **serving**:

1. the default market-maker persona knobs are applied to the engine;
2. accounts are provisioned into the registry (idempotently);
3. each contract is registered with the market maker;
4. each underlying's opening price is set through the price seam — a journaled
   step whose market-maker quotes **vivify** the leaf books; then
5. the venue flips to serving.

After the flip, a runtime hierarchy create/delete is **refused** — the instrument
set is a seed-time manifest input. (There is no REST hierarchy-create: a leaf book
is established by an order that vivifies it, which the opening-price seed drives;
the inherited `POST /api/v1/underlyings/…` routes are refusal stubs.)

## Schema

| Section | Key | Meaning |
|---|---|---|
| `[market_maker]` | `default_persona` | the persona applied globally (one global engine config) |
| `[market_maker.personas.<name>]` | `spread_multiplier` / `size_scalar` / `directional_skew` | quoting knobs (clamped to `[0.1,10]` / `[0,1]` / `[-1,1]`) |
| `[instruments.<UNDERLYING>]` | `opening_price_cents` | opening price in **integer cents** (positive) |
| | `expirations` | absolute `YYYYMMDD` dates (`DateTime`; a relative `Days` value is refused) |
| | `strikes` | strike ladder in whole units (non-empty, distinct, positive) |
| | `styles` | `["call", "put"]`; both when omitted |
| | `persona` | the persona bound to this underlying (must be defined). NOTE: the market-maker engine currently holds ONE global persona config, so per-underlying persona *knobs* collapse to the single default persona (per-underlying differentiation lands in v0.5); the binding is validated and selects the underlying's quoting |
| `[accounts.<ID>]` | `permissions` | `["read"]` / `["read","trade"]` / `["admin"]` (non-empty) |
| | `owner` | optional 64-hex STP owner; derived from the id when omitted |
| | `fix_username` / `fix_password` | FIX credential (plaintext password, hashed with Argon2id at boot, never stored) |
| | `fix_sender_comp_id` / `fix_target_comp_id` | optional FIX comp-id binding (both or neither) |

Money is **always integer cents** (never a float). A typo inside any seed table
aborts startup naming the key. Re-running the seeder is idempotent — an account or
instrument already present at the same specs is a no-op; a conflicting spec (a
different opening price, or an account id at different permissions) is an error.
