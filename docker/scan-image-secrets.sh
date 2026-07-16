#!/usr/bin/env bash
# docker/scan-image-secrets.sh — the #26 no-baked-secrets gate
# (docs/08-threat-model.md §7, §9; docs/TESTING.md §14).
#
# Usage:
#   docker/scan-image-secrets.sh <image-ref>
#
# Asserts that fauxchange's OWN baked content — the compiled binary, the baked
# `seeds/default.toml` scenario, and anything else a `COPY` instruction places
# in the image — plus the image's baked CONFIG (`ENV` / `Labels`) carries no
# secret of these four known shapes:
#
#   1. No PRIVATE KEY block other than the ONE known, reviewed, non-secret
#      `JwtAuth::dev()` fixture (src/auth.rs `DEV_CERT_PEM` / `DEV_KEY_PEM` —
#      "a published, well-known dev keypair — NOT a real credential"). Every
#      `-----BEGIN ... PRIVATE KEY-----` block found in FILE content is hashed
#      (SHA-256) and compared against the pinned KNOWN_DEV_KEY_SHA256 below;
#      an unknown hash is a REAL finding and fails the scan. A PEM header
#      found in image CONFIG (`ENV` / `Labels`, see scope below) is ALWAYS a
#      finding, whitelist or not — an env var / label can never legitimately
#      carry the dev PEM, so there is nothing to whitelist there. This is a
#      release-gate BACKSTOP — the primary control on these keys is
#      `JwtAuth::release_gated` (src/auth.rs, refuses them at auth STARTUP
#      unless `FAUXCHANGE_DEV` is set); this script proves their bytes are
#      still exactly the reviewed fixture and nothing else, in every image
#      build.
#   2. No `DATABASE_URL`-shaped connection string carrying embedded
#      credentials (`postgres(ql)://user:pass@...`) — a bare `postgres://`
#      scheme-prefix LITERAL (verified locally: `sqlx-postgres`'s own URL
#      parser embeds one) is explicitly NOT a finding; only a scheme +
#      `user:pass@` authority is.
#   3. No `AUTH_BOOTSTRAP_SECRET` assignment carrying an actual VALUE
#      (`AUTH_BOOTSTRAP_SECRET=...` / `AUTH_BOOTSTRAP_SECRET = "..."`) — the
#      bare env-var NAME (verified locally: `std::env::var("AUTH_BOOTSTRAP_SECRET")`
#      compiles the literal name string into the binary's `.rodata`, harmless
#      and expected) is explicitly NOT a finding; only a `name=value` /
#      `name = "value"` SHAPE is. An `ENV AUTH_BOOTSTRAP_SECRET=<value>` baked
#      into image CONFIG is this same shape and is scanned too (see scope).
#   4. Every `fix_password = "..."` occurrence in the baked seed manifest (or
#      in image CONFIG) equals EXACTLY the one known, documented dev fixture
#      (`seeds/default.toml`'s header: "CLEARLY-LABELLED DEV credentials for
#      local use only") — any OTHER value is a REAL finding.
#
# Scope, deliberately, and stated honestly: this is NOT an exhaustive secret
# scan of the whole image. It covers exactly two places a fauxchange build
# can bake a secret, and only the four known shapes above:
#
#   (a) FILE CONTENT in layers this repo's OWN Dockerfile `COPY` instructions
#       introduce. Layers are classified from the image's OWN history
#       (`docker save`'s config blob `.history[]`, correlated in order with
#       `.rootfs.diff_ids[]` / `manifest.json`'s `.Layers[]` — every
#       `empty_layer: false` history entry maps 1:1, in order, to one real
#       layer). A layer is selected ONLY when its `created_by` denotes a
#       `COPY` instruction (BuildKit: `COPY <src> <dst> # buildkit`; classic
#       builder: `/bin/sh -c #(nop) COPY file:... in <dst>`) — i.e. repo
#       content, regardless of WHERE in the image it lands (`/usr/local/bin`,
#       `/app`, `/etc`, `/opt`, anywhere). Every file in a selected layer is
#       extracted and scanned — not a path-prefix allow-list.
#
#       Verified locally (#26 follow-up) against BOTH runtime targets: a
#       `runtime-slim` image's history has exactly 5 real layers — the
#       `debian:bookworm-slim` base layer, the `apt-get install ca-certificates
#       curl` `RUN` layer, a `WORKDIR /app` layer (Debian's WORKDIR creates a
#       real, if tiny, layer here), and the two `COPY` layers (binary, seed
#       manifest) — only the last two are selected. A `runtime-distroless`
#       image has 21 real layers (Google's own `bazel build ...` base-image
#       layers, `WORKDIR`, then the same two `COPY` layers) — again only the
#       two `COPY` layers are selected. Neither base's own history entries
#       match the `COPY` pattern, so the base/`RUN`/`WORKDIR` layers —
#       including the ones carrying GnuPG's own internal test-key fixtures
#       (`gpgv`/`libgcrypt`'s `test_ecdh` / `test_known_sig` / `test_sig`
#       PRIVATE KEY blocks used for `apt`'s package-signature verification) —
#       are excluded BY CONSTRUCTION, not by a path allow-list that could miss
#       a secret `COPY`'d somewhere else. That is Debian's / the base image
#       publisher's supply chain (covered by pinning the base image to a known
#       tag/digest — a PROVENANCE control, docs/08 §8), not a fauxchange-baked
#       secret; scanning it here would be noisy and off-topic.
#
#       SAFETY NET: if the image's history cannot be reliably correlated with
#       its layers (unexpected `docker save` shape, count mismatch), or if
#       classification finds ZERO `COPY` layers, this is a FAIL (exit 1) —
#       never a silent PASS. When in doubt this script fails CLOSED (scans
#       more / errors out), never open.
#
#   (b) IMAGE CONFIG — `docker inspect`'s `.Config.Env` and `.Config.Labels`
#       — checked against the SAME four shapes. This exists because `docker
#       save` layer-tar extraction NEVER inspects image config: an
#       `ENV AUTH_BOOTSTRAP_SECRET=<real-secret>` baked into the Dockerfile
#       lives in the config JSON, not in any layer's file content, and would
#       otherwise be invisible to (a) entirely (verified locally, #26
#       follow-up). Benign base-image `ENV` (`PATH`, `SSL_CERT_FILE`, `LANG`,
#       etc.) and non-secret `Labels` are untouched — only the four shapes
#       above are findings, not "any ENV is suspicious".
#
#   OUT of scope, honestly: base-image / `RUN`-introduced file content (a
#   provenance control via base-image pinning, not this script's job); secret
#   shapes other than the four listed; anything not reachable via `docker
#   inspect` CONFIG or a repo `COPY` layer (e.g. a secret baked by a `RUN`
#   step that writes a file — that is a coding-rules violation this script
#   does not currently catch, since #26's threat model is "a `COPY`'d secret"
#   and "a baked config value", not an arbitrary `RUN`).
#
# Works against BOTH runtime targets (`runtime-slim` / `runtime-distroless`,
# docker/Dockerfile) — verified locally against both (see above).
#
# Exit 0 and a "PASS" line on success; exit 1 and a "FAIL" line naming every
# finding on failure. Never echoes a real secret VALUE it finds (only the
# pattern/location/key-name) — a scan script must not itself become a leak
# vector.

set -euo pipefail

IMAGE="${1:?usage: docker/scan-image-secrets.sh <image-ref>}"

# --- Known, reviewed, non-secret dev fixtures (#26) -------------------------
# Pinned by exact content SHA-256 — NOT a loose substring match — so an
# accidental REAL key, or even a legitimately ROTATED dev key, still fails
# the scan until this pin is deliberately updated in the SAME PR that changes
# the fixture (mirrors deny.toml / .cargo/audit.toml's "pin + document +
# review to change" pattern elsewhere in this repo). Computed once by
# extracting the actual embedded PEM bytes from a built image
# (`fauxchange:slim`, verified locally) and hashing them — NOT transcribed by
# hand from src/auth.rs, so it is exactly what really ships. This whitelist
# applies ONLY to file content (scope (a) above) — an image CONFIG match
# (scope (b)) is never whitelisted, see the header note on PRIVATE KEY blocks.
KNOWN_DEV_CERT_SHA256="af2e44d65337725ca2b8562f196acc41480a22c78664a217184664d79a3805c3"
KNOWN_DEV_KEY_SHA256="6690715f024e2c3f771d469f4547e0aeb4e0116d340c9feaf595c2dfc6d71320"
KNOWN_DEV_FIX_PASSWORD="dev-taker-secret-change-me"

WORKDIR="$(mktemp -d)"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

echo "scan-image-secrets: saving $IMAGE to inspect its layers"
docker save "$IMAGE" -o "$WORKDIR/image.tar"
mkdir -p "$WORKDIR/oci"
tar -xf "$WORKDIR/image.tar" -C "$WORKDIR/oci"

MANIFEST="$WORKDIR/oci/manifest.json"
if [[ ! -f "$MANIFEST" ]]; then
    echo "scan-image-secrets: FAIL — no manifest.json in 'docker save' output for $IMAGE (unexpected save format)" >&2
    exit 1
fi

CONFIG_REL="$(jq -r '.[0].Config' "$MANIFEST")"
CONFIG_PATH="$WORKDIR/oci/$CONFIG_REL"
if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "scan-image-secrets: FAIL — manifest.json names a Config blob ($CONFIG_REL) missing from 'docker save' output for $IMAGE" >&2
    exit 1
fi

# --- Classify layers: repo-introduced (COPY) vs base/RUN (excluded) --------
# See the header's scope section (a) for the full rationale. Correlates the
# image config's `history[]` (ordered, `empty_layer` flags which entries have
# no corresponding layer) against `manifest.json`'s `.Layers[]` by POSITION —
# the documented Docker/OCI image-spec invariant. Fails CLOSED (exit 1, via
# the `if !` wrapper below) rather than guess when the counts don't line up.
COPY_LAYERS_FILE="$WORKDIR/copy_layers.txt"
if ! python3 - "$MANIFEST" "$CONFIG_PATH" >"$COPY_LAYERS_FILE" <<'PYEOF'
import json
import re
import sys

manifest_path, config_path = sys.argv[1:3]

with open(manifest_path, "r", encoding="utf-8") as handle:
    manifest = json.load(handle)
layers = manifest[0]["Layers"]

with open(config_path, "r", encoding="utf-8") as handle:
    config = json.load(handle)
history = config.get("history", [])
diff_ids = config.get("rootfs", {}).get("diff_ids", [])

non_empty_history = [h for h in history if not h.get("empty_layer", False)]

if len(non_empty_history) != len(layers) or len(diff_ids) != len(layers):
    print(
        "scan-image-secrets: history/layer count mismatch "
        f"(non-empty history={len(non_empty_history)}, diff_ids={len(diff_ids)}, "
        f"manifest layers={len(layers)}) -- COPY/RUN correlation unreliable, "
        "failing CLOSED rather than guess",
        file=sys.stderr,
    )
    sys.exit(1)

# BuildKit's `created_by` for a COPY instruction: `COPY <src> <dst> # buildkit`
# (verified locally against this repo's own runtime-slim/runtime-distroless
# images). The classic (non-BuildKit) builder instead wraps it as
# `/bin/sh -c #(nop) COPY file:... in <dst>`. Either shape is repo-introduced
# content; a RUN, the base FROM layer's own creation record, and metadata-only
# instructions (already excluded via empty_layer above) are not.
COPY_CREATED_BY = re.compile(r"(?:^\s*COPY\b)|(?:#\(nop\)\s*COPY\b)")

copy_layers = []
excluded = 0
for entry, layer in zip(non_empty_history, layers):
    if COPY_CREATED_BY.search(entry.get("created_by", "")):
        copy_layers.append(layer)
    else:
        excluded += 1

print(
    f"scan-image-secrets: classified {len(layers)} real layer(s) via image "
    f"history: {len(copy_layers)} repo-introduced COPY layer(s) selected for "
    f"scanning, {excluded} base/RUN layer(s) excluded (provenance-pinned by "
    "base-image digest, not fauxchange-baked)",
    file=sys.stderr,
)

for layer in copy_layers:
    print(layer)
PYEOF
then
    echo "scan-image-secrets: FAIL — could not reliably classify which layers of $IMAGE are repo-introduced (COPY) content from image history; failing closed rather than risk silently skipping a baked secret" >&2
    exit 1
fi

COPY_LAYERS=()
while IFS= read -r layer; do
    [[ -n "$layer" ]] && COPY_LAYERS+=("$layer")
done <"$COPY_LAYERS_FILE"

if [[ "${#COPY_LAYERS[@]}" -eq 0 ]]; then
    echo "scan-image-secrets: FAIL — image history classified zero repo-introduced (COPY) layers in $IMAGE; the image does not look like a fauxchange build (or the Dockerfile COPY instructions changed in a way this script's BuildKit created_by pattern no longer recognizes)" >&2
    exit 1
fi

SCANNED_LAYERS=0
FINDINGS_FILE="$WORKDIR/findings.txt"
: >"$FINDINGS_FILE"

for layer in "${COPY_LAYERS[@]}"; do
    layer_path="$WORKDIR/oci/$layer"
    SCANNED_LAYERS=$((SCANNED_LAYERS + 1))
    echo "scan-image-secrets: scanning COPY layer $layer (repo-introduced content — every path, not a prefix allow-list)"

    extract_dir="$WORKDIR/extract-$SCANNED_LAYERS"
    mkdir -p "$extract_dir"
    # Extract EVERYTHING this layer carries — a COPY layer contains only
    # what this repo's own Dockerfile placed there (plus any parent-directory
    # entries BuildKit adds along the destination path), so unlike the base/
    # apt layers (excluded above, by classification, not by member filtering)
    # there is no GnuPG-test-key / unrelated base-image content to filter out
    # here.
    tar -xf "$layer_path" -C "$extract_dir"

    python3 - "$extract_dir" "$FINDINGS_FILE" \
        "$KNOWN_DEV_CERT_SHA256" "$KNOWN_DEV_KEY_SHA256" "$KNOWN_DEV_FIX_PASSWORD" <<'PYEOF'
import hashlib
import os
import re
import sys

extract_dir, findings_path, known_cert_sha256, known_key_sha256, known_fix_password = sys.argv[1:6]

PEM_BLOCK = re.compile(
    rb"-----BEGIN ([A-Z ]*(?:CERTIFICATE|PRIVATE KEY))-----.*?-----END \1-----\n?",
    re.DOTALL,
)
DB_URL_WITH_CREDS = re.compile(rb"postgres(?:ql)?://[^\s/@]+:[^\s/@]+@")
BOOTSTRAP_SECRET_VALUE = re.compile(rb'AUTH_BOOTSTRAP_SECRET\s*=\s*\S')
FIX_PASSWORD_ASSIGNMENT = re.compile(rb'fix_password\s*=\s*"([^"]*)"')

findings = []
known_hashes = {known_cert_sha256, known_key_sha256}

for root, _dirs, files in os.walk(extract_dir):
    for name in files:
        path = os.path.join(root, name)
        try:
            with open(path, "rb") as handle:
                data = handle.read()
        except OSError:
            continue

        for match in PEM_BLOCK.finditer(data):
            label = match.group(1).decode("ascii", "replace")
            if b"CERTIFICATE" in label.encode() and b"PRIVATE" not in label.encode():
                # Certificates are public by construction (they carry only the
                # public key) — not a secret on their own; only PRIVATE KEY
                # material is gated.
                continue
            digest = hashlib.sha256(match.group(0)).hexdigest()
            if digest not in known_hashes:
                findings.append(
                    f"UNKNOWN PRIVATE KEY block in {path} (sha256={digest}) — "
                    f"not the known dev fixture"
                )

        if DB_URL_WITH_CREDS.search(data):
            findings.append(f"DATABASE_URL-shaped credentialed connection string in {path}")

        if BOOTSTRAP_SECRET_VALUE.search(data):
            findings.append(f"AUTH_BOOTSTRAP_SECRET assigned a VALUE in {path}")

        for fp_match in FIX_PASSWORD_ASSIGNMENT.finditer(data):
            value = fp_match.group(1).decode("utf-8", "replace")
            if value != known_fix_password:
                findings.append(
                    f"fix_password in {path} is NOT the known dev fixture "
                    f"(got a DIFFERENT value, {len(value)} chars)"
                )

with open(findings_path, "a", encoding="utf-8") as out:
    for finding in findings:
        out.write(finding + "\n")
PYEOF
done

# --- Image CONFIG scan: .Config.Env / .Config.Labels (#26 follow-up) -------
# See the header's scope section (b). `docker save` layer-tar extraction
# above never inspects image config — an `ENV AUTH_BOOTSTRAP_SECRET=<value>`
# baked into the Dockerfile lives here, not in any layer's file content.
echo "scan-image-secrets: inspecting $IMAGE image CONFIG (Env / Labels — baked at build time, distinct from layer file content)"
CONFIG_ENV_JSON="$(docker inspect --format '{{json .Config.Env}}' "$IMAGE")"
CONFIG_LABELS_JSON="$(docker inspect --format '{{json .Config.Labels}}' "$IMAGE")"

python3 - "$FINDINGS_FILE" "$KNOWN_DEV_FIX_PASSWORD" "$CONFIG_ENV_JSON" "$CONFIG_LABELS_JSON" <<'PYEOF'
import json
import re
import sys

findings_path, known_fix_password, env_json, labels_json = sys.argv[1:5]

# No PEM whitelist here — unlike file content, an ENV/Label can never
# legitimately carry the dev PEM (or any PEM), so any match is a finding,
# full stop (see the header note on scope (b)).
PRIVATE_KEY_HEADER = re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----")
DB_URL_WITH_CREDS = re.compile(rb"postgres(?:ql)?://[^\s/@]+:[^\s/@]+@")
BOOTSTRAP_SECRET_VALUE = re.compile(rb'AUTH_BOOTSTRAP_SECRET\s*=\s*\S')
FIX_PASSWORD_ASSIGNMENT = re.compile(rb'fix_password\s*=\s*"([^"]*)"')

findings = []


def check_entry(context, key, value):
    # Match against KEY=VALUE reconstructed, same shapes as file content —
    # but the finding message below NEVER includes `value`, only `key`
    # (never echo a real secret value).
    combined = f"{key}={value}".encode("utf-8", "replace")

    if PRIVATE_KEY_HEADER.search(combined):
        findings.append(
            f"UNKNOWN PRIVATE KEY header baked into image {context} '{key}' — "
            f"ENV/Labels must never carry key material (no dev-fixture exemption here)"
        )
    if DB_URL_WITH_CREDS.search(combined):
        findings.append(f"DATABASE_URL-shaped credentialed connection string baked into image {context} '{key}'")
    if BOOTSTRAP_SECRET_VALUE.search(combined):
        findings.append(f"AUTH_BOOTSTRAP_SECRET assigned a VALUE baked into image {context} '{key}'")
    for fp_match in FIX_PASSWORD_ASSIGNMENT.finditer(combined):
        fp_value = fp_match.group(1).decode("utf-8", "replace")
        if fp_value != known_fix_password:
            findings.append(
                f"fix_password baked into image {context} '{key}' is NOT the known dev fixture "
                f"(got a DIFFERENT value, {len(fp_value)} chars)"
            )


for entry in json.loads(env_json) or []:
    if not isinstance(entry, str) or "=" not in entry:
        continue
    key, _, value = entry.partition("=")
    check_entry("Config.Env", key, value)

for key, value in (json.loads(labels_json) or {}).items():
    check_entry("Config.Labels", key, "" if value is None else str(value))

with open(findings_path, "a", encoding="utf-8") as out:
    for finding in findings:
        out.write(finding + "\n")
PYEOF

if [[ -s "$FINDINGS_FILE" ]]; then
    echo "scan-image-secrets: FAIL — $IMAGE has $(wc -l <"$FINDINGS_FILE" | tr -d ' ') finding(s):" >&2
    sed 's/^/  - /' "$FINDINGS_FILE" >&2
    exit 1
fi

echo "scan-image-secrets: PASS — $IMAGE ($SCANNED_LAYERS repo-introduced COPY layer(s) + image Config.Env/Config.Labels scanned): no secret of the four known shapes found beyond the known, reviewed dev fixture (seeds/default.toml fix_password + the embedded JwtAuth::dev() keypair). Base/RUN layers are OUT of scope — their provenance is controlled by base-image digest pinning, not this scan."
