# Secret Storage

Reference for developers touching Flowfile's secret-storage subsystem. For the
operator's manual on backup, recovery, and threat scenarios, see
[`users/visual-editor/catalog/secrets.md`](../users/visual-editor/catalog/secrets.md).

## Where things live

| Concern | File |
|---|---|
| Envelope format + crypto primitives | `shared/crypto/envelope.py` |
| Master-key fetch (core, with auto-generation) | `flowfile_core/flowfile_core/auth/secrets.py` |
| Master-key fetch (worker) | `flowfile_worker/flowfile_worker/secrets.py` |
| High-level wrappers (`encrypt_secret`, `decrypt_secret`, `store_secret`) | `flowfile_core/flowfile_core/secret_manager/secret_manager.py` |
| Worker wrappers (same surface, worker-side master-key lookup) | `flowfile_worker/flowfile_worker/secrets.py` |
| Audit helper | `flowfile_core/flowfile_core/secret_manager/audit.py` |
| Audit table model | `SecretAccessEvent` in `flowfile_core/flowfile_core/database/models.py` |
| Audit table migration | `flowfile_core/flowfile_core/alembic/versions/012_secret_access_events.py` |
| CRUD routes (audit-wired) | `flowfile_core/flowfile_core/routes/secrets.py` |

Both `flowfile_core` and `flowfile_worker` delegate the actual crypto to
`shared/crypto/envelope.py`. Each service owns its own master-key resolution
(different backends) but they agree byte-for-byte on the envelope.

## Envelope format

```
$ffsec$1$<user_id>$<fernet_token>
```

Stored as plain text in `secrets.encrypted_value`. The `<user_id>` is embedded
so the worker can decrypt without separately being told whose secret it is.
The leading `1` is a version digit — see [Adding envelope v2](#adding-envelope-v2).

Legacy raw Fernet tokens (no `$ffsec$` prefix) are still readable via
`decrypt_secret_envelope(..., user_id=...)` for backwards compatibility.

## Shared crypto API

`from shared.crypto.envelope import ...`

| Symbol | Purpose |
|---|---|
| `KEY_DERIVATION_VERSION` | HKDF salt — `b"flowfile-secrets-v1"`. Changing this invalidates every existing secret. |
| `SECRET_FORMAT_PREFIX` | `"$ffsec$1$"` |
| `derive_user_key(master_key, user_id)` | HKDF-SHA256 → 32-byte URL-safe base64 Fernet key. Deterministic. |
| `parse_v1_envelope(blob) → (user_id, token)` | Splits a v1 envelope. Raises `ValueError` on malformed input. |
| `encrypt_secret_envelope(master_key, plaintext, user_id)` | Emit a v1 envelope. |
| `decrypt_secret_envelope(master_key, blob, user_id=None)` | Decrypt a v1 envelope (user_id read from blob) or a legacy raw token. |

Don't import `shared.crypto.envelope` directly from a route handler. Use the
`secret_manager` wrappers — they fetch the master key locally and delegate.

```python
from flowfile_core.secret_manager.secret_manager import encrypt_secret, decrypt_secret

token = encrypt_secret("hunter2", user_id=current_user.id)
plaintext = decrypt_secret(token).get_secret_value()
```

## Master-key resolution

**Core** (`flowfile_core/auth/secrets.py::get_master_key`)

1. `FLOWFILE_MODE=docker` → `get_docker_secret_key()` reads
   `FLOWFILE_MASTER_KEY` env var or `/run/secrets/flowfile_master_key`.
   Raises `RuntimeError` if neither is configured.
2. Otherwise → `SecureStorage.get_password("flowfile", "master_key")`.
   Auto-generates on first call and persists to
   `<storage>/flowfile.json.enc` encrypted under `<storage>/.secret_key`.
   The auto-generation branch emits a multi-line `logger.warning(...)` with
   both file paths and an 8-char SHA-256 fingerprint. Fires once.

**Worker** (`flowfile_worker/secrets.py::get_master_key`)

1. `TEST_MODE=1` → use `FLOWFILE_MASTER_KEY` if set, else generate a fresh
   per-process random key cached in module-level `_TEST_MASTER_KEY`.
2. `FLOWFILE_MODE=docker` → same Docker resolution as core.
3. Otherwise → read from worker's local `SecureStorage`. Worker does **not**
   auto-generate; it raises `ValueError` if the key is missing. (Core is
   expected to be the generation point in non-Docker deployments; worker
   reads what core wrote.)

In production, core and worker share the key by both reading the same
`FLOWFILE_MASTER_KEY` env var (Docker) or the same on-disk
`flowfile.json.enc` (local install).

## Audit log

Table: `secret_access_events`.

| Column | Type | Notes |
|---|---|---|
| `id` | int PK | |
| `user_id` | FK → `users.id`, indexed | |
| `secret_id` | FK → `secrets.id` ON DELETE SET NULL | preserves audit row after deletion |
| `secret_name` | string, indexed, nullable | denormalized — survives the FK going NULL |
| `action` | string, indexed | `create` / `list` / `read` / `delete` |
| `result_status` | string | `success` / `error` |
| `error` | text, nullable | short snake_case codes: `not_found`, `duplicate_name`, … |
| `source` | string, default `api` | reserved for future non-API emitters |
| `ip_address` | string, nullable | best-effort; honors `X-Forwarded-For` |
| `created_at` | datetime, indexed | server-side `now()` |

### Recording an event from a new route

```python
from flowfile_core.secret_manager import audit

audit.record_event(
    audit.SecretEvent(
        user_id=current_user.id,
        action="read",
        result_status="success",
        secret_name=name,
        secret_id=row.id,
        ip_address=request.client.host if request.client else None,
    ),
    db=db,
)
db.commit()
```

`record_event` adds the row to the caller's session — the caller is responsible
for committing. Exceptions during the audit insert are logged and swallowed
rather than failing the user-facing operation that already succeeded.

### What is *not* recorded

Per-secret decrypt activity during flow execution. The audit log is for the
API CRUD boundary. Logging every decrypt would dominate the table with
routine flow-run activity. If per-decrypt accounting becomes load-bearing,
add a separate event source rather than expanding this one.

## Test mode

`TEST_MODE=1` (set by `flowfile_worker/tests/conftest.py`) makes
`get_master_key()` synthesize a per-process random key when
`FLOWFILE_MASTER_KEY` isn't set. Round-trip tests work without external setup.

Integration tests that need core and worker to agree on the key must export
`FLOWFILE_MASTER_KEY` explicitly — see
`flowfile_core/tests/test_auth_e2e.py` for the pattern.

No hardcoded key material lives in source.

## Adding envelope v2

The dispatch in `decrypt_secret_envelope` is set up to branch on the version
digit:

```python
if encrypted_value.startswith(SECRET_FORMAT_PREFIX):       # "$ffsec$1$"
    ...
elif encrypted_value.startswith(SECRET_FORMAT_PREFIX_V2):  # "$ffsec$2$"  (add this)
    ...
```

To introduce a v2:

1. Add `SECRET_FORMAT_PREFIX_V2 = "$ffsec$2$"` in `envelope.py`.
2. Add `parse_v2_envelope` and a v2 decrypt branch.
3. Flip `encrypt_secret_envelope` to emit v2 (or split write paths).
4. Decide a re-write policy for existing v1 rows: lazy on read, an explicit
   admin-triggered migrate, or leave both formats forever. The contract is
   that v1 must remain decryptable — never break old envelopes.

The natural shape for KEK-rotation support is
`$ffsec$2$<kek_version>$<user_id>$<token>` plus a `master_key_versions` table
tracking which KEK ID is active; old envelopes get re-wrapped lazily on read.
