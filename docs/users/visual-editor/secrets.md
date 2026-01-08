# Secrets

Store sensitive credentials like database passwords and API keys securely.

## How It Works

Secrets are encrypted using a master key before being stored. When a flow needs a credential, Flowfile decrypts it on-demand. The actual values are never exposed in flow definitions or logs.

## Master Key

The master key encrypts all secrets. Without it, secrets cannot be decrypted.

### Location by Mode

| Mode | Master Key Location |
|------|---------------------|
| **Docker** | `/run/secrets/flowfile_master_key` (from `master_key.txt`) |
| **Desktop (Electron)** | `~/.config/flowfile/.secret_key` (auto-generated) |
| **Python API** | Uses desktop location or environment variable |

### Generating a Master Key

```bash
openssl rand -base64 32 > master_key.txt
```

!!! danger "Keep your master key safe"
    - Back it up securely (password manager, encrypted storage)
    - Losing it = losing access to all encrypted secrets
    - Never commit to version control

## Creating Secrets

1. Open **Settings** → **Secrets**
2. Click **Add Secret**
3. Enter a name (e.g., `prod_database_password`)
4. Enter the value
5. Save

## Using Secrets in Flows

When configuring connections (database, cloud storage, API), select the secret by name instead of typing the credential directly.

Secrets are referenced by name in flow definitions:
```json
{
  "password": {"secret": "prod_database_password"}
}
```

## Encryption Details

- **Algorithm**: Fernet (AES-128-CBC + HMAC-SHA256)
- **Isolation**: Each user's secrets are stored separately
- **Storage**: Encrypted values in SQLite database
