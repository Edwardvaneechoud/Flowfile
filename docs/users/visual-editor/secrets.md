# Secrets

Store sensitive credentials like database passwords and API keys securely.

## How It Works

Secrets are encrypted using a master key before storage. When a flow needs a credential, Flowfile decrypts it on-demand. The actual values never appear in flow definitions or logs.

## Master Key

The master key encrypts all secrets. Without it, secrets cannot be decrypted.

### Configuration by Mode

| Mode | Configuration |
|------|---------------|
| **Docker** | `FLOWFILE_MASTER_KEY` env variable, or generate via setup wizard |
| **Desktop** | Auto-generated at `~/.config/flowfile/.secret_key` |
| **Python API** | Uses desktop location or `FLOWFILE_MASTER_KEY` env variable |

### Generating a Master Key

**Via Setup Wizard (Docker):**

On first start without a master key, Flowfile shows a setup screen. Click **Generate Master Key**, copy it, and add to your `.env` file.

<!-- IMAGE: setup_wizard_key.png - Setup wizard with generated key displayed -->
![Setup Wizard](../../assets/images/guides/docker-deployment/setup_wizard.png)

**Manually:**

```bash
# Python
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Or openssl (for file-based approach)
openssl rand -base64 32
```

!!! danger "Protect your master key"
    - Back it up securely
    - Never commit to version control
    - Losing it = losing access to all encrypted secrets

## Creating Secrets

1. Open **Settings** → **Secrets**
2. Click **Add Secret**
3. Enter name (e.g., `prod_database_password`)
4. Enter value
5. Save

<!-- IMAGE: secrets_panel.png - Secrets management panel showing list of secrets -->
![Secrets Panel](../../assets/images/guides/secrets/secrets_panel.png)

## Using Secrets

Reference secrets by name when configuring connections. The encrypted value is decrypted at runtime.

## Encryption

- **Algorithm**: Fernet (AES-128-CBC + HMAC-SHA256)
- **Isolation**: Each user's secrets stored separately
- **Storage**: Encrypted in SQLite database
