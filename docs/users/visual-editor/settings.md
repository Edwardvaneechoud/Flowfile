# Settings

Configure Flowfile's appearance and behavior.

## Theme

Click the theme icon in the top navigation bar to switch between:

| Mode | Description |
|------|-------------|
| **Light** | White backgrounds |
| **Dark** | Reduced brightness for low-light |
| **System** | Follows OS preference |

Your preference persists across sessions.

## User Management

*Available in Docker mode only.*

Manage team access through the Admin panel. Click your username → **Admin**.

### Creating Users

1. Click **Add User**
2. Enter username, email, full name
3. Set temporary password
4. Optionally grant admin privileges
5. Click **Create**

New users must change their password on first login.

### Password Requirements

- Minimum 8 characters
- At least one uppercase letter
- At least one lowercase letter
- At least one number

### Admin Privileges

Admins can:

- Create, modify, and delete users
- View all users in the system
- Grant/revoke admin status

### Deleting Users

Deleting a user removes their:

- Account and login access
- Stored secrets
- Saved connections

Flow definitions are preserved.

## Related

- [Secrets](secrets.md) - Encrypted credential storage
- [Docker Reference](tutorials/docker-deployment.md) - Docker configuration
