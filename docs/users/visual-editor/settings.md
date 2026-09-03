# Settings

Configure Flowfile's appearance and behavior.

## The Settings menu

The **Settings** gear icon in the left sidebar opens the Settings menu, grouped by purpose.

| Group | Items | Notes |
|-------|-------|-------|
| **Connections** | **All connections**: Overview, Database, Cloud Storage, Kafka, Google Analytics, Secrets, AI Providers | Opens the [Connections](connections.md) page on **Overview**; the other names are its in-page tabs. [Secrets](catalog/secrets.md) covers encrypted credential storage. |
| **Execution** | Python Kernels; Performance | Python Kernels manages the containers described in [Kernel Execution](kernels.md). **Performance** is admin-only in Docker. |
| **Preferences** | Privacy; Backups | Privacy holds the telemetry consent described in [Telemetry & Privacy](../telemetry.md). **Backups** is admin-only in Docker — see [Database backups](../deployment/backups.md). |
| **Extensions** | Node Designer, Custom Nodes, Community Nodes | [Node Designer](node-designer.md) authors and publishes nodes; [Community Nodes](community-nodes.md) browses and installs shared ones. |
| **Workspace** | Project; File Manager, User Groups, User Management | Project is [project tracking](../projects.md). File Manager, User Groups and User Management are Docker only; User Management is also admin-only. [Sharing](../deployment/sharing.md) covers groups and grants. |

## Theme

Open the **Help & more** menu (the **?** button at the bottom of the sidebar) and click **Dark mode** / **Light mode** to switch between:

| Mode | Description |
|------|-------------|
| **Light** | White backgrounds |
| **Dark** | Reduced brightness for low-light |

Until you choose, Flowfile follows the operating-system theme. Your preference persists across sessions.

## User Management

*Available in Docker mode only.*

Manage team access from the User Management page. In the sidebar, click **Settings** (gear icon), then **Workspace → User Management**.

![Admin panel showing the user list](../../assets/images/guides/settings/user_management.png)

### Creating Users

1. Click **Add User**
2. Enter username, email, full name
3. Set temporary password
4. Optionally grant admin privileges
5. Click **Create**

New users must change their password on first login.

### Password Requirements

- Minimum 8 characters
- At least one number
- At least one special character (`!@#$%^&*()_+-=[]{}|;:,.<>?`)

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

## Usage telemetry

Flowfile can send anonymous usage events — opt-in, off by default, and never enabled without your explicit consent. Manage your answer any time under **Settings → Preferences → Privacy**; the one-time consent dialog never asks again after you decline. [Telemetry & Privacy](../telemetry.md) documents every event and field that can be sent.

## Related

- [Secrets](catalog/secrets.md) - Encrypted credential storage
- [Docker](../deployment/docker.md) - Docker deployment
- [Telemetry & Privacy](../telemetry.md) - What anonymous usage telemetry sends, and how to turn it off
