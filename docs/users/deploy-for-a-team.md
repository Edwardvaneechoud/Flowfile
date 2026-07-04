# Run Flowfile for a team

Someone has to turn "this tool is great on my laptop" into something a team logs into — maybe that's you as the IT admin, the platform engineer who owns the data stack, or the team lead who volunteered and now needs it to not become a second job. The responsibilities are the same either way: authentication, secrets that stay secret, who-sees-what, backups, and a service that survives upgrades. This is the operator's route through all of it.

The mental model to hold: multi-user Flowfile is **three services and two kinds of state**. A core API, a compute worker, and a frontend run as containers; everything worth backing up lives in two places — internal storage (catalog database, table data, logs) and user data (flows, uploads, outputs). Every operational question below reduces to one of those five pieces.

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: mental-model architecture diagram — frontend :8080, core, worker as containers; two labeled storage volumes underneath (internal storage: catalog DB + Delta tables + logs; user data: flows + uploads); optional kernel containers to the side with the Docker socket line -->

## 1. Pick the shape

Solo users don't need any of this — the [desktop app](deployment/desktop.md) or the [pip install](deployment/python.md) runs everything locally with nothing to operate. The [Docker deployment](deployment/docker.md) is the team shape: real authentication (JWT), per-user encrypted secrets, a shared catalog with access control, and group-based sharing. The [deployment overview](deployment/index.md) compares all four editions if you're still choosing.

## 2. First boot, secured

Getting a stack up is deliberately boring:

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile
docker compose up -d          # frontend on :8080
```

The first-run wizard walks you through the master key. What's *not* optional is understanding what that key is: it encrypts every credential your users will store — database passwords, cloud keys, API tokens. Lose it and those secrets are cryptographically gone; there is no recovery path. Generate it, put it in `.env`, and back it up like the key it is:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

The shipped compose also boots with development fallbacks for `JWT_SECRET_KEY` and `FLOWFILE_INTERNAL_TOKEN` so a first `up` doesn't fail — convenient for evaluation, unacceptable for production. The [production checklist](deployment/docker.md) walks every value that must be replaced before real users log in.

For an actual server, skip the hand-assembly: the [flowfile-hosting kit](https://github.com/Edwardvaneechoud/flowfile-hosting) runs the published images version-pinned behind HTTPS (Caddy, Cloudflare Tunnel, or LAN), and its installer generates the secrets above instead of leaving fallbacks in place — plus `make update` / `backup` / `restore` / `health` for day two.

## 3. Understand who sees what

The authorization model is worth ten minutes before the first complaint of "I can't see my colleague's table" — because that behavior is the design, not a bug. Multi-user Flowfile is **private by default**: users see what they own plus what's been shared with a group they belong to. Sharing is explicit and layered:

- A global admin creates [user groups](deployment/sharing.md); group owners manage their own membership from there.
- Resource owners grant a group **use** (run it, read it) or **manage** (edit and re-share) per resource.
- A grant on a catalog namespace cascades to everything inside it — the practical way to open a team area.
- Shared secrets are use-only by design: flows run with them, nobody reads them. And a manage-grantee who repoints a shared connection at a new host must re-enter its credentials, so a shared credential can't be quietly harvested.

If your team wants "everyone sees everything," you create that deliberately: one group, everyone in it, a few namespace grants.

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: mental-model diagram of the sharing model — users belonging to groups; a group receiving a use-grant on one connection and a manage-grant on a namespace; the namespace grant cascading down to the tables and flows inside it; one secret marked use-only with a crossed-out eye -->


## 4. Know where the data lives

Back up two things and you can rebuild anything: the `flowfile-internal-storage` volume (catalog database, Delta table data, logs) and the user-data volume (flows, uploads, outputs). The [Docker reference](deployment/docker.md) maps every volume and environment variable; catalog table data can optionally land in S3 instead of the local volume when the host shouldn't hold data.

## 5. Day two

- **Scheduling** — the shipped compose enables the scheduler, so users' [schedules](visual-editor/catalog/schedules.md) fire without any action from you.
- **Scripted operations** — everything the UI does headlessly goes through the [CLI](deployment/cli.md): flows in cron or CI, demo catalog seeding for onboarding sessions.
- **Python-script nodes** — [kernels](visual-editor/kernels.md) run user Python in isolated Docker containers. Core needs the Docker socket for this; kernel images are versioned separately from the app, and no kernels means the rest of the product still works — it's an opt-in capability, not a dependency.
- **Workspace as code** — [Projects](projects.md) mirror flows, connections (credential-free), and catalog metadata into a git folder. In Docker mode it's admin-only and enabled with `FLOWFILE_ENABLE_PROJECTS` — worth turning on just as a disaster-recovery layer, since a project folder plus the master key rebuilds a workspace.

## 6. Keep it current

Application images track the project version; kernel images version independently and only refresh when their tag changes. The upgrade rhythm is unexciting by design:

```bash
docker compose pull && docker compose up -d
```

Flows and data live in the volumes, so upgrades don't touch them. Tags and kernel-image specifics live in the [Docker reference](deployment/docker.md).

---

**Fastest first taste:** clone the repo, `docker compose up -d`, open `http://localhost:8080`, and let the wizard walk you through the master key.
