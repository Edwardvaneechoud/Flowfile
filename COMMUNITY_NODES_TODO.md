# Community Nodes — launch TODO (monorepo side)

The community-node feature lives in `flowfile_core/flowfile_core/flowfile/community_nodes/`
(+ `routes/community_nodes.py`, `routes/community_github.py`). The registry repo is
`github.com/Edwardvaneechoud/flowfile-community-nodes` and is already live for testing —
its CI currently installs the validator from **this feature branch**, not PyPI.

## Remaining before real launch

- [ ] **Cut a PyPI release** that includes `flowfile_core.flowfile.community_nodes`
      (the next version after 0.13.0 — e.g. 0.13.1). This is the single gate that lets
      the registry's CI install the validator from PyPI instead of the feature branch.
- [ ] **After the release, in the registry repo** (see its `LAUNCH_TODO.md`):
      pin `registry/config.json` → `validator_flowfile_version` / `min_supported_app_version`
      to the released version, and revert the two `TEST_FLOWFILE_SPEC` git-installs back
      to the `flowfile==<pinned>` PyPI install.

## Already done (reference)

- [x] `COMMUNITY_GITHUB_CLIENT_ID_DEFAULT` in `flowfile_core/configs/settings.py` set to
      the registered OAuth App client id (device-flow publishing ships enabled).
- [x] Registry repo pushed + configured (branch protection/ruleset, Discussions
      "Node Ratings", `REGISTRY_PUSH_TOKEN` PAT so the index/popularity bots can push).
- [x] Full publish → PR → CI (validate + dry-run, incl. kernel-env nodes) → merge →
      index → install loop verified end to end.

## Post-launch roadmap (from the plan)

Yank-at-run, capability-delta re-consent on updates, Pillow media re-encode gate,
auto-merge for trusted version-bump PRs.
