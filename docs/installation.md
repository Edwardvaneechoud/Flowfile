# Installation

Five ways to run Flowfile, from zero-install to a team server. The Python package is the recommended default — it brings the visual editor, the Python API, and all services in one install.

| Path | Best for | Requires |
|---|---|---|
| Python package | Most users — the full platform locally | Python 3.10–3.13 |
| Desktop app | A double-clickable app, no Python | macOS, Windows, or Linux |
| Docker | Teams: auth, sharing, a shared catalog | Docker |
| Browser (Lite) | Trying it with nothing installed | A browser tab |
| From source | Contributing to Flowfile | Poetry + Node |

=== ":material-language-python: pip (recommended)"

    ```bash
    pip install flowfile
    flowfile run ui
    ```

    Your browser opens the Flowfile designer. If it doesn't, go to [http://127.0.0.1:63578/ui#/main/designer](http://127.0.0.1:63578/ui#/main/designer) manually. Running `flowfile` with no arguments prints the installed version.

    Details and the Python-package specifics: [Python Package](users/deployment/python.md).

=== ":material-monitor: Desktop app"

    Signed installers, straight from the latest release:

    <div class="ff-install-cards">
    <a class="ff-install-card" id="ff-dl-mac-arm" href="https://github.com/edwardvaneechoud/Flowfile/releases/latest">
    <span class="ff-detected-badge">DETECTED</span>
    <svg viewBox="0 0 24 24" fill="currentColor"><path d="M17.05 12.54c-.03-2.5 2.04-3.7 2.13-3.76-1.16-1.7-2.97-1.93-3.61-1.96-1.54-.16-3 .9-3.78.9-.78 0-1.98-.88-3.26-.85-1.68.02-3.22.97-4.08 2.47-1.74 3.02-.44 7.49 1.25 9.94.83 1.2 1.82 2.55 3.12 2.5 1.25-.05 1.72-.8 3.23-.8 1.51 0 1.93.8 3.25.78 1.34-.02 2.19-1.22 3.01-2.43.95-1.39 1.34-2.74 1.36-2.81-.03-.01-2.6-1-2.62-3.98zM14.56 4.6c.69-.83 1.15-1.99 1.02-3.14-.99.04-2.19.66-2.9 1.49-.64.74-1.2 1.92-1.05 3.05 1.1.09 2.24-.56 2.93-1.4z"/></svg>
    <strong>macOS</strong>
    <small>Apple Silicon · .dmg</small>
    </a>
    <a class="ff-install-card" id="ff-dl-mac-intel" href="https://github.com/edwardvaneechoud/Flowfile/releases/latest">
    <span class="ff-detected-badge">DETECTED</span>
    <svg viewBox="0 0 24 24" fill="currentColor"><path d="M17.05 12.54c-.03-2.5 2.04-3.7 2.13-3.76-1.16-1.7-2.97-1.93-3.61-1.96-1.54-.16-3 .9-3.78.9-.78 0-1.98-.88-3.26-.85-1.68.02-3.22.97-4.08 2.47-1.74 3.02-.44 7.49 1.25 9.94.83 1.2 1.82 2.55 3.12 2.5 1.25-.05 1.72-.8 3.23-.8 1.51 0 1.93.8 3.25.78 1.34-.02 2.19-1.22 3.01-2.43.95-1.39 1.34-2.74 1.36-2.81-.03-.01-2.6-1-2.62-3.98zM14.56 4.6c.69-.83 1.15-1.99 1.02-3.14-.99.04-2.19.66-2.9 1.49-.64.74-1.2 1.92-1.05 3.05 1.1.09 2.24-.56 2.93-1.4z"/></svg>
    <strong>macOS</strong>
    <small>Intel · .dmg</small>
    </a>
    <a class="ff-install-card" id="ff-dl-win" href="https://github.com/edwardvaneechoud/Flowfile/releases/latest">
    <span class="ff-detected-badge">DETECTED</span>
    <svg viewBox="0 0 24 24" fill="currentColor"><path d="M3 5.5 10.5 4.4v7.1H3V5.5zm0 13 7.5 1.1v-7H3v5.9zM11.5 4.25 21 3v8.5h-9.5v-7.25zm0 15.5L21 21v-8.5h-9.5v7.25z"/></svg>
    <strong>Windows</strong>
    <small>x64 · .exe</small>
    </a>
    <a class="ff-install-card" id="ff-dl-linux" href="https://github.com/edwardvaneechoud/Flowfile/releases/latest">
    <span class="ff-detected-badge">DETECTED</span>
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3c-2.2 0-3.5 1.6-3.5 3.8 0 1.5-.4 2.6-1.2 3.9-1 1.6-2 3.2-2 5 0 2 1.4 3.3 3.2 3.3h7c1.8 0 3.2-1.3 3.2-3.3 0-1.8-1-3.4-2-5-.8-1.3-1.2-2.4-1.2-3.9C15.5 4.6 14.2 3 12 3z"/><path d="M9.5 19c-1 1-2.6 1.2-3.5.5m11.5-.5c1 1 2.6 1.2 3.5.5" stroke-width="1.4"/><circle cx="10.6" cy="8" r="0.3" fill="currentColor"/><circle cx="13.4" cy="8" r="0.3" fill="currentColor"/><path d="M11 9.6c.5.5 1.5.5 2 0"/></svg>
    <strong>Linux</strong>
    <small>Debian/Ubuntu · .deb</small>
    </a>
    </div>

    <p class="ff-install-version"><span id="ff-install-version">Latest release</span> · <a href="https://github.com/edwardvaneechoud/Flowfile/releases/latest">all platforms, checksums &amp; signatures</a></p>

    Per-OS steps: [Desktop App](users/deployment/desktop.md).

=== ":material-docker: Docker (teams)"

    **Deploying on a server?** The [hosting kit](https://github.com/Edwardvaneechoud/flowfile-hosting) runs the published images (version-pinned) behind HTTPS — Caddy with Let's Encrypt, a Cloudflare Tunnel, or plain LAN — with a guided installer that generates secrets and checks your DNS:

    ```bash
    git clone https://github.com/Edwardvaneechoud/flowfile-hosting.git && cd flowfile-hosting
    ./install.sh
    ```

    **Evaluating locally?** The main repo's compose builds from source:

    ```bash
    git clone https://github.com/edwardvaneechoud/Flowfile.git && cd Flowfile
    docker compose up -d      # frontend at http://localhost:8080
    ```

    Security checklist and operations: [Docker deployment](users/deployment/docker.md).

=== ":material-web: Browser, no install"

    [Flowfile Lite](users/deployment/lite.md) runs the visual editor entirely in your browser at [demo.flowfile.org](https://demo.flowfile.org) — a subset of the full product (no backend services, databases, scheduler, or AI), good for a first look and small files.

=== ":material-source-branch: From source"

    Clone the repo and see [For Developers](for-developers/index.md) for the Poetry + npm setup.

---

**Installed?** The [Quickstart](quickstart.md) builds your first real pipeline in about ten minutes. Choosing between these shapes for a team is the [deployment overview](users/deployment/index.md)'s job.

<script>
(function () {
  // Direct-download cards: resolve the latest release's assets. Cards fall back
  // to the releases/latest page when the API or a pattern match fails.
  var cards = {
    "ff-dl-mac-arm":  function (n) { return /(aarch64|arm64).*\.dmg$/i.test(n); },
    "ff-dl-mac-intel":function (n) { return /\.dmg$/i.test(n) && !/(aarch64|arm64)/i.test(n); },
    "ff-dl-win":      function (n) { return /\.(exe|msi)$/i.test(n); },
    "ff-dl-linux":    function (n) { return /\.deb$/i.test(n); }
  };
  fetch("https://api.github.com/repos/edwardvaneechoud/Flowfile/releases/latest")
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (rel) {
      if (!rel || !rel.assets) return;
      var v = document.getElementById("ff-install-version");
      if (v && rel.tag_name) v.textContent = "Version " + rel.tag_name.replace(/^v/, "");
      Object.keys(cards).forEach(function (id) {
        var el = document.getElementById(id);
        if (!el) return;
        var asset = rel.assets.find(function (a) { return cards[id](a.name); });
        if (asset) el.href = asset.browser_download_url;
      });
    })
    .catch(function () { /* keep releases/latest fallbacks */ });

  var ua = navigator.userAgent;
  var detected = /Mac/i.test(ua) ? "ff-dl-mac-arm"
    : /Win/i.test(ua) ? "ff-dl-win"
    : (/Linux/i.test(ua) && !/Android/i.test(ua)) ? "ff-dl-linux" : null;
  if (detected) {
    var el = document.getElementById(detected);
    if (el) el.classList.add("ff-detected");
  }
})();
</script>
