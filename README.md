# Athena-feed-mirror

RSS / Atom feed mirror runner that fetches **geo- or network-blocked**
sources from a GitHub Actions egress IP and writes JSON artefacts back
to this repo's `main` branch every 30 minutes. Companion repo to
[Athena](https://github.com/tsotsoyang/Athena); designed as the Tier 3
mechanism in
[Athena's six-tier feed-source strategy](https://github.com/tsotsoyang/Athena/blob/main/docs/feed-sources-strategy.md).

## Why this exists

Athena's Phison-network egress times out or `ConnectError`s on a handful
of public-internet sources (`ESM China`, `news.skhynix.com/feed/`,
Cloudflare-fronted `ai.meta.com/research`, `arxiv-sanity-lite.com`,
intermittent `openai.com/news/rss.xml`). All of these are reachable
from GitHub-Actions egress. Running the fetcher here and publishing the
results as static JSON files lets Athena pull them via
`https://raw.githubusercontent.com/tsotsoyang/Athena-feed-mirror/main/feed_mirror/{source}.json`
without any auth, with no infrastructure on Athena's side.

## How it works

```
                 GitHub Actions cron (every 30 min)
                              │
                              ▼
              configs/feed_mirror_targets.yaml
                              │
                              ▼
                 scripts/run_feed_mirror.py
                  ├── fetch each target via feedparser
                  ├── on 0 entries → autodiscover <link rel="alternate">
                  └── per-target try/except → errors[] in JSON
                              │
                              ▼
                feed_mirror/{source}.json  ← committed back to main
                              │
                              ▼
   Athena: fetch_mirrored_feed("esmchina")
   GET https://raw.githubusercontent.com/tsotsoyang/Athena-feed-mirror/main/feed_mirror/esmchina.json
```

## JSON contract

Every file under `feed_mirror/` follows this shape — Athena's
`fetch_mirrored_feed` primitive (in `hv-primitives`) consumes it
verbatim.

```json
{
  "source": "esmchina",
  "fetched_at": "2026-05-28T07:00:00Z",
  "next_refresh_at": "2026-05-28T07:30:00Z",
  "feed_url": "https://www.esmchina.com/rss",
  "entries": [
    {
      "title": "China NAND vendor X announces…",
      "url": "https://www.esmchina.com/…/article",
      "published_at": "2026-05-28T05:00:00Z",
      "summary": "First two paragraphs cleaned to plain text…",
      "author": "ESM China editorial"
    }
  ],
  "errors": []
}
```

`errors` is an array of `{stage, message}` objects when the fetch
partially failed (e.g. autodiscovery exhausted, network timeout). When
empty, the entries list is authoritative.

## Cadence + retention

- **Cron**: every 30 minutes (`*/30 * * * *` UTC). GH Actions latency
  is typically <60 s on top.
- **Retention**: each run overwrites the JSON in place. Git history
  preserves all prior versions for audit / replay. A periodic
  `git gc` may be wired in later if history grows excessive.

## Adding a target

Edit [`configs/feed_mirror_targets.yaml`](configs/feed_mirror_targets.yaml).
Each entry:

```yaml
- source: esmchina            # used as feed_mirror/{source}.json filename
  name: ESM China             # human label
  url: https://www.esmchina.com/rss
  fallback_urls:              # optional — tried in order if url returns 0 entries
    - https://www.esmchina.com/feed
  max_entries: 30             # cap per fetch
```

`source` should be lowercase, hyphen-allowed, filesystem-safe. Once
merged to `main`, the next cron run picks it up automatically.

## Local smoke run

```bash
pip install -r requirements.txt
python scripts/run_feed_mirror.py --once
```

`--once` short-circuits the cron path and runs every target sequentially
against the live network, writing files under `feed_mirror/`.

## Initial mirror targets

| Source | Why it needs mirroring |
|---|---|
| `meta-ai-research` | Cloudflare WAF / TLS fingerprint rejects Athena-egress (ConnectError 3/3 in Athena round-5 probe). |
| `arxiv-sanity-lite` | Geo / WAF rejects Athena-egress (ConnectError 3/3 in Athena round-5 probe). |
| `esmchina` | 12 s+ timeout from Athena-egress; reachable from GH Actions. |
| `skhynix-newsroom` | 12 s+ timeout from Athena-egress; kills the rss_blog batch upstream. |

## License

MIT — same as Athena. See [LICENSE](LICENSE).
