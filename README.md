# Athena-feed-mirror

RSS / Atom feed mirror runner that fetches **geo- or network-blocked**
sources from a GitHub Actions egress IP and writes JSON artefacts back
to this repo's `main` branch every 4 hours (KP-R11; it was every 30
minutes). Since KP-R11 each entry can also carry the **full article text**
(see [Full-text content](#full-text-content-kp-r11) - read the licensing
and access notes there before changing anything about this repo's
visibility). Companion repo to
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
with no infrastructure on Athena's side. **Once KP-R11 ships this repo is
private and Athena authenticates with a read-only token** - see
[Access](#access).

## How it works

```
                 GitHub Actions cron (every 4 h)
                              │
                              ▼
              configs/feed_mirror_targets.yaml
                              │
                              ▼
                 scripts/run_feed_mirror.py
                  phase 1 (always)
                  ├── fetch each target via feedparser
                  ├── on 0 entries → autodiscover <link rel="alternate">
                  ├── per-target try/except → errors[] in JSON
                  └── write every feed_mirror/{source}.json   <- feed update is safe here
                  phase 2 (best effort, <= 120 s, <= 60 articles)
                  └── fetch + extract article text for entries without it,
                      rewrite that source's JSON after each article
                              │
                              ▼
                feed_mirror/{source}.json  ← committed back to main
                              │
                              ▼
   Athena: fetch_mirrored_feed("esmchina")
   GET https://raw.githubusercontent.com/tsotsoyang/Athena-feed-mirror/main/feed_mirror/esmchina.json
       Authorization: token <read-only fine-grained token>
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
      "id": "https://www.esmchina.com/…/article",
      "title": "China NAND vendor X announces…",
      "url": "https://www.esmchina.com/…/article",
      "published_at": "2026-05-28T05:00:00Z",
      "summary": "First two paragraphs cleaned to plain text…",
      "content": "Full article text extracted with trafilatura (<= 32,000 chars)…",
      "author": "ESM China editorial"
    }
  ],
  "errors": []
}
```

`errors` is an array of `{stage, message}` objects when the fetch
partially failed (e.g. autodiscovery exhausted, network timeout). When
empty, the entries list is authoritative.

`id` and `content` are **optional, additive** fields (KP-R11); a consumer
that ignores them keeps working. Athena's `fetch_mirrored_feed` maps
`content` to `metadata.fulltext.text`, ignores a non-string value, and
truncates it to its own `FULLTEXT_TEXT_MAX_CHARS` (32,000).

## Full-text content (KP-R11)

- `id` is the feed's guid, else the entry URL. `content` is optional: the
  article body extracted with [trafilatura](https://github.com/adbar/trafilatura)
  (Apache-2.0 from 1.8 on - `requirements.txt` pins `>=1.8`; earlier
  releases were GPL), a stripped string of at most **32,000 characters**.
  The key is **absent** (never `null`/empty) when extraction failed, was not
  attempted yet, was skipped (see below), or was dropped to keep the file
  under 1.5 MB (the oldest entries lose their text first).
- **Best effort, after the feed.** Every `{source}.json` is written - atomically,
  with the feed update and any article text already known - *before* the first
  article is requested. A slow site, a hung extraction, an unexpected error or
  a killed step can cost article text; it cannot cost the feed update, and it
  never changes the runner's exit status.
- **Incremental.** Published text is reused from the previous JSON; only entry
  ids with no text yet are fetched. Per run: at most **60 attempts** and **120 s**
  in total (`--content-max-new`, `--content-budget-s`), sources interleaved
  round-robin so one site cannot spend the budget; per article: **15 s** total
  wall-clock, **2 MB** read, **2 s** minimum gap between two requests to one
  host, `robots.txt` honoured (fail-open when it cannot be read), only
  `http(s)` to public addresses (no loopback / private / link-local, redirects
  re-checked per hop).
- **Failures.** A transient failure (timeout, 5xx, 429, network, empty
  extraction) is retried on later runs, at most **3 attempts per entry**
  in total. A permanent one (401/402/403/404/410/451, other 4xx, non-HTML,
  `robots.txt` disallow, extracted text under 200 characters - a paywall
  teaser or stub) uses all attempts at once. Nothing is bypassed: paywalled
  items simply stay without `content`.
- **State.** `feed_mirror/_state/content_state.json` holds
  `{source: {entry_id: {status: ok|empty|dropped, attempts, reason?}}}`,
  pruned on every run to the entries currently in each feed (at most 500 per
  source). The JSON files, not this file, are the source of truth for text;
  deleting the state file only costs retries. Athena never reads it.
- **Per-target opt-out.** `fetch_content: false` on a target in
  `configs/feed_mirror_targets.yaml` skips article fetching for that source
  (use it for sources whose terms you do not want to test, or whose text is
  not useful). `python scripts/run_feed_mirror.py --no-content` skips it for
  everything and writes the pre-KP-R11 contract (no state file).

### Access

The repo is **private** (an owner step, done *before* this change is merged):
the files carry third-party article text. Athena sends a fine-grained,
read-only (`Contents: read`, this repo only) token as
`Authorization: token <t>`, stored in Athena Settings as `feed_mirror_token`.
Without it every raw file answers 404 and Athena reports the source as failed
by name. The token expires after 365 days - put the date in a calendar; never
write the token into this repo.

### Licensing and permitted use

The code in this repo is MIT-licensed ([LICENSE](LICENSE)). **The mirrored
article text is not**: it is third-party content that belongs to its
publishers, it is **not** covered by the MIT license, and it is stored here
only for private, internal research use by Athena. Do not redistribute it,
publish it, or make this repo public. See [NOTICE](NOTICE).

### Rollback, and what cannot be rolled back

- *Stop writing article text* (repo stays private): revert the content change
  (or run with `--no-content` in the workflow). The next run publishes JSON
  without `content`; also delete `feed_mirror/_state/`. Athena needs no
  change: no `content` simply means no full text.
- **Going public again is not a rollback.** Once article text has been
  committed, git history keeps it forever: removing the field, force-pushing,
  or squashing does not remove it from GitHub's side - merged PRs keep
  `refs/pull/<n>/head` and their diffs, which a repo owner cannot delete, and
  the bot's own commits on `main` accumulate the text run after run. The only
  clean way back to a public repo is **to delete the repository and recreate
  it from a tree that never contained article text** (or ask GitHub Support to
  purge cached views and refs). Treat "private" as permanent once this ships.

## Cadence + retention

- **Cron**: every 4 hours (`23 */4 * * *` UTC; minute 23 avoids the
  top-of-the-hour queue). A run takes ~8 minutes (feed phase ~5, article text
  <= 2, commit), so ~6 runs/day = ~1,440 Actions minutes/month on a private
  repo; the earlier 30-minute schedule was already throttled by GitHub to a few
  runs a day and Athena only reads the mirror once a day (20:00 Asia/Taipei).
  `next_refresh_at` in each JSON is written as `fetched_at + 4 h`.
- **Timeouts**: the "Run mirror" step is killed after 15 minutes, the job after
  20. Both ceilings sit far above the nominal ~8 minutes and the step ceiling is
  below the job ceiling, so a hang kills only the step and the commit step
  still publishes what was written (`continue-on-error`).
- **Retention**: each run overwrites the JSON in place; files are replaced
  atomically. Git history preserves all prior versions, including every
  article text ever committed (see Rollback above). Growth is bounded: a file is
  capped at 1.5 MB, text is fetched once per entry, and the state file is
  pruned. A periodic `git gc` may be wired in later if history grows excessive.

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
# feeds only, no article text:
python scripts/run_feed_mirror.py --once --no-content
# write somewhere disposable, small content budget:
python scripts/run_feed_mirror.py --output-dir /tmp/mirror --content-budget-s 60 --content-max-new 10
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

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q
```

No test touches the network for article text: the content tests stub
`_fetch_article` or run the real function against a local `127.0.0.1` server.

## License

The **code** is MIT - same as Athena. See [LICENSE](LICENSE). The mirrored
**article text** is third-party content and is **not** MIT-licensed; see
[Licensing and permitted use](#licensing-and-permitted-use) and [NOTICE](NOTICE).
