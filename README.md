# @absolutejs/sync-adapters

CRDT backend adapters for [`@absolutejs/sync`](https://github.com/absolutejs/sync) — a
private workspace monorepo. Each adapter is published as its own package and implements
the `TextCrdtAdapter` contract from `@absolutejs/sync/crdt`; install only the one you want.

| Adapter      | Package                      | Backend                   | Status |
| ------------ | ---------------------------- | ------------------------- | ------ |
| `capacitor/` | `@absolutejs/sync-capacitor` | Native SQLite + lifecycle | ✅     |
| `yjs/`       | `@absolutejs/sync-yjs`       | Yjs                       | ✅     |
| `automerge/` | `@absolutejs/sync-automerge` | Automerge                 | ✅     |
| `loro/`      | `@absolutejs/sync-loro`      | Loro                      | ✅     |

## Why adapters

`@absolutejs/sync` ships a **zero-dependency, first-party** CRDT kit at
`@absolutejs/sync/crdt` (a PN-counter, an LWW register, and an RGA collaborative-text
type). That keeps the core package dependency-free and is enough for offline-merge and
moderate collaboration.

Production-scale collaborative text is a genuine footgun — unbounded tombstone growth,
full-state sync cost, and edit-interleaving subtleties — and **Yjs** is the community
staple that solves them. Rather than pull it into the core, the Yjs integration lives
here as a first-party-maintained adapter behind the same `CrdtText` / `TextCrdtAdapter`
contract: swap `rgaText` for `yjsText` and every call site stays the same.

## Why a monorepo

Adapters share the `TextCrdtAdapter` contract from `@absolutejs/sync`. Keeping them in one
workspace means a contract change can update every adapter in a single PR, with one CI and
one `bun install` — while each still publishes independently so consumers pull only the
backend (and its heavy deps) they actually use.

## Develop

```bash
bun install          # installs every workspace member
bun run typecheck    # across all adapters
bun run test         # across all adapters
bun run build        # across all adapters
```

## License

Apache License 2.0 — each published subpackage ships its own LICENSE.
