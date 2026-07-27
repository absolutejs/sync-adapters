# @absolutejs/sync-yjs

A [Yjs](https://github.com/yjs/yjs)-backed collaborative-text CRDT for
[`@absolutejs/sync`](https://github.com/absolutejs/sync), behind the same
`CrdtText` / `TextCrdtAdapter` contract as the core's zero-dependency `rgaText`.

`@absolutejs/sync/crdt` ships a first-party RGA text CRDT that's great for
offline-merge and moderate collaboration. When you need production-scale
collaborative text — efficient deltas, no unbounded tombstone growth, the
interleaving guarantees the community relies on — Yjs is the staple. This adapter
lets you swap it in without touching your call sites.

## Install

```bash
bun add @absolutejs/sync-yjs yjs
```

`@absolutejs/sync` is a peer dependency (you already have it). `yjs` is a runtime
dependency you install alongside.

## Use

```ts
import { yjsText } from '@absolutejs/sync-yjs';
// ...instead of: import { rgaText } from '@absolutejs/sync/crdt';

// Client — a live doc per replica (e.g. per tab/device)
const doc = yjsText.create('tab-a', initialStateFromServer);
doc.setText(textarea.value); // reconciles via a minimal diff
socket.send(doc.state()); // broadcast the serialized (base64) state
doc.merge(incomingState); // fold a peer's state into local edits
doc.text(); // the merged visible text
const cursor = doc.anchorAt?.(textarea.selectionStart); // stable relative cursor
doc.indexOfAnchor?.(cursor ?? null); // follows concurrent inserts/deletes

// Server — merge-on-write so concurrent writes combine instead of clobbering
const next = yjsText.merge(storedState, incomingState);
// store `next`; yjsText.textOf(next) is the plain text
```

The serialized state is a base64 string of a Yjs update — JSON-safe for the sync
engine's change feed and for storing as a row field. `merge` is
commutative/associative/idempotent, so the order writes arrive in doesn't matter.

## The contract

This package implements `TextCrdtAdapter<string>` from `@absolutejs/sync/crdt`:

| Member                      | What it does                                                  |
| --------------------------- | ------------------------------------------------------------- |
| `create(replica, initial?)` | A live `CrdtText` doc: `text/setText/merge/state`.            |
| `merge(a, b)`               | Combine two persisted states (server-side, no live instance). |
| `empty()`                   | The serialized state of an empty document.                    |
| `textOf(state)`             | The plain text of a persisted state.                          |

## License

Apache License 2.0. See [LICENSE](./LICENSE).
