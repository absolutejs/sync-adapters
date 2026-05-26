# @absolutejs/sync-loro

A [Loro](https://loro.dev)-backed collaborative-text CRDT for
[`@absolutejs/sync`](https://github.com/absolutejs/sync), behind the same
`CrdtText` / `TextCrdtAdapter` contract as the core's zero-dependency `rgaText`
and `@absolutejs/sync-yjs`.

`@absolutejs/sync/crdt` ships a first-party RGA text CRDT that's great for
offline-merge and moderate collaboration. Loro is a fast, Rust/wasm CRDT library;
this adapter lets you swap it in without touching your call sites.

## Install

```bash
bun add @absolutejs/sync-loro loro-crdt
```

`@absolutejs/sync` is a peer dependency. `loro-crdt` is a runtime dependency you
install alongside.

## Use

```ts
import { loroText } from '@absolutejs/sync-loro';
// ...instead of: import { rgaText } from '@absolutejs/sync/crdt';

// Server — declare the CRDT field with this backend
engine.registerCrdt('doc', { state: loroText });

// Client — same hook, just pass the backend's factory
const doc = useCollaborativeText({
	collection: 'doc',
	field: 'state',
	id: 'shared',
	url,
	create: (replica) => createLoroText(replica)
});
```

The serialized state is a base64 string of a Loro snapshot — JSON-safe for the
sync engine's change feed. `merge` is commutative/associative/idempotent.

## The contract

Implements `TextCrdtAdapter<string>` from `@absolutejs/sync/crdt`: `create`,
`merge`, `empty`, `textOf`. The `replica` argument is accepted for contract
compatibility; Loro assigns a peer id internally.

## License

CC BY-NC 4.0
