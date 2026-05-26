/**
 * A Yjs-backed collaborative-text CRDT for `@absolutejs/sync`.
 *
 * `@absolutejs/sync` ships a zero-dependency RGA text CRDT (`rgaText`) that is
 * fine for offline-merge and moderate collaboration. Yjs is the community staple
 * for production-scale collaborative text — it solves the real footguns (tombstone
 * growth, efficient deltas, interleaving). This adapter wraps Yjs behind the exact
 * same `CrdtText` / `TextCrdtAdapter` contract, so swapping `rgaText` for `yjsText`
 * needs no other change at the call site.
 *
 * The serialized state is a **base64 string** of a Yjs update, so it stays
 * JSON-safe for transport over the sync engine's change feed and storage as a row
 * field. Merges are commutative/associative/idempotent (Yjs guarantees it), so a
 * merge-on-write mutation combines concurrent edits without clobbering.
 */
import type { CrdtText, TextCrdtAdapter } from '@absolutejs/sync/crdt';
import * as Y from 'yjs';

const TEXT_KEY = 'text';

// Isomorphic base64 (browser + Bun both expose btoa/atob), so the adapter runs
// unchanged on the client (live editing) and the server (merge-on-write).
const toBase64 = (bytes: Uint8Array): string => {
	let binary = '';
	for (const byte of bytes) {
		binary += String.fromCharCode(byte);
	}

	return btoa(binary);
};

const fromBase64 = (base64: string): Uint8Array => {
	const binary = atob(base64);
	const bytes = new Uint8Array(binary.length);
	for (let index = 0; index < binary.length; index += 1) {
		bytes[index] = binary.charCodeAt(index);
	}

	return bytes;
};

const encode = (doc: Y.Doc): string => toBase64(Y.encodeStateAsUpdate(doc));

const apply = (doc: Y.Doc, state: string) => {
	if (state.length > 0) {
		Y.applyUpdate(doc, fromBase64(state));
	}
};

// A stable, deterministic Yjs clientID per replica id (Yjs needs distinct
// clients to keep distinct identities; deriving it from `replica` also makes
// merges reproducible instead of depending on a random per-instance id).
const clientIdFor = (replica: string): number => {
	let hash = 0;
	for (let index = 0; index < replica.length; index += 1) {
		hash = (Math.imul(hash, 31) + replica.charCodeAt(index)) | 0;
	}

	return hash >>> 0;
};

const docFrom = (replica: string, initial?: string): Y.Doc => {
	const doc = new Y.Doc();
	doc.clientID = clientIdFor(replica);
	if (initial !== undefined) {
		apply(doc, initial);
	}

	return doc;
};

/** Reconcile `text` to `next` by editing only the changed middle (keep the
 * common prefix/suffix), so two replicas editing different spots merge. */
const reconcile = (text: Y.Text, next: string) => {
	const current = text.toString();
	if (current === next) {
		return;
	}
	let prefix = 0;
	const maxPrefix = Math.min(current.length, next.length);
	while (prefix < maxPrefix && current[prefix] === next[prefix]) {
		prefix += 1;
	}
	let suffix = 0;
	while (
		suffix < maxPrefix - prefix &&
		current[current.length - 1 - suffix] === next[next.length - 1 - suffix]
	) {
		suffix += 1;
	}
	const removed = current.length - prefix - suffix;
	const inserted = next.slice(prefix, next.length - suffix);
	if (removed > 0) {
		text.delete(prefix, removed);
	}
	if (inserted.length > 0) {
		text.insert(prefix, inserted);
	}
};

/** Create a live Yjs-backed collaborative-text doc for `replica`. */
export const createYjsText = (
	replica: string,
	initial?: string
): CrdtText<string> => {
	const doc = docFrom(replica, initial);
	const text = doc.getText(TEXT_KEY);
	// State vector at the last sync point (a take or a merge). takeDelta encodes
	// every update since this vector and then advances it; merge advances it too,
	// so a remote op merged in isn't re-broadcast on the next take.
	let lastVector = Y.encodeStateVector(doc);

	return {
		merge: (state) => {
			apply(doc, state);
			lastVector = Y.encodeStateVector(doc);
		},
		setText: (next) => doc.transact(() => reconcile(text, next)),
		state: () => encode(doc),
		takeDelta: () => {
			const update = Y.encodeStateAsUpdate(doc, lastVector);
			lastVector = Y.encodeStateVector(doc);

			return toBase64(update);
		},
		text: () => text.toString()
	};
};

/**
 * The Yjs collaborative-text backend as a {@link TextCrdtAdapter}. Drop-in for
 * the first-party `rgaText`: `create` mints a live doc, `merge` combines two
 * persisted states server-side, `empty`/`textOf` are conveniences.
 */
export const yjsText: TextCrdtAdapter<string> = {
	create: createYjsText,
	empty: () => encode(new Y.Doc()),
	merge: (a, b) => {
		const doc = new Y.Doc();
		apply(doc, a);
		apply(doc, b);

		return encode(doc);
	},
	textOf: (state) => {
		const doc = new Y.Doc();
		apply(doc, state);

		return doc.getText(TEXT_KEY).toString();
	}
};
