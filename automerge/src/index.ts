/**
 * An Automerge-backed collaborative-text CRDT for `@absolutejs/sync`, behind the
 * same `CrdtText` / `TextCrdtAdapter` contract as the first-party `rgaText` and
 * `@absolutejs/sync-yjs`. Automerge is a mature, battle-tested CRDT library;
 * swapping `rgaText` for `automergeText` needs no other change at the call site.
 *
 * The serialized state is a **base64 string** of an Automerge document, so it
 * stays JSON-safe for the sync engine's change feed and row storage. Merges are
 * commutative/associative/idempotent, so a merge-on-write mutation combines
 * concurrent edits without clobbering.
 */
import type { CrdtText, TextCrdtAdapter } from '@absolutejs/sync/crdt';
import * as Automerge from '@automerge/automerge';

type Doc = { text: string };

// Isomorphic base64 (browser + Bun expose btoa/atob), so the adapter runs
// unchanged on the client and the server.
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

const emptyDoc = (): Automerge.Doc<Doc> => Automerge.from<Doc>({ text: '' });
const encode = (doc: Automerge.Doc<Doc>): string =>
	toBase64(Automerge.save(doc));
const decode = (state: string): Automerge.Doc<Doc> =>
	Automerge.load<Doc>(fromBase64(state));
const load = (state: string): Automerge.Doc<Doc> =>
	state.length > 0 ? decode(state) : emptyDoc();

// Reconcile `text` to `next` by editing only the changed middle (keep the common
// prefix/suffix), so two replicas editing different spots merge.
const reconcile = (
	doc: Automerge.Doc<Doc>,
	next: string
): Automerge.Doc<Doc> => {
	const current = doc.text;
	if (current === next) {
		return doc;
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

	return Automerge.change(doc, (draft) => {
		Automerge.splice(draft, ['text'], prefix, removed, inserted);
	});
};

/** Create a live Automerge-backed collaborative-text doc for `replica`. */
export const createAutomergeText = (
	replica: string,
	initial?: string
): CrdtText<string> => {
	let doc = initial !== undefined ? load(initial) : emptyDoc();

	return {
		merge: (state) => {
			if (state.length > 0) {
				doc = Automerge.merge(doc, decode(state));
			}
		},
		setText: (next) => {
			doc = reconcile(doc, next);
		},
		state: () => encode(doc),
		text: () => doc.text
	};
};

/**
 * The Automerge collaborative-text backend as a {@link TextCrdtAdapter}. Drop-in
 * for the first-party `rgaText`. The `replica` argument is accepted for contract
 * compatibility; Automerge manages actor identity internally.
 */
export const automergeText: TextCrdtAdapter<string> = {
	create: createAutomergeText,
	empty: () => encode(emptyDoc()),
	merge: (a, b) => {
		const merged =
			b.length > 0 ? Automerge.merge(load(a), decode(b)) : load(a);

		return encode(merged);
	},
	textOf: (state) => load(state).text
};
