/**
 * A [Loro](https://loro.dev)-backed collaborative-text CRDT for `@absolutejs/sync`,
 * behind the same `CrdtText` / `TextCrdtAdapter` contract as the first-party
 * `rgaText` and `@absolutejs/sync-yjs`. Loro is a fast, Rust/wasm CRDT library;
 * swapping `rgaText` for `loroText` needs no other change at the call site.
 *
 * The serialized state is a **base64 string** of a Loro snapshot, so it stays
 * JSON-safe for the sync engine's change feed and row storage. Merges are
 * commutative/associative/idempotent.
 */
import type { CrdtText, TextCrdtAdapter } from '@absolutejs/sync/crdt';
import { LoroDoc } from 'loro-crdt';

const TEXT_KEY = 'text';

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

const snapshot = (doc: LoroDoc): string =>
	toBase64(doc.export({ mode: 'snapshot' }));

const docFrom = (state?: string): LoroDoc => {
	const doc = new LoroDoc();
	if (state !== undefined && state.length > 0) {
		doc.import(fromBase64(state));
	}

	return doc;
};

// Reconcile `text` to `next` by editing only the changed middle.
const reconcile = (text: ReturnType<LoroDoc['getText']>, next: string) => {
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

/** Create a live Loro-backed collaborative-text doc for `replica`. */
export const createLoroText = (
	replica: string,
	initial?: string
): CrdtText<string> => {
	const doc = docFrom(initial);
	const text = doc.getText(TEXT_KEY);

	return {
		merge: (state) => {
			if (state.length > 0) {
				doc.import(fromBase64(state));
			}
		},
		setText: (next) => {
			reconcile(text, next);
			doc.commit();
		},
		state: () => snapshot(doc),
		text: () => text.toString()
	};
};

/**
 * The Loro collaborative-text backend as a {@link TextCrdtAdapter}. Drop-in for
 * the first-party `rgaText`. The `replica` argument is accepted for contract
 * compatibility; Loro assigns a peer id internally.
 */
export const loroText: TextCrdtAdapter<string> = {
	create: createLoroText,
	empty: () => snapshot(new LoroDoc()),
	merge: (a, b) => {
		const doc = docFrom(a);
		if (b.length > 0) {
			doc.import(fromBase64(b));
		}

		return snapshot(doc);
	},
	textOf: (state) => docFrom(state).getText(TEXT_KEY).toString()
};
