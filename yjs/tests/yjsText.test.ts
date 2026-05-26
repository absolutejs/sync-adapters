import { describe, expect, test } from 'bun:test';
import { createYjsText, yjsText } from '../src/index';

describe('Yjs collaborative text', () => {
	test('local insert and delete read back as plain text', () => {
		const doc = createYjsText('a');
		doc.setText('hello world');
		doc.setText('hello'); // delete " world" via the minimal diff
		expect(doc.text()).toBe('hello');
	});

	test('concurrent inserts from two replicas both survive and converge', () => {
		const a = createYjsText('a');
		a.setText('hello');
		const base = a.state();

		const b = createYjsText('b', base);
		// Both type at the same spot before syncing.
		a.setText('hello from A');
		b.setText('hello from B');

		// Exchange state in opposite orders.
		a.merge(b.state());
		b.merge(a.state());

		expect(a.text()).toBe(b.text());
		expect(a.text()).toContain('from A');
		expect(a.text()).toContain('from B');
	});

	test('a delete on one replica survives a merge from the other', () => {
		const a = createYjsText('a');
		a.setText('abcdef');
		const b = createYjsText('b', a.state());

		a.setText('def'); // a drops "abc"
		b.setText('abcdefghi'); // b appends "ghi"

		a.merge(b.state());
		b.merge(a.state());
		expect(a.text()).toBe(b.text());
		expect(a.text()).toBe('defghi');
	});
});

describe('Yjs delta-state (takeDelta)', () => {
	test('takeDelta carries the local edit and applies on a remote replica', () => {
		const a = createYjsText('a');
		a.setText('hello');
		const b = createYjsText('b', a.state());

		a.setText('hello world');
		const delta = a.takeDelta!();
		// Subsequent takes (no new edits) return an effectively empty update.
		expect(a.takeDelta!().length).toBeLessThan(delta.length);

		b.merge(delta);
		expect(b.text()).toBe('hello world');
	});

	test('takeDelta excludes ops that arrived via merge (no re-broadcast)', () => {
		const a = createYjsText('a');
		a.setText('seed');
		a.takeDelta!(); // flush the seed
		const b = createYjsText('b');
		b.setText('seed');

		a.merge(b.state());
		// Merge advanced the sync vector — a take right after returns nothing new.
		const after = a.takeDelta!();
		const empty = a.takeDelta!();
		expect(after.length).toBe(empty.length);
	});
});

describe('yjsText adapter', () => {
	test('empty/textOf round-trip', () => {
		expect(yjsText.textOf(yjsText.empty())).toBe('');
		const doc = yjsText.create('a');
		doc.setText('hi');
		expect(yjsText.textOf(doc.state())).toBe('hi');
	});

	test('server-side merge is commutative and idempotent', () => {
		const a = yjsText.create('a');
		a.setText('one ');
		const b = yjsText.create('b', a.state());
		b.setText('one two');

		const ab = yjsText.merge(a.state(), b.state());
		const ba = yjsText.merge(b.state(), a.state());
		expect(yjsText.textOf(ab)).toBe(yjsText.textOf(ba));
		// Re-merging a state already folded in changes nothing.
		expect(yjsText.textOf(yjsText.merge(ab, a.state()))).toBe(
			yjsText.textOf(ab)
		);
	});
});
