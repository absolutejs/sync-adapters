import { describe, expect, test } from 'bun:test';
import { automergeText, createAutomergeText } from '../src/index';

describe('Automerge collaborative text', () => {
	test('local insert and delete read back as plain text', () => {
		const doc = createAutomergeText('a');
		doc.setText('hello world');
		doc.setText('hello');
		expect(doc.text()).toBe('hello');
	});

	test('concurrent edits from two replicas converge', () => {
		// Share a base so both replicas have a common document root.
		const base = automergeText.empty();
		const a = createAutomergeText('a', base);
		a.setText('hello');
		const shared = a.state();

		const b = createAutomergeText('b', shared);
		a.setText('hello from A');
		b.setText('hello from B');
		a.merge(b.state());
		b.merge(a.state());

		expect(a.text()).toBe(b.text());
		expect(a.text()).toContain('from A');
		expect(a.text()).toContain('from B');
	});
});

describe('automergeText adapter', () => {
	test('empty/textOf round-trip', () => {
		expect(automergeText.textOf(automergeText.empty())).toBe('');
		const doc = createAutomergeText('a', automergeText.empty());
		doc.setText('hi');
		expect(automergeText.textOf(doc.state())).toBe('hi');
	});

	test('server-side merge is commutative and idempotent', () => {
		const base = automergeText.empty();
		const a = createAutomergeText('a', base);
		a.setText('one ');
		const b = createAutomergeText('b', a.state());
		b.setText('one two');

		const ab = automergeText.merge(a.state(), b.state());
		const ba = automergeText.merge(b.state(), a.state());
		expect(automergeText.textOf(ab)).toBe(automergeText.textOf(ba));
		expect(automergeText.textOf(automergeText.merge(ab, a.state()))).toBe(
			automergeText.textOf(ab)
		);
	});
});
