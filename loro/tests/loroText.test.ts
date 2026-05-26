import { describe, expect, test } from 'bun:test';
import { createLoroText, loroText } from '../src/index';

describe('Loro collaborative text', () => {
	test('local insert and delete read back as plain text', () => {
		const doc = createLoroText('a');
		doc.setText('hello world');
		doc.setText('hello');
		expect(doc.text()).toBe('hello');
	});

	test('concurrent edits from two replicas converge', () => {
		const a = createLoroText('a');
		a.setText('hello');
		const b = createLoroText('b', a.state());
		a.setText('hello from A');
		b.setText('hello from B');
		a.merge(b.state());
		b.merge(a.state());

		expect(a.text()).toBe(b.text());
		expect(a.text()).toContain('from A');
		expect(a.text()).toContain('from B');
	});
});

describe('loroText adapter', () => {
	test('empty/textOf round-trip', () => {
		expect(loroText.textOf(loroText.empty())).toBe('');
		const doc = createLoroText('a');
		doc.setText('hi');
		expect(loroText.textOf(doc.state())).toBe('hi');
	});

	test('server-side merge is commutative and idempotent', () => {
		const a = createLoroText('a');
		a.setText('one ');
		const b = createLoroText('b', a.state());
		b.setText('one two');

		const ab = loroText.merge(a.state(), b.state());
		const ba = loroText.merge(b.state(), a.state());
		expect(loroText.textOf(ab)).toBe(loroText.textOf(ba));
		expect(loroText.textOf(loroText.merge(ab, a.state()))).toBe(
			loroText.textOf(ab)
		);
	});
});
