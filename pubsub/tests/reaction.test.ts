import { expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import { createPubSubReaction, type PubSubReactionOptions } from '../src';
const setup = (
	process: PubSubReactionOptions['process'],
	onError?: PubSubReactionOptions['onError']
) => {
	const emitter = new EventEmitter();
	let closed = 0,
		acked = 0,
		nacked = 0;
	const subscription = Object.assign(emitter, {
		close: async () => {
			closed++;
		}
	});
	const source = createPubSubReaction({
		subscription:
			subscription as unknown as PubSubReactionOptions['subscription'],
		process,
		onError
	});
	const message = {
		id: 'm1',
		data: Buffer.from('event'),
		attributes: { source: 'test' },
		publishTime: new Date(),
		ack: () => {
			acked++;
		},
		nack: () => {
			nacked++;
		}
	};
	return {
		source,
		emitter,
		message,
		counts: () => ({ closed, acked, nacked })
	};
};
test('acks only after durable processing; stop drains work before closing', async () => {
	let commit!: () => void;
	const fixture = setup(
		() =>
			new Promise<void>((resolve) => {
				commit = resolve;
			})
	);
	await fixture.source.start(() => {});
	fixture.emitter.emit('message', fixture.message);
	expect(fixture.counts()).toEqual({ closed: 0, acked: 0, nacked: 0 });
	const stopped = fixture.source.stop();
	expect(fixture.counts().closed).toBe(0);
	commit();
	await stopped;
	expect(fixture.counts()).toEqual({ closed: 1, acked: 1, nacked: 0 });
	expect(fixture.emitter.listenerCount('message')).toBe(0);
	expect(fixture.emitter.listenerCount('error')).toBe(0);
	await fixture.source.stop();
	expect(fixture.counts().closed).toBe(1);
	expect(() => fixture.source.start(() => {})).toThrow('has stopped');
});
test('failed processing nacks even if diagnostics throw; never acks', async () => {
	const fixture = setup(
		async () => {
			throw new Error('transaction rolled back');
		},
		() => {
			throw new Error('diagnostic failure');
		}
	);
	await fixture.source.start(() => {});
	await fixture.source.start(() => {});
	expect(fixture.emitter.listenerCount('message')).toBe(1);
	fixture.emitter.emit('message', fixture.message);
	await fixture.source.stop();
	expect(fixture.counts()).toEqual({ closed: 1, acked: 0, nacked: 1 });
});
test('passes only event data and leaves caller-owned subscription open', async () => {
	let received: unknown;
	const fixture = setup(async (message) => {
		received = message;
	});
	const reaction = createPubSubReaction({
		subscription:
			fixture.emitter as unknown as PubSubReactionOptions['subscription'],
		closeOnStop: false,
		process: async (message) => {
			received = message;
		}
	});
	await reaction.start(() => {});
	fixture.emitter.emit('message', fixture.message);
	await reaction.stop();
	expect(Object.keys(received as object).sort()).toEqual([
		'attributes',
		'data',
		'id',
		'publishTime'
	]);
	expect(fixture.counts()).toEqual({ closed: 0, acked: 1, nacked: 0 });
});
test('reports SDK stream errors', async () => {
	const errors: unknown[] = [];
	const fixture = setup(
		async () => {},
		(error) => errors.push(error)
	);
	await fixture.source.start(() => {});
	const error = new Error('authorization denied');
	fixture.emitter.emit('error', error);
	expect(errors).toEqual([error]);
	await fixture.source.stop();
});
