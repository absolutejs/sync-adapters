import { expect, test } from 'bun:test';
import { installCapacitorSyncLifecycle } from '../src/index';

test('refreshes and cleans up native resume/connectivity listeners', async () => {
	let resume: (() => void) | undefined;
	let network:
		| ((status: {
				connected: boolean;
				connectionType: 'wifi' | 'none';
		  }) => void)
		| undefined;
	let disconnected = 0;
	let removed = 0;
	const remove = await installCapacitorSyncLifecycle({
		client: { reconnect: () => (disconnected += 1) },
		lifecycle: {
			getState: async () => 'active',
			onChange: async () => () => undefined,
			onResume: async (listener) => {
				resume = listener;
				return () => {
					removed += 1;
				};
			}
		},
		network: {
			getStatus: async () => ({
				connected: true,
				connectionType: 'wifi'
			}),
			onChange: async (listener) => {
				network = listener;
				return () => {
					removed += 1;
				};
			}
		}
	});
	resume?.();
	network?.({ connected: false, connectionType: 'none' });
	network?.({ connected: true, connectionType: 'wifi' });
	expect(disconnected).toBe(2);
	await remove();
	await remove();
	expect(removed).toBe(2);
});
