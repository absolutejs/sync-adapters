import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';

/* Adapter package: everything rides the `sync/crdt-adapter` implementation.
 * `yjsText` is a ready-made TextCrdtAdapter constant — no options, no env. */
export const manifest = defineManifest<Record<never, never>>()({
	contract: 2,
	identity: {
		accent: '#8acb88',
		category: 'sync',
		description:
			'Yjs-backed `TextCrdtAdapter` for `@absolutejs/sync` — the industry-staple CRDT engine behind the same collaborative-text call sites as the built-in `rgaText`. State is stored as a base64 Yjs update, so documents written by other Yjs tooling interoperate.',
		docsUrl: 'https://github.com/absolutejs/sync-adapters/tree/main/yjs',
		name: '@absolutejs/sync-yjs',
		tagline: 'Collaborative text editing powered by Yjs.'
	},
	implements: [
		defineImplementation<never>()({
			contract: 'sync/crdt-adapter',
			factory: 'yjsText',
			from: '@absolutejs/sync-yjs',
			title: 'Yjs',
			wiring: {
				code: 'yjsText',
				imports: [{ from: '@absolutejs/sync-yjs', names: ['yjsText'] }]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
