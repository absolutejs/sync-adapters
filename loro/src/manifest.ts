import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';

/* Adapter package: everything rides the `sync/crdt-adapter` implementation.
 * `loroText` is a ready-made TextCrdtAdapter constant — no options. */
export const manifest = defineManifest<Record<never, never>>()({
	contract: 2,
	identity: {
		accent: '#4f8cc9',
		category: 'sync',
		description:
			'Loro-backed `TextCrdtAdapter` for `@absolutejs/sync` — the Rust/WASM Loro CRDT engine behind the same collaborative-text call sites as the built-in `rgaText`. State is stored as a base64 Loro snapshot.',
		docsUrl: 'https://github.com/absolutejs/sync-adapters/tree/main/loro',
		name: '@absolutejs/sync-loro',
		tagline: 'Collaborative text editing powered by Loro.'
	},
	implements: [
		defineImplementation<never>()({
			contract: 'sync/crdt-adapter',
			factory: 'loroText',
			from: '@absolutejs/sync-loro',
			title: 'Loro',
			wiring: {
				code: 'loroText',
				imports: [
					{ from: '@absolutejs/sync-loro', names: ['loroText'] }
				]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
