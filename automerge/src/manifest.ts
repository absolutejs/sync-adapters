import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';

/* Adapter package: everything rides the `sync/crdt-adapter` implementation.
 * `automergeText` is a ready-made TextCrdtAdapter constant — no options. */
export const manifest = defineManifest<Record<never, never>>()({
	contract: 2,
	identity: {
		accent: '#e9445f',
		category: 'sync',
		description:
			'Automerge-backed `TextCrdtAdapter` for `@absolutejs/sync` — Automerge 3 behind the same collaborative-text call sites as the built-in `rgaText`. State is stored as a base64 Automerge document, so it interoperates with other Automerge tooling.',
		docsUrl:
			'https://github.com/absolutejs/sync-adapters/tree/main/automerge',
		name: '@absolutejs/sync-automerge',
		tagline: 'Collaborative text editing powered by Automerge.'
	},
	implements: [
		defineImplementation<never>()({
			contract: 'sync/crdt-adapter',
			factory: 'automergeText',
			from: '@absolutejs/sync-automerge',
			title: 'Automerge',
			wiring: {
				code: 'automergeText',
				imports: [
					{
						from: '@absolutejs/sync-automerge',
						names: ['automergeText']
					}
				]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
