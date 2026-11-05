import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';
import type { PubSubReactionOptions } from './index';
export const manifest = defineManifest<PubSubReactionOptions>()({
	contract: 2,
	identity: {
		accent: '#4285f4',
		category: 'sync',
		name: '@absolutejs/sync-pubsub',
		description:
			'Google Cloud Pub/Sub StreamingPull reaction with acknowledgment after durable processing and lifecycle-owned shutdown.',
		docsUrl: 'https://github.com/absolutejs/sync-adapters/tree/main/pubsub',
		tagline: 'Receive provider events through an open connection.'
	},
	implements: [
		defineImplementation<PubSubReactionOptions>()({
			contract: 'sync/change-source',
			factory: 'createPubSubReaction',
			from: '@absolutejs/sync-pubsub',
			title: 'Google Cloud Pub/Sub',
			settings: Type.Object({
				closeOnStop: Type.Optional(Type.Boolean({ default: true }))
			}),
			wiring: {
				code: 'createPubSubReaction({ subscription, process })',
				imports: [
					{
						from: '@absolutejs/sync-pubsub',
						names: ['createPubSubReaction']
					}
				]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
