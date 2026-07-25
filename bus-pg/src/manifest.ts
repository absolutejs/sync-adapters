import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';
import type { PostgresClusterBusOptions } from './index';

/* Adapter package: everything rides the `sync/cluster-bus` implementation.
 * `sql` is instance-valued (a postgres.js client) → wiring TODO binding;
 * postgres.js only has a default export, which WiringImport can't express
 * (known v1 limitation), so the wiring documents the import instead. */
export const manifest = defineManifest<PostgresClusterBusOptions>()({
	contract: 2,
	identity: {
		accent: '#336791',
		category: 'sync',
		description:
			'Postgres `LISTEN/NOTIFY` `ClusterBus` for `@absolutejs/sync` — horizontal scale across server instances without standing up Redis. Small batches ride the NOTIFY payload inline; oversized ones spill to a durable `sync_cluster_spill` table (auto-created, pruned via `bus.vacuum()`). `bus.listenerHealth()` self-probes the dedicated LISTEN connection while cumulative `bus.metrics()` counters surface fan-out health.',
		docsUrl: 'https://github.com/absolutejs/sync-adapters/tree/main/bus-pg',
		name: '@absolutejs/sync-bus-pg',
		tagline: 'Scale live updates across servers with your Postgres.'
	},
	implements: [
		defineImplementation<PostgresClusterBusOptions>()({
			contract: 'sync/cluster-bus',
			factory: 'createPostgresClusterBus',
			from: '@absolutejs/sync-bus-pg',
			requires: {
				env: [
					{
						description:
							'Postgres connection string (the NOTIFY channel and spill table live here)',
						example: 'postgres://user:pass@host/db',
						key: 'DATABASE_URL',
						secret: true
					}
				],
				peers: [
					{
						name: 'postgres',
						range: '^3.0.0',
						reason: 'postgres.js client — publish, LISTEN, and spill fetch ride one Sql instance'
					}
				],
				services: [
					{
						description:
							'Carries change fan-out between server instances',
						id: 'postgres'
					}
				]
			},
			settings: Type.Object({
				channel: Type.Optional(
					Type.String({
						description:
							'LISTEN/NOTIFY channel name. Two engines on the same Postgres can stay separate by using different channels. Default absolutejs_sync_cluster.',
						title: 'Channel name'
					})
				),
				listenerHealth: Type.Optional(
					Type.Union(
						[
							Type.Literal(false, {
								description:
									'Disable automatic probes when another owner calls probeListener() on its own cadence.'
							}),
							Type.Object({
								probeIntervalMs: Type.Optional(
									Type.Integer({
										default: 15000,
										minimum: 1,
										title: 'Listener probe interval (ms)'
									})
								),
								probeTimeoutMs: Type.Optional(
									Type.Integer({
										default: 5000,
										minimum: 1,
										title: 'Listener probe timeout (ms)'
									})
								)
							})
						],
						{
							description:
								'End-to-end self-probe configuration for the dedicated LISTEN connection.',
							title: 'Listener health monitoring'
						}
					)
				),
				spill: Type.Optional(
					Type.Union(
						[
							Type.Literal('overflow'),
							Type.Literal('always'),
							Type.Literal('never')
						],
						{
							description:
								'How oversized messages are handled: overflow (default — inline when small, table-backed when over the NOTIFY payload cap), always (every message durable, slower), never (throw on oversized).',
							title: 'Spill strategy'
						}
					)
				)
			}),
			title: 'Postgres LISTEN/NOTIFY (durable spill, no new infrastructure)',
			wiring: {
				code: [
					'// TODO: create the postgres.js client — postgres.js only has a default',
					"// export, which manifest imports can't express:",
					"//   import postgres from 'postgres';",
					"//   const sql = postgres(${env.DATABASE_URL} ?? '');",
					'createPostgresClusterBus({ sql, ...${settings} })'
				].join('\n'),
				imports: [
					{
						from: '@absolutejs/sync-bus-pg',
						names: ['createPostgresClusterBus']
					}
				]
			}
		}),
		defineImplementation<PostgresClusterBusOptions>()({
			contract: 'messaging/channel-bus',
			factory: 'createPostgresChannelBus',
			from: '@absolutejs/sync-bus-pg',
			requires: {
				services: [
					{ description: 'Typed process fan-out', id: 'postgres' }
				]
			},
			settings: Type.Object({
				channel: Type.Optional(Type.String()),
				listenerHealth: Type.Optional(
					Type.Union([
						Type.Literal(false),
						Type.Object({
							probeIntervalMs: Type.Optional(
								Type.Integer({ minimum: 1 })
							),
							probeTimeoutMs: Type.Optional(
								Type.Integer({ minimum: 1 })
							)
						})
					])
				),
				spill: Type.Optional(
					Type.Union([
						Type.Literal('overflow'),
						Type.Literal('always'),
						Type.Literal('never')
					])
				)
			}),
			title: 'Typed Postgres channel bus',
			wiring: {
				code: 'createPostgresChannelBus({ sql, ...${settings} })',
				imports: [
					{
						from: '@absolutejs/sync-bus-pg',
						names: ['createPostgresChannelBus']
					}
				]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
