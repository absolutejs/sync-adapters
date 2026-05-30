/**
 * Postgres LISTEN/NOTIFY cluster bus for `@absolutejs/sync` — horizontal scale
 * across instances without standing up Redis. Implements the
 * {@link import('@absolutejs/sync/engine').ClusterBus} contract: an instance
 * publishes its committed changes; peers subscribed via `LISTEN <channel>`
 * receive them (filtering out their own by `origin`).
 *
 * The 8000-byte NOTIFY payload limit is the only sharp edge. Small batches go
 * inline as JSON; oversized batches spill into a `sync_cluster_spill` table
 * and the notification carries a pointer (`{ spill: <row-id> }`) the receiver
 * uses to fetch + delete the row.
 *
 * **Cross-instance resume (sync 1.17.0+):** the bus envelope carries
 * `originVersion` end-to-end, so a client that reconnects to a DIFFERENT
 * instance gets a catch-up diff from that instance's log of peer-broadcasted
 * changes — no fresh snapshot, no sticky sessions required. Pair with a
 * stable `SyncEngineOptions.instanceId` per shard so the resume cursor
 * survives shard restarts.
 *
 * Caveats inherited from the engine seam:
 *   - Best-effort delivery. NOTIFY can be lost if the listener connection
 *     drops mid-stream; the spill table is durable but the inline path isn't.
 *     For at-most-once semantics that suffices (every instance also has its
 *     own change log for resume); for at-least-once cross-instance, run the
 *     bus with `spill: 'always'` (every message round-trips through the table).
 *   - With sync < 1.17.0, `originVersion` is undefined on the wire — receiving
 *     instances log peer changes at version 0, which means any cross-instance
 *     resume falls back to a fresh snapshot. Bump to sync 1.17.0+ to enable.
 */
import type {
	ClusterBus,
	ClusterMessage
} from '@absolutejs/sync/engine';
import type { Sql } from 'postgres';

/** Maximum bytes the inline NOTIFY payload may carry. Default is conservative;
 * Postgres caps `NOTIFY` payloads at 8000 bytes (compile-time, MAX_NOTIFY_PAYLOAD).
 * We leave some headroom for the wrapper envelope. */
const INLINE_PAYLOAD_BUDGET = 6000;

const SPILL_TABLE_DDL = `
	create table if not exists sync_cluster_spill (
		id bigserial primary key,
		message jsonb not null,
		created_at timestamptz not null default now()
	)
`;

export type PostgresClusterBusOptions = {
	/**
	 * The `postgres` (https://github.com/porsager/postgres) client. We need
	 * both a regular SQL connection (for publish + spill fetch) and the
	 * ability to listen on a channel; `postgres` exposes both via the same
	 * `Sql` instance.
	 */
	sql: Sql;
	/**
	 * Channel name passed to `LISTEN` / `pg_notify`. Defaults to
	 * `'absolutejs_sync_cluster'`. Two engines on the same Postgres can scope
	 * themselves to different channels by overriding this.
	 */
	channel?: string;
	/**
	 * Spill strategy. `'overflow'` (default): inline JSON when small, table-
	 * backed when oversized. `'always'`: every message goes through the spill
	 * table (durable, slower). `'never'`: throws if a message exceeds the
	 * inline budget — useful in tests to assert payload-size discipline.
	 */
	spill?: 'overflow' | 'always' | 'never';
	/**
	 * Called when the listener encounters an error (parse failure, missing
	 * spill row, etc). Defaults to `console.warn`.
	 */
	onError?: (error: unknown) => void;
};

/**
 * Returned alongside the bus so apps can periodically prune old spill rows.
 * Inline messages never touch the table; only the (rare) oversized batch does.
 *
 * Why we don't delete inline on consume: Postgres NOTIFY broadcasts to every
 * listener on the channel, including the publisher's OWN listener. If the
 * publisher deletes the spill row after reading it, the other N-1 peers find
 * it gone. We instead let the row outlive the broadcast and prune by age.
 */
export type PostgresClusterBus = ClusterBus & {
	/** Delete spill rows older than `olderThanMs` (default 60_000). */
	vacuum: (olderThanMs?: number) => Promise<number>;
	/**
	 * Operator-shaped cumulative counters for the cluster-bus chokepoint.
	 * Scrape on a 30s interval to attribute cross-instance fan-out cost
	 * and detect a silently-broken cluster (received plateaus, errors
	 * climb). Added in 0.1.2.
	 */
	metrics: () => PostgresClusterBusMetrics;
};

/**
 * Cumulative counters since `createPostgresClusterBus()`. Added in 0.1.2.
 *
 * - `published` / `received` — envelopes the bus put on / pulled off the
 *   channel. A receiver counts a message ONCE here, regardless of
 *   whether it ignored it by origin downstream.
 * - `publishedInline` / `publishedSpilled` — split of the `published`
 *   total by envelope path; a healthy mostly-small workload has
 *   `publishedSpilled` near zero.
 * - `spillFetched` / `spillFetchFailed` — receiver side of the spill
 *   path. `spillFetchFailed` climbing means a spill row was vacuumed
 *   before every listener read it (vacuum window too aggressive).
 * - `spillVacuumed` — rows pruned by `vacuum()` since start.
 * - `publishErrors` / `subscribeErrors` — invocations that threw before
 *   the message left / was delivered. `subscribeErrors` increments
 *   when `onError` fires; `publishErrors` when `publish()` rejects.
 */
export type PostgresClusterBusMetrics = {
	published: number;
	publishedInline: number;
	publishedSpilled: number;
	received: number;
	spillFetched: number;
	spillFetchFailed: number;
	spillVacuumed: number;
	publishErrors: number;
	subscribeErrors: number;
};

type Envelope =
	| { kind: 'inline'; message: ClusterMessage }
	| { kind: 'spill'; id: string };

/**
 * Build a {@link ClusterBus} that publishes via `pg_notify` and subscribes via
 * `LISTEN`. Pass the returned bus to `engine.connectCluster(bus)` on every
 * instance you want to fan out to.
 *
 * @example
 * ```ts
 * import postgres from 'postgres';
 * import { createPostgresClusterBus } from '@absolutejs/sync-bus-pg';
 *
 * const sql = postgres(process.env.DATABASE_URL!);
 * const bus = createPostgresClusterBus({ sql });
 * await engine.connectCluster(bus);
 * ```
 */
export const createPostgresClusterBus = (
	options: PostgresClusterBusOptions
): PostgresClusterBus => {
	const channel = options.channel ?? 'absolutejs_sync_cluster';
	const spillMode = options.spill ?? 'overflow';
	const baseOnError =
		options.onError ?? ((error) => console.warn('[sync-bus-pg]', error));
	const { sql } = options;

	// 0.1.2: cumulative operator counters. Single source of truth — both
	// the publish and subscribe paths increment here; metrics() returns a
	// shallow copy.
	const counters: PostgresClusterBusMetrics = {
		published: 0,
		publishedInline: 0,
		publishedSpilled: 0,
		publishErrors: 0,
		received: 0,
		spillFetchFailed: 0,
		spillFetched: 0,
		spillVacuumed: 0,
		subscribeErrors: 0
	};
	const onError = (error: unknown) => {
		counters.subscribeErrors += 1;
		baseOnError(error);
	};

	// Lazily ensure the spill table exists on first publish/subscribe that
	// might need it. Concurrent calls are safe (`create table if not exists`).
	let spillReady: Promise<void> | undefined;
	const ensureSpill = (): Promise<void> => {
		if (spillReady !== undefined) return spillReady;
		spillReady = sql.unsafe(SPILL_TABLE_DDL).then(() => undefined);

		return spillReady;
	};

	const publish = async (message: ClusterMessage): Promise<void> => {
		try {
			const inline = JSON.stringify({
				kind: 'inline',
				message
			} satisfies Envelope);

			const useInline =
				spillMode === 'never' ||
				(spillMode === 'overflow' &&
					inline.length <= INLINE_PAYLOAD_BUDGET);

			if (useInline) {
				if (
					spillMode === 'never' &&
					inline.length > INLINE_PAYLOAD_BUDGET
				) {
					throw new Error(
						`[sync-bus-pg] payload ${inline.length} bytes exceeds inline budget ${INLINE_PAYLOAD_BUDGET}; spill is 'never'`
					);
				}
				// Wrap in pg_notify — payload is a single text arg.
				await sql`select pg_notify(${channel}, ${inline})`;
				counters.publishedInline += 1;
				counters.published += 1;

				return;
			}

			// Oversized inline OR spillMode === 'always': insert + notify the id.
			// JSON-stringify + cast at the SQL boundary — `sql.json`'s JSONValue
			// type is narrower than the structured `ClusterMessage` shape.
			await ensureSpill();
			const serialized = JSON.stringify(message);
			const rows = await sql<{ id: string }[]>`
				insert into sync_cluster_spill (message) values (${serialized}::jsonb)
				returning id
			`;
			const id = rows[0]?.id;
			if (id === undefined) {
				throw new Error(
					'[sync-bus-pg] spill insert returned no row id'
				);
			}
			const envelope: Envelope = { kind: 'spill', id };
			await sql`select pg_notify(${channel}, ${JSON.stringify(envelope)})`;
			counters.publishedSpilled += 1;
			counters.published += 1;
		} catch (error) {
			counters.publishErrors += 1;
			throw error;
		}
	};

	const subscribe = async (
		onMessage: (message: ClusterMessage) => void
	): Promise<() => Promise<void>> => {
		// `postgres`'s listen() opens a dedicated listener connection that
		// invokes the callback per NOTIFY. It returns `{ unlisten }` so we
		// can cleanly detach.
		const handle = await sql.listen(channel, (payload) => {
			void (async () => {
				try {
					const envelope = JSON.parse(payload) as Envelope;
					if (envelope.kind === 'inline') {
						counters.received += 1;
						onMessage(envelope.message);

						return;
					}
					// kind === 'spill' — fetch the row.
					await ensureSpill();
					const rows = await sql<{ message: ClusterMessage | string }[]>`
						select message from sync_cluster_spill where id = ${envelope.id}
					`;
					const row = rows[0];
					if (row === undefined) {
						counters.spillFetchFailed += 1;
						onError(
							new Error(
								`[sync-bus-pg] spill row ${envelope.id} not found`
							)
						);

						return;
					}
					counters.spillFetched += 1;
					counters.received += 1;
					// postgres-js returns jsonb columns as text by default — only
					// some configurations auto-parse to objects. Normalize both
					// shapes so a future driver/version change can't break us
					// silently.
					const parsed =
						typeof row.message === 'string'
							? (JSON.parse(row.message) as ClusterMessage)
							: row.message;
					onMessage(parsed);
					// Intentionally NOT deleting here — see `vacuum()` below.
				} catch (error) {
					onError(error);
				}
			})();
		});

		return async () => {
			await handle.unlisten();
		};
	};

	const vacuum = async (olderThanMs = 60_000): Promise<number> => {
		await ensureSpill();
		const result = await sql<{ id: string }[]>`
			delete from sync_cluster_spill
			where created_at < now() - (${olderThanMs} || ' milliseconds')::interval
			returning id
		`;
		counters.spillVacuumed += result.length;

		return result.length;
	};

	const metrics = (): PostgresClusterBusMetrics => ({ ...counters });

	return { metrics, publish, subscribe, vacuum };
};
