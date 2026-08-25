import { Database } from 'bun:sqlite';
import type { CapacitorSyncSqliteConnection } from '../../src/index';

/** Executes the narrow Capacitor wrapper contract against real in-memory SQLite. */
export const createFakeSqliteConnection = (): CapacitorSyncSqliteConnection => {
	const database = new Database(':memory:', { strict: true });
	return {
		open: async () => undefined,
		execute: async (statements) => {
			database.exec(statements);
			return { changes: { changes: 0 } };
		},
		beginTransaction: async () => {
			database.exec('BEGIN IMMEDIATE');
			return { changes: { changes: 0 } };
		},
		commitTransaction: async () => {
			database.exec('COMMIT');
			return { changes: { changes: 0 } };
		},
		rollbackTransaction: async () => {
			database.exec('ROLLBACK');
			return { changes: { changes: 0 } };
		},
		run: async (statement, values = []) => {
			const result = database.query(statement).run(...values);
			return {
				changes: {
					changes: result.changes,
					lastId: Number(result.lastInsertRowid)
				}
			};
		},
		query: async (statement, values = []) => ({
			values: database.query(statement).all(...values) as Record<
				string,
				unknown
			>[]
		})
	};
};
