import { expect, test } from 'bun:test';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { existsSync, readFileSync } from 'node:fs';
import manifest from '../package.json';

test('tested native dependencies satisfy published peer ranges', () => {
	for (const name of [
		'@absolutejs/devices',
		'@absolutejs/devices-capacitor',
		'@absolutejs/sync'
	] as const) {
		let directory = dirname(fileURLToPath(import.meta.resolve(name)));
		while (!existsSync(join(directory, 'package.json'))) {
			const parent = dirname(directory);
			if (parent === directory)
				throw new Error(`Missing manifest for ${name}`);
			directory = parent;
		}
		const installed = JSON.parse(
			readFileSync(join(directory, 'package.json'), 'utf8')
		);
		expect(installed.name).toBe(name);
		expect(installed.version).toBe(manifest.devDependencies[name]);
		expect(
			Bun.semver.satisfies(
				installed.version,
				manifest.peerDependencies[name]
			)
		).toBe(true);
	}
});
