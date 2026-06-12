import { defineConfig } from 'vitest/config';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
	resolve: {
		alias: {
			'cloudflare:workers': path.resolve(__dirname, 'test/mocks/cloudflare-workers.ts'),
		},
	},
	test: {
		globals: true,
		environment: 'node',
		include: ['test/**/*.spec.ts'],
		// Exclude Worker integration tests that require Cloudflare runtime
		exclude: [
			'test/integration/**',
		],
	},
});
