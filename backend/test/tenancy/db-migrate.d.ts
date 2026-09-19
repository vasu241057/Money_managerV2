declare module '../../../scripts/db-migrate.mjs' {
	export const MIGRATIONS_DIR: string;
	export const BOOKKEEPING_DDL: string;
	export const BACKSTOP_ASSERT_SQL: string;
	export function checksumOf(contents: string): string;
	export function parseDevVars(text: string): Record<string, string>;
	export function listMigrationFiles(dir?: string): string[];
	export function findTransactionControl(contents: string): string | null;
	export function computePlan(
		files: string[],
		checksums: Map<string, string>,
		applied: Map<string, string>,
	): { applied: string[]; pending: string[]; drifted: string[]; unknownApplied: string[] };
	export function sqlLiteral(value: unknown): string;
	export function composeApplySql(filename: string, contents: string, checksum: string): string;
	export function composeBaselineSql(entries: Array<{ filename: string; checksum: string }>): string;
	export function parseArgs(argv: string[]): {
		command: string;
		options: { dryRun: boolean; force: boolean; to: string | null; through: string | null };
	};
}
