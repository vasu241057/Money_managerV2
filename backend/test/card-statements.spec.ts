import { describe, expect, it } from 'vitest';

import type { SqlClient } from '../src/lib/db/client';
import { maskNumericTokens } from '../src/lib/masking';
import { resolveMaskedStatement } from '../src/lib/statements';
import {
	persistCardStatement,
	relinkStatementsForCreditCard,
} from '../src/services/card-statements.service';

type QueryHandler = (query: string, values: unknown[]) => unknown | Promise<unknown>;

function normalizeQuery(strings: TemplateStringsArray): string {
	return strings.join(' ').replace(/\s+/g, ' ').trim();
}

function createSqlMock(handlers: QueryHandler[]): SqlClient {
	let cursor = 0;
	const tag = async (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown> => {
		const query = normalizeQuery(strings);
		const handler = handlers[cursor];
		cursor += 1;
		if (!handler) {
			throw new Error(`Unexpected query execution: ${query}`);
		}
		return handler(query, values);
	};
	return tag as unknown as SqlClient;
}

const USER_ID = '11111111-1111-4111-8111-111111111111';
const EMAIL_ID = '22222222-2222-4222-8222-222222222222';
const CARD_ID = '33333333-3333-4333-8333-333333333333';

const INTERNAL_DATE = '2026-07-12T09:00:00.000Z';

// Builds a masked YES-Bank-style statement and the model's token answer the
// way the pipeline would: mask the text, then reference the token ids.
function maskStatementText(): { tokens: Map<string, string>; ids: string[] } {
	const text =
		'Statement for Card Number 3561XXXXXXXX6279 ' +
		'Statement Period: 13/06/2026 To 12/07/2026 ' +
		'Total Amount Due: Rs. 34,073.18 Minimum Amount Due: Rs. 681.46 ' +
		'Payment Due Date: 01/08/2026';
	const masked = maskNumericTokens(text);
	return { tokens: masked.tokens, ids: Array.from(masked.tokens.keys()) };
}

describe('resolveMaskedStatement', () => {
	it('resolves all four fields from token references', () => {
		const { tokens } = maskStatementText();
		// Token order in the masked text: N1=3561, N2=6279, N3..N8 period
		// dates, N9=34, N10=073.18 — amounts split on the thousands dot? No:
		// the regex keeps '34,073.18' as one token. Recompute: N1=3561,
		// N2=6279, N3=13, N4=06, N5=2026, N6=12, N7=07, N8=2026,
		// N9=34,073.18, N10=681.46, N11=01, N12=08, N13=2026.
		const resolved = resolveMaskedStatement(
			{
				card_last4_token: 'N2',
				total_due_token: 'N9',
				min_due_token: 'N10',
				due_date_text: '<N11>/<N12>/<N13>',
				period_text: '<N3>/<N4>/<N5> To <N6>/<N7>/<N8>',
				confidence: 0.95,
			},
			tokens,
			INTERNAL_DATE,
			0.3,
		);

		expect(resolved).toMatchObject({
			card_last4: '6279',
			total_due_in_paise: 3407318,
			min_due_in_paise: 68146,
			due_date: '2026-08-01',
			period_start: '2026-06-13',
			period_end: '2026-07-12',
		});
	});

	it('parses spaced-dash periods (HDFC "15 Jun, 2026 - 14 Jul, 2026") without splitting dd-Mon-yyyy dates', () => {
		const masked = maskNumericTokens('Billing Period: 15 Jun, 2026 - 14 Jul, 2026');
		const resolved = resolveMaskedStatement(
			{
				card_last4_token: null,
				total_due_token: null,
				min_due_token: null,
				due_date_text: null,
				period_text: '<N1> Jun, <N2> - <N3> Jul, <N4>',
				confidence: 0.9,
			},
			masked.tokens,
			'2026-07-14T09:00:00.000Z',
			0.3,
		);
		expect(resolved).toMatchObject({ period_start: '2026-06-15', period_end: '2026-07-14' });

		const kotak = maskNumericTokens('Transactions Details from 26-May-2026 to 25-Jun-2026');
		const kotakResolved = resolveMaskedStatement(
			{
				card_last4_token: null,
				total_due_token: null,
				min_due_token: null,
				due_date_text: null,
				period_text: '<N1>-May-<N2> to <N3>-Jun-<N4>',
				confidence: 0.9,
			},
			kotak.tokens,
			'2026-06-25T09:00:00.000Z',
			0.3,
		);
		expect(kotakResolved).toMatchObject({ period_start: '2026-05-26', period_end: '2026-06-25' });
	});

	it('never falls back to the email date for a missing due date, and nulls contradictory fields', () => {
		const { tokens } = maskStatementText();
		const resolved = resolveMaskedStatement(
			{
				card_last4_token: 'N2',
				total_due_token: 'N10', // 681.46 as total
				min_due_token: 'N9', // 34,073.18 as min — min > total must null
				due_date_text: 'gibberish with no date',
				period_text: null,
				confidence: 0.9,
			},
			tokens,
			INTERNAL_DATE,
			0.3,
		);
		expect(resolved).not.toBeNull();
		expect(resolved?.due_date).toBeNull();
		expect(resolved?.min_due_in_paise).toBeNull();
		expect(resolved?.total_due_in_paise).toBe(68146);
	});

	it('returns null when no field resolves or confidence is below threshold', () => {
		const { tokens } = maskStatementText();
		expect(
			resolveMaskedStatement(
				{
					card_last4_token: 'N2',
					total_due_token: null,
					min_due_token: null,
					due_date_text: null,
					period_text: null,
					confidence: 0.9,
				},
				tokens,
				INTERNAL_DATE,
				0.3,
			),
		).toBeNull();
		expect(
			resolveMaskedStatement(
				{ total_due_token: 'N9', confidence: 0.1 },
				tokens,
				INTERNAL_DATE,
				0.3,
			),
		).toBeNull();
		expect(resolveMaskedStatement(null, tokens, INTERNAL_DATE, 0.3)).toBeNull();
	});
});

describe('persistCardStatement', () => {
	const RESOLVED = {
		card_last4: '6279',
		total_due_in_paise: 3407318,
		min_due_in_paise: 68146,
		due_date: '2026-08-01',
		period_start: '2026-06-13',
		period_end: '2026-07-12',
		confidence: 0.95,
	};

	it('links the sole matching active card and derives its billing cycle day', async () => {
		const queries: string[] = [];
		const values: unknown[][] = [];
		const sql = createSqlMock([
			(query, vals) => {
				queries.push(query);
				values.push(vals);
				return [];
			},
			(query, vals) => {
				queries.push(query);
				values.push(vals);
				return [];
			},
		]);

		const outcome = await persistCardStatement(sql, USER_ID, EMAIL_ID, RESOLVED, [
			{ id: CARD_ID, last4: '6279' },
			{ id: 'other', last4: '9999' },
		]);

		expect(outcome).toEqual({
			persisted: true,
			linked_credit_card_id: CARD_ID,
			duplicate: false,
		});
		expect(queries[0]).toContain('insert into public.card_statements');
		// USER-source rows survive email re-harvest
		expect(queries[0]).toContain("card_statements.source = 'EMAIL'");
		// cycle day 12 from period_end 2026-07-12, only where currently null
		expect(queries[1]).toContain('billing_cycle_day is null');
		expect(values[1]).toContain(12);
	});

	it('parks on ambiguous or missing last4 (credit_card_id null, no cycle-day write)', async () => {
		const inserts: unknown[][] = [];
		const sql = createSqlMock([
			(_query, vals) => {
				inserts.push(vals);
				return [];
			},
		]);

		const outcome = await persistCardStatement(sql, USER_ID, EMAIL_ID, RESOLVED, [
			{ id: CARD_ID, last4: '6279' },
			{ id: 'twin', last4: '6279' },
		]);

		expect(outcome.linked_credit_card_id).toBeNull();
		expect(outcome.persisted).toBe(true);
		// second mock handler absent — cycle-day update must not have run
	});

	it('merges missing fields into the existing row on a duplicate cycle instead of throwing', async () => {
		const queries: string[] = [];
		const sql = createSqlMock([
			() => {
				const error = new Error('duplicate') as Error & { code: string };
				error.code = '23505';
				throw error;
			},
			// merge-fill of the existing EMAIL row
			query => {
				queries.push(query);
				return [];
			},
		]);

		const outcome = await persistCardStatement(sql, USER_ID, EMAIL_ID, RESOLVED, []);
		expect(outcome).toEqual({ persisted: false, linked_credit_card_id: null, duplicate: true });
		expect(queries[0]).toContain('coalesce(cs.period_start');
		expect(queries[0]).toContain("cs.source = 'EMAIL'");
	});
});

describe('relinkStatementsForCreditCard', () => {
	it('links parked rows and derives the cycle day from the latest period end', async () => {
		const queries: string[] = [];
		const values: unknown[][] = [];
		const sql = createSqlMock([
			(query) => {
				queries.push(query);
				return [
					{ id: 's1', period_end: '2026-06-12' },
					{ id: 's2', period_end: '2026-07-12' },
				];
			},
			(query, vals) => {
				queries.push(query);
				values.push(vals);
				return [];
			},
		]);

		const count = await relinkStatementsForCreditCard(sql, USER_ID, CARD_ID, '6279');
		expect(count).toBe(2);
		expect(queries[0]).toContain('credit_card_id is null');
		expect(values[0]).toContain(12);
	});
});

describe('parseUpsertCardStatementRequest', () => {
	it('accepts a partial manual bill and rejects contradictions', async () => {
		const { parseUpsertCardStatementRequest } = await import(
			'../src/services/card-statements.service'
		);
		const parsed = parseUpsertCardStatementRequest({
			credit_card_id: CARD_ID,
			total_due_in_paise: 1695500,
			due_date: '2026-08-03',
		});
		expect(parsed.total_due_in_paise).toBe(1695500);
		expect(parsed.min_due_in_paise).toBeNull();

		expect(() =>
			parseUpsertCardStatementRequest({ credit_card_id: CARD_ID }),
		).toThrowError(/At least one/);
		expect(() =>
			parseUpsertCardStatementRequest({
				credit_card_id: CARD_ID,
				total_due_in_paise: 100,
				min_due_in_paise: 200,
			}),
		).toThrowError(/cannot exceed/);
		expect(() =>
			parseUpsertCardStatementRequest({
				credit_card_id: CARD_ID,
				due_date: '03-08-2026',
			}),
		).toThrowError(/YYYY-MM-DD/);
		expect(() =>
			parseUpsertCardStatementRequest({
				credit_card_id: CARD_ID,
				total_due_in_paise: 100,
				period_start: '2026-06-15',
			}),
		).toThrowError(/together/);
	});
});

describe('upsertUserCardStatement', () => {
	it('updates the existing cycle row as USER instead of inserting a duplicate', async () => {
		const { upsertUserCardStatement } = await import(
			'../src/services/card-statements.service'
		);
		const queries: string[] = [];
		const STMT_ROW = {
			id: 'st-1',
			user_id: USER_ID,
			credit_card_id: CARD_ID,
			card_last4: '9876',
			period_start: null,
			period_end: null,
			statement_date: null,
			total_due_in_paise: '1695500',
			min_due_in_paise: null,
			due_date: '2026-08-03',
			source: 'USER',
			source_raw_email_id: null,
			created_at: '2026-07-19T10:00:00.000Z',
			updated_at: '2026-07-19T10:00:00.000Z',
		};
		const sql = createSqlMock([
			() => [{ id: CARD_ID, last4: '9876' }],
			query => {
				queries.push(query);
				return [{ id: 'st-1' }];
			},
			query => {
				queries.push(query);
				return [STMT_ROW];
			},
		]);

		const statement = await upsertUserCardStatement(sql, USER_ID, {
			credit_card_id: CARD_ID,
			total_due_in_paise: 1695500,
			min_due_in_paise: null,
			due_date: '2026-08-03',
			period_start: null,
			period_end: null,
		});

		expect(statement.source).toBe('USER');
		expect(statement.total_due_in_paise).toBe(1695500);
		expect(queries[1]).toContain("source = 'USER'");
	});
});
