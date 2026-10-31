// Two complete tenants for adversarial tenancy tests. Every user-owned table
// gets at least one row per user, so a cross-tenant mutation anywhere shows
// up in the snapshot diff. One brand merchant is shared by both; each tenant
// also owns a private payee (P2P merchant + alias on the SAME search key), so
// owner-scoped lookups can be proven to resolve per user. The pre-ownership
// shape (a P2P row referenced by both users) lives only in the migration
// rehearsal spec, which seeds it before the Task 4a file is applied.

import type { SqlClient } from '../../src/lib/db/client';

export interface TenantFixture {
	label: string;
	userId: string;
	connectionId: string;
	bankAccountId: string;
	cardAccountId: string;
	creditCardId: string;
	expenseCategoryId: string;
	incomeCategoryId: string;
	hiddenSystemCategoryId: string;
	/** This tenant's private payee (global_merchants.owner_user_id = userId). */
	p2pMerchantId: string;
	/** Same key for every tenant: proves owned aliases resolve per user. */
	p2pAliasKey: string;
	rawEmailIds: string[];
	eventIds: string[];
	expenseTxnId: string;
	incomeTxnId: string;
	needsReviewTxnId: string;
	deletedTxnId: string;
	searchKey: string;
	ruleId: string;
	relationshipId: string;
	seriesId: string;
	statementId: string;
	monthStart: string;
}

export interface TwoTenantFixture {
	a: TenantFixture;
	b: TenantFixture;
	sharedMerchantId: string;
	sharedAliasKey: string;
	systemExpenseCategoryId: string;
}

export const FIXTURE_P2P_ALIAS_KEY = 'FIXTUREPERSON';

interface IdRow {
	id: string;
}

function one<T>(rows: T[], what: string): T {
	const row = rows[0];
	if (!row) throw new Error(`fixture: expected one row for ${what}`);
	return row;
}

async function seedSharedMerchants(sql: SqlClient): Promise<{
	sharedMerchantId: string;
	sharedAliasKey: string;
}> {
	const sharedAliasKey = 'FIXTUREBRAND';
	const brand = one(
		await sql<IdRow[]>`
			insert into public.global_merchants (canonical_name, type, source, confirmed)
			values (${'Fixture Brand'}, ${'MERCHANT'}, ${'SEED'}, ${true})
			returning id
		`,
		'shared merchant',
	);
	await sql`
		insert into public.global_merchant_aliases (search_key, merchant_id)
		values (${sharedAliasKey}, ${brand.id})
	`;
	return {
		sharedMerchantId: brand.id,
		sharedAliasKey,
	};
}

export async function seedTenant(
	sql: SqlClient,
	label: string,
	shared: { sharedMerchantId: string },
	// Each tenant hides a different system category so "restore the other
	// user's hidden category" is a genuine cross-tenant request.
	hiddenSystemCategoryName: string,
): Promise<TenantFixture> {
	const upper = label.toUpperCase();
	const user = one(
		await sql<IdRow[]>`
			insert into public.users (email, auth_provider, auth_subject)
			values (${`${label}@example.test`}, ${'clerk'}, ${`user_fixture_${label}`})
			returning id
		`,
		'user',
	);
	const userId = user.id;

	const connection = one(
		await sql<IdRow[]>`
			insert into public.oauth_connections (user_id, provider, email_address, sync_status)
			values (${userId}, ${'google'}, ${`${label}@gmail.test`}, ${'ACTIVE'})
			returning id
		`,
		'connection',
	);

	const bankAccount = one(
		await sql<IdRow[]>`
			insert into public.accounts (user_id, name, type, instrument_last4)
			values (${userId}, ${`${label} bank`}, ${'bank'}, ${'1111'})
			returning id
		`,
		'bank account',
	);
	const cardAccount = one(
		await sql<IdRow[]>`
			insert into public.accounts (user_id, name, type, instrument_last4)
			values (${userId}, ${`${label} card`}, ${'card'}, ${'2222'})
			returning id
		`,
		'card account',
	);
	const creditCard = one(
		await sql<IdRow[]>`
			insert into public.user_credit_cards (user_id, account_id, card_label, first4, last4)
			values (${userId}, ${cardAccount.id}, ${`${label} main card`}, ${'4111'}, ${'2222'})
			returning id
		`,
		'credit card',
	);

	const expenseCategory = one(
		await sql<IdRow[]>`
			insert into public.categories (user_id, name, type, is_system)
			values (${userId}, ${`${label} eats`}, ${'expense'}, ${false})
			returning id
		`,
		'expense category',
	);
	const incomeCategory = one(
		await sql<IdRow[]>`
			insert into public.categories (user_id, name, type, is_system)
			values (${userId}, ${`${label} side income`}, ${'income'}, ${false})
			returning id
		`,
		'income category',
	);
	const hiddenSystem = one(
		await sql<IdRow[]>`
			select c.id from public.categories as c
			where c.user_id is null and c.is_system = true and c.name = ${hiddenSystemCategoryName} and c.type = ${'expense'}
			limit 1
		`,
		'system category to hide',
	);
	await sql`
		insert into public.user_hidden_categories (user_id, category_id)
		values (${userId}, ${hiddenSystem.id})
	`;

	// The tenant's private payee: same display name and alias key as the
	// other tenant's, distinguishable only by owner.
	const person = one(
		await sql<IdRow[]>`
			insert into public.global_merchants (canonical_name, type, source, confirmed, owner_user_id)
			values (${'Fixture Person'}, ${'P2P'}, ${'AI'}, ${false}, ${userId})
			returning id
		`,
		'p2p merchant',
	);
	await sql`
		insert into public.global_merchant_aliases (search_key, merchant_id, owner_user_id)
		values (${FIXTURE_P2P_ALIAS_KEY}, ${person.id}, ${userId})
	`;

	const rawEmailIds: string[] = [];
	for (let index = 0; index < 4; index += 1) {
		const rawEmail = one(
			await sql<IdRow[]>`
				insert into public.raw_emails (
					user_id, oauth_connection_id, source_id, internal_date, clean_text,
					from_address, subject, status
				)
				values (
					${userId}, ${connection.id}, ${`${label}-msg-${index}`}, ${'2026-09-01T10:00:00Z'},
					${`${label} body ${index}`}, ${'alerts@bank.test'}, ${`Alert ${index}`}, ${'PROCESSED'}
				)
				returning id
			`,
			'raw email',
		);
		rawEmailIds.push(rawEmail.id);
	}

	const searchKey = `${upper}PAYEE`;
	const eventSpecs = [
		{ direction: 'debit', amount: 50_000, ts: '2026-09-02T10:00:00Z' },
		{ direction: 'credit', amount: 20_000, ts: '2026-09-03T10:00:00Z' },
		{ direction: 'debit', amount: 30_000, ts: '2026-09-04T10:00:00Z' },
		{ direction: 'debit', amount: 10_000, ts: '2026-09-05T10:00:00Z' },
	];
	const eventIds: string[] = [];
	for (let index = 0; index < eventSpecs.length; index += 1) {
		const spec = eventSpecs[index] as (typeof eventSpecs)[number];
		const event = one(
			await sql<IdRow[]>`
				insert into public.financial_events (
					user_id, raw_email_id, extraction_index, direction, amount_in_paise,
					txn_timestamp, payment_method, counterparty_raw, search_key
				)
				values (
					${userId}, ${rawEmailIds[index] ?? null}, ${0}, ${spec.direction}, ${spec.amount},
					${spec.ts}, ${'upi'}, ${`${label} payee`}, ${searchKey}
				)
				returning id
			`,
			'financial event',
		);
		eventIds.push(event.id);
	}

	const expenseTxn = one(
		await sql<IdRow[]>`
			insert into public.transactions (
				user_id, financial_event_id, account_id, category_id, merchant_id,
				amount_in_paise, type, txn_date, status, classification_source
			)
			values (
				${userId}, ${eventIds[0] ?? null}, ${bankAccount.id}, ${expenseCategory.id}, ${shared.sharedMerchantId},
				${50_000}, ${'expense'}, ${'2026-09-02T10:00:00Z'}, ${'VERIFIED'}, ${'USER'}
			)
			returning id
		`,
		'expense transaction',
	);
	const incomeTxn = one(
		await sql<IdRow[]>`
			insert into public.transactions (
				user_id, financial_event_id, account_id, category_id,
				amount_in_paise, type, txn_date, status, classification_source
			)
			values (
				${userId}, ${eventIds[1] ?? null}, ${bankAccount.id}, ${incomeCategory.id},
				${20_000}, ${'income'}, ${'2026-09-03T10:00:00Z'}, ${'VERIFIED'}, ${'USER'}
			)
			returning id
		`,
		'income transaction',
	);
	const needsReviewTxn = one(
		await sql<IdRow[]>`
			insert into public.transactions (
				user_id, financial_event_id, merchant_id,
				amount_in_paise, type, txn_date, status, classification_source, ai_confidence_score
			)
			values (
				${userId}, ${eventIds[2] ?? null}, ${person.id},
				${30_000}, ${'expense'}, ${'2026-09-04T10:00:00Z'}, ${'NEEDS_REVIEW'}, ${'AI'}, ${0.55}
			)
			returning id
		`,
		'needs-review transaction',
	);
	const deletedTxn = one(
		await sql<IdRow[]>`
			insert into public.transactions (
				user_id, financial_event_id, amount_in_paise, type, txn_date, status,
				classification_source, user_deleted_at
			)
			values (
				${userId}, ${eventIds[3] ?? null}, ${10_000}, ${'expense'}, ${'2026-09-05T10:00:00Z'},
				${'VERIFIED'}, ${'USER'}, ${'2026-09-06T10:00:00Z'}
			)
			returning id
		`,
		'deleted transaction',
	);

	const rule = one(
		await sql<IdRow[]>`
			insert into public.user_merchant_rules (user_id, search_key, merchant_id, custom_category_id)
			values (${userId}, ${searchKey}, ${shared.sharedMerchantId}, ${expenseCategory.id})
			returning id
		`,
		'merchant rule',
	);
	await sql`
		insert into public.user_merchant_rule_categories (user_merchant_rule_id, transaction_type, category_id)
		values (${rule.id}, ${'expense'}, ${expenseCategory.id})
	`;
	await sql`
		insert into public.user_merchant_category_rules (user_id, merchant_id, transaction_type, category_id)
		values (${userId}, ${shared.sharedMerchantId}, ${'expense'}, ${expenseCategory.id})
	`;

	const relationship = one(
		await sql<IdRow[]>`
			insert into public.transaction_relationships (
				user_id, source_transaction_id, target_transaction_id, kind, created_by, confidence
			)
			values (${userId}, ${incomeTxn.id}, ${expenseTxn.id}, ${'REFUND'}, ${'USER'}, ${0.9})
			returning id
		`,
		'relationship',
	);

	const series = one(
		await sql<IdRow[]>`
			insert into public.recurring_series (
				user_id, fingerprint, search_key, category_id, direction, cadence, expected_day,
				amount_min_in_paise, amount_max_in_paise
			)
			values (
				${userId}, ${`${label}-rent`}, ${`${upper}RENT`}, ${expenseCategory.id}, ${'debit'}, ${'MONTHLY'}, ${1},
				${1_000_000}, ${1_000_000}
			)
			returning id
		`,
		'recurring series',
	);

	const statement = one(
		await sql<IdRow[]>`
			insert into public.card_statements (
				user_id, credit_card_id, card_last4, due_date, total_due_in_paise, min_due_in_paise, source
			)
			values (${userId}, ${creditCard.id}, ${'2222'}, ${'2026-09-30'}, ${100_000}, ${5_000}, ${'USER'})
			returning id
		`,
		'card statement',
	);

	const monthStart = '2026-09-01';
	await sql`
		insert into public.monthly_budgets (user_id, month_start, limit_in_paise)
		values (${userId}, ${monthStart}, ${5_000_000})
	`;
	await sql`
		insert into public.user_category_budget_rules (user_id, category_id, treatment)
		values (${userId}, ${expenseCategory.id}, ${'INCLUDE'})
	`;
	await sql`
		insert into public.budget_suggestion_dismissals (user_id, kind, fingerprint)
		values (${userId}, ${'RECURRING_PATTERN'}, ${`${label}-dismissed`})
	`;

	const callId = crypto.randomUUID();
	await sql`
		insert into public.ai_call_runs (id, user_id, operation, item_count, status, started_at)
		values (${callId}, ${userId}, ${'EMAIL_NORMALIZATION'}, ${1}, ${'SUCCESS'}, ${'2026-09-01T10:00:00Z'})
	`;
	await sql`
		insert into public.ai_usage_events (
			provider, model, operation, status, latency_ms, user_id, call_id, attempt_number
		)
		values (${'opencode'}, ${'fixture-model'}, ${'EMAIL_NORMALIZATION'}, ${'SUCCESS'}, ${10}, ${userId}, ${callId}, ${1})
	`;

	return {
		label,
		userId,
		connectionId: connection.id,
		bankAccountId: bankAccount.id,
		cardAccountId: cardAccount.id,
		creditCardId: creditCard.id,
		expenseCategoryId: expenseCategory.id,
		incomeCategoryId: incomeCategory.id,
		hiddenSystemCategoryId: hiddenSystem.id,
		p2pMerchantId: person.id,
		p2pAliasKey: FIXTURE_P2P_ALIAS_KEY,
		rawEmailIds,
		eventIds,
		expenseTxnId: expenseTxn.id,
		incomeTxnId: incomeTxn.id,
		needsReviewTxnId: needsReviewTxn.id,
		deletedTxnId: deletedTxn.id,
		searchKey,
		ruleId: rule.id,
		relationshipId: relationship.id,
		seriesId: series.id,
		statementId: statement.id,
		monthStart,
	};
}

export async function seedTwoTenants(sql: SqlClient): Promise<TwoTenantFixture> {
	const shared = await seedSharedMerchants(sql);
	const a = await seedTenant(sql, 'alpha', shared, 'Pets');
	const b = await seedTenant(sql, 'bravo', shared, 'Education');
	const systemExpense = one(
		await sql<IdRow[]>`
			select c.id from public.categories as c
			where c.user_id is null and c.is_system = true and c.name = ${'Groceries'} and c.type = ${'expense'}
			limit 1
		`,
		'system expense category',
	);
	return { a, b, ...shared, systemExpenseCategoryId: systemExpense.id };
}

/** A fresh financial event for `userId`, for tests that need an unlinked event. */
export async function insertLooseEvent(
	sql: SqlClient,
	userId: string,
	rawEmailId: string | null,
	extractionIndex: number,
): Promise<string> {
	const row = one(
		await sql<IdRow[]>`
			insert into public.financial_events (
				user_id, raw_email_id, extraction_index, direction, amount_in_paise,
				txn_timestamp, payment_method, counterparty_raw, search_key
			)
			values (
				${userId}, ${rawEmailId}, ${extractionIndex}, ${'debit'}, ${12_345},
				${'2026-09-07T10:00:00Z'}, ${'upi'}, ${'loose payee'}, ${'LOOSEPAYEE'}
			)
			returning id
		`,
		'loose event',
	);
	return row.id;
}
