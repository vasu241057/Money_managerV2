// The adversarial tenancy matrix: every id-shaped slot of every service,
// called as user A with user B's identifiers. A case passes when the service
// rejects with a 4xx (or, where flagged, no-ops) AND neither user's rows
// change. `routes` ties each case to the HTTP routes it covers, which
// route-enrollment.spec.ts checks against the real route table so a new
// id-bearing route fails CI until it is enrolled here.

import type { SqlClient } from '../../src/lib/db/client';
import { deleteAccount, updateAccount } from '../../src/services/accounts.service';
import {
	confirmRecurringPattern,
	createTransactionRelationship,
	deleteCategoryBudgetRule,
	deleteTransactionRelationship,
	listRelationshipTargets,
	setCategoryBudgetRule,
	updateRecurringSeries,
	upsertBudgetPlan,
} from '../../src/services/budget.service';
import { upsertUserCardStatement } from '../../src/services/card-statements.service';
import {
	createCategory,
	deleteCategory,
	restoreCategory,
	updateCategory,
} from '../../src/services/categories.service';
import {
	createUserCreditCard,
	deleteUserCreditCard,
	updateUserCreditCard,
} from '../../src/services/credit-cards.service';
import {
	listGlobalMerchants,
	listUserMerchantTransactions,
	setUserDescriptorCategory,
	setUserMerchantCategory,
} from '../../src/services/merchants.service';
import {
	createManualTransaction,
	deleteTransaction,
	restoreTransaction,
	reviewTransaction,
	updateTransaction,
} from '../../src/services/transactions.service';
import type { TwoTenantFixture } from './fixtures';

export interface MatrixContext {
	sql: SqlClient;
	fx: TwoTenantFixture;
}

export interface MatrixCase {
	name: string;
	/** `METHOD /mount/path` keys, exactly as route-enrollment.spec.ts derives them. */
	routes: string[];
	/** The call may resolve (a silent no-op is acceptable) as long as nothing changed. */
	allowNoop?: boolean;
	run: (ctx: MatrixContext) => Promise<unknown>;
	/** Extra assertion when the call resolves (e.g. a listing must not leak B's rows). */
	assertResolved?: (result: unknown, ctx: MatrixContext) => void;
}

/** Routes whose request BODY carries ids; they have no `:param` but still need coverage. */
export const BODY_ID_ROUTES: readonly string[] = [
	'POST /categories',
	'POST /transactions',
	'POST /credit-cards',
	'POST /card-statements',
	'POST /budget/relationships',
	'POST /budget/recurring-series',
];

/** Id-bearing routes that are not tenant-scoped by design. */
export const ROUTE_ALLOWLIST: readonly string[] = [
	// Admin-only global job schedule; the :job param is a job name, not a tenant id.
	'PUT /ai-usage/job-schedules/:job',
	// Admin-only: :userId names whose daily caps to override (Task 6); the
	// router 404s for anyone but the configured admin before reading it.
	'PUT /ai-usage/users/:userId/limits',
	// :month is a calendar month, not an identifier; the plan body is covered.
	'PUT /budget/months/:month',
];

const TXN_DATE = '2026-09-08T10:00:00Z';

export const MATRIX_CASES: readonly MatrixCase[] = [
	// ---- accounts ----
	{
		name: 'update another user’s account',
		routes: ['PATCH /accounts/:accountId'],
		run: ({ sql, fx }) => updateAccount(sql, fx.a.userId, fx.b.bankAccountId, { name: 'hijacked' }),
	},
	{
		name: 'delete another user’s account',
		routes: ['DELETE /accounts/:accountId'],
		run: ({ sql, fx }) => deleteAccount(sql, fx.a.userId, fx.b.bankAccountId),
	},

	// ---- categories ----
	{
		name: 'update another user’s category',
		routes: ['PATCH /categories/:categoryId'],
		run: ({ sql, fx }) => updateCategory(sql, fx.a.userId, fx.b.expenseCategoryId, { name: 'hijacked' }),
	},
	{
		name: 'delete another user’s category',
		routes: ['DELETE /categories/:categoryId'],
		run: ({ sql, fx }) => deleteCategory(sql, fx.a.userId, fx.b.expenseCategoryId),
	},
	{
		name: 'restore a system category only the other user hid',
		routes: ['POST /categories/:categoryId/restore'],
		run: ({ sql, fx }) => restoreCategory(sql, fx.a.userId, fx.b.hiddenSystemCategoryId),
	},
	{
		name: 'create a category under another user’s parent',
		routes: ['POST /categories'],
		run: ({ sql, fx }) =>
			createCategory(sql, fx.a.userId, {
				name: 'nested hijack',
				type: 'expense',
				parent_id: fx.b.expenseCategoryId,
			}),
	},

	// ---- transactions ----
	{
		name: 'update another user’s transaction',
		routes: ['PATCH /transactions/:transactionId'],
		run: ({ sql, fx }) => updateTransaction(sql, fx.a.userId, fx.b.expenseTxnId, { user_note: 'hijacked' }),
	},
	{
		name: 'move own transaction onto another user’s account',
		routes: ['PATCH /transactions/:transactionId'],
		run: ({ sql, fx }) =>
			updateTransaction(sql, fx.a.userId, fx.a.expenseTxnId, { account_id: fx.b.bankAccountId }),
	},
	{
		name: 'categorize own transaction with another user’s category',
		routes: ['PATCH /transactions/:transactionId'],
		run: ({ sql, fx }) =>
			updateTransaction(sql, fx.a.userId, fx.a.expenseTxnId, { category_id: fx.b.expenseCategoryId }),
	},
	{
		name: 'link own transaction to another user’s credit card',
		routes: ['PATCH /transactions/:transactionId'],
		run: ({ sql, fx }) =>
			updateTransaction(sql, fx.a.userId, fx.a.expenseTxnId, {
				account_id: null,
				credit_card_id: fx.b.creditCardId,
			}),
	},
	{
		name: 'delete another user’s transaction',
		routes: ['DELETE /transactions/:transactionId'],
		run: ({ sql, fx }) => deleteTransaction(sql, fx.a.userId, fx.b.expenseTxnId),
	},
	{
		name: 'restore another user’s deleted transaction',
		routes: ['POST /transactions/:transactionId/restore'],
		run: ({ sql, fx }) => restoreTransaction(sql, fx.a.userId, fx.b.deletedTxnId),
	},
	{
		name: 'review another user’s transaction',
		routes: ['POST /transactions/:transactionId/review'],
		run: ({ sql, fx }) =>
			reviewTransaction(sql, fx.a.userId, fx.b.needsReviewTxnId, { category_id: fx.a.expenseCategoryId }),
	},
	{
		name: 'review own transaction with another user’s category',
		routes: ['POST /transactions/:transactionId/review'],
		run: ({ sql, fx }) =>
			reviewTransaction(sql, fx.a.userId, fx.a.needsReviewTxnId, { category_id: fx.b.expenseCategoryId }),
	},
	{
		name: 'create a manual transaction on another user’s account',
		routes: ['POST /transactions'],
		run: ({ sql, fx }) =>
			createManualTransaction(sql, fx.a.userId, {
				amount_in_paise: 999,
				type: 'expense',
				txn_date: TXN_DATE,
				account_id: fx.b.bankAccountId,
			}),
	},
	{
		name: 'create a manual transaction with another user’s category',
		routes: ['POST /transactions'],
		run: ({ sql, fx }) =>
			createManualTransaction(sql, fx.a.userId, {
				amount_in_paise: 999,
				type: 'expense',
				txn_date: TXN_DATE,
				category_id: fx.b.expenseCategoryId,
			}),
	},

	// ---- credit cards ----
	{
		name: 'update another user’s credit card',
		routes: ['PATCH /credit-cards/:creditCardId'],
		run: ({ sql, fx }) => updateUserCreditCard(sql, fx.a.userId, fx.b.creditCardId, { card_label: 'hijacked' }),
	},
	{
		name: 'rebase own credit card onto another user’s account',
		routes: ['PATCH /credit-cards/:creditCardId'],
		run: ({ sql, fx }) =>
			updateUserCreditCard(sql, fx.a.userId, fx.a.creditCardId, { account_id: fx.b.cardAccountId }),
	},
	{
		name: 'delete another user’s credit card',
		routes: ['DELETE /credit-cards/:creditCardId'],
		run: ({ sql, fx }) => deleteUserCreditCard(sql, fx.a.userId, fx.b.creditCardId),
	},
	{
		name: 'create a credit card backed by another user’s account',
		routes: ['POST /credit-cards'],
		run: ({ sql, fx }) =>
			createUserCreditCard(sql, fx.a.userId, {
				account_id: fx.b.cardAccountId,
				card_label: 'hijack card',
				first4: '4111',
				last4: '7777',
			}),
	},

	// ---- card statements ----
	{
		name: 'file a statement against another user’s credit card',
		routes: ['POST /card-statements'],
		run: ({ sql, fx }) =>
			upsertUserCardStatement(sql, fx.a.userId, {
				credit_card_id: fx.b.creditCardId,
				total_due_in_paise: 1000,
				min_due_in_paise: 100,
				due_date: '2026-10-31',
				period_start: null,
				period_end: null,
			}),
	},

	// ---- budget ----
	{
		name: 'set a budget rule on another user’s category',
		routes: ['PUT /budget/category-rules/:categoryId'],
		run: ({ sql, fx }) =>
			setCategoryBudgetRule(sql, fx.a.userId, fx.b.expenseCategoryId, { treatment: 'EXCLUDE' }),
	},
	{
		name: 'delete another user’s budget rule',
		routes: ['DELETE /budget/category-rules/:categoryId'],
		allowNoop: true,
		run: ({ sql, fx }) => deleteCategoryBudgetRule(sql, fx.a.userId, fx.b.expenseCategoryId),
	},
	{
		name: 'save a budget plan naming another user’s category',
		routes: ['PUT /budget/months/:month/plan'],
		run: ({ sql, fx }) =>
			upsertBudgetPlan(sql, fx.a.userId, '2026-09', {
				limit_in_paise: 100_000,
				category_rules: [{ category_id: fx.b.expenseCategoryId, treatment: 'INCLUDE' }],
			}),
	},
	{
		name: 'link own refund to another user’s purchase',
		routes: ['POST /budget/relationships'],
		run: ({ sql, fx }) =>
			createTransactionRelationship(sql, fx.a.userId, {
				source_transaction_id: fx.a.needsReviewTxnId,
				target_transaction_id: fx.b.expenseTxnId,
				kind: 'REIMBURSEMENT',
			}),
	},
	{
		name: 'link another user’s refund to own purchase',
		routes: ['POST /budget/relationships'],
		run: ({ sql, fx }) =>
			createTransactionRelationship(sql, fx.a.userId, {
				source_transaction_id: fx.b.incomeTxnId,
				target_transaction_id: fx.a.expenseTxnId,
				kind: 'REIMBURSEMENT',
			}),
	},
	{
		name: 'delete another user’s relationship',
		routes: ['DELETE /budget/relationships/:relationshipId'],
		allowNoop: true,
		run: ({ sql, fx }) => deleteTransactionRelationship(sql, fx.a.userId, fx.b.relationshipId),
	},
	{
		name: 'list refund targets for another user’s transaction',
		routes: ['GET /budget/relationship-targets/:sourceTransactionId'],
		allowNoop: true,
		run: ({ sql, fx }) => listRelationshipTargets(sql, fx.a.userId, fx.b.incomeTxnId),
		assertResolved: (result) => {
			if (!Array.isArray(result) || result.length !== 0) {
				throw new Error('relationship targets leaked rows for a foreign transaction');
			}
		},
	},
	{
		name: 'pause another user’s recurring series',
		routes: ['PATCH /budget/recurring-series/:seriesId'],
		run: ({ sql, fx }) => updateRecurringSeries(sql, fx.a.userId, fx.b.seriesId, { status: 'PAUSED' }),
	},
	{
		name: 'confirm a recurring pattern from another user’s transactions',
		routes: ['POST /budget/recurring-series'],
		run: ({ sql, fx }) =>
			confirmRecurringPattern(sql, fx.a.userId, {
				fingerprint: 'cross-pattern',
				transaction_ids: [fx.b.expenseTxnId, fx.b.needsReviewTxnId, fx.b.deletedTxnId],
				treatment: 'INCLUDE',
			}),
	},

	// ---- merchants ----
	{
		name: 'set a merchant category rule with another user’s category',
		routes: ['PUT /merchants/:merchantId/category'],
		run: ({ sql, fx }) =>
			setUserMerchantCategory(sql, fx.a.userId, fx.sharedMerchantId, {
				transaction_type: 'expense',
				category_id: fx.b.expenseCategoryId,
				backfill: true,
			}),
	},
	{
		name: 'set a descriptor rule for a counterparty only the other user has',
		routes: ['PUT /merchants/:merchantId/descriptor-category'],
		run: ({ sql, fx }) =>
			setUserDescriptorCategory(sql, fx.a.userId, fx.sharedMerchantId, {
				search_key: fx.b.searchKey,
				transaction_type: 'expense',
				category_id: fx.a.expenseCategoryId,
				backfill: true,
			}),
	},
	{
		name: 'list a shared merchant’s transactions',
		routes: ['GET /merchants/:merchantId/transactions'],
		allowNoop: true,
		run: ({ sql, fx }) => listUserMerchantTransactions(sql, fx.a.userId, fx.sharedMerchantId, 100),
		assertResolved: (result, { fx }) => {
			const ids = (result as Array<{ transaction_id: string }>).map((row) => row.transaction_id);
			if (ids.includes(fx.b.expenseTxnId)) {
				throw new Error('merchant transaction list leaked another user’s transaction');
			}
		},
	},

	// ---- private payees (Task 4a): B's owned merchant is invisible to A ----
	{
		name: 'search merchants: the other user’s private payee never appears',
		routes: ['GET /merchants'],
		allowNoop: true,
		run: ({ sql, fx }) => listGlobalMerchants(sql, fx.a.userId, { q: null, limit: 1000 }),
		assertResolved: (result, { fx }) => {
			const ids = (result as Array<{ id: string }>).map((row) => row.id);
			if (ids.includes(fx.b.p2pMerchantId)) {
				throw new Error('merchant search leaked another user’s private payee');
			}
			if (!ids.includes(fx.a.p2pMerchantId) || !ids.includes(fx.sharedMerchantId)) {
				throw new Error('merchant search dropped the user’s own payee or a shared brand');
			}
		},
	},
	{
		name: 'set a merchant category rule on the other user’s private payee',
		routes: ['PUT /merchants/:merchantId/category'],
		run: ({ sql, fx }) =>
			setUserMerchantCategory(sql, fx.a.userId, fx.b.p2pMerchantId, {
				transaction_type: 'expense',
				category_id: fx.a.expenseCategoryId,
				backfill: true,
			}),
	},
	{
		name: 'set a descriptor rule on the other user’s private payee',
		routes: ['PUT /merchants/:merchantId/descriptor-category'],
		run: ({ sql, fx }) =>
			setUserDescriptorCategory(sql, fx.a.userId, fx.b.p2pMerchantId, {
				search_key: fx.a.searchKey,
				transaction_type: 'expense',
				category_id: fx.a.expenseCategoryId,
				backfill: true,
			}),
	},
	{
		name: 'list the other user’s private payee transactions',
		routes: ['GET /merchants/:merchantId/transactions'],
		allowNoop: true,
		run: ({ sql, fx }) => listUserMerchantTransactions(sql, fx.a.userId, fx.b.p2pMerchantId, 100),
		assertResolved: (result) => {
			if ((result as unknown[]).length !== 0) {
				throw new Error('private payee transaction list leaked another user’s rows');
			}
		},
	},
	{
		name: 'create a manual transaction linked to the other user’s private payee',
		routes: ['POST /transactions'],
		run: ({ sql, fx }) =>
			createManualTransaction(sql, fx.a.userId, {
				amount_in_paise: 1_000,
				type: 'expense',
				txn_date: '2026-09-08T10:00:00Z',
				account_id: fx.a.bankAccountId,
				category_id: fx.a.expenseCategoryId,
				merchant_id: fx.b.p2pMerchantId,
				user_note: null,
				instrument_id: null,
				counterparty_raw: null,
			}),
	},
	{
		name: 'patch a transaction onto the other user’s private payee',
		routes: ['PATCH /transactions/:transactionId'],
		run: ({ sql, fx }) =>
			updateTransaction(sql, fx.a.userId, fx.a.expenseTxnId, { merchant_id: fx.b.p2pMerchantId }),
	},
	{
		name: 'review a transaction onto the other user’s private payee',
		routes: ['POST /transactions/:transactionId/review'],
		run: ({ sql, fx }) =>
			reviewTransaction(sql, fx.a.userId, fx.a.needsReviewTxnId, { merchant_id: fx.b.p2pMerchantId }),
	},
];
