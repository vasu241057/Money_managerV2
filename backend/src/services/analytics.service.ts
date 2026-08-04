import type {
	AccountAllTimeSummary,
	AnalyticsSummaryResponse,
	MonthlyAccountSlice,
	MonthlyCategorySlice,
	MonthlySummary,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import { toNullableString, toRequiredString, toSafeInteger } from '../lib/db/serialization';

// All month bucketing happens in one timezone, chosen once. The frontend
// must bucket with the same zone or the two will disagree at month edges.
const SUMMARY_TIME_ZONE = 'Asia/Kolkata';

interface MonthTypeRowRaw {
	month: unknown;
	type: unknown;
	amount: unknown;
	cnt: unknown;
}

interface MonthCategoryRowRaw {
	month: unknown;
	category_id: unknown;
	category_name: unknown;
	amount: unknown;
	cnt: unknown;
}

interface MonthAccountRowRaw {
	month: unknown;
	account_id: unknown;
	expense: unknown;
	credit: unknown;
	cnt: unknown;
}

interface AccountAllTimeRowRaw {
	account_id: unknown;
	income: unknown;
	expense: unknown;
}

interface ReviewRowRaw {
	total: unknown;
	last_3_days: unknown;
}

function emptyMonth(month: string): MonthlySummary {
	return {
		month,
		income_in_paise: 0,
		expense_in_paise: 0,
		transfer_in_paise: 0,
		txn_count: 0,
		expense_by_category: [],
		expense_by_account: [],
	};
}

export async function getAnalyticsSummary(
	sql: SqlClient,
	userId: string,
): Promise<AnalyticsSummaryResponse> {
	const [totalRows, categoryRows, accountRows, allTimeAccountRows, reviewRows] = await Promise.all([
		sql<MonthTypeRowRaw[]>`
			select
				to_char(t.txn_date at time zone ${SUMMARY_TIME_ZONE}, 'YYYY-MM') as month,
				t.type::text as type,
				sum(t.amount_in_paise)::bigint as amount,
				count(*)::int as cnt
			from public.transactions as t
			where t.user_id = ${userId}
				and t.user_deleted_at is null
			group by 1, 2
		`,
		sql<MonthCategoryRowRaw[]>`
			select
				to_char(t.txn_date at time zone ${SUMMARY_TIME_ZONE}, 'YYYY-MM') as month,
				t.category_id,
				c.name as category_name,
				sum(t.amount_in_paise)::bigint as amount,
				count(*)::int as cnt
			from public.transactions as t
			left join public.categories as c on c.id = t.category_id
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and t.type = 'expense'
			group by 1, t.category_id, c.name
		`,
		sql<MonthAccountRowRaw[]>`
			select
				to_char(t.txn_date at time zone ${SUMMARY_TIME_ZONE}, 'YYYY-MM') as month,
				t.account_id,
				coalesce(
					sum(t.amount_in_paise) filter (where t.type = 'expense'),
					0
				)::bigint as expense,
				coalesce(
					sum(t.amount_in_paise) filter (
						where t.type = 'income' and a.type = 'card'
					),
					0
				)::bigint as credit,
				count(*)::int as cnt
			from public.transactions as t
			left join public.accounts as a
				on a.id = t.account_id
				and a.user_id = t.user_id
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and (
					t.type = 'expense'
					or (t.type = 'income' and a.type = 'card')
				)
			group by 1, t.account_id
		`,
		sql<AccountAllTimeRowRaw[]>`
			select
				t.account_id,
				coalesce(sum(t.amount_in_paise) filter (where t.type = 'income'), 0)::bigint as income,
				coalesce(sum(t.amount_in_paise) filter (where t.type = 'expense'), 0)::bigint as expense
			from public.transactions as t
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and t.account_id is not null
			group by t.account_id
		`,
		sql<ReviewRowRaw[]>`
			select
				count(*)::int as total,
				count(*) filter (
					where (t.txn_date at time zone ${SUMMARY_TIME_ZONE})::date
						>= ((now() at time zone ${SUMMARY_TIME_ZONE})::date - 2)
				)::int as last_3_days
			from public.transactions as t
			where t.user_id = ${userId}
				and t.user_deleted_at is null
				and t.status = 'NEEDS_REVIEW'
		`,
	]);

	const monthsByKey = new Map<string, MonthlySummary>();
	const monthFor = (key: string): MonthlySummary => {
		const existing = monthsByKey.get(key);
		if (existing) return existing;
		const created = emptyMonth(key);
		monthsByKey.set(key, created);
		return created;
	};

	let allTimeIncome = 0;
	let allTimeExpense = 0;
	for (const row of totalRows) {
		const month = monthFor(toRequiredString(row.month, 'summary.month'));
		const type = toRequiredString(row.type, 'summary.type');
		const amount = toSafeInteger(row.amount, 'summary.amount');
		const count = toSafeInteger(row.cnt, 'summary.cnt');
		month.txn_count += count;
		if (type === 'income') {
			month.income_in_paise += amount;
			allTimeIncome += amount;
		} else if (type === 'expense') {
			month.expense_in_paise += amount;
			allTimeExpense += amount;
		} else {
			month.transfer_in_paise += amount;
		}
	}

	for (const row of categoryRows) {
		const month = monthFor(toRequiredString(row.month, 'summary.month'));
		const slice: MonthlyCategorySlice = {
			category_id: toNullableString(row.category_id, 'summary.category_id'),
			category_name: toNullableString(row.category_name, 'summary.category_name'),
			amount_in_paise: toSafeInteger(row.amount, 'summary.amount'),
			txn_count: toSafeInteger(row.cnt, 'summary.cnt'),
		};
		month.expense_by_category.push(slice);
	}

	for (const row of accountRows) {
		const month = monthFor(toRequiredString(row.month, 'summary.month'));
		const expense = toSafeInteger(row.expense, 'summary.expense');
		const credit = toSafeInteger(row.credit, 'summary.credit');
		const slice: MonthlyAccountSlice = {
			account_id: toNullableString(row.account_id, 'summary.account_id'),
			expense_in_paise: expense,
			credit_in_paise: credit,
			net_spend_in_paise: expense - credit,
			txn_count: toSafeInteger(row.cnt, 'summary.cnt'),
		};
		month.expense_by_account.push(slice);
	}

	const accounts: AccountAllTimeSummary[] = allTimeAccountRows.map(row => ({
		account_id: toRequiredString(row.account_id, 'summary.account_id'),
		income_in_paise: toSafeInteger(row.income, 'summary.income'),
		expense_in_paise: toSafeInteger(row.expense, 'summary.expense'),
	}));

	const reviewRow = reviewRows[0];
	const review = {
		total: reviewRow ? toSafeInteger(reviewRow.total, 'summary.review_total') : 0,
		last_3_days: reviewRow ? toSafeInteger(reviewRow.last_3_days, 'summary.review_recent') : 0,
	};

	const months = Array.from(monthsByKey.values()).sort((a, b) =>
		a.month.localeCompare(b.month),
	);
	for (const month of months) {
		month.expense_by_category.sort((a, b) => b.amount_in_paise - a.amount_in_paise);
		month.expense_by_account.sort((a, b) => b.net_spend_in_paise - a.net_spend_in_paise);
	}

	return {
		months,
		all_time: { income_in_paise: allTimeIncome, expense_in_paise: allTimeExpense },
		accounts,
		review,
	};
}
