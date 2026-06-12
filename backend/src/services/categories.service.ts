import type {
	CategoryRow,
	CreateCategoryRequest,
	UpdateCategoryRequest,
} from '../../../shared/types';
import type { SqlClient } from '../lib/db/client';
import {
	toBoolean,
	toIsoDateTime,
	toNullableString,
	toRequiredString,
} from '../lib/db/serialization';
import { badRequest, notFound } from '../lib/http/errors';
import {
	asRecord,
	parseNullableString,
	parseNullableUuid,
	parseRequiredString,
} from '../lib/http/validation';

const CATEGORY_TYPES = new Set(['income', 'expense', 'transfer'] as const);

interface CategoryRowRaw {
	id: unknown;
	user_id: unknown;
	parent_id: unknown;
	name: unknown;
	type: unknown;
	icon: unknown;
	is_system: unknown;
	created_at: unknown;
}

interface ParentCategoryRow {
	id: string;
	user_id: string | null;
	type: CategoryRow['type'];
	parent_id?: string | null;
}

function parseCategoryType(value: unknown, fieldName: string): CategoryRow['type'] {
	if (typeof value !== 'string' || !CATEGORY_TYPES.has(value as CategoryRow['type'])) {
		throw badRequest(
			'INVALID_PAYLOAD',
			`${fieldName} must be one of: income, expense, transfer`,
		);
	}

	return value as CategoryRow['type'];
}

function mapCategoryRow(row: CategoryRowRaw): CategoryRow {
	return {
		id: toRequiredString(row.id, 'categories.id'),
		user_id: toNullableString(row.user_id, 'categories.user_id'),
		parent_id: toNullableString(row.parent_id, 'categories.parent_id'),
		name: toRequiredString(row.name, 'categories.name'),
		type: toRequiredString(row.type, 'categories.type') as CategoryRow['type'],
		icon: toNullableString(row.icon, 'categories.icon'),
		is_system: toBoolean(row.is_system, 'categories.is_system'),
		created_at: toIsoDateTime(row.created_at, 'categories.created_at'),
	};
}

export function parseCreateCategoryRequest(payload: unknown): CreateCategoryRequest {
	const body = asRecord(payload);

	return {
		name: parseRequiredString(body.name, 'name'),
		type: parseCategoryType(body.type, 'type'),
		icon: parseNullableString(body.icon, 'icon'),
		parent_id: parseNullableUuid(body.parent_id, 'parent_id'),
	};
}

export function parseUpdateCategoryRequest(payload: unknown): UpdateCategoryRequest {
	const body = asRecord(payload);
	const updateRequest: UpdateCategoryRequest = {};

	if (body.name !== undefined) {
		updateRequest.name = parseRequiredString(body.name, 'name');
	}

	if (body.type !== undefined) {
		updateRequest.type = parseCategoryType(body.type, 'type');
	}

	if (body.icon !== undefined) {
		updateRequest.icon = parseNullableString(body.icon, 'icon');
	}

	if (body.parent_id !== undefined) {
		updateRequest.parent_id = parseNullableUuid(body.parent_id, 'parent_id');
	}

	if (Object.keys(updateRequest).length === 0) {
		throw badRequest('INVALID_PAYLOAD', 'At least one category field must be provided for update');
	}

	return updateRequest;
}

async function validateParentCategory(
	sql: SqlClient,
	userId: string,
	parentId: string | null,
	childType: CategoryRow['type'],
): Promise<void> {
	if (!parentId) {
		return;
	}

	const parentRows = await sql<ParentCategoryRow[]>`
		select c.id, c.user_id, c.type
		from public.categories as c
		where c.id = ${parentId}
		limit 1
	`;

	if (parentRows.length === 0) {
		throw badRequest('INVALID_PAYLOAD', 'parent_id must reference an existing category');
	}

	const parent = parentRows[0];
	if (parent.user_id !== null && parent.user_id !== userId) {
		throw badRequest(
			'INVALID_PAYLOAD',
			'parent_id must belong to the same user or be a system category',
		);
	}

	if (parent.type !== childType) {
		throw badRequest(
			'INVALID_PAYLOAD',
			'parent_id category type must match the child category type',
		);
	}
}

async function validateCategoryCycle(
	sql: SqlClient,
	categoryId: string,
	parentId: string | null,
): Promise<void> {
	if (!parentId) {
		return;
	}

	if (parentId === categoryId) {
		throw badRequest('INVALID_PAYLOAD', 'parent_id cannot reference the category itself');
	}

	const cycleRows = await sql<{ id: string }[]>`
		with recursive parent_chain as (
			select c.id, c.parent_id
			from public.categories as c
			where c.id = ${parentId}

			union all

			select p.id, p.parent_id
			from public.categories as p
			join parent_chain as pc
				on p.id = pc.parent_id
		)
		select pc.id
		from parent_chain as pc
		where pc.id = ${categoryId}
		limit 1
	`;

	if (cycleRows.length > 0) {
		throw badRequest('INVALID_PAYLOAD', 'parent_id cannot reference a descendant category');
	}
}

export async function listCategories(
	sql: SqlClient,
	userId: string,
	type?: CategoryRow['type'],
): Promise<CategoryRow[]> {
	const hasTypeFilter = Boolean(type);
	const rows = await sql<CategoryRowRaw[]>`
		select
			c.id,
			c.user_id,
			c.parent_id,
			c.name,
			c.type,
			c.icon,
			c.is_system,
			c.created_at
		from public.categories as c
		where (c.user_id = ${userId} or c.is_system = true)
			and (not ${hasTypeFilter} or c.type = ${type ?? null})
		order by c.is_system desc, c.name asc, c.id asc
	`;

	return rows.map(mapCategoryRow);
}

export async function createCategory(
	sql: SqlClient,
	userId: string,
	input: CreateCategoryRequest,
): Promise<CategoryRow> {
	await validateParentCategory(sql, userId, input.parent_id ?? null, input.type);

	const rows = await sql<CategoryRowRaw[]>`
		insert into public.categories (
			user_id,
			parent_id,
			name,
			type,
			icon,
			is_system
		)
		values (
			${userId},
			${input.parent_id ?? null},
			${input.name},
			${input.type},
			${input.icon ?? null},
			false
		)
		returning
			id,
			user_id,
			parent_id,
			name,
			type,
			icon,
			is_system,
			created_at
	`;

	if (rows.length === 0) {
		throw badRequest('CATEGORY_CREATE_FAILED', 'Failed to create category');
	}

	return mapCategoryRow(rows[0]);
}

export async function updateCategory(
	sql: SqlClient,
	userId: string,
	categoryId: string,
	input: UpdateCategoryRequest,
): Promise<CategoryRow> {
	const existingRows = await sql<ParentCategoryRow[]>`
		select c.id, c.user_id, c.type, c.parent_id
		from public.categories as c
		where c.id = ${categoryId}
		limit 1
	`;

	if (existingRows.length === 0 || existingRows[0].user_id !== userId) {
		throw notFound('CATEGORY_NOT_FOUND', 'Category not found');
	}

	const finalType = input.type ?? existingRows[0].type;
	const finalParentId =
		input.parent_id !== undefined ? (input.parent_id ?? null) : (existingRows[0].parent_id ?? null);

	if (input.parent_id !== undefined || input.type !== undefined) {
		await validateCategoryCycle(sql, categoryId, finalParentId);
		await validateParentCategory(sql, userId, finalParentId, finalType);
	}

	const hasNameUpdate = input.name !== undefined;
	const hasTypeUpdate = input.type !== undefined;
	const hasIconUpdate = input.icon !== undefined;
	const hasParentUpdate = input.parent_id !== undefined;

	const rows = await sql<CategoryRowRaw[]>`
		update public.categories as c
		set
			name = case when ${hasNameUpdate} then ${input.name ?? null} else c.name end,
			type = case when ${hasTypeUpdate} then ${input.type ?? null} else c.type end,
			icon = case when ${hasIconUpdate} then ${input.icon ?? null} else c.icon end,
			parent_id = case when ${hasParentUpdate} then ${input.parent_id ?? null} else c.parent_id end
		where c.id = ${categoryId}
			and c.user_id = ${userId}
			and c.is_system = false
		returning
			id,
			user_id,
			parent_id,
			name,
			type,
			icon,
			is_system,
			created_at
	`;

	if (rows.length === 0) {
		throw notFound('CATEGORY_NOT_FOUND', 'Category not found');
	}

	return mapCategoryRow(rows[0]);
}

export async function deleteCategory(
	sql: SqlClient,
	userId: string,
	categoryId: string,
): Promise<void> {
	const rows = await sql<{ id: string }[]>`
		delete from public.categories as c
		where c.id = ${categoryId}
			and c.user_id = ${userId}
			and c.is_system = false
		returning c.id
	`;

	if (rows.length === 0) {
		throw notFound('CATEGORY_NOT_FOUND', 'Category not found');
	}
}
