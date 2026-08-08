const DISABLED_VALUES = new Set(['0', 'off', 'false', 'no']);

/**
 * Break-glass override for merchant mapping. The schedule in job_schedules is
 * the normal on/off switch (that is what the admin UI writes); this exists only
 * so the mapper can be forced off from the environment without a database
 * round trip, and it can never force it *on*.
 *
 * Unset — the overwhelmingly common case — means "the schedule decides", and
 * the schedule ships disabled, so an absent variable still maps to a mapper
 * that does not run until someone deliberately enables it.
 */
export function isMerchantMappingKillSwitched(env: Env): boolean {
	const normalized = env.MERCHANT_MAPPING_ENABLED?.trim().toLowerCase();
	return normalized !== undefined && DISABLED_VALUES.has(normalized);
}
