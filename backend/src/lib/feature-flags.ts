const ENABLED_VALUES = new Set(['1', 'on', 'true', 'yes']);

/**
 * Merchant mapping is deliberately fail-closed. It can change global merchant
 * identities, aliases, and recent transaction links, so an absent, misspelled,
 * or unrecognized value must never enable it.
 */
export function isMerchantMappingEnabled(env: Env): boolean {
	const normalized = env.MERCHANT_MAPPING_ENABLED?.trim().toLowerCase();
	return normalized !== undefined && ENABLED_VALUES.has(normalized);
}
