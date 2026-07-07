import { describe, expect, it } from 'vitest';

import {
	isAllowedSender,
	parseFromAddress,
	resolveSenderGateConfig,
} from '../src/lib/sender-gate';

function envWith(overrides: Record<string, string>): Env {
	return overrides as unknown as Env;
}

describe('sender-gate', () => {
	describe('parseFromAddress', () => {
		it('extracts the address from a display-name header', () => {
			expect(parseFromAddress('HDFC Bank InstaAlerts <alerts@hdfcbank.net>')).toBe(
				'alerts@hdfcbank.net',
			);
		});

		it('normalizes bare addresses to lowercase', () => {
			expect(parseFromAddress('Alerts@ICICIBANK.com')).toBe('alerts@icicibank.com');
		});

		it('returns null for empty or unparseable headers', () => {
			expect(parseFromAddress(null)).toBeNull();
			expect(parseFromAddress('   ')).toBeNull();
			expect(parseFromAddress('not an email header')).toBeNull();
		});
	});

	describe('isAllowedSender', () => {
		const entries = ['federal', 'hdfc', 'sbi.co.in', 'payments-noreply@google.com'];

		function allowed(header: string | null): boolean {
			return isAllowedSender(header, parseFromAddress(header), entries);
		}

		it('matches brand tokens anywhere in the From header', () => {
			// Address domain carries the brand.
			expect(allowed('Federal Bank <no-reply@federal.bank>')).toBe(true);
			expect(allowed('alerts@federalbank.co.in')).toBe(true);
			// Only the display name carries the brand (ESP-hosted sender).
			expect(allowed('HDFC Bank InstaAlerts <mailer@some-esp.example>')).toBe(true);
		});

		it('accepts brand-token lookalikes by design (rejected downstream)', () => {
			expect(allowed('hdfc@gmail.com')).toBe(true);
		});

		it('matches domain entries exactly or by subdomain suffix', () => {
			expect(allowed('alerts@sbi.co.in')).toBe(true);
			expect(allowed('alerts@alerts.sbi.co.in')).toBe(true);
			expect(allowed('alerts@sbi.co.in.evil.com')).toBe(false);
		});

		it('allows exact full-address entries without widening the domain', () => {
			expect(allowed('Google Pay <payments-noreply@google.com>')).toBe(true);
			expect(allowed('someone-else@google.com')).toBe(false);
		});

		it('rejects headers with no matching entry', () => {
			expect(allowed('A Friend <friend@example.com>')).toBe(false);
			expect(allowed(null)).toBe(false);
		});
	});

	describe('resolveSenderGateConfig', () => {
		it('defaults to enabled with no built-in entries (sender_rules DB is the source)', () => {
			const config = resolveSenderGateConfig(envWith({}));
			expect(config.enabled).toBe(true);
			expect(config.entries).toEqual([]);
		});

		it('can be disabled via EMAIL_SENDER_GATE=off', () => {
			expect(resolveSenderGateConfig(envWith({ EMAIL_SENDER_GATE: 'off' })).enabled).toBe(false);
			expect(resolveSenderGateConfig(envWith({ EMAIL_SENDER_GATE: 'on' })).enabled).toBe(true);
		});

		it('appends extra entries from EMAIL_SENDER_ALLOWLIST_EXTRA', () => {
			const config = resolveSenderGateConfig(
				envWith({ EMAIL_SENDER_ALLOWLIST_EXTRA: ' alerts@mybank.com , mybank.co.in ,' }),
			);
			expect(config.entries).toContain('alerts@mybank.com');
			expect(config.entries).toContain('mybank.co.in');
		});
	});
});
