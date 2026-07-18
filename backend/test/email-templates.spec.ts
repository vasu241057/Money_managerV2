import { describe, expect, it } from 'vitest';

import type { UUID } from '../../shared/types';
import type { EmailTemplate, TemplateSlotMap } from '../src/lib/email-templates';
import {
	applyTemplate,
	compileSkeleton,
	induceExtractTemplate,
	induceIgnoreTemplate,
	normalizeForTemplate,
	selfVerifyTemplate,
	skeletonHash,
} from '../src/lib/email-templates';
import { maskNumericTokens } from '../src/lib/masking';

const TEMPLATE_ID = '00000000-0000-4000-8000-0000000000t1' as UUID;
const INTERNAL_DATE = '2026-07-01T10:00:00.000Z';

// Masking order: N1=123.45, N2=9876, N3=05, N4=07, N5=26,
// N6=5544332211 (ref), N7=18002586161 (helpline).
const SOURCE_TEXT =
	'Dear Customer, Rs.123.45 has been debited from your account **9876 on 05-07-26 to VPA zepto@ybl. UPI Ref No 5544332211. Not you? Call 18002586161.';

// Same format: different merchant, amount, date, ref length.
const VARIANT_TEXT =
	'Dear Customer, Rs.9,999.00 has been debited from your account **1234 on 06-07-26 to VPA swiggy@icici. UPI Ref No 99887766550011. Not you? Call 18002586161.';

function induceFromSource() {
	const masked = maskNumericTokens(SOURCE_TEXT);
	return induceExtractTemplate({
		masked_text: masked.masked,
		tokens: masked.tokens,
		amount_token: 'N1',
		date_text: '<N3>-<N4>-<N5>',
		instrument_token: 'N2',
		counterparty_raw_masked: 'zepto@ybl',
		direction: 'debit',
		payment_method: 'upi',
		confidence: 0.9,
	});
}

function asTemplate(skeleton: string, slotMap: TemplateSlotMap): EmailTemplate {
	return {
		id: TEMPLATE_ID,
		from_address: 'alerts@hdfcbank.net',
		skeleton,
		slot_map: slotMap,
		hit_count: 0,
		failure_count: 0,
	};
}

describe('email template induction', () => {
	it('induces a skeleton with slot ordinals, a merchant wildcard, and ref wildcards', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		// Boilerplate stays literal; merchant span and unreferenced numbers
		// (ref, helpline) become wildcards.
		expect(induced.skeleton).toContain('has been debited from your account');
		expect(induced.skeleton).toContain('{{M}}');
		expect(induced.skeleton).toContain('{{W}}');
		expect(induced.skeleton).not.toContain('zepto');
		expect(induced.skeleton).not.toContain('5544332211');

		// Reading order: amount, instrument, then the three date parts.
		expect(induced.slot_map.kind).toBe('EXTRACT');
		expect(induced.slot_map.amount_ordinal).toBe(1);
		expect(induced.slot_map.instrument_ordinal).toBe(2);
		expect(induced.slot_map.date_pattern).toBe('{{3}}-{{4}}-{{5}}');
		expect(induced.slot_map.merchant).toEqual({ kind: 'WILDCARD' });
		expect(induced.slot_map.direction).toBe('debit');
	});

	it('self-verifies the induced template against its own source email', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		// 05-07-26 (DD-MM-YY) midnight IST.
		const verified = selfVerifyTemplate(
			induced.skeleton,
			induced.slot_map,
			SOURCE_TEXT,
			INTERNAL_DATE,
			{
				amount_in_paise: 12_345,
				txn_timestamp: '2026-07-04T18:30:00.000Z',
				instrument_last4: '9876',
				counterparty_raw: 'zepto@ybl',
			},
		);
		expect(verified).toBe(true);

		// A wrong expectation (e.g. the AI had answered differently) fails.
		const misVerified = selfVerifyTemplate(
			induced.skeleton,
			induced.slot_map,
			SOURCE_TEXT,
			INTERNAL_DATE,
			{
				amount_in_paise: 99_999,
				txn_timestamp: '2026-07-04T18:30:00.000Z',
				instrument_last4: '9876',
				counterparty_raw: 'zepto@ybl',
			},
		);
		expect(misVerified).toBe(false);
	});

	it('falls back to a FIXED merchant when the name appears more than once', () => {
		const text =
			'Payment alert from your bank. Payment of Rs.100.00 to Swiggy completed on 05-07-26. Swiggy thanks you for ordering. Ref 1234567890.';
		const masked = maskNumericTokens(text);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N2>-<N3>-<N4>',
			instrument_token: null,
			counterparty_raw_masked: 'Swiggy',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.8,
		});

		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.slot_map.merchant).toEqual({ kind: 'FIXED', name: 'Swiggy' });
		expect(induced.skeleton).not.toContain('{{M}}');
	});

	it('refuses to learn a FIXED merchant whose name contains volatile digits', () => {
		// The model paraphrased the counterparty, so the span is not found in
		// the text (0 occurrences → FIXED), and the name carries this email's
		// digits — replaying "POS 4521" onto future emails would go stale.
		// N1=99.00, N2=4521, N3=05, N4=07, N5=26, N6=ref, N7=helpline.
		const text =
			'Alert: Rs.99.00 spent at POS 4521 KIRANA STORE on 05-07-26 using your card. Ref 5544332211. Call 18002586161 for disputes.';
		const masked = maskNumericTokens(text);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N3>-<N4>-<N5>',
			instrument_token: null,
			counterparty_raw_masked: 'KIRANA STORE (POS <N2>)',
			direction: 'debit',
			payment_method: 'card',
			confidence: 0.8,
		});
		expect(induced.ok).toBe(false);
		if (induced.ok) return;
		expect(induced.reason).toContain('volatile');
	});

	it('still learns a WILDCARD merchant whose digits vary per email (VPA with digits)', () => {
		// N1=123.45, N2=9876, N3=05, N4=07, N5=26, N6=4521, N7=ref, N8=helpline.
		const text =
			'Dear Customer, Rs.123.45 has been debited from your account **9876 on 05-07-26 to VPA store4521@ybl. UPI Ref No 5544332211. Not you? Call 18002586161.';
		const masked = maskNumericTokens(text);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N3>-<N4>-<N5>',
			instrument_token: 'N2',
			counterparty_raw_masked: 'store<N6>@ybl',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.9,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.slot_map.merchant).toEqual({ kind: 'WILDCARD' });

		// The wildcard resolves each email's own digits — nothing goes stale.
		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			text.replace('Rs.123.45', 'Rs.50.00').replace('store4521@ybl', 'market77@icici'),
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('extracted');
		if (applied.outcome !== 'extracted') return;
		expect(applied.extraction.counterparty_raw).toBe('market77@icici');
		expect(applied.extraction.amount_in_paise).toBe(5_000);
	});

	it('refuses to learn from an email that is mostly numbers (static-text guard)', () => {
		const text = 'Rs.10.00 to a@b on 05-07-26';
		const masked = maskNumericTokens(text);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N2>-<N3>-<N4>',
			instrument_token: null,
			counterparty_raw_masked: 'a@b',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.8,
		});

		expect(induced.ok).toBe(false);
		if (induced.ok) return;
		expect(induced.reason).toContain('generic');
	});

	it('refuses to learn when the amount token is not in the email (hallucinated ref)', () => {
		const masked = maskNumericTokens(SOURCE_TEXT);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N99',
			date_text: null,
			instrument_token: null,
			counterparty_raw_masked: 'zepto@ybl',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.8,
		});
		expect(induced.ok).toBe(false);
	});

	it('drops a date whose refs are hallucinated instead of failing induction', () => {
		const masked = maskNumericTokens(SOURCE_TEXT);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N88>-<N89>',
			instrument_token: 'N2',
			counterparty_raw_masked: 'zepto@ybl',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.8,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.slot_map.date_pattern).toBeNull();
	});
});

describe('email template matching', () => {
	it('extracts a different merchant/amount/date from the same format with no model involvement', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			VARIANT_TEXT,
			INTERNAL_DATE,
		);

		expect(applied.outcome).toBe('extracted');
		if (applied.outcome !== 'extracted') return;
		expect(applied.extraction.amount_in_paise).toBe(999_900);
		expect(applied.extraction.counterparty_raw).toBe('swiggy@icici');
		expect(applied.extraction.instrument_last4).toBe('1234');
		// 06-07-26 (DD-MM-YY) midnight IST.
		expect(applied.extraction.txn_timestamp).toBe('2026-07-05T18:30:00.000Z');
		expect(applied.extraction.direction).toBe('debit');
		expect(applied.extraction.payment_method).toBe('upi');
		expect(applied.extraction.confidence).toBe(0.9);
	});

	it('survives whitespace drift and ref words whose digit runs split differently', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		// Extra whitespace/newlines plus an alphanumeric ref that masks into
		// multiple placeholders inside one word ({{W}} covers the whole word,
		// so slot ordinals stay stable).
		const driftedText =
			'Dear Customer,  Rs.55.00 has been\ndebited from your   account **9876 on 05-07-26 to VPA drift@upi. UPI Ref No AB12CD34EF. Not you? Call 18002586161.';

		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			driftedText,
			INTERNAL_DATE,
		);

		expect(applied.outcome).toBe('extracted');
		if (applied.outcome !== 'extracted') return;
		expect(applied.extraction.amount_in_paise).toBe(5_500);
		expect(applied.extraction.counterparty_raw).toBe('drift@upi');
	});

	it('does not match a structurally different email (extra sentence)', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		const differentText =
			'Dear Customer, Rs.100.00 has been debited from your account **9876 on 05-07-26 to VPA x@upi. UPI Ref No 5544332211. Your available balance is Rs.5,000.00. Not you? Call 18002586161.';

		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			differentText,
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('no_match');
	});

	it('learns a month wildcard so month-name dates work across months', () => {
		// N1=200.00, N2=9876, N3=05, N4=26, ref N5.
		const monthText =
			'Dear Customer, Rs.200.00 has been debited from your account **9876 on 05-Jul-26 to VPA month@upi. UPI Ref No 5544332211. Not you? Call 18002586161.';
		const masked = maskNumericTokens(monthText);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N3>-Jul-<N4>',
			instrument_token: 'N2',
			counterparty_raw_masked: 'month@upi',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.9,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.skeleton).toContain('{{MON}}');
		expect(induced.skeleton).not.toContain('Jul');

		// Still reproduces its own (July) source answer.
		expect(
			selfVerifyTemplate(induced.skeleton, induced.slot_map, monthText, INTERNAL_DATE, {
				amount_in_paise: 20_000,
				txn_timestamp: '2026-07-04T18:30:00.000Z',
				instrument_last4: '9876',
				counterparty_raw: 'month@upi',
			}),
		).toBe(true);

		// An August email of the same format now resolves locally too.
		const augustApplied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			monthText.replace('05-Jul-26', '09-Aug-26').replace('month@upi', 'other@upi'),
			INTERNAL_DATE,
		);
		expect(augustApplied.outcome).toBe('extracted');
		if (augustApplied.outcome !== 'extracted') return;
		// 09-Aug-26 midnight IST.
		expect(augustApplied.extraction.txn_timestamp).toBe('2026-08-08T18:30:00.000Z');
		expect(augustApplied.extraction.counterparty_raw).toBe('other@upi');

		// A non-month word in the month slot is a different email: no match
		// (falls through to AI), not a template failure.
		const bogusApplied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			monthText.replace('05-Jul-26', '05-Xyz-26'),
			INTERNAL_DATE,
		);
		expect(bogusApplied.outcome).toBe('no_match');
	});

	it('handles "Mon DD, YYYY" formats across months (ICICI style)', () => {
		// N1=4321, N2=1,250.00, N3="18," (masking keeps the trailing comma
		// inside the token), N4=2026, N5=9215676766 — so the model sees
		// "on May <N3> <N4>" and copies it comma-free.
		const iciciText =
			'Dear Customer, your ICICI Bank Credit Card XX4321 has been used for a transaction of Rs.1,250.00 on May 18, 2026 at AMAZON PAY. Info: SMS BLOCK to 9215676766.';
		const masked = maskNumericTokens(iciciText);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N2',
			date_text: 'May <N3> <N4>',
			instrument_token: 'N1',
			counterparty_raw_masked: 'AMAZON PAY',
			direction: 'debit',
			payment_method: 'card',
			confidence: 0.9,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.skeleton).toContain('{{MON}}');

		const juneApplied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			iciciText.replace('May 18, 2026', 'June 7, 2026').replace('AMAZON PAY', 'SWIGGY LTD'),
			INTERNAL_DATE,
		);
		expect(juneApplied.outcome).toBe('extracted');
		if (juneApplied.outcome !== 'extracted') return;
		// June 7, 2026 midnight IST.
		expect(juneApplied.extraction.txn_timestamp).toBe('2026-06-06T18:30:00.000Z');
		expect(juneApplied.extraction.counterparty_raw).toBe('SWIGGY LTD');
		expect(juneApplied.extraction.instrument_last4).toBe('4321');
	});

	it('drops a date pattern containing digits the model fabricated', () => {
		const masked = maskNumericTokens(SOURCE_TEXT);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N3>-<N4>-2099',
			instrument_token: 'N2',
			counterparty_raw_masked: 'zepto@ybl',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.8,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.slot_map.date_pattern).toBeNull();
	});

	it('drops a date pattern whose month name has no counterpart in the email', () => {
		// The email is numeric (05-07-26) but the model answered "05-Jul-26";
		// baking "Jul" into the pattern would stamp July onto future matches.
		const masked = maskNumericTokens(SOURCE_TEXT);
		const induced = induceExtractTemplate({
			masked_text: masked.masked,
			tokens: masked.tokens,
			amount_token: 'N1',
			date_text: '<N3>-Jul-<N5>',
			instrument_token: 'N2',
			counterparty_raw_masked: 'zepto@ybl',
			direction: 'debit',
			payment_method: 'upi',
			confidence: 0.8,
		});
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.slot_map.date_pattern).toBeNull();
	});

	it('extracts a large amount instead of failing the template (review is forced downstream)', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		// ₹99,00,000 — far over the review threshold. The template must still
		// extract it (a big transaction is not a template defect); the engine
		// forces NEEDS_REVIEW at persist time.
		const hugeText = VARIANT_TEXT.replace('Rs.9,999.00', 'Rs.99,00,000.00');
		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			hugeText,
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('extracted');
		if (applied.outcome !== 'extracted') return;
		expect(applied.extraction.amount_in_paise).toBe(990_000_000);
	});

	it('reports matched_invalid for a corrupt slot map (amount ordinal out of range)', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		const applied = applyTemplate(
			asTemplate(induced.skeleton, { ...induced.slot_map, amount_ordinal: 99 }),
			VARIANT_TEXT,
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('matched_invalid');
	});

	it('falls back to the email internal date when the template has no date pattern', () => {
		const induced = induceFromSource();
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		const applied = applyTemplate(
			asTemplate(induced.skeleton, { ...induced.slot_map, date_pattern: null }),
			VARIANT_TEXT,
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('extracted');
		if (applied.outcome !== 'extracted') return;
		expect(applied.extraction.txn_timestamp).toBe(INTERNAL_DATE);
	});
});

describe('IGNORE templates', () => {
	const OTP_TEXT =
		'Dear Customer, your OTP for netbanking login is 482913. It is valid for 10 minutes. Do not share it with anyone for safety reasons.';

	it('learns and applies an IGNORE template for recurring non-financial formats', () => {
		const masked = maskNumericTokens(OTP_TEXT);
		const induced = induceIgnoreTemplate(OTP_TEXT, masked.masked);
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;
		expect(induced.slot_map.kind).toBe('IGNORE');

		expect(
			selfVerifyTemplate(induced.skeleton, induced.slot_map, OTP_TEXT, INTERNAL_DATE, null),
		).toBe(true);

		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			OTP_TEXT.replace('482913', '771122'),
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('ignore');
	});

	it('never learns an IGNORE template from an email that mentions money', () => {
		const text =
			'Dear Customer, Rs.500.00 was debited for your order 12345. This is a confirmation, no action needed from your side.';
		const masked = maskNumericTokens(text);
		const induced = induceIgnoreTemplate(text, masked.masked);
		expect(induced.ok).toBe(false);
		if (induced.ok) return;
		expect(induced.reason).toContain('money');
	});

	it('does not ignore a structurally different email from the same sender', () => {
		const masked = maskNumericTokens(OTP_TEXT);
		const induced = induceIgnoreTemplate(OTP_TEXT, masked.masked);
		expect(induced.ok).toBe(true);
		if (!induced.ok) return;

		const applied = applyTemplate(
			asTemplate(induced.skeleton, induced.slot_map),
			'Dear Customer, Rs.750.00 has been debited from your account **9876 today.',
			INTERNAL_DATE,
		);
		expect(applied.outcome).toBe('no_match');
	});
});

describe('skeleton plumbing', () => {
	it('escapes regex specials in static text', () => {
		const skeleton = normalizeForTemplate('Total (incl. tax): Rs.<N1> [paid] *thanks*').replace(
			'<N1>',
			'{{N}}',
		);
		const compiled = compileSkeleton(skeleton);
		const target = normalizeForTemplate('Total (incl. tax): Rs.<N7> [paid] *thanks*');
		expect(compiled.regex.test(target)).toBe(true);
		expect(compiled.regex.test(target.replace('[paid]', 'Xpaid1'))).toBe(false);
	});

	it('hashes skeletons stably and distinctly', () => {
		expect(skeletonHash('a {{N}} b')).toBe(skeletonHash('a {{N}} b'));
		expect(skeletonHash('a {{N}} b')).not.toBe(skeletonHash('a {{N}} c'));
	});
});
