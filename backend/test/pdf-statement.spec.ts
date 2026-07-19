import { describe, expect, it } from 'vitest';

import {
	decodeBase64Url,
	extractStatementFieldsFromItems,
	pickPdfAttachment,
	validatePdfFields,
	type PdfTextItem,
} from '../src/lib/pdf-statement';

const REF_ISO = '2026-07-14T09:00:00.000Z';

function item(text: string, x: number, y: number, page = 1): PdfTextItem {
	return { text, x, y, page };
}

describe('extractStatementFieldsFromItems', () => {
	it('reads a Kotak-style layout (labels left, values right on the same row)', () => {
		const items = [
			item('Monthly statement for your Zen Credit Card X6331', 300, 700),
			item('Billing Date', 480, 640),
			item('25-Jun-2026', 700, 640),
			item('Total Amount Due (TAD)', 480, 620),
			item('₹ 17,246.67', 700, 620),
			item('Minimum Amount Due (MAD)', 480, 600),
			item('₹ 172.47', 700, 600),
			item('Due Date', 480, 580),
			item('13-Jul-2026', 700, 580),
			item('Transactions Details from 26-May-2026 to 25-Jun-2026', 250, 300, 2),
		];

		const fields = extractStatementFieldsFromItems(items, '2026-06-25T09:00:00.000Z');
		expect(fields).toEqual({
			card_last4: '6331',
			total_due_in_paise: 1724667,
			min_due_in_paise: 17247,
			due_date: '2026-07-13',
			period_start: '2026-05-26',
			period_end: '2026-06-25',
		});
	});

	it('reads an HDFC-style boxed layout (labels above values) with interleaved noise', () => {
		const items = [
			item('Credit Card No.', 300, 700),
			item('00361010XXXX9732', 500, 700),
			item('TOTAL AMOUNT DUE', 620, 600),
			item('MINIMUM DUE', 560, 560),
			item('DUE DATE', 660, 560),
			// values BELOW their labels — plus a decoy amount far away
			item('₹16,955.00', 622, 580),
			item('₹850.00', 558, 540),
			item('03 Aug, 2026', 658, 540),
			item('₹2,06,000.00', 100, 400), // credit limit decoy, wrong column
			item('Billing Period', 300, 660),
			item('15 Jun, 2026 - 14 Jul, 2026', 300, 645),
			// breakup table must not steal the total anchor
			item('Details of Total Amount Due (in ₹)', 80, 300),
		];

		const fields = extractStatementFieldsFromItems(items, REF_ISO);
		expect(fields).toEqual({
			card_last4: '9732',
			total_due_in_paise: 1695500,
			min_due_in_paise: 85000,
			due_date: '2026-08-03',
			period_start: '2026-06-15',
			period_end: '2026-07-14',
		});
	});

	it('reads a YES-style layout where labels and values share one run', () => {
		const items = [
			item('Statement for YES BANK Card Number 3561XXXXXXXX6279', 200, 700),
			item('Statement Period: 13/06/2026 To 12/07/2026', 150, 650),
			item('Total Amount Due: Rs. 34,073.18', 150, 600),
			item('Minimum Amount Due: Rs. 681.46', 150, 580),
			item('Payment Due Date: 01/08/2026', 150, 560),
		];

		const fields = extractStatementFieldsFromItems(items, '2026-07-12T09:00:00.000Z');
		expect(fields).toEqual({
			card_last4: '6279',
			total_due_in_paise: 3407318,
			min_due_in_paise: 68146,
			due_date: '2026-08-01',
			period_start: '2026-06-13',
			period_end: '2026-07-12',
		});
	});

	it('reads a Slice-style layout (unlabeled yearless period, "Due on", currency-marked amounts)', () => {
		// Statement email arrives ~21 Apr; cycle 21 Mar - 20 Apr, due 5 May.
		const items = [
			item("Vasu's Credit card statement", 100, 720),
			item('XXXX XXXX XXXX 3660', 100, 700),
			item('21 MAR - 20 APR', 100, 680),
			item('₹1,999', 120, 640),
			item('.00', 210, 640),
			item('Due on 5 May', 120, 620),
			item('Total amount due', 100, 300),
			item('₹1,999.00', 600, 300),
			item('Min amount due ₹200.00', 100, 285),
		];

		const fields = extractStatementFieldsFromItems(items, '2026-04-21T09:00:00.000Z');
		expect(fields).toEqual({
			card_last4: '3660',
			total_due_in_paise: 199900,
			min_due_in_paise: 20000,
			due_date: '2026-05-05',
			period_start: '2026-03-21',
			period_end: '2026-04-20',
		});
	});

	it('anchors a December-cycle period across the year boundary', () => {
		const items = [item('21 DEC - 20 JAN', 100, 680)];
		const fields = extractStatementFieldsFromItems(items, '2027-01-21T09:00:00.000Z');
		expect(fields.period_start).toBe('2026-12-21');
		expect(fields.period_end).toBe('2027-01-20');
	});

	it('returns all-null fields for an empty or label-less document', () => {
		expect(extractStatementFieldsFromItems([], REF_ISO)).toEqual({
			card_last4: null,
			total_due_in_paise: null,
			min_due_in_paise: null,
			due_date: null,
			period_start: null,
			period_end: null,
		});
	});

	it('validatePdfFields nulls a min above total and an implausible due date', () => {
		const validated = validatePdfFields(
			{
				card_last4: '1234',
				total_due_in_paise: 1000,
				min_due_in_paise: 5000,
				due_date: '2027-06-01',
				period_start: null,
				period_end: null,
			},
			REF_ISO,
		);
		expect(validated.min_due_in_paise).toBeNull();
		expect(validated.due_date).toBeNull();
		expect(validated.total_due_in_paise).toBe(1000);
	});
});

describe('pickPdfAttachment', () => {
	it('walks nested parts and prefers the largest PDF', () => {
		const picked = pickPdfAttachment({
			mimeType: 'multipart/mixed',
			parts: [
				{ mimeType: 'text/html', body: { data: 'x' } },
				{
					mimeType: 'multipart/alternative',
					parts: [
						{
							mimeType: 'application/pdf',
							filename: 'logo.pdf',
							body: { attachmentId: 'small', size: 100 },
						},
					],
				},
				{
					mimeType: 'application/octet-stream',
					filename: '400078_statement.PDF',
					body: { attachmentId: 'big', size: 90000 },
				},
			],
		});
		expect(picked).toEqual({ attachmentId: 'big', filename: '400078_statement.PDF' });
	});

	it('returns null when no PDF part exists', () => {
		expect(
			pickPdfAttachment({ mimeType: 'text/html', body: { data: 'x' } }),
		).toBeNull();
	});
});

describe('decodeBase64Url', () => {
	it('decodes url-safe base64 with missing padding', () => {
		// "PDF-1.7" in base64url
		const bytes = decodeBase64Url('UERGLTEuNw');
		expect(new TextDecoder().decode(bytes)).toBe('PDF-1.7');
	});
});
