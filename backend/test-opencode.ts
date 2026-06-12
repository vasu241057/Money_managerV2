// Quick smoke test for OpenCode Go API integration
// Usage: npx tsx test-opencode.ts
// Set OPENCODE_GO_API_KEY in backend/.dev.vars or export it

const API_KEY = process.env.OPENCODE_GO_API_KEY || '';
const BASE_URL = 'https://opencode.ai/zen/go/v1/chat/completions';

const SYSTEM_PROMPT = `You are a financial transaction extractor for Indian banking and UPI SMS/email alerts.
Extract ALL monetary transactions from the email text.

Required JSON output format (strict — no other keys):
{
  "transactions": [
    {
      "direction": "debit" | "credit",
      "amount_in_paise": number,
      "txn_timestamp": "ISO-8601 string",
      "payment_method": "upi" | "credit_card" | "debit_card" | "netbanking" | "cash" | "unknown",
      "counterparty_raw": "string or null",
      "instrument_last4": "string or null",
      "category_guess": "food" | "transport" | "shopping" | "utilities" | "entertainment" | "health" | "education" | "rent" | "salary" | "investment" | "transfer" | "insurance" | "travel" | "subscription" | "other" | null,
      "confidence": number
    }
  ]
}

CRITICAL RULES:
- amount_in_paise: Convert rupees to paise. Rs.100.50 = 10050
- ALWAYS return valid JSON. No markdown, no code fences, no additional text.`;

const SAMPLE_EMAIL = `Subject: UPI Transaction Alert

Dear Customer,

A UPI transaction of Rs.1,250.00 has been debited from your account XXXXXX1234 on 2026-06-05 at 10:30 AM.
Transaction reference: 123456789
Merchant: Swiggy
UPI ID: swiggy@icici

Your current balance is Rs.45,230.50.

If you did not make this transaction, please contact your bank immediately.`;

async function test() {
  if (!API_KEY) {
    console.error('OPENCODE_GO_API_KEY is not set. Export it or add to .dev.vars');
    process.exit(1);
  }

  console.log('Sending test to OpenCode Go...\n');
  console.log('Email:', SAMPLE_EMAIL.slice(0, 80), '...\n');

  const response = await fetch(BASE_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'deepseek-v4-flash',
      temperature: 0,
      max_tokens: 800,
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: `Analyze this email and extract all financial transactions:\n---\n${SAMPLE_EMAIL}\n---` },
      ],
      response_format: { type: 'json_object' },
    }),
  });

  const body = await response.json();

  if (!response.ok) {
    console.error('❌ API returned error:', response.status);
    console.error(JSON.stringify(body, null, 2));
    process.exit(1);
  }

  console.log('✅ API call succeeded!');

  const content = body.choices?.[0]?.message?.content;
  if (!content) {
    console.error('❌ No content in response');
    process.exit(1);
  }

  const cleaned = content.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '').trim();
  let parsed;
  try {
    parsed = JSON.parse(cleaned);
  } catch {
    console.error('❌ Failed to parse JSON response:');
    console.error(cleaned);
    process.exit(1);
  }

  console.log('\nParsed response:');
  console.log(JSON.stringify(parsed, null, 2));

  const txns = parsed.transactions;
  if (!txns || txns.length === 0) {
    console.error('❌ No transactions extracted');
    process.exit(1);
  }

  console.log(`\n✅ Extracted ${txns.length} transaction(s):`);
  for (const txn of txns) {
    console.log(`  - Rs.${(txn.amount_in_paise / 100).toFixed(2)} | ${txn.direction} | ${txn.payment_method} | ${txn.counterparty_raw} | confidence: ${txn.confidence}`);
  }
}

test().catch((err) => {
  console.error('❌ Test failed:', err.message);
  process.exit(1);
});
