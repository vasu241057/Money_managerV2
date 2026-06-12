# Money Manager — AI Extraction Synopsis
## For evaluating local/small-model suitability

---

## The Task

Extract structured financial transaction data from Indian banking and UPI SMS/email alerts.

**Input:** Raw email text (200-500 characters, English)
**Output:** Strict JSON with transaction details

---

## Input Examples

**Example 1 — UPI Debit (HDFC Bank):**
```
HDFC BANK --> Dear Customer, Rs.167.00 has been debited from account 5960
to VPA zeptomarketplac744706.rzp@rxaxis ZEPTO MARKETPLACE PRIVATE LIMITED
on 03-03-26.
```

**Example 2 — Credit Card (ICICI):**
```
Your ICICI Bank Credit Card XX8006 has been used for a transaction of
INR 777.00 on Mar 04, 2026 at 12:11:41. Info: AMAZON PAY IN GROCERY.
```

**Example 3 — UPI Credit (Slice):**
```
Hi Vasu, You have received ₹1 via UPI in your slice bank account xx1110.
Transaction date 14-Mar-26. From VASU KHANDELWAL.
```

**Example 4 — Non-financial (newsletter, OTP, statement summary):**
```
Your monthly credit card statement is ready. Log in to view. No transactions.
```

---

## Expected Output

```json
{
  "transactions": [
    {
      "direction": "debit",
      "amount_in_paise": 16700,
      "txn_timestamp": "2026-03-03T00:00:00.000Z",
      "payment_method": "upi",
      "counterparty_raw": "ZEPTO MARKETPLACE PRIVATE LIMITED",
      "instrument_last4": "5960",
      "confidence": 0.95
    }
  ]
}
```

If no financial transaction is found: `{ "transactions": [] }`

---

## Field Definitions

| Field | Type | Description |
|-------|------|-------------|
| direction | debit/credit | Money OUT vs IN |
| amount_in_paise | integer | Rupees × 100. ₹167.00 → 16700. NO decimals |
| txn_timestamp | ISO-8601 | Date/time from email text |
| payment_method | upi/credit_card/debit_card/netbanking/cash/unknown | How the payment was made |
| counterparty_raw | string|null | Merchant, person, or bank name |
| instrument_last4 | string|null | Last 4 digits of card/account if visible |
| confidence | 0.0-1.0 | How confident the extraction is |

---

## Key Challenges (why a small model might struggle)

1. **Rupees → Paise conversion:** ₹167.00 → 16700. Models default to copying the number as-is.
2. **Indian terminology:** UPI, VPA, IFSC, netbanking, NEFT, IMPS — not in Western training data.
3. **Date formats:** "14-Mar-26", "Mar 04, 2026", "03-03-26" — varied, need ISO-8601 output.
4. **Direction inference:** "debited from" = debit, "received" = credit. Subtle linguistic cues.
5. **Empty responses:** Must return `{"transactions":[]}` for marketing/newsletter emails, not hallucinate.
6. **Strict JSON only:** No markdown fences, no extra text, valid parseable JSON every time.

---

## Token Profile (per request)

| Component | Approx tokens |
|-----------|--------------|
| System prompt (rules + format) | ~800 |
| Email text | ~150-250 |
| Expected output | ~100-200 |
| **Total per email** | **~1100-1300** |

Processing ~100 emails/day = ~130K tokens/day. Tiny for even a local model.

---

## Model Recommendations

| Tier | Model | Size | Why |
|------|-------|------|-----|
| **Smallest viable** | Qwen 2.5 3B | 3B params | Decent multilingual. Might stumble on paise conversion |
| **Recommended minimum** | Llama 3.1 8B / Qwen 2.5 7B | 7-8B params | Strong instruction following, good JSON output. Fits in 8GB VRAM (q4 quant) |
| **Comfortable** | Mistral Small / Gemma 2 9B | 9-27B | Near-API quality for structured extraction |
| **Overkill** | Llama 3.3 70B+ | 70B+ | Matches GPT-4o-mini but needs 40GB+ VRAM |

**For a laptop with 8-16GB RAM:** Llama 3.1 8B (q4_K_M quant via Ollama/llama.cpp) would be the practical choice. Qwen 2.5 7B is a strong alternative with better Indian-language awareness.

**For a MacBook (unified memory):** Qwen 2.5 14B or Mistral Nemo 12B run comfortably on 16GB M-series chips.

**For CPU-only (no GPU):** Phi-3 Mini 3.8B or Llama 3.2 3B with q4 quant. Slower (~10-30s per email) but functional.

---

## What Would Make It More Reliable Locally

1. **Few-shot examples:** Include 2-3 worked examples in the prompt (adds ~500 tokens)
2. **Structured output mode:** Use `json_object` response_format if the local inference engine supports it (Ollama, vLLM do)
3. **Post-processing validation:** Already implemented — bad JSON/amounts are caught server-side
4. **Fine-tuning:** 50-100 labeled Indian banking emails would dramatically improve a 3B model
