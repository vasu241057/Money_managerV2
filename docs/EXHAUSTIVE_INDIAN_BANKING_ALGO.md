# Indian Banking Email/SMS Extraction — Exhaustive Analysis & Algorithm

## PART 1: Complete Taxonomy of Indian Transaction Messages

---

### 1. UPI TRANSACTIONS

UPI alerts come from two sources: the **sender's bank** (debit alert) and the **receiver's bank/app** (credit alert). Same transaction generates TWO emails.

#### 1a. UPI Debit (Bank sends — "money left your account")

**HDFC Bank:**
```
HDFC BANK --> Dear Customer, Rs.167.00 has been debited from account 5960
to VPA zeptomarketplac744706.rzp@rxaxis ZEPTO MARKETPLACE PRIVATE LIMITED
on 03-03-26. Your UPI transaction reference number is 180152627669.
```

**ICICI Bank:**
```
ICICI Bank Alert: Rs.450.00 debited from a/c XX1234 on 05-Jun-26.
To: swiggy@icici. UPI Ref: 123456789012.
```

**SBI:**
```
SBI Alert: Your a/c XX5678 debited by Rs.299.00 on 04-03-2026
towards UPI payment to PHONEPE. UPI Ref: 987654321098.
Avl Bal: Rs.12,345.67
```

**Axis Bank:**
```
Axis Bank: INR 1,250.00 spent on your a/c XX9012 at AMAZON via UPI.
Txn ID: AX12345678. Date: 03-Mar-2026.
```

**Kotak Mahindra:**
```
Kotak Alert: Rs.890.00 paid from a/c XX3456 to UPI ID netflix@axisbank.
Ref No: 234567890123. Available Balance: INR 45,230.50
```

**Yes Bank:**
```
Yes Bank: Rs.599.00 debited from your account XX7890.
Merchant: ZOMATO LIMITED. UPI Ref: 345678901234. Bal: INR 8,765.00
```

**Slice (fintech app):**
```
Hi Vasu, Slice Alert! Rs.1,500.00 debited from your slice account xx1110.
To: AMAZON PAY INDIA PRIVATE LIMITED. Transaction date: 14-Mar-26.
```

#### 1b. UPI Credit (Receiver's app/bank — "money arrived")

**HDFC Bank:**
```
HDFC BANK --> Your a/c XX1234 credited with Rs.5,000.00 on 03-03-26.
From: VPA rahul.sharma@okhdfcbank. UPI Ref: 456789012345.
```

**Slice (receive):**
```
Hi Vasu, You have received ₹5,000 via UPI in your slice bank account xx1110.
Transaction date 14-Mar-26. From VASU KHANDELWAL. RRN 102909556487.
Avl. Bal. ₹1,46,198.72!
```

**PhonePe/Google Pay (app notification → email):**
```
PhonePe: You received Rs.250.00 from Rajesh Kumar (rajesh@okicici).
UPI Ref: 567890123456. Credited to HDFC Bank a/c XX1234.
```

#### 1c. UPI Edge Cases

| Edge Case | Example |
|-----------|---------|
| **Self-transfer** | "Rs.1.00 debited from 5960 to VPA vasukhandelwa@slc Vasu Khandelwal" — same person, different accounts. Should be `transfer` type |
| **Failed/reversed UPI** | "Your UPI txn of Rs.500 to swiggy@icici could not be completed. Ref: XXX. Amount will be reversed." |
| **Partial UPI reversal** | "Rs.100.00 refunded from ZOMATO. UPI Ref: XXX. Refund Ref: YYY." |
| **UPI collect request** | "Vasu Khandelwal is requesting Rs.500 via UPI. Open your UPI app to approve." — NOT a transaction |
| **UPI mandate/autopay** | "Your UPI AutoPay of Rs.299 to Netflix has been set up. First debit on 15-Mar-26." — NOT a transaction yet |
| **Split UPI** | PhonePe "Split bill" — multiple debits to multiple people in one notification. Multiple transactions. |
| **UPI with cashback** | "Rs.450 paid via UPI to Swiggy. You earned ₹15 cashback!" — ignore cashback, extract main txn |
| **UPI lite** | "Rs.50.00 debited via UPI LITE from your wallet." — wallet, not bank. Different instrument. |
| **UPI on credit card** | Some banks allow UPI on RuPay credit cards. "Rs.1,200 paid via UPI on your RuPay CC XX8006." — payment_method should be credit_card, not upi |

---

### 2. CREDIT CARD TRANSACTIONS

Credit card alerts have a distinct structure: card number (masked), transaction amount, merchant, date/time.

#### 2a. Domestic (INR) Credit Card

**ICICI Bank:**
```
Your ICICI Bank Credit Card XX8006 has been used for a transaction of
INR 777.00 on Mar 04, 2026 at 12:11:41. Info: AMAZON PAY IN GROCERY.
Available Credit Limit: INR 45,230.00
```

**HDFC Bank:**
```
HDFC Bank Credit Card Alert: INR 2,499.00 spent on your card ending 4242
at FLIPKART INTERNET on 05-Jun-2026. Avl Credit Limit: INR 78,500.00
```

**SBI Card:**
```
SBI Card Alert: Transaction of Rs.1,850.00 at UBER INDIA on your card
ending 5678. Date: 04-Mar-2026. Available Limit: Rs.1,12,340.00
```

**Axis Bank:**
```
Axis Bank: Your CC XX3456 used for INR 650.00 at NETFLIX MUMBAI.
Txn Date: 03-03-2026.
```

**Kotak Credit Card:**
```
Kotak Credit Card Alert: Rs.3,200.00 spent at MAKEMYTRIP on card XX9012.
Date & Time: 05-Mar-2026 14:30:00. Avl Limit: Rs.2,45,000
```

**Amex (American Express):**
```
Amex Alert: USD 49.99 charged to your card ending 1008 at AMAZON WEB SERVICES.
Date: 04-Mar-2026. MR points earned: 98.
```

#### 2b. International (Foreign Currency) Credit Card

**HDFC Bank:**
```
HDFC Bank: USD 15.49 spent on card ending 4242 at NETFLIX.COM.
Txn Date: 06-Jun-2026. Approx INR 1,293.67.
```

**ICICI:**
```
ICICI Bank: EUR 25.00 charged on CC XX8006 at BOOKING.COM AMSTERDAM.
Date: 05-Mar-2026. INR equivalent: Rs.2,250.00.
```

**Kotak:**
```
Kotak: GBP 12.50 debited on card XX9012. Merchant: TFL.GOV.UK.
Date: 04-03-2026. Forex markup: 3.5%. Approx INR 1,325.00.
```

#### 2c. Credit Card EMI Conversion Alerts

```
Your transaction of INR 12,000 at AMAZON has been converted to 3-month EMI.
Monthly EMI: Rs.4,120 (incl. interest). First debit: 15-Apr-2026.
```
This is NOT a new transaction — the original already exists. It's a conversion notification.

#### 2d. Credit Card Bill Payment (NOT a purchase)

```
HDFC Bank: Your credit card bill of Rs.15,230.00 has been paid via auto-debit
from a/c XX5960. Payment Date: 05-Mar-2026.
```
This IS a transaction — a transfer from bank to credit card. Type: `transfer`.

#### 2e. Credit Card Edge Cases

| Edge Case | Example |
|-----------|---------|
| **Contactless tap** | "Rs.200 contactless payment at STARBUCKS on card XX8006." |
| **Tokenized card** | "Tokenized transaction of Rs.999 at AMAZON via device token ending H7K2." — no physical card number visible |
| **Card not present** | "Online transaction of Rs.1,500 at SWIGGY on card ending 4242." |
| **International + DCC** | "USD 25.00 at AMAZON.COM. DCC applied: INR 2,175.00 (rate 87.0)." — use INR amount if available |
| **Cash withdrawal (ATM)** | "Rs.5,000 cash withdrawn from ATM using card ending 4242. Fee: Rs.300." — separate fee as another txn? |
| **EMI reversal** | "EMI for txn at FLIPKART has been cancelled. Original amount Rs.24,000 will be reinstated." |
| **Annual fee** | "Your credit card annual fee of Rs.1,499 + GST has been charged." — `expense` with category `Bills` |
| **Late payment fee** | "Late payment fee of Rs.500 + GST charged on your card." |
| **Fuel surcharge reversal** | "Fuel surcharge of Rs.12.50 reversed for txn at HP PETROL. Net debit remains Rs.500." — ignore the reversal if original txn exists |
| **Double-swipe / duplicate** | Same txn appears in both SMS and email. Deduplicate by amount+date+last4. |

---

### 3. DEBIT CARD TRANSACTIONS

Debit card alerts are rarer now (UPI has cannibalized them) but still appear for POS/online.

**HDFC Bank:**
```
HDFC Bank: Rs.2,500.00 spent using Debit Card ending 7890 at RELIANCE DIGITAL.
Date: 05-Mar-2026. Avl Bal: Rs.67,890.00
```

**SBI:**
```
SBI Alert: INR 1,200.00 debited via Debit Card xx5678.
Merchant: BIG BAZAAR. Date: 04-03-2026. Bal: Rs.45,230.00
```

Edge cases:
- **POS with PIN**: "Rs.800 debited via Chip+PIN at DMART on card xx5678."
- **Contactless**: No difference from credit card contactless
- **International debit**: "USD 35.00 spent on Debit Card xx5678 at UBER USA."
- **Debit card on e-com**: "Rs.1,499 paid via Debit Card ending 7890 to AMAZON SELLER SERVICES."

---

### 4. NETBANKING / NEFT / IMPS / RTGS TRANSFERS

These are account-to-account transfers via internet banking.

**HDFC Bank (NEFT):**
```
HDFC Bank: Rs.25,000.00 transferred via NEFT to A/c XXXXXX1234
of RAHUL SHARMA on 05-Mar-2026. Ref: NEFT-HDFC-123456789.
Avl Bal: Rs.1,45,230.00
```

**ICICI (IMPS):**
```
ICICI Bank: IMPS transfer of Rs.5,000.00 to A/c 9876543210
(ROHIT GUPTA) initiated. Ref: IMPS123456789012. Date: 04-Mar-2026.
```

**SBI (RTGS):**
```
SBI Alert: RTGS transfer of Rs.2,00,000.00 to beneficiary M/S ABC CORP.
UTR: SBI123456789012. Date: 05-03-2026.
```

**Axis Bank:**
```
Axis Bank: Funds Transfer of INR 10,000 to A/c XXXXXX5678 (SELF).
Transfer Mode: IMPS. Ref: AX12345678.
```

Edge cases:
- **Self-transfer**: "to A/c XXXXXX5678 (SELF)" — should be `transfer` type
- **Scheduled transfer**: "Your scheduled transfer of Rs.5,000 will be processed on 15-Mar-2026." — NOT a transaction yet
- **Standing instruction**: "Standing instruction of Rs.10,000 executed to A/c XXX." — IS a transaction
- **Beneficiary addition**: "New beneficiary ROHIT SHARMA added to your account." — NOT a transaction

---

### 5. INCOMING TRANSFERS (Credit to Account)

**HDFC Bank (salary/NEFT credit):**
```
HDFC Bank: Your a/c XX1234 credited with Rs.1,25,000.00 on 01-Mar-2026.
By: ABC TECHNOLOGIES PVT LTD (Salary). Mode: NEFT.
Avl Bal: Rs.2,45,230.00
```

**ICICI:**
```
ICICI Bank Alert: Rs.15,000.00 credited to a/c XX5678 from
RAJESH KUMAR via IMPS. Ref: IMPS987654321098.
```

Edge cases:
- **Salary**: Usually marked as "SALARY" in narration
- **Interest credit**: "Interest of Rs.234.50 credited to your SB account."
- **Cashback credit**: "Cashback of Rs.50.00 credited to your account from SWIGGY."
- **Dividend**: "Dividend of Rs.500.00 credited from MUTUAL FUND."
- **Tax refund**: "Income Tax refund of Rs.12,340.00 credited. Assessment Year 2025-26."

---

### 6. ATM / CASH TRANSACTIONS

**Any Bank:**
```
SBI Alert: Rs.10,000.00 withdrawn from ATM at SBI ATM MUMBAI.
Card ending 5678. Date: 05-Mar-2026. Avl Bal: Rs.89,230.00
```

Edge cases:
- **ATM declined**: "Your cash withdrawal of Rs.5,000 at HDFC ATM was declined due to insufficient funds." — NOT a transaction
- **Mini statement at ATM**: "Mini statement printed at ATM." — NOT a transaction
- **Cash deposit via CDM**: "Rs.25,000.00 deposited via Cash Deposit Machine at SBI Branch." — credit transaction

---

### 7. NON-FINANCIAL EMAILS (Must Be Filtered Out)

| Type | Example triggers |
|------|-----------------|
| **OTP messages** | "Your OTP for transaction is 123456. Valid for 10 mins." |
| **Login alerts** | "Your account was accessed from a new device at 14:00." |
| **Monthly statements** | "Your credit card statement for Feb 2026 is ready. Total due: Rs.15,230." |
| **Bill reminders** | "Your bill of Rs.799 for Vi mobile is due on 15-Mar-2026." — not yet paid |
| **KYC updates** | "Please update your KYC documents before 31-Mar-2026." |
| **Marketing/Promotions** | "Get 10% cashback on Amazon purchases this weekend!" |
| **Welcome emails** | "Welcome to HDFC Bank! Your account has been opened." |
| **EMI conversion offers** | "Convert your Rs.24,000 Amazon purchase to EMI." |
| **Card delivery** | "Your new credit card has been dispatched via BlueDart." |
| **Block/Unblock alerts** | "Your card XX8006 has been temporarily blocked." |
| **Limit enhancement** | "Your credit limit has been increased to Rs.5,00,000." |
| **Beneficiary added** | "New payee added to your netbanking." |

---

### 8. FORMAT VARIATIONS ACROSS BANKS

| Bank | Currency prefix | Date format | Last4 format | Balance label |
|------|----------------|-------------|-------------|---------------|
| HDFC | Rs./INR/USD | 03-03-26, Mar 04 | "account 5960", "card ending 4242" | Avl Bal: |
| ICICI | INR, Rs. | Mar 04, 2026 | "XX8006", "a/c XX1234" | Available Limit: |
| SBI | Rs./INR | 04-03-2026 | "xx5678" | Avl Bal: |
| Axis | INR, Rs. | 03-Mar-2026 | "XX9012", "a/c XX3456" | — |
| Kotak | Rs./INR | 05-Mar-2026 | "XX3456", "card XX9012" | Available Balance: |
| Yes Bank | Rs. | 05-Jun-26 | "XX7890" | Bal: |
| Amex | USD/EUR/GBP | 04-Mar-2026 | "ending 1008" | — |
| Slice | ₹ | 14-Mar-26 | "xx1110" | Avl. Bal. |
| PhonePe/GPay | Rs. | 14-Mar-26 | VPA handles | — |

---

### 9. AMOUNT FORMAT VARIATIONS

| Format | Example | Paise equivalent |
|--------|---------|-----------------|
| Indian comma | Rs.1,25,000.00 | 12500000 |
| No commas | INR 125000.00 | 12500000 |
| No paise | Rs.500 | 50000 |
| Paise present | Rs.167.00 | 16700 |
| Decimal paise | Rs.270.97 | 27097 |
| US format | USD 49.99 | 4999 (store as USD?) |
| EUR format | EUR 25,00 | 2500 (or 250000? ambiguous!) |
| Lakh/crore in text | "Rs.1.5 Lakh" | 15000000 |
| Negative amounts | "-Rs.500" (reversal) | — |
| With spaces | "INR 1, 250.00" | — |
| Currency code prefix | "INR 777.00" | 77700 |

---

### 10. DATE FORMAT VARIATIONS

| Format | Example | ISO-8601 equivalent |
|--------|---------|-------------------|
| DD-MM-YY | 03-03-26 | 2026-03-03 |
| DD-Mon-YYYY | 04-Mar-2026 | 2026-03-04 |
| Mon DD, YYYY | Mar 04, 2026 | 2026-03-04 |
| DD/MM/YYYY | 05/03/2026 | 2026-03-05 |
| YYYY-MM-DD | 2026-03-04 | 2026-03-04 |
| DD Month YYYY | 05 March 2026 | 2026-03-05 |
| With time | Mar 04, 2026 at 12:11:41 | 2026-03-04T12:11:41 |
| Txn date vs email date | Email arrives 1 day later | Use txn date from body, not email internal_date |

---

## PART 2: The Bulletproof Algorithm

### Phase 0: Pre-processing
1. Normalize whitespace. Collapse \r\n → \n. Remove extra spaces.
2. Convert to lowercase for keyword matching (keep original for amount/date extraction).
3. Truncate to ~500 chars for AI (remove after "Avl Bal", "Available Limit", "RRN", "Regards", "©").
4. If text is empty OR < 20 chars → IGNORE.

### Phase 1: Non-financial Filter (Fast Reject)
Run BEFORE amount extraction. Check for these markers:
- OTP / "one time password" / "verification code"
- "statement is ready" / "view your statement"
- "bill due" / "payment reminder" (unpaid, not a txn)
- "welcome to" / "congratulations" / "offer" / "promotion"
- "KYC" / "update your" / "verify your"
- "blocked" / "unblocked" / "dispatched" / "delivered"
- "login" / "logged in" / "new device" / "password changed"

If matched → IGNORE (no transaction extraction attempted).

### Phase 2: Amount Extraction
1. Find ALL currency-amount pairs with regex: `(₹|Rs\.?|INR|USD|EUR|GBP)?\s*([0-9,]+(?:\.[0-9]{1,2})?)` or `([0-9,]+(?:\.[0-9]{1,2})?)\s*(₹|Rs\.?|INR)`
2. For each candidate, compute a "transaction score":
   - +10: within 50 chars of direction word (debited/credited/spent/paid/sent/received)
   - +5: within 100 chars of direction word
   - -20: within 30 chars of balance word (balance/bal/avl/available/limit/outstanding/due)
   - +3: appears in first 200 chars of email
   - -10: appears after "Avl Bal" or "Available Balance" or "Available Limit"
3. Take the highest-scored candidate as the transaction amount.
4. If no candidate scores >0 → IGNORE (no discernible transaction amount).
5. Convert: strip commas → parse float → multiply by 100 → round → integer (paise).
6. Multiple transactions in one email: repeat for EACH direction cue pair. One debit and one credit in same email = two separate transactions (not a reversal).

### Phase 3: Direction Detection
1. Check within 100 chars of the amount position.
2. Priority order of keyword groups:
   - **Debit**: debited, spent, paid, sent, withdrawn, purchase, charged, swiped
   - **Credit**: credited, received, refund, deposited, reversed, cashback (if >₹10), interest, dividend
   - **Transfer**: transferred, NEFT, IMPS, RTGS, fund transfer
3. If both debit AND credit keywords exist near amount → check which is closer. If equal distance, check which appears first in the email.
4. Special case: "reversed" / "refund" → check if there's a corresponding debit in the same email. If yes, treat both. If credit-only with "refund" → it's a standalone credit.
5. Self-transfer detection: if counterparty name matches account holder AND payment_method is upi/netbanking → mark as `transfer` type, not debit/credit.

### Phase 4: Payment Method Detection
1. UPI indicators (in order of confidence):
   - "UPI" keyword present → 100%
   - "VPA" present → 100%
   - "@okhdfcbank", "@okicici", "@oksbi", "@okaxis", "@ybl", "@ibl", "@apl" → 100%
   - "UTR" / "UPI Ref" / "RRN" present → 90%
2. Credit card indicators:
   - "credit card" / "CC XX" / "card ending" / "card XX" → credit_card
   - But if UPI on RuPay credit card → credit_card, not upi
3. Debit card indicators:
   - "debit card" / "DC XX" → debit_card
   - But "card" alone with bank account number → could be either
4. Netbanking indicators:
   - "NEFT" / "IMPS" / "RTGS" / "netbanking" / "fund transfer" → netbanking
5. ATM:
   - "ATM" + "withdrawn" / "cash withdrawal" / "cash deposit" → atm (sub-type of cash)
6. Cash:
   - "cash deposit" / "cash withdrawal" / "CDM" / "counter withdrawal"
7. Unknown if none matched.

### Phase 5: Instrument/Owner Identification
1. **Card last4 extraction**:
   - Match: `XX(\d{4})`, `ending (\d{4})`, `card (\d{4})`, `account (\d{4})`, `xx(\d{4})`
   - For UPI: look for "account (\d{4})" or "a/c (\d{4})" or "xx(\d{4})"
   - NOT the UPI reference number (which is 12 digits). Filter by length: exactly 4 digits.
2. **Bank name detection** (from email headers and body):
   - "HDFC Bank", "ICICI Bank", "SBI", "Axis Bank", "Kotak", "Yes Bank", "IDFC", "IndusInd", "PNB", "Canara", "BOB", "Slice", "Fi", "Jupiter", "DBS"
   - For mapping to user accounts: match bank name substring against account names

### Phase 6: Merchant Counterparty Extraction
1. **UPI debit**: "to VPA [UPI_ID] [MERCHANT NAME]" or "to [MERCHANT]"
2. **UPI credit**: "From [SENDER]" or "from VPA [UPI_ID]"
3. **Credit card**: "Info: [MERCHANT]" or "at [MERCHANT]" or "Merchant: [MERCHANT]"
4. **Netbanking**: "to A/c [NUMBER] of [NAME]" or "beneficiary [NAME]"
5. **ATM**: ATM location after "at"
6. Cleanup: remove trailing dots, "LTD", "PVT LTD", "LIMITED", "PRIVATE LIMITED", "INDIA" — lowercase, normalize.
7. If counterparty is a known aggregator (RAZORPAY, CCAVENUE, BILLDESK, PAYU, CASHFREE, PHONEPE, BHARATPE), look for the ACTUAL merchant name in the narration after the aggregator reference.

### Phase 7: Date Extraction
1. Try structured formats first:
   - `on DD-MM-YY` or `on DD-MM-YYYY`
   - `on DD-Mon-YYYY` (Mar = March, etc.)
   - `Mon DD, YYYY`
   - `Date: DD/MM/YYYY`
   - `Transaction date DD-Mon-YY`
2. If no explicit date → use email's `internal_date` from Gmail.
3. If time present ("at HH:MM:SS", "14:30:00") → include in ISO timestamp.
4. Timezone: default to IST (+05:30) unless specified otherwise.

### Phase 8: Edge Case Resolution

| Scenario | Logic |
|----------|-------|
| **Multiple amounts, one transaction** | Score each amount. Closest to direction word wins. Ignore balance amounts. |
| **Multiple transactions in one email** | Split by "and also" or multiple UPI refs. Each direction+amount pair is separate. |
| **Same transaction, two alerts** (SMS + email) | Deduplicate: same amount + same date + same last4 within 1 minute → merge. |
| **Cashback/reward points in same email** | If "cashback" / "reward" with small amount (≤₹50) AND there's a larger txn → ignore cashback, keep main txn. If standalone cashback > ₹50 → treat as credit. |
| **Reversal/refund** | If "reversed" AND no original txn extracted → credit. If original txn exists + reversal → mark both, reconcile as net-zero pair. |
| **International currency** | Extract USD/EUR/GBP amount. If INR equivalent present, use INR. If not, store original currency + amount for user review. |
| **EMI conversion notice** | "converted to EMI" / "EMI of" → IGNORE. Original txn already captured. |
| **Bill payment (not purchase)** | "bill paid" / "payment to [BANK NAME] card" → expense with category "Bills" or "Credit Card Payment". |
| **Balance amount mistaken as txn** | Any amount following "Avl Bal", "Available Balance", "Available Limit", "Bal:" within 30 chars → ignore. |
| **₹0 transaction** | "Rs.0.00 charged for verification" → IGNORE. validation hold, not real. |
| **Txn reference number as amount** | UPI ref numbers are 12 digits. Filter: amounts above ₹10,00,000 (1 crore paise = 10,00,00,000 paise) → treat as suspicious, flag for review. |
| **RRN/UTR/Ref as instrument_last4** | These are 12+ digit numbers. Don't extract as last4. Check length. |
| **No amount found but direction present** | "Transaction completed successfully" without amount → UNRECOGNIZED. Can't use. |
| **Garbled/HTML-entity text** | `&amp;` → `&`, `&lt;` → `<`, `&#8377;` → `₹`, `&nbsp;` → space. Normalize before processing. |
| **Multi-line split format** | Some banks put amount on line 1, merchant on line 3. Need context window ±3 lines around amount. |
| **FROM field in header vs body** | Email "From" header is the bank. Don't use as counterparty. Use body text. |

---

### Phase 9: Post-extraction Validation
1. Amount in paise must be: > 0, < 10,00,00,000 (₹10 lakh). Outside this → flag.
2. Direction must be debit/credit (not null).
3. Payment method must be one of: upi, credit_card, debit_card, netbanking, cash, unknown.
4. If multiple transactions extracted from one email, check for:
   - Exact duplicates → merge.
   - Same amount + opposite direction → likely reversal pair.
   - Same amount + same direction → likely duplicate alert (SMS + email). Flag one for review.
5. Cross-reference against user's categories. If category_id doesn't match any known UUID → fallback to Miscellaneous.

---

### Phase 10: Identity Resolution (Account + Category Matching)
1. **Account matching** (DO NOT ask AI — do it in code):
   - Match `instrument_last4` against user's accounts where `instrument_last4` column matches.
   - For UPI: match bank name substring against account names (e.g., "HDFC" → "HDFC UPI").
   - For credit_card: match last4 against card accounts.
   - If no match → leave `account_id` null. User can assign manually.
2. **Category matching**:
   - AI picks `category_id` + `sub_category_id` from the user's actual category list.
   - If AI can't pick → fallback to Miscellaneous category ID.
   - If no Miscellaneous category exists → mark as NEEDS_REVIEW.

---

## PART 3: Known Failure Modes (Edge Cases < 0.1%)

1. **SMS abbreviated format**: "HDFC: Rs.500 debited. UPI: 12345. Bal: 45000" — very short, no merchant, no date.
2. **Non-English**: Hindi/Tamil SMS from regional banks. "Aapke account se Rs.500 nikale gaye."
3. **Screenshots embedded as images**: Some banks send statement PDFs with embedded images. Text extraction gets garbled.
4. **Forwarded emails**: Someone forwards their bank alert to you. The "From" is a person, not a bank. Pattern: "Fwd: HDFC Bank Alert..." with added text.
5. **Multi-account alerts**: "Your HDFC savings XX1234 and HDFC current XX5678 have been debited." — multiple accounts, need to attribute each.
6. **Joint account**: "Your joint account XX1234 (with Priya Sharma) debited" — extra text after account number.
7. **Pre-auth/hold**: "Rs.1.00 has been authorized on your card for verification." — NOT a real charge. Will be reversed.
8. **Wallet load**: "Rs.5,000 loaded to your PhonePe wallet from HDFC Bank." — transfer type. Source is bank, destination is wallet.
9. **UPI number (not VPA)**: "UPI payment to 9876543210@upi" — phone number as UPI ID. Not a traditional VPA.
10. **Recurring/SIP**: "Your monthly SIP of Rs.10,000 has been executed." — debit for investment. Category: Investment.
