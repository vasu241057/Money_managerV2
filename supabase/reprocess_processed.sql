-- Re-process already PROCESSED emails.
-- Run this in Supabase SQL Editor, then use the internal requeue endpoint.

-- Step 1: Delete transactions + financial_events for PROCESSED emails
DELETE FROM public.transactions
WHERE financial_event_id IN (
  SELECT fe.id FROM public.financial_events fe
  JOIN public.raw_emails re ON re.id = fe.raw_email_id
  WHERE re.user_id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0'
    AND re.status = 'PROCESSED'
);

DELETE FROM public.financial_events
WHERE raw_email_id IN (
  SELECT re.id FROM public.raw_emails re
  WHERE re.user_id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0'
    AND re.status = 'PROCESSED'
);

-- Step 2: Reset raw_emails back to PENDING_EXTRACTION
UPDATE public.raw_emails
SET status = 'PENDING_EXTRACTION'
WHERE user_id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0'
  AND status = 'PROCESSED';

-- Step 3: Verify
SELECT status, count(*) FROM public.raw_emails
WHERE user_id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0'
GROUP BY status;

-- Then run:
-- curl -X POST "http://localhost:8787/internal/requeue/pending?limit=10"
