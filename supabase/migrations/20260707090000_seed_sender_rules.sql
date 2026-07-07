-- Seed sender_rules with the allowlist previously hardcoded in
-- backend/src/lib/sender-gate.ts. The DB is now the exhaustive source of
-- sender rules; the code keeps only the EMAIL_SENDER_ALLOWLIST_EXTRA env
-- escape hatch.
insert into public.sender_rules (pattern, decision, sender_type, source)
values
  -- Bank brand tokens (substring of the whole From header)
  ('hdfc', 'ALLOW', 'BANK', 'SEED'),
  ('icici', 'ALLOW', 'BANK', 'SEED'),
  ('sbi', 'ALLOW', 'BANK', 'SEED'),
  ('axis', 'ALLOW', 'BANK', 'SEED'),
  ('kotak', 'ALLOW', 'BANK', 'SEED'),
  ('federal', 'ALLOW', 'BANK', 'SEED'),
  ('idfc', 'ALLOW', 'BANK', 'SEED'),
  ('indusind', 'ALLOW', 'BANK', 'SEED'),
  ('yesbank', 'ALLOW', 'BANK', 'SEED'),
  ('yes bank', 'ALLOW', 'BANK', 'SEED'),
  ('rbl', 'ALLOW', 'BANK', 'SEED'),
  ('aubank', 'ALLOW', 'BANK', 'SEED'),
  ('au bank', 'ALLOW', 'BANK', 'SEED'),
  ('au small finance', 'ALLOW', 'BANK', 'SEED'),
  ('bandhan', 'ALLOW', 'BANK', 'SEED'),
  ('canara', 'ALLOW', 'BANK', 'SEED'),
  ('baroda', 'ALLOW', 'BANK', 'SEED'),
  ('punjab national', 'ALLOW', 'BANK', 'SEED'),
  ('union bank', 'ALLOW', 'BANK', 'SEED'),
  ('hsbc', 'ALLOW', 'BANK', 'SEED'),
  ('standard chartered', 'ALLOW', 'BANK', 'SEED'),
  ('stanchart', 'ALLOW', 'BANK', 'SEED'),
  ('citi', 'ALLOW', 'BANK', 'SEED'),
  ('dbs', 'ALLOW', 'BANK', 'SEED'),
  ('amex', 'ALLOW', 'BANK', 'SEED'),
  ('american express', 'ALLOW', 'BANK', 'SEED'),
  -- Fintech / card issuer / payment app brand tokens
  ('slice', 'ALLOW', 'FINTECH', 'SEED'),
  ('paytm', 'ALLOW', 'FINTECH', 'SEED'),
  ('phonepe', 'ALLOW', 'FINTECH', 'SEED'),
  ('jupiter', 'ALLOW', 'FINTECH', 'SEED'),
  ('onecard', 'ALLOW', 'FINTECH', 'SEED'),
  ('mobikwik', 'ALLOW', 'FINTECH', 'SEED'),
  ('freecharge', 'ALLOW', 'FINTECH', 'SEED'),
  -- Domain entries (brand token would be too short/ambiguous)
  ('fi.money', 'ALLOW', 'FINTECH', 'SEED'),
  ('cred.club', 'ALLOW', 'FINTECH', 'SEED'),
  ('sc.com', 'ALLOW', 'BANK', 'SEED'),
  -- Google Pay transaction mails come from this exact address; the bare
  -- google.com domain is far too broad to allowlist.
  ('payments-noreply@google.com', 'ALLOW', 'WALLET', 'SEED')
on conflict (pattern) do nothing;
