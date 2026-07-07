-- Sender metadata for raw emails.
-- from_address powers the sender allowlist gate (and auditing of gated mail);
-- subject helps spot wrongly-gated bank senders without storing the body.
alter table public.raw_emails
  add column if not exists from_address text,
  add column if not exists subject text;

comment on column public.raw_emails.from_address is
  'Normalized sender email address parsed from the From header; null when unavailable.';
comment on column public.raw_emails.subject is
  'Email subject (truncated); kept for auditing sender-gate decisions.';
