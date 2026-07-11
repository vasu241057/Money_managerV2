-- Self-learning email templates: skeletons induced from AI extractions,
-- keyed by exact sender address. A matching email is extracted locally
-- (kind EXTRACT) or marked ignored (kind IGNORE) without an AI call.
-- Templates are global (bank formats are the same for every user); all
-- user-specific resolution (categories, rules, verification) happens at
-- persist time through the identity graph.

create table public.email_templates (
    id uuid primary key default gen_random_uuid(),
    from_address text not null,
    skeleton_hash text not null,
    skeleton text not null,
    slot_map jsonb not null,
    status text not null default 'ACTIVE'
        check (status in ('ACTIVE', 'DISABLED')),
    hit_count integer not null default 0,
    failure_count integer not null default 0,
    learned_from_raw_email_id uuid
        references public.raw_emails (id) on delete set null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    last_hit_at timestamptz,
    constraint email_templates_sender_skeleton_unique
        unique (from_address, skeleton_hash)
);

comment on table public.email_templates is
    'Sender-keyed extraction templates learned from AI answers; matched emails skip the model call.';
comment on column public.email_templates.skeleton is
    'Whitespace-collapsed masked email with {{N}}/{{n}}/{{W}}/{{M}} slot markers.';
comment on column public.email_templates.slot_map is
    'Which slots hold the amount/date/instrument/merchant plus fixed direction and payment method.';

create index email_templates_sender_lookup_idx
    on public.email_templates (from_address, status);
