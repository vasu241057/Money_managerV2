-- Monthly spend breakdown by user credit card label.

select
  t.user_id,
  date_trunc('month', t.txn_date) as month,
  ucc.id as credit_card_id,
  ucc.card_label,
  ucc.issuer_name,
  ucc.network,
  ucc.first4,
  ucc.last4,
  sum(t.amount_in_paise)::bigint as total_spend_in_paise,
  count(*)::bigint as transaction_count
from public.transactions t
join public.user_credit_cards ucc
  on ucc.id = t.credit_card_id
 and ucc.user_id = t.user_id
join public.financial_events fe
  on fe.id = t.financial_event_id
 and fe.user_id = t.user_id
where t.type = 'expense'
  and fe.status = 'ACTIVE'
group by
  t.user_id,
  date_trunc('month', t.txn_date),
  ucc.id,
  ucc.card_label,
  ucc.issuer_name,
  ucc.network,
  ucc.first4,
  ucc.last4
order by month desc, total_spend_in_paise desc;
