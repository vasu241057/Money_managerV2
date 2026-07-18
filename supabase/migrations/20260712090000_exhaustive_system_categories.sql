-- Exhaustive system category taxonomy (July 2026 categorization overhaul).
--
-- Three-level tree: structural roots (Income/Expense/Transfer, never
-- assignable) → parent categories (Food, Transport, …) → subcategories
-- (Groceries, Fuel, …). Adds the per-type generic fallbacks (Miscellaneous /
-- Other Income / Other Transfer) that replace bare roots as the engine's
-- fallback category. Names are unique within a type — the merchant mapper's
-- category enum and the AI prompt rely on that.
--
-- Idempotent by design, same pattern as 20260308171000_seed_system_data.sql.

set search_path = public;

-- ==========================================
-- 1) New parent categories (children of the structural roots).
--    Existing parents from the base seed (Food, Transport, Shopping, Bills,
--    Health, Entertainment, Credit Card Spend, Salary, Refund, Cashback,
--    Card Bill Payment, Wallet Transfer, Broker Transfer) are untouched.
-- ==========================================
with parent_seed(seed_id, root_name, root_type, name, type, icon) as (
  values
    -- Expense
    ('00000000-0000-0000-0000-000000000117'::uuid, 'Expense', 'expense', 'Housing', 'expense', 'Home'),
    ('00000000-0000-0000-0000-000000000118'::uuid, 'Expense', 'expense', 'Education', 'expense', 'GraduationCap'),
    ('00000000-0000-0000-0000-000000000119'::uuid, 'Expense', 'expense', 'Travel', 'expense', 'Plane'),
    ('00000000-0000-0000-0000-000000000120'::uuid, 'Expense', 'expense', 'Personal Care', 'expense', 'Sparkles'),
    ('00000000-0000-0000-0000-000000000121'::uuid, 'Expense', 'expense', 'Financial', 'expense', 'Landmark'),
    ('00000000-0000-0000-0000-000000000122'::uuid, 'Expense', 'expense', 'Gifts & Donations', 'expense', 'Gift'),
    ('00000000-0000-0000-0000-000000000123'::uuid, 'Expense', 'expense', 'Pets', 'expense', 'PawPrint'),
    ('00000000-0000-0000-0000-000000000124'::uuid, 'Expense', 'expense', 'Subscriptions', 'expense', 'MonitorPlay'),
    ('00000000-0000-0000-0000-000000000125'::uuid, 'Expense', 'expense', 'Family & Kids', 'expense', 'Baby'),
    ('00000000-0000-0000-0000-000000000126'::uuid, 'Expense', 'expense', 'Miscellaneous', 'expense', 'Circle'),
    -- Income
    ('00000000-0000-0000-0000-000000000127'::uuid, 'Income', 'income', 'Freelance & Business', 'income', 'Briefcase'),
    ('00000000-0000-0000-0000-000000000128'::uuid, 'Income', 'income', 'Investments', 'income', 'TrendingUp'),
    ('00000000-0000-0000-0000-000000000129'::uuid, 'Income', 'income', 'Gifts Received', 'income', 'Gift'),
    ('00000000-0000-0000-0000-000000000130'::uuid, 'Income', 'income', 'Other Income', 'income', 'CirclePlus'),
    -- Transfer (deliberately minimal)
    ('00000000-0000-0000-0000-000000000131'::uuid, 'Transfer', 'transfer', 'Self Transfer', 'transfer', 'ArrowLeftRight'),
    ('00000000-0000-0000-0000-000000000132'::uuid, 'Transfer', 'transfer', 'Other Transfer', 'transfer', 'CircleDashed')
)
insert into public.categories (id, user_id, parent_id, name, type, icon, is_system)
select
  s.seed_id,
  null,
  root.id,
  s.name,
  s.type,
  s.icon,
  true
from parent_seed s
join public.categories root
  on root.user_id is null
 and root.is_system = true
 and root.parent_id is null
 and root.name = s.root_name
 and root.type = s.root_type
where not exists (
  select 1
  from public.categories c
  where c.user_id is null
    and c.is_system = true
    and c.parent_id = root.id
    and c.name = s.name
    and c.type = s.type
);

-- ==========================================
-- 2) Subcategories (children of parent categories — existing and new).
--    Parents are matched by unique (name, type) among system categories.
-- ==========================================
with sub_seed(seed_id, parent_name, name, type, icon) as (
  values
    -- Food
    ('00000000-0000-0000-0000-000000000141'::uuid, 'Food', 'Groceries', 'expense', 'ShoppingCart'),
    ('00000000-0000-0000-0000-000000000142'::uuid, 'Food', 'Restaurants', 'expense', 'UtensilsCrossed'),
    ('00000000-0000-0000-0000-000000000143'::uuid, 'Food', 'Food Delivery', 'expense', 'Bike'),
    ('00000000-0000-0000-0000-000000000144'::uuid, 'Food', 'Snacks & Beverages', 'expense', 'Coffee'),
    -- Transport
    ('00000000-0000-0000-0000-000000000145'::uuid, 'Transport', 'Fuel', 'expense', 'Fuel'),
    ('00000000-0000-0000-0000-000000000146'::uuid, 'Transport', 'Public Transport', 'expense', 'Bus'),
    ('00000000-0000-0000-0000-000000000147'::uuid, 'Transport', 'Taxi & Rideshare', 'expense', 'CarTaxiFront'),
    ('00000000-0000-0000-0000-000000000148'::uuid, 'Transport', 'Parking & Tolls', 'expense', 'ParkingCircle'),
    ('00000000-0000-0000-0000-000000000149'::uuid, 'Transport', 'Vehicle Maintenance', 'expense', 'Wrench'),
    -- Shopping
    ('00000000-0000-0000-0000-000000000150'::uuid, 'Shopping', 'Clothing & Fashion', 'expense', 'Shirt'),
    ('00000000-0000-0000-0000-000000000151'::uuid, 'Shopping', 'Electronics', 'expense', 'Smartphone'),
    ('00000000-0000-0000-0000-000000000152'::uuid, 'Shopping', 'Home & Furniture', 'expense', 'Armchair'),
    ('00000000-0000-0000-0000-000000000153'::uuid, 'Shopping', 'Beauty & Cosmetics', 'expense', 'Palette'),
    -- Bills
    ('00000000-0000-0000-0000-000000000154'::uuid, 'Bills', 'Electricity', 'expense', 'Zap'),
    ('00000000-0000-0000-0000-000000000155'::uuid, 'Bills', 'Water', 'expense', 'Droplets'),
    ('00000000-0000-0000-0000-000000000156'::uuid, 'Bills', 'Gas', 'expense', 'Flame'),
    ('00000000-0000-0000-0000-000000000157'::uuid, 'Bills', 'Internet & Broadband', 'expense', 'Wifi'),
    ('00000000-0000-0000-0000-000000000158'::uuid, 'Bills', 'Mobile Recharge', 'expense', 'Signal'),
    ('00000000-0000-0000-0000-000000000159'::uuid, 'Bills', 'DTH & Cable', 'expense', 'Tv'),
    -- Health
    ('00000000-0000-0000-0000-000000000160'::uuid, 'Health', 'Doctor & Consultation', 'expense', 'Stethoscope'),
    ('00000000-0000-0000-0000-000000000161'::uuid, 'Health', 'Medicines & Pharmacy', 'expense', 'Pill'),
    ('00000000-0000-0000-0000-000000000162'::uuid, 'Health', 'Lab Tests', 'expense', 'TestTube'),
    ('00000000-0000-0000-0000-000000000163'::uuid, 'Health', 'Fitness & Gym', 'expense', 'Dumbbell'),
    -- Entertainment
    ('00000000-0000-0000-0000-000000000164'::uuid, 'Entertainment', 'Movies & Shows', 'expense', 'Clapperboard'),
    ('00000000-0000-0000-0000-000000000165'::uuid, 'Entertainment', 'Games', 'expense', 'Gamepad2'),
    ('00000000-0000-0000-0000-000000000166'::uuid, 'Entertainment', 'Events & Concerts', 'expense', 'Ticket'),
    ('00000000-0000-0000-0000-000000000167'::uuid, 'Entertainment', 'OTT & Streaming', 'expense', 'MonitorPlay'),
    -- Housing
    ('00000000-0000-0000-0000-000000000168'::uuid, 'Housing', 'Rent', 'expense', 'Home'),
    ('00000000-0000-0000-0000-000000000169'::uuid, 'Housing', 'Home Maintenance', 'expense', 'Hammer'),
    ('00000000-0000-0000-0000-000000000170'::uuid, 'Housing', 'House Help', 'expense', 'HandHelping'),
    -- Education
    ('00000000-0000-0000-0000-000000000171'::uuid, 'Education', 'School & College Fees', 'expense', 'School'),
    ('00000000-0000-0000-0000-000000000172'::uuid, 'Education', 'Courses & Coaching', 'expense', 'BookOpen'),
    ('00000000-0000-0000-0000-000000000173'::uuid, 'Education', 'Books & Supplies', 'expense', 'Book'),
    -- Travel
    ('00000000-0000-0000-0000-000000000174'::uuid, 'Travel', 'Flights', 'expense', 'Plane'),
    ('00000000-0000-0000-0000-000000000175'::uuid, 'Travel', 'Trains & Buses', 'expense', 'TrainFront'),
    ('00000000-0000-0000-0000-000000000176'::uuid, 'Travel', 'Hotels & Stays', 'expense', 'BedDouble'),
    ('00000000-0000-0000-0000-000000000177'::uuid, 'Travel', 'Trip Activities', 'expense', 'MapPin'),
    -- Personal Care
    ('00000000-0000-0000-0000-000000000178'::uuid, 'Personal Care', 'Salon & Grooming', 'expense', 'Scissors'),
    ('00000000-0000-0000-0000-000000000179'::uuid, 'Personal Care', 'Spa & Wellness', 'expense', 'Flower2'),
    -- Financial
    ('00000000-0000-0000-0000-000000000180'::uuid, 'Financial', 'Loan EMI', 'expense', 'Landmark'),
    ('00000000-0000-0000-0000-000000000181'::uuid, 'Financial', 'Bank Fees & Charges', 'expense', 'BadgePercent'),
    ('00000000-0000-0000-0000-000000000182'::uuid, 'Financial', 'Insurance Premiums', 'expense', 'Shield'),
    ('00000000-0000-0000-0000-000000000183'::uuid, 'Financial', 'Taxes', 'expense', 'Receipt'),
    -- Gifts & Donations
    ('00000000-0000-0000-0000-000000000184'::uuid, 'Gifts & Donations', 'Gifts Given', 'expense', 'Gift'),
    ('00000000-0000-0000-0000-000000000185'::uuid, 'Gifts & Donations', 'Charity & Donations', 'expense', 'HeartHandshake'),
    -- Pets
    ('00000000-0000-0000-0000-000000000186'::uuid, 'Pets', 'Pet Food & Supplies', 'expense', 'Bone'),
    ('00000000-0000-0000-0000-000000000187'::uuid, 'Pets', 'Vet & Grooming', 'expense', 'PawPrint'),
    -- Subscriptions
    ('00000000-0000-0000-0000-000000000188'::uuid, 'Subscriptions', 'Software & Apps', 'expense', 'AppWindow'),
    ('00000000-0000-0000-0000-000000000189'::uuid, 'Subscriptions', 'Memberships', 'expense', 'IdCard'),
    -- Family & Kids
    ('00000000-0000-0000-0000-000000000190'::uuid, 'Family & Kids', 'Childcare', 'expense', 'Baby'),
    ('00000000-0000-0000-0000-000000000191'::uuid, 'Family & Kids', 'Kids Activities', 'expense', 'Blocks'),
    -- Salary (existing parent from base seed)
    ('00000000-0000-0000-0000-000000000192'::uuid, 'Salary', 'Full-time Salary', 'income', 'Briefcase'),
    ('00000000-0000-0000-0000-000000000193'::uuid, 'Salary', 'Bonus & Incentives', 'income', 'Award'),
    ('00000000-0000-0000-0000-000000000194'::uuid, 'Salary', 'Part-time & Stipend', 'income', 'Clock'),
    -- Freelance & Business
    ('00000000-0000-0000-0000-000000000195'::uuid, 'Freelance & Business', 'Freelance Projects', 'income', 'Laptop'),
    ('00000000-0000-0000-0000-000000000196'::uuid, 'Freelance & Business', 'Business Income', 'income', 'Store'),
    -- Investments
    ('00000000-0000-0000-0000-000000000197'::uuid, 'Investments', 'Stocks & Mutual Funds', 'income', 'LineChart'),
    ('00000000-0000-0000-0000-000000000198'::uuid, 'Investments', 'Dividends & Interest', 'income', 'Percent'),
    ('00000000-0000-0000-0000-000000000199'::uuid, 'Investments', 'Crypto', 'income', 'Bitcoin'),
    ('00000000-0000-0000-0000-000000000200'::uuid, 'Investments', 'Rental Income', 'income', 'Building'),
    -- Refund (existing parent from base seed)
    ('00000000-0000-0000-0000-000000000201'::uuid, 'Refund', 'Purchase Refund', 'income', 'Undo2'),
    ('00000000-0000-0000-0000-000000000202'::uuid, 'Refund', 'Reimbursements', 'income', 'ReceiptText')
)
insert into public.categories (id, user_id, parent_id, name, type, icon, is_system)
select
  s.seed_id,
  null,
  p.id,
  s.name,
  s.type,
  s.icon,
  true
from sub_seed s
join public.categories p
  on p.user_id is null
 and p.is_system = true
 and p.parent_id is not null
 and p.name = s.parent_name
 and p.type = s.type
where not exists (
  select 1
  from public.categories c
  where c.user_id is null
    and c.is_system = true
    and c.parent_id = p.id
    and c.name = s.name
    and c.type = s.type
);
