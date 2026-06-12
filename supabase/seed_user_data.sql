-- Seed accounts & categories for Money Manager. Run in Supabase SQL Editor.

-- ============================================================
-- ACCOUNTS
-- ============================================================
INSERT INTO public.accounts (user_id, name, type) VALUES
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Cash',                     'cash'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'HDFC UPI',                 'bank'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'PNB UPI',                  'bank'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Slice UPI',                'bank'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Diners HDFC',              'card'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'ICICI Sapphiro MasterCard','card'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'ICICI Sapphiro Amex',      'card'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Swiggy HDFC',              'card'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Amazon ICICI Card',        'card'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Yes Bank Kiwi Card',       'card'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Kotak Card',               'card');

-- ============================================================
-- CATEGORIES (parents)
-- ============================================================
INSERT INTO public.categories (user_id, name, icon, type) VALUES
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Food',           'Utensils',     'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Transport',      'Car',          'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Shopping',       'ShoppingBag',  'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Entertainment',  'Film',         'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Bills',          'Receipt',      'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Health',         'HeartPulse',   'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Salary',         'Briefcase',    'income'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Investment',     'TrendingUp',   'income'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Adi',            'Circle',       'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Amazon',         'Circle',       'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Miscellaneous',  'Circle',       'expense'),
  ('a661a921-cdb0-503b-a810-4b25e4f15ca0', 'Vices',          'Circle',       'expense');

-- ============================================================
-- CATEGORIES (sub-categories)
-- ============================================================
WITH parents AS (
  SELECT id, name FROM public.categories WHERE user_id = 'a661a921-cdb0-503b-a810-4b25e4f15ca0'
)
INSERT INTO public.categories (user_id, parent_id, name, icon, type)
SELECT 'a661a921-cdb0-503b-a810-4b25e4f15ca0', p.id, s.name, 'Circle', s.p_type
FROM (VALUES
  ('Food',           'Breakfast',           'expense'),
  ('Food',           'Lunch',               'expense'),
  ('Food',           'Dinner',              'expense'),
  ('Food',           'Groceries',           'expense'),
  ('Food',           'Restaurants',         'expense'),
  ('Food',           'Snacks',              'expense'),
  ('Transport',      'Airport',             'expense'),
  ('Transport',      'Cab',                 'expense'),
  ('Transport',      'Auto',                'expense'),
  ('Transport',      'Fuel',                'expense'),
  ('Transport',      'Public Transport',    'expense'),
  ('Transport',      'Taxi',                'expense'),
  ('Shopping',       'Clothes',             'expense'),
  ('Shopping',       'Electronics',         'expense'),
  ('Shopping',       'Home',                'expense'),
  ('Entertainment',  'Movies',              'expense'),
  ('Entertainment',  'Games',               'expense'),
  ('Entertainment',  'Events',              'expense'),
  ('Entertainment',  'Subscriptions',       'expense'),
  ('Bills',          'Electricity',         'expense'),
  ('Bills',          'Water',               'expense'),
  ('Bills',          'Internet',            'expense'),
  ('Bills',          'Medical',             'expense'),
  ('Bills',          'Phone recharge',      'expense'),
  ('Health',         'Doctor',              'expense'),
  ('Health',         'Medicine',            'expense'),
  ('Health',         'Fitness',             'expense'),
  ('Health',         'Skincare',            'expense'),
  ('Salary',         'Full-time',           'income'),
  ('Salary',         'Freelance',           'income'),
  ('Investment',     'Stocks',              'income'),
  ('Investment',     'Crypto',              'income'),
  ('Investment',     'Real Estate',         'income'),
  ('Miscellaneous',  'Coupons/gift cards',  'expense'),
  ('Vices',          'Sutta',               'expense'),
  ('Vices',          'Alcohol',             'expense'),
  ('Vices',          'Ganja',               'expense'),
  ('Vices',          'Hash',                'expense')
) AS s(parent_name, name, p_type)
JOIN parents p ON p.name = s.parent_name;
