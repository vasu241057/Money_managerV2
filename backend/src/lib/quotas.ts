// Fair-use cap defaults (Task 6, decision D7), in a module of their own so the
// workers and the quota service can share them without importing config.ts
// (which the unit specs replace with a getAppConfig-only mock).
//
// Far above one person's volume: the caps exist to bound what a single
// runaway mailbox can cost, not to meter ordinary use. user_limits overrides
// them per user; USER_DAILY_EMAIL_FETCH_CAP / USER_DAILY_AI_CALL_CAP move the
// defaults.
export const DEFAULT_USER_DAILY_EMAIL_FETCH_CAP = 500;
export const DEFAULT_USER_DAILY_AI_CALL_CAP = 200;
