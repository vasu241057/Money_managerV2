type MaybePromise<T> = T | Promise<T>;

export type AuthTokenProvider = () => MaybePromise<string | null>;

interface ClerkSessionLike {
  getToken?: () => Promise<string | null>;
}

interface ClerkLike {
  session?: ClerkSessionLike;
}

declare global {
  interface Window {
    Clerk?: ClerkLike;
    __MONEY_MANAGER_CLERK_JWT__?: string;
  }
}

async function defaultAuthTokenProvider(): Promise<string | null> {
  if (typeof window === 'undefined') {
    return null;
  }

  if (window.__MONEY_MANAGER_CLERK_JWT__) {
    return window.__MONEY_MANAGER_CLERK_JWT__;
  }

  if (window.Clerk?.session?.getToken) {
    return window.Clerk.session.getToken();
  }

  return null;
}

let authTokenProvider: AuthTokenProvider = defaultAuthTokenProvider;

export function setAuthTokenProvider(provider: AuthTokenProvider): void {
  authTokenProvider = provider;
}

export async function getAuthToken(): Promise<string | null> {
  return authTokenProvider();
}
