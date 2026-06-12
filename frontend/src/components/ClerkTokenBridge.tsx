import { useAuth } from '@clerk/react';
import { useEffect } from 'react';
import { setAuthTokenProvider } from '../lib/auth-token';

export function ClerkTokenBridge() {
  const { getToken } = useAuth();

  useEffect(() => {
    setAuthTokenProvider(async () => {
      const token = await getToken();
      return token ?? null;
    });

    return () => {
      setAuthTokenProvider(() => null);
    };
  }, [getToken]);

  return null;
}
