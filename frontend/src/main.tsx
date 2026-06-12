import { StrictMode, type ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import { ClerkProvider } from '@clerk/react'
import { QueryClientProvider } from '@tanstack/react-query'
import './index.css'
import App from './App.tsx'
import { queryClient } from './lib/query-client'

const EnvClerkProvider = ClerkProvider as unknown as (props: {
  afterSignOutUrl?: string;
  children: ReactNode;
}) => ReactNode

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    {/* Clerk reads VITE_CLERK_PUBLISHABLE_KEY from Vite env; do not pass publishableKey manually. */}
    <EnvClerkProvider afterSignOutUrl="/">
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </EnvClerkProvider>
  </StrictMode>,
)
