import { UserButton } from '@clerk/react';

interface AppUserButtonProps {
  gmailConnected: boolean;
  requiresReconnect: boolean;
  statusLabel: string;
  connectLabel: string;
  onConnectGmail: () => void;
  onDisconnectGmail: () => void;
  onOpenAccounts: () => void;
  onOpenCategories: () => void;
}

export function AppUserButton({
  gmailConnected,
  requiresReconnect,
  statusLabel,
  connectLabel,
  onConnectGmail,
  onDisconnectGmail,
  onOpenAccounts,
  onOpenCategories,
}: AppUserButtonProps) {
  return (
    <UserButton>
      <UserButton.MenuItems>
        <UserButton.Action
          label={`Gmail: ${statusLabel}`}
          labelIcon={<StatusDot connected={gmailConnected} />}
          onClick={() => {}}
        />
        {gmailConnected ? (
          <UserButton.Action
            label="Disconnect Gmail"
            labelIcon={<GmailDisconnectIcon />}
            onClick={onDisconnectGmail}
          />
        ) : (
          <UserButton.Action
            label={connectLabel}
            labelIcon={<GmailConnectIcon />}
            onClick={onConnectGmail}
          />
        )}
        <UserButton.Action
          label="Accounts"
          labelIcon={<AccountsIcon />}
          onClick={onOpenAccounts}
        />
        <UserButton.Action
          label="Categories"
          labelIcon={<CategoriesIcon />}
          onClick={onOpenCategories}
        />
      </UserButton.MenuItems>
    </UserButton>
  );
}

function StatusDot({ connected }: { connected: boolean }) {
  return (
    <svg viewBox="0 0 24 24" fill="none" width="16" height="16">
      <circle cx="12" cy="12" r="5" fill={connected ? '#22c55e' : '#6b7280'} />
      {connected && <circle cx="12" cy="12" r="10" stroke="#22c55e" strokeWidth="1.5" strokeOpacity="0.3" />}
    </svg>
  );
}

function GmailConnectIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" width="16" height="16">
      <path d="M12 5v14M5 12h14" stroke="#22c55e" strokeWidth="2" strokeLinecap="round" />
    </svg>
  );
}

function GmailDisconnectIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" width="16" height="16">
      <path d="M18 6L6 18M6 6l12 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function AccountsIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" width="16" height="16">
      <path d="M3 6h18M3 12h18M3 18h18" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function CategoriesIcon() {
  return (
    <svg viewBox="0 0 24 24" fill="none" width="16" height="16">
      <rect x="3" y="3" width="8" height="8" rx="1" stroke="currentColor" strokeWidth="1.5" />
      <rect x="13" y="3" width="8" height="8" rx="1" stroke="currentColor" strokeWidth="1.5" />
      <rect x="3" y="13" width="8" height="8" rx="1" stroke="currentColor" strokeWidth="1.5" />
      <rect x="13" y="13" width="8" height="8" rx="1" stroke="currentColor" strokeWidth="1.5" />
    </svg>
  );
}
