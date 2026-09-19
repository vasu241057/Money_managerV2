import { useAuth } from '@clerk/react';
import { useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, Loader2, Trash2 } from 'lucide-react';
import { useState } from 'react';
import { apiClient, toErrorMessage } from '../../lib/api-client';
import { SettingsPageHeader } from '../ui/SettingsPageHeader';

interface DeleteAccountSettingsProps {
	onBack: () => void;
	onDeleted: () => void;
}

// Mirrors DELETE_ACCOUNT_CONFIRMATION in shared/types.ts: the backend refuses
// the request unless the body carries exactly this word.
const CONFIRMATION_WORD = 'DELETE';

const REMOVED_ITEMS = [
	'Every transaction, account, card, category and budget',
	'Imported emails and everything learned from them',
	'Your Gmail connection (access is revoked at Google)',
	'Your sign-in, so this email can no longer open the app',
];

export function DeleteAccountSettings({ onBack, onDeleted }: DeleteAccountSettingsProps) {
	const { signOut } = useAuth();
	const queryClient = useQueryClient();
	const [typed, setTyped] = useState('');
	const [isDeleting, setIsDeleting] = useState(false);
	const [error, setError] = useState<string | null>(null);
	const confirmed = typed.trim() === CONFIRMATION_WORD;

	const handleDelete = async () => {
		if (!confirmed || isDeleting) return;
		setIsDeleting(true);
		setError(null);
		try {
			await apiClient.deleteCurrentUserAccount();
			// Nothing cached may outlive the account; the session ends with it.
			queryClient.clear();
			await signOut();
			onDeleted();
		} catch (requestError) {
			setError(toErrorMessage(requestError));
			setIsDeleting(false);
		}
	};

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<SettingsPageHeader title="Delete account" subtitle="Permanent · cannot be undone" onBack={onBack} />

			<main className="flex-1 overflow-y-auto px-5 pt-6 pb-24 min-h-0">
				<div className="w-full max-w-[720px] mx-auto">
					<section className="mb-6">
						<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<div className="flex items-start gap-3 px-4 py-4">
								<span
									className="w-10 h-10 rounded-[10px] flex items-center justify-center shrink-0"
									style={{ background: 'color-mix(in srgb, var(--ledger-negative) 10%, transparent)', color: 'var(--ledger-negative)' }}
								>
									<AlertTriangle size={18} />
								</span>
								<div className="min-w-0 flex-1">
									<p className="text-sm font-bold">This erases everything, immediately</p>
									<p className="text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
										There is no recovery window and no export. Signing up again starts from an empty account.
									</p>
								</div>
							</div>
							<ul className="px-4 pb-4 space-y-1.5">
								{REMOVED_ITEMS.map(item => (
									<li key={item} className="flex items-start gap-2 text-xs" style={{ color: 'var(--ledger-fg)' }}>
										<span className="mt-[6px] w-1.5 h-1.5 rounded-full shrink-0" style={{ background: 'var(--ledger-negative)' }} />
										<span>{item}</span>
									</li>
								))}
							</ul>
						</div>
					</section>

					<section>
						<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3" style={{ color: 'var(--ledger-muted)' }}>
							Confirmation
						</p>
						<div className="rounded-[14px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<div className="px-4 py-4">
								<label htmlFor="delete-account-confirm" className="block text-xs font-semibold mb-2">
									Type <span className="tnum font-bold" style={{ color: 'var(--ledger-negative)' }}>{CONFIRMATION_WORD}</span> to continue
								</label>
								<input
									id="delete-account-confirm"
									type="text"
									autoComplete="off"
									autoCapitalize="characters"
									spellCheck={false}
									value={typed}
									onChange={event => setTyped(event.target.value)}
									disabled={isDeleting}
									placeholder={CONFIRMATION_WORD}
									className="w-full h-11 rounded-[9px] px-3 text-sm font-semibold tracking-[0.12em] focus-visible:outline-none focus-visible:ring-2 disabled:opacity-60"
									style={{ background: 'var(--ledger-bg)', border: '1px solid var(--ledger-rule)', color: 'var(--ledger-fg)' }}
								/>
							</div>

							{error && (
								<p className="px-4 py-2.5 text-xs" style={{ color: 'var(--ledger-negative)', borderTop: '1px solid var(--ledger-rule)' }}>
									{error}
								</p>
							)}

							<div className="px-4 py-3" style={{ background: 'var(--ledger-bg)', borderTop: '1px solid var(--ledger-rule)' }}>
								<button
									type="button"
									onClick={() => { void handleDelete(); }}
									disabled={!confirmed || isDeleting}
									className="w-full h-11 rounded-[9px] text-sm font-semibold flex items-center justify-center gap-2 disabled:opacity-40"
									style={{ background: 'var(--ledger-negative)', color: 'var(--ledger-bg)' }}
								>
									{isDeleting ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
									{isDeleting ? 'Deleting…' : 'Delete my account'}
								</button>
							</div>
						</div>
					</section>
				</div>
			</main>
		</div>
	);
}
