import type { CSSProperties } from 'react';

// Ledger-styled loading placeholders: shimmering rule-bars (the same motif
// as the app icon) instead of generic spinners. Callers pass sizing and
// rounding via className.

export function SkeletonBar({
	className = '',
	delayMs = 0,
}: {
	className?: string;
	delayMs?: number;
}) {
	return (
		<span
			className={`block animate-pulse ${className}`}
			style={{ background: 'var(--ledger-rule)', animationDelay: `${delayMs}ms` }}
		/>
	);
}

// Donut placeholder for the analytics pie.
export function SkeletonDonut({ size = 176 }: { size?: number }) {
	return (
		<div
			className="rounded-full animate-pulse mx-auto"
			style={{
				width: size,
				height: size,
				border: `${Math.round(size * 0.16)}px solid var(--ledger-rule)`,
			}}
		/>
	);
}

const skeletonCardStyle: CSSProperties = {
	background: 'var(--ledger-card)',
	border: '1px solid var(--ledger-rule)',
};

// A transaction row placeholder: icon circle, label + sublabel, amount.
export function SkeletonTxnRow({ delayMs = 0, isLast = false }: { delayMs?: number; isLast?: boolean }) {
	return (
		<div
			className="flex items-center justify-between p-3.5"
			style={!isLast ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
		>
			<div className="flex items-center gap-3">
				<SkeletonBar className="w-9 h-9 rounded-full" delayMs={delayMs} />
				<div className="flex flex-col gap-1.5">
					<SkeletonBar className="h-3 w-28 rounded-full" delayMs={delayMs + 60} />
					<SkeletonBar className="h-2 w-20 rounded-full" delayMs={delayMs + 120} />
				</div>
			</div>
			<SkeletonBar className="h-3 w-16 rounded-full" delayMs={delayMs + 180} />
		</div>
	);
}

// Home: balance hero, sync line, review callout, recent-activity card.
export function HomeSkeleton() {
	return (
		<div aria-label="Loading home">
			<section className="mb-6">
				<div className="flex items-center justify-between mb-3">
					<SkeletonBar className="h-2.5 w-24 rounded-full" />
					<SkeletonBar className="h-6 w-32 rounded-full" delayMs={100} />
				</div>
				<div className="flex items-baseline gap-2 mb-4">
					<SkeletonBar className="h-4 w-4 rounded-full self-center" />
					<SkeletonBar className="h-10 w-44 rounded-[8px]" delayMs={120} />
					<SkeletonBar className="h-5 w-10 rounded-[6px]" delayMs={240} />
				</div>
				<div className="flex items-center justify-between pt-4" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
					<div className="flex-1 flex flex-col gap-2">
						<SkeletonBar className="h-2 w-14 rounded-full" />
						<SkeletonBar className="h-4 w-24 rounded-[5px]" delayMs={80} />
					</div>
					<div className="flex-1 flex flex-col items-end gap-2">
						<SkeletonBar className="h-2 w-14 rounded-full" delayMs={160} />
						<SkeletonBar className="h-4 w-24 rounded-[5px]" delayMs={240} />
					</div>
				</div>
			</section>
			<div className="flex items-center gap-2 mb-5">
				<SkeletonBar className="w-1.5 h-1.5 rounded-full" />
				<SkeletonBar className="h-2.5 w-40 rounded-full" delayMs={100} />
			</div>
			<SkeletonBar className="h-[68px] w-full rounded-[10px] mb-8" delayMs={150} />
			<section>
				<SkeletonBar className="h-2.5 w-28 rounded-full mb-3" />
				<div className="rounded-[10px] overflow-hidden" style={skeletonCardStyle}>
					{[0, 1, 2, 3].map((i) => (
						<SkeletonTxnRow key={i} delayMs={i * 120} isLast={i === 3} />
					))}
				</div>
			</section>
		</div>
	);
}

// Ledger: date-grouped transaction cards.
export function LedgerListSkeleton() {
	return (
		<div aria-label="Loading transactions">
			{[0, 1].map((group) => (
				<div key={group} className="mb-6">
					<div className="flex items-center justify-between mb-2 px-1">
						<SkeletonBar className="h-2.5 w-24 rounded-full" delayMs={group * 200} />
						<SkeletonBar className="h-2.5 w-14 rounded-full" delayMs={group * 200 + 80} />
					</div>
					<div className="rounded-[10px] overflow-hidden" style={skeletonCardStyle}>
						{[0, 1, 2].map((i) => (
							<SkeletonTxnRow key={i} delayMs={group * 200 + i * 120} isLast={i === 2} />
						))}
					</div>
				</div>
			))}
		</div>
	);
}

// Settings: connection card with stats grid, then section rows.
export function SettingsSkeleton() {
	return (
		<div aria-label="Loading settings">
			<section className="mb-8">
				<SkeletonBar className="h-2.5 w-24 rounded-full mb-4" />
				<div className="rounded-[10px] overflow-hidden" style={skeletonCardStyle}>
					<div
						className="px-4 py-3 flex items-center justify-between"
						style={{ borderBottom: '1px solid var(--ledger-rule)' }}
					>
						<div className="flex items-center gap-2.5">
							<SkeletonBar className="w-2 h-2 rounded-full" />
							<SkeletonBar className="h-3 w-40 rounded-full" delayMs={80} />
						</div>
						<SkeletonBar className="h-2.5 w-16 rounded-full" delayMs={160} />
					</div>
					<div className="grid grid-cols-3">
						{[0, 1, 2, 3, 4, 5].map((i) => (
							<div
								key={i}
								className="px-3 py-2.5 flex flex-col items-center gap-1.5"
								style={{
									borderRight: (i + 1) % 3 !== 0 ? '1px solid var(--ledger-rule)' : undefined,
									borderBottom: i < 3 ? '1px solid var(--ledger-rule)' : undefined,
								}}
							>
								<SkeletonBar className="h-4 w-10 rounded-[5px]" delayMs={i * 80} />
								<SkeletonBar className="h-2 w-14 rounded-full" delayMs={i * 80 + 60} />
							</div>
						))}
					</div>
					<div className="px-4 py-3" style={{ borderTop: '1px solid var(--ledger-rule)' }}>
						<SkeletonBar className="h-11 w-full rounded-[8px]" delayMs={200} />
					</div>
				</div>
			</section>
			<section className="mb-8">
				<SkeletonBar className="h-2.5 w-20 rounded-full mb-4" />
				{[0, 1].map((i) => (
					<div key={i} className="flex items-center justify-between py-2">
						<SkeletonBar className="h-3.5 w-32 rounded-full" delayMs={i * 150} />
						<SkeletonBar className="h-2.5 w-20 rounded-full" delayMs={i * 150 + 80} />
					</div>
				))}
			</section>
			<section>
				<SkeletonBar className="h-2.5 w-24 rounded-full mb-4" />
				<div className="flex gap-2 mb-4">
					<SkeletonBar className="h-8 w-24 rounded-full" />
					<SkeletonBar className="h-8 w-24 rounded-full" delayMs={120} />
				</div>
				<div className="rounded-[10px] overflow-hidden" style={skeletonCardStyle}>
					{[0, 1, 2, 3].map((i) => (
						<div
							key={i}
							className="px-4 py-3 flex items-center justify-between"
							style={i < 3 ? { borderBottom: '1px solid var(--ledger-rule)' } : undefined}
						>
							<SkeletonBar className="h-3 w-28 rounded-full" delayMs={i * 100} />
							<SkeletonBar className="h-3 w-8 rounded-full" delayMs={i * 100 + 60} />
						</div>
					))}
				</div>
			</section>
		</div>
	);
}

// Three ragged ledger rules — the compact "something is being written"
// loader for overlays and small regions.
export function SkeletonRules({ caption }: { caption?: string }) {
	return (
		<div className="flex flex-col items-center gap-3" aria-label={caption ?? 'Loading'}>
			<div className="flex flex-col gap-2 w-40">
				<SkeletonBar className="h-2.5 w-24 rounded-full" />
				<SkeletonBar className="h-2.5 w-40 rounded-full" delayMs={150} />
				<SkeletonBar className="h-2.5 w-16 rounded-full" delayMs={300} />
			</div>
			{caption && (
				<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
					{caption}
				</p>
			)}
		</div>
	);
}
