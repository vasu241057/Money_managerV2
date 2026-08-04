import { useState } from 'react';
import {
	Check,
	ChevronDown,
	EyeOff,
	Loader2,
	Pencil,
	Plus,
	RotateCcw,
	Trash2,
	X,
} from 'lucide-react';
import { useCategories } from '../../hooks/useCategories';
import { SettingsPageHeader } from '../ui/SettingsPageHeader';

interface CategorySettingsProps {
	onBack: () => void;
}

const inputStyle: React.CSSProperties = {
	background: 'var(--ledger-bg)',
	border: '1px solid var(--ledger-rule)',
	color: 'var(--ledger-fg)',
	fontSize: '16px',
};

export function CategorySettings({ onBack }: CategorySettingsProps) {
	const [categoryType, setCategoryType] = useState<'expense' | 'income'>('expense');
	const [expandedCategoryId, setExpandedCategoryId] = useState<string | null>(null);
	const [newCategoryName, setNewCategoryName] = useState('');
	const [newSubNames, setNewSubNames] = useState<Record<string, string>>({});
	const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);
	const [renamingId, setRenamingId] = useState<string | null>(null);
	const [renameValue, setRenameValue] = useState('');
	const [hiddenOpen, setHiddenOpen] = useState(false);
	const {
		categories: allCategories,
		hiddenCategories,
		addCategory,
		renameCategory,
		deleteCategory,
		restoreCategory,
		addSubCategory,
		deleteSubCategory,
		isLoading,
		error,
	} = useCategories();

	const categories = allCategories.filter(category => category.type === categoryType);
	const hiddenForType = hiddenCategories.filter(category => category.type === categoryType);

	const selectCategoryType = (type: 'expense' | 'income') => {
		setCategoryType(type);
		setExpandedCategoryId(null);
		setConfirmDeleteId(null);
		setRenamingId(null);
		setRenameValue('');
		setHiddenOpen(false);
	};

	const addNewCategory = () => {
		const name = newCategoryName.trim();
		if (!name || isLoading) return;
		void addCategory({ name, type: categoryType, icon: 'Circle', subCategories: [] });
		setNewCategoryName('');
	};

	const addNewSubcategory = (categoryId: string) => {
		const name = newSubNames[categoryId]?.trim() ?? '';
		if (!name || isLoading) return;
		void addSubCategory(categoryId, name);
		setNewSubNames(current => ({ ...current, [categoryId]: '' }));
	};

	const commitRename = () => {
		const name = renameValue.trim();
		if (renamingId && name) void renameCategory(renamingId, name);
		setRenamingId(null);
		setRenameValue('');
	};

	return (
		<div className="flex-1 flex flex-col min-h-0 relative" style={{ background: 'var(--ledger-bg)', color: 'var(--ledger-fg)' }}>
			<SettingsPageHeader title="Categories" subtitle="Organize transaction labels" onBack={onBack} />

			<nav className="shrink-0 px-4 sm:px-5" style={{ background: 'var(--ledger-bg)', borderBottom: '1px solid var(--ledger-rule)' }} aria-label="Category type">
				<div className="w-full max-w-[720px] mx-auto grid grid-cols-2" role="tablist">
					{(['expense', 'income'] as const).map(type => (
						<button
							type="button"
							key={type}
							role="tab"
							aria-selected={categoryType === type}
							onClick={() => selectCategoryType(type)}
							className="relative h-11 text-xs font-bold capitalize"
							style={{ color: categoryType === type ? 'var(--ledger-fg)' : 'var(--ledger-muted)' }}
						>
							{type}
							<span
								className="absolute left-3 right-3 bottom-0 h-0.5 rounded-full"
								style={{ background: 'var(--ledger-accent)', opacity: categoryType === type ? 1 : 0 }}
							/>
						</button>
					))}
				</div>
			</nav>

			<main className="flex-1 overflow-y-auto px-4 sm:px-5 pt-5 pb-24 min-h-0">
				<div className="w-full max-w-[720px] mx-auto">
					<div className="mb-5">
						<p className="text-sm leading-relaxed" style={{ color: 'var(--ledger-muted)' }}>
							Edit your {categoryType} categories and open a category to manage its subcategories.
						</p>
					</div>

					<section className="mb-5">
						<p className="text-[10px] font-bold uppercase tracking-[0.2em] mb-3" style={{ color: 'var(--ledger-muted)' }}>
							Add category
						</p>
						<div className="flex items-center gap-2 rounded-[12px] p-2" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
							<input
								type="text"
								value={newCategoryName}
								onChange={event => setNewCategoryName(event.target.value)}
								onKeyDown={event => { if (event.key === 'Enter') addNewCategory(); }}
								placeholder={`New ${categoryType} category`}
								aria-label={`New ${categoryType} category`}
								className="h-10 min-w-0 flex-1 rounded-[8px] px-3 outline-none"
								style={inputStyle}
							/>
							<button
								type="button"
								onClick={addNewCategory}
								disabled={!newCategoryName.trim() || isLoading}
								className="w-10 h-10 rounded-[8px] flex items-center justify-center shrink-0 active:scale-95 disabled:opacity-50"
								style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }}
								aria-label="Add category"
							>
								{isLoading ? <Loader2 size={16} className="animate-spin" /> : <Plus size={18} />}
							</button>
						</div>
					</section>

					<section>
						<div className="flex items-baseline justify-between gap-3 mb-3">
							<p className="text-[10px] font-bold uppercase tracking-[0.2em]" style={{ color: 'var(--ledger-muted)' }}>
								Active categories
							</p>
							<span className="text-[11px] tnum" style={{ color: 'var(--ledger-muted)' }}>
								{categories.length}
							</span>
						</div>

						{isLoading && allCategories.length === 0 ? (
							<div className="flex items-center gap-2 py-6 justify-center" style={{ color: 'var(--ledger-muted)' }}>
								<Loader2 size={16} className="animate-spin" />
								<span className="text-sm">Loading categories…</span>
							</div>
						) : categories.length === 0 ? (
							<div className="rounded-[12px] px-4 py-8 text-center" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
								<p className="text-sm font-semibold">No {categoryType} categories yet</p>
								<p className="text-xs mt-1" style={{ color: 'var(--ledger-muted)' }}>Add the first one above.</p>
							</div>
						) : (
							<div className="space-y-3">
								{categories.map(category => {
									const isExpanded = expandedCategoryId === category.id;
									const isRenaming = renamingId === category.id;
									return (
										<article key={category.id} className="rounded-[12px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
											{isRenaming ? (
												<div className="flex items-center gap-2 p-3">
													<input
														type="text"
														value={renameValue}
														autoFocus
														onChange={event => setRenameValue(event.target.value)}
														onKeyDown={event => {
															if (event.key === 'Enter') commitRename();
															if (event.key === 'Escape') { setRenamingId(null); setRenameValue(''); }
														}}
														className="h-10 min-w-0 flex-1 rounded-[8px] px-3 font-semibold outline-none"
														style={inputStyle}
													/>
													<button type="button" onClick={commitRename} disabled={!renameValue.trim()} className="w-9 h-9 flex items-center justify-center disabled:opacity-40" style={{ color: 'var(--ledger-accent)' }} aria-label="Save category name"><Check size={17} /></button>
													<button type="button" onClick={() => { setRenamingId(null); setRenameValue(''); }} className="w-9 h-9 flex items-center justify-center" style={{ color: 'var(--ledger-muted)' }} aria-label="Cancel rename"><X size={17} /></button>
												</div>
											) : (
												<div className="flex items-stretch">
													<button
														type="button"
														onClick={() => setExpandedCategoryId(isExpanded ? null : category.id)}
														className="min-w-0 flex-1 flex items-center gap-2.5 px-4 py-3.5 text-left"
														aria-expanded={isExpanded}
													>
														<ChevronDown size={17} className={`shrink-0 transition-transform ${isExpanded ? '' : '-rotate-90'}`} style={{ color: 'var(--ledger-muted)' }} />
														<span className="min-w-0 flex-1">
															<span className="flex items-center gap-2">
																<span className="text-sm font-bold truncate">{category.name}</span>
																{category.isSystem && <span className="text-[8px] font-bold uppercase tracking-wider rounded px-1.5 py-0.5" style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}>Built-in</span>}
															</span>
															<span className="block text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>
																{category.subCategories.length} subcategor{category.subCategories.length === 1 ? 'y' : 'ies'}
															</span>
														</span>
													</button>

													{category.isSystem ? (
														<button type="button" onClick={() => { void deleteCategory(category.id); }} className="w-11 shrink-0 flex items-center justify-center" style={{ color: 'var(--ledger-muted)' }} aria-label={`Hide ${category.name}`} title="Hide category"><EyeOff size={15} /></button>
													) : (
														<div className="flex shrink-0">
															<button type="button" onClick={() => { setRenamingId(category.id); setRenameValue(category.name); setConfirmDeleteId(null); }} className="w-10 flex items-center justify-center" style={{ color: 'var(--ledger-muted)' }} aria-label={`Rename ${category.name}`}><Pencil size={14} /></button>
															<button type="button" onClick={() => { setConfirmDeleteId(category.id); setRenamingId(null); }} className="w-10 flex items-center justify-center" style={{ color: 'var(--ledger-negative)' }} aria-label={`Delete ${category.name}`}><Trash2 size={15} /></button>
														</div>
													)}
												</div>
											)}

											{confirmDeleteId === category.id && (
												<div className="px-4 py-3" style={{ background: 'var(--ledger-amber-bg)', borderTop: '1px solid var(--ledger-rule)' }}>
													<p className="text-xs leading-relaxed" style={{ color: 'var(--ledger-amber)' }}>Delete “{category.name}” and its subcategories? Categories used by transactions can’t be deleted.</p>
													<div className="flex gap-2 mt-3">
														<button type="button" onClick={() => { void deleteCategory(category.id); setConfirmDeleteId(null); }} className="h-9 px-4 rounded-[8px] text-xs font-bold" style={{ background: 'var(--ledger-negative)', color: '#fff' }}>Delete</button>
														<button type="button" onClick={() => setConfirmDeleteId(null)} className="h-9 px-3 text-xs font-semibold" style={{ color: 'var(--ledger-muted)' }}>Cancel</button>
													</div>
												</div>
											)}

											{isExpanded && (
												<div style={{ borderTop: '1px solid var(--ledger-rule)' }}>
													{category.subCategories.length === 0 ? (
														<p className="px-4 py-3 text-xs" style={{ color: 'var(--ledger-muted)' }}>No subcategories yet</p>
													) : category.subCategories.map((sub, index) => {
														const isSystem = category.subCategorySystem?.[sub] ?? false;
														return (
															<div key={sub} className="flex items-center justify-between gap-3 px-4 py-2.5" style={{ borderBottom: index < category.subCategories.length - 1 ? '1px solid var(--ledger-rule)' : undefined }}>
																<span className="text-sm truncate">{sub}</span>
																{isSystem ? (
																	<button type="button" onClick={() => { const id = category.subCategoryIds?.[sub]; if (id) void deleteCategory(id); }} className="w-8 h-8 flex items-center justify-center shrink-0" style={{ color: 'var(--ledger-muted)' }} aria-label={`Hide ${sub}`}><EyeOff size={13} /></button>
																) : (
																	<button type="button" onClick={() => { void deleteSubCategory(category.id, sub); }} className="w-8 h-8 flex items-center justify-center shrink-0" style={{ color: 'var(--ledger-muted)' }} aria-label={`Delete ${sub}`}><X size={13} /></button>
																)}
															</div>
														);
													})}

													<div className="flex items-center gap-2 p-2.5" style={{ background: 'var(--ledger-bg)', borderTop: '1px solid var(--ledger-rule)' }}>
														<input
															type="text"
															value={newSubNames[category.id] ?? ''}
															onChange={event => setNewSubNames(current => ({ ...current, [category.id]: event.target.value }))}
															onKeyDown={event => { if (event.key === 'Enter') addNewSubcategory(category.id); }}
															placeholder="Add subcategory"
															aria-label={`Add a subcategory to ${category.name}`}
															className="h-9 min-w-0 flex-1 rounded-[7px] px-2.5 outline-none"
															style={inputStyle}
														/>
														<button type="button" onClick={() => addNewSubcategory(category.id)} disabled={!newSubNames[category.id]?.trim() || isLoading} className="w-9 h-9 rounded-[7px] flex items-center justify-center shrink-0 disabled:opacity-50" style={{ background: 'var(--ledger-accent)', color: 'var(--ledger-bg)' }} aria-label="Add subcategory"><Plus size={15} /></button>
													</div>
												</div>
											)}
										</article>
									);
								})}
							</div>
						)}
					</section>

					{hiddenForType.length > 0 && (
						<section className="mt-6 pt-5" style={{ borderTop: '1px dashed var(--ledger-rule)' }}>
							<button type="button" onClick={() => setHiddenOpen(open => !open)} className="w-full rounded-[12px] px-4 py-3 flex items-center gap-3 text-left" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }} aria-expanded={hiddenOpen}>
								<span className="w-9 h-9 rounded-[9px] flex items-center justify-center shrink-0" style={{ background: 'var(--ledger-rule)', color: 'var(--ledger-muted)' }}><EyeOff size={15} /></span>
								<span className="min-w-0 flex-1">
									<span className="block text-sm font-bold">Hidden categories</span>
									<span className="block text-[11px] mt-0.5" style={{ color: 'var(--ledger-muted)' }}>{hiddenForType.length} hidden · tap to restore</span>
								</span>
								<ChevronDown size={16} className={`transition-transform ${hiddenOpen ? '' : '-rotate-90'}`} style={{ color: 'var(--ledger-muted)' }} />
							</button>

							{hiddenOpen && (
								<div className="mt-2 rounded-[12px] overflow-hidden" style={{ background: 'var(--ledger-card)', border: '1px solid var(--ledger-rule)' }}>
									{hiddenForType.map((hidden, index) => (
										<div key={hidden.id} className="flex items-center justify-between gap-3 px-4 py-3" style={{ borderBottom: index < hiddenForType.length - 1 ? '1px solid var(--ledger-rule)' : undefined }}>
											<span className="text-sm truncate" style={{ color: 'var(--ledger-muted)' }}>{hidden.parentName ? `${hidden.parentName} · ${hidden.name}` : hidden.name}</span>
											<button type="button" onClick={() => { void restoreCategory(hidden.id); }} className="h-8 px-2 flex items-center gap-1.5 text-xs font-semibold shrink-0" style={{ color: 'var(--ledger-accent)' }}><RotateCcw size={12} />Restore</button>
										</div>
									))}
								</div>
							)}
						</section>
					)}

					{error && <p className="mt-4 text-xs" role="alert" style={{ color: 'var(--ledger-negative)' }}>{error}</p>}
				</div>
			</main>
		</div>
	);
}
