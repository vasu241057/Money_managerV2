import React, { useState } from 'react';
import { useCategories } from "../hooks/useCategories";
import { useAccounts } from "../hooks/useAccounts";
import { Button } from "./ui/Button";
import { Input } from "./ui/Input";
import { BottomSheetSelect } from "./ui/BottomSheetSelect";
import { BottomSheetDatePicker } from "./ui/BottomSheetDatePicker";
import { X } from "lucide-react";
import "../styles/transaction-form.css";
import type { Transaction } from "../hooks/useTransactions";

interface TransactionFormProps {
  onSubmit: (transaction: Omit<Transaction, 'id'> | Transaction) => Promise<unknown>;
  onClose: () => void;
  initialData?: Transaction | null;
}

export function TransactionForm({ onSubmit, onClose, initialData = null }: TransactionFormProps) {
  const { categories, isLoading: categoriesLoading, error: categoriesError } = useCategories();
  const { accounts, isLoading: accountsLoading, error: accountsError } = useAccounts();
  const [type, setType] = useState<'income' | 'expense' | 'transfer'>(initialData?.type ?? 'expense');
  const [amount, setAmount] = useState(initialData?.amount?.toString() || "");
  const [date, setDate] = useState(
    initialData?.date 
      ? new Date(initialData.date).toISOString().split("T")[0] 
      : new Date().toISOString().split("T")[0]
  );

  const [categoryIdState, setCategoryId] = useState(initialData?.categoryId || "");
  const [subCategory, setSubCategory] = useState(
    initialData?.subCategory || ""
  );
  const [accountIdState, setAccount] = useState(initialData?.accountId ?? "");
  const [description, setDescription] = useState(initialData?.description || "");
  const isEditing = Boolean(initialData?.id);
  const isBankDetectedTransaction = isEditing && initialData?.isManual === false;
  const isTransferReadOnly = isEditing && initialData?.type === 'transfer';
  const immutableFieldsLocked = isBankDetectedTransaction || isTransferReadOnly;

  const fallbackCategoryId =
    !categoryIdState && initialData
      ? categories.find(category => category.name === initialData.category)?.id ?? ''
      : '';
  const categoryId = categoryIdState || fallbackCategoryId;
  const account = accountIdState === '' && !isEditing ? accounts[0]?.id || '' : accountIdState;

  const handleCategoryChange = (newCategoryId: string) => {
    setCategoryId(newCategoryId);
    // Reset subCategory if it doesn't exist in the new category
    const cat = categories.find(c => c.id === newCategoryId);
    if (cat && subCategory && !cat.subCategories?.includes(subCategory)) {
      setSubCategory("");
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (isTransferReadOnly) {
      alert('Transfer editing is not supported in this milestone.');
      return;
    }

    if (!amount) {
      alert("Please enter an amount");
      return;
    }

    const selectedCategory = categories.find((c) => c.id === categoryId);
    const selectedAccount = accounts.find((a) => a.id === account);
    const selectedSubCategoryId = subCategory
      ? selectedCategory?.subCategoryIds?.[subCategory]
      : undefined;

    try {
      await onSubmit({
        ...(initialData?.id ? { id: initialData.id } : {}),
        amount: parseFloat(amount),
        type,
        date: new Date(date),
        category: selectedCategory?.name || "Uncategorized",
        categoryId: categoryId || null,
        subCategory,
        subCategoryId: selectedSubCategoryId,
        accountId: account || null,
        accountName: selectedAccount?.name,
        description,
      } as Transaction);

      // Small delay to ensure iOS handles the event correctly before unmounting
      setTimeout(() => {
        onClose();
      }, 100);
    } catch (error) {
      console.error("Error saving transaction:", error);
      const errorMessage = error instanceof Error ? error.message : "An error occurred while saving the transaction.";
      alert(errorMessage);
    }
  };

  const selectedCategoryObj = categories.find((c) => c.id === categoryId);
  const subCategories = selectedCategoryObj?.subCategories || [];
  const showFormDisabledState = categoriesLoading || accountsLoading;
  const combinedError = categoriesError ?? accountsError;

  return (
    <div className="modal-overlay">
      <div className="modal-content">
        <div className="modal-header">
          <h2>{initialData ? "Edit Transaction" : "Add Transaction"}</h2>
          <button onClick={onClose} className="close-btn">
            <X size={24} />
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          {combinedError && <p style={{ color: '#dc2626', marginBottom: 12 }}>{combinedError}</p>}
          <div className="type-toggle-container">
            <div className="type-toggle">
              <button
                type="button"
                className={`type-btn ${
                  type === "expense" ? "active expense" : ""
                }`}
                onClick={() => setType("expense")}
                disabled={immutableFieldsLocked}
              >
                Expense
              </button>
              <button
                type="button"
                className={`type-btn ${
                  type === "income" ? "active income" : ""
                }`}
                onClick={() => setType("income")}
                disabled={immutableFieldsLocked}
              >
                Income
              </button>
            </div>
          </div>

          {isBankDetectedTransaction && (
            <p style={{ marginBottom: 8 }}>
              Amount, type, and date cannot be changed for bank-detected transactions.
            </p>
          )}

          {isTransferReadOnly && (
            <p style={{ marginBottom: 8 }}>
              Transfer editing is disabled until dedicated transfer UX is shipped.
            </p>
          )}

          <Input
            type="number"
            placeholder="0"
            className="amount-input"
            value={amount}
            onChange={(e) => setAmount(e.target.value)}
            disabled={immutableFieldsLocked}
          />

          <BottomSheetDatePicker
            label="Date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            error={!date ? "Date is required" : ""}
            disabled={immutableFieldsLocked}
          />

          <div className="category-section">
            <label className="input-label">Category</label>
            <div className="category-chips">
              {categories
                .filter((c) => c.type === type)
                .map((cat) => (
                  <button
                    type="button"
                    key={cat.id}
                    className={`chip ${categoryId === cat.id ? "active" : ""}`}
                    onClick={() => handleCategoryChange(cat.id)}
                  >
                    {cat.name}
                  </button>
                ))}
            </div>
          </div>

          {subCategories.length > 0 && (
            <div className="category-section" style={{ marginTop: 12 }}>
              <label className="input-label">Sub-category</label>
              <div className="category-chips">
                {subCategories.map((sub) => (
                  <button
                    type="button"
                    key={sub}
                    className={`chip ${subCategory === sub ? "active" : ""}`}
                    onClick={() => setSubCategory(sub)}
                    style={{ fontSize: "12px", padding: "4px 10px" }}
                  >
                    {sub}
                  </button>
                ))}
              </div>
            </div>
          )}

          <BottomSheetSelect
            label="Account"
            value={account}
            onChange={(e) => setAccount(e.target.value)}
            options={[
              { value: '', label: 'No account' },
              ...accounts.map(acc => ({ value: acc.id, label: acc.name })),
            ]}
          />

          <Input
            label="Note"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Add a note"
          />

          <Button
            type="submit"
            variant="primary"
            className="submit-btn"
            disabled={showFormDisabledState || isTransferReadOnly}
          >
            Save Transaction
          </Button>
        </form>
      </div>
    </div>
  );
}
