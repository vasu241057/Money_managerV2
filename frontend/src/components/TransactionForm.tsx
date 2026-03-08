import React, { useState } from "react";
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
  onSubmit: (transaction: Omit<Transaction, 'id'> | Transaction) => void;
  onClose: () => void;
  initialData?: Transaction | null;
}

export function TransactionForm({ onSubmit, onClose, initialData = null }: TransactionFormProps) {
  const { categories } = useCategories();
  const { accounts } = useAccounts();
  const [type, setType] = useState<'income' | 'expense'>(initialData?.type || "expense");
  const [amount, setAmount] = useState(initialData?.amount?.toString() || "");
  const [date, setDate] = useState(
    initialData?.date 
      ? new Date(initialData.date).toISOString().split("T")[0] 
      : new Date().toISOString().split("T")[0]
  );

  // Initialize state directly from props
  // We use a key on the component in App.tsx to force re-mount when initialData changes,
  // so we don't need useEffect to sync state with props.
  const initialCategoryId = initialData
    ? categories.find((c) => c.name === initialData.category)?.id
    : "";

  const [categoryId, setCategoryId] = useState(initialCategoryId || "");
  const [subCategory, setSubCategory] = useState(
    initialData?.subCategory || ""
  );
  const [account, setAccount] = useState(initialData?.accountId || accounts[0]?.name || "Cash");
  const [description, setDescription] = useState(initialData?.description || "");

  const handleCategoryChange = (newCategoryId: string) => {
    setCategoryId(newCategoryId);
    // Reset subCategory if it doesn't exist in the new category
    const cat = categories.find(c => c.id === newCategoryId);
    if (cat && subCategory && !cat.subCategories?.includes(subCategory)) {
      setSubCategory("");
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (!amount) {
      alert("Please enter an amount");
      return;
    }
    if (!categoryId) {
      alert("Please select a category");
      return;
    }

    const selectedCategory = categories.find((c) => c.id === categoryId);

    try {
      onSubmit({
        ...(initialData?.id ? { id: initialData.id } : {}),
        amount: parseFloat(amount),
        type,
        date: new Date(date),
        category: selectedCategory?.name || "Uncategorized",
        subCategory,
        accountId: account,
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
          <div className="type-toggle-container">
            <div className="type-toggle">
              <button
                type="button"
                className={`type-btn ${
                  type === "expense" ? "active expense" : ""
                }`}
                onClick={() => setType("expense")}
              >
                Expense
              </button>
              <button
                type="button"
                className={`type-btn ${
                  type === "income" ? "active income" : ""
                }`}
                onClick={() => setType("income")}
              >
                Income
              </button>
            </div>
          </div>

          <Input
            type="number"
            placeholder="0"
            className="amount-input"
            value={amount}
            onChange={(e) => setAmount(e.target.value)}
          />

          <BottomSheetDatePicker
            label="Date"
            value={date}
            onChange={(e) => setDate(e.target.value)}
            error={!date ? "Date is required" : ""}
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
            options={accounts.map(acc => ({ value: acc.name, label: acc.name }))}
          />

          <Input
            label="Note"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Add a note"
          />

          <Button type="submit" variant="primary" className="submit-btn">
            Save Transaction
          </Button>
        </form>
      </div>
    </div>
  );
}
