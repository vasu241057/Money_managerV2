import { useMemo, useState } from 'react';
import { X } from 'lucide-react';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';
import type { Transaction } from '../hooks/useTransactions';
import { analyzeSpending } from '../services/ai';
import { Sparkles, Loader2 } from 'lucide-react';
import '../styles/analytics-modal.css';

interface AnalyticsModalProps {
  transactions: Transaction[];
  onClose: () => void;
}

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d', '#ffc658', '#ff7300'];

export function AnalyticsModal({ transactions, onClose }: AnalyticsModalProps) {
  const [analysis, setAnalysis] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [viewMode, setViewMode] = useState<'category' | 'account'>('category');

  const categoryData = useMemo(() => {
    const expenses = transactions.filter(t => t.type === 'expense');
    const categoryMap = new Map<string, number>();

    expenses.forEach(t => {
      const current = categoryMap.get(t.category) || 0;
      categoryMap.set(t.category, current + t.amount);
    });

    const data = Array.from(categoryMap.entries())
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);

    const total = data.reduce((acc, curr) => acc + curr.value, 0);

    return { data, total };
  }, [transactions]);

  const accountData = useMemo(() => {
    const expenses = transactions.filter(t => t.type === 'expense');
    const accountMap = new Map<string, number>();

    expenses.forEach(t => {
      const accountName = t.accountId || 'Cash';
      const current = accountMap.get(accountName) || 0;
      accountMap.set(accountName, current + t.amount);
    });

    const data = Array.from(accountMap.entries())
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);

    const total = data.reduce((acc, curr) => acc + curr.value, 0);

    return { data, total };
  }, [transactions]);

  const currentData = viewMode === 'category' ? categoryData : accountData;

  return (
    <div className="analytics-modal-overlay" onClick={(e) => {
      if (e.target === e.currentTarget) onClose();
    }}>
      <div className="modal-content analytics-modal">
        <div className="modal-header">
          <h2>Spending Analysis</h2>
          <button className="close-btn" onClick={onClose}>
            <X size={24} />
          </button>
        </div>

        <div className="analytics-tabs">
          <button 
            className={`tab-btn ${viewMode === 'category' ? 'active' : ''}`}
            onClick={() => setViewMode('category')}
          >
            Category
          </button>
          <button 
            className={`tab-btn ${viewMode === 'account' ? 'active' : ''}`}
            onClick={() => setViewMode('account')}
          >
            Account
          </button>
        </div>

        <div className="analytics-content">
          <div className="chart-container">
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={currentData.data}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {currentData.data.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value: number) => `₹${value.toFixed(2)}`} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </div>

          <div className="category-list">
            <h3>{viewMode === 'category' ? 'Category' : 'Account'} Breakdown</h3>
            {currentData.data.map((item, index) => (
              <div key={item.name} className="category-item">
                <div className="category-info">
                  <span className="color-dot" style={{ backgroundColor: COLORS[index % COLORS.length] }}></span>
                  <span className="category-name">{item.name}</span>
                </div>
                <div className="category-amount">
                  <span className="amount">₹{item.value.toFixed(2)}</span>
                  <span className="percentage">
                    {((item.value / currentData.total) * 100).toFixed(1)}%
                  </span>
                </div>
              </div>
            ))}
            {currentData.data.length === 0 && (
              <p className="no-data">No expense data available for analysis.</p>
            )}
          </div>
        </div>

        <div className="ai-section">
          <div className="ai-header">
            <h3><Sparkles size={18} className="ai-icon" /> AI Insights</h3>
            {!analysis && !isLoading && (
              <button 
                className="analyze-btn" 
                onClick={async () => {
                  setIsLoading(true);
                  setError(null);
                  try {
                    const result = await analyzeSpending(transactions);
                    setAnalysis(result);
                  } catch (err) {
                    setError(err instanceof Error ? err.message : "Failed to generate analysis");
                  } finally {
                    setIsLoading(false);
                  }
                }}
              >
                Analyze Spending
              </button>
            )}
          </div>

          {isLoading && (
            <div className="ai-loading">
              <Loader2 size={24} className="animate-spin" />
              <p>Analyzing your spending habits...</p>
            </div>
          )}

          {error && (
            <div className="ai-error">
              <p>{error}</p>
            </div>
          )}

          {analysis && (
            <div className="ai-result">
              <p>{analysis}</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
