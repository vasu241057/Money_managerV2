import '../styles/dashboard.css';
import type { Transaction } from '../hooks/useTransactions';

interface DashboardProps {
  transactions: Transaction[];
}

export function Dashboard({ transactions, onBalanceClick }: DashboardProps & { onBalanceClick?: () => void }) {
  const currentMonthTransactions = transactions.filter(t => {
    const date = new Date(t.date);
    const now = new Date();
    return date.getMonth() === now.getMonth() && date.getFullYear() === now.getFullYear();
  });

  const totalBalance = transactions.reduce((acc, t) => {
    return t.type === 'income' ? acc + t.amount : acc - t.amount;
  }, 0);

  const income = currentMonthTransactions
    .filter(t => t.type === 'income')
    .reduce((acc, t) => acc + t.amount, 0);

  const expense = currentMonthTransactions
    .filter(t => t.type === 'expense')
    .reduce((acc, t) => acc + t.amount, 0);

  return (
    <div className="dashboard">
      <div className="balance-card" onClick={onBalanceClick}>
        <span className="label">Total Balance</span>
        <h1 className="amount">₹{totalBalance.toFixed(2)}</h1>
        <div className="stats-row">
          <div className="stat income">
            <span className="stat-label">Income (This Month)</span>
            <span className="stat-amount">+₹{income.toFixed(2)}</span>
          </div>
          <div className="stat expense">
            <span className="stat-label">Expense (This Month)</span>
            <span className="stat-amount">-₹{expense.toFixed(2)}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
