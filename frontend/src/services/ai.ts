import type { Transaction } from '../hooks/useTransactions';

export async function analyzeSpending(transactions: Transaction[]): Promise<string> {


  // Prepare a summary of transactions to send to the AI
  const expenses = transactions.filter(t => t.type === 'expense');
  
  // Group by category to provide structure, but include individual transaction details (notes)
  const expensesByCategory = new Map<string, Transaction[]>();
  expenses.forEach(t => {
    const list = expensesByCategory.get(t.category) || [];
    list.push(t);
    expensesByCategory.set(t.category, list);
  });

  let summary = "";
  expensesByCategory.forEach((txs, category) => {
    const total = txs.reduce((sum, t) => sum + t.amount, 0);
    summary += `\nCategory: ${category} (Total: ₹${total.toFixed(2)})\n`;
    txs.forEach(t => {
       summary += `- ₹${t.amount.toFixed(2)}${t.description ? ` (Note: ${t.description})` : ''}\n`;
    });
  });

  const totalExpense = expenses.reduce((acc, t) => acc + t.amount, 0);

  const prompt = `
    You are a financial advisor. Analyze the following spending summary for this month:
    
    Total Expense: ₹${totalExpense.toFixed(2)}
    
    Detailed Breakdown:
    ${summary}
    
    Please provide a brief, insightful analysis of the spending habits and 1-2 actionable tips to save money. 
    Pay attention to the specific notes/descriptions of the transactions to give more personalized advice.
    Keep it friendly and concise (max 3-4 sentences).
  `;

  try {
    const response = await fetch('/api/analyze', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ prompt })
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || 'Failed to fetch analysis');
    }

    const data = await response.json();
    return data.choices[0]?.message?.content || "No analysis could be generated.";
  } catch (error) {
    console.error("AI Analysis Error:", error);
    throw error;
  }
}
