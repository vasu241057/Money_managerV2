export function rupeesToPaise(amount: number): number {
  return Math.round(amount * 100);
}

export function paiseToRupees(amountInPaise: number): number {
  return amountInPaise / 100;
}
