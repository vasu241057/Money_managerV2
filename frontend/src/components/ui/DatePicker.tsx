import React from 'react';
import '../../styles/ui.css';

interface DatePickerProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
}

export function DatePicker({ label, error, ...props }: DatePickerProps) {
  return (
    <div className="input-group">
      {label && <label className="input-label">{label}</label>}
      <input 
        type="date" 
        className="input-field date-field" 
        {...props} 
      />
      {error && <span className="input-error">{error}</span>}
    </div>
  );
}
