import { useState } from 'react';
import { ChevronDown, Check } from 'lucide-react';
import '../../styles/ui.css';

interface BottomSheetSelectProps {
  label?: string;
  value: string;
  onChange: (e: { target: { value: string } }) => void;
  options: { value: string; label: string }[];
  error?: string;
}

export function BottomSheetSelect({ label, value, onChange, options, error }: BottomSheetSelectProps) {
  const [isOpen, setIsOpen] = useState(false);

  const selectedLabel = options.find(opt => opt.value === value)?.label || value;

  return (
    <div className="input-group">
      {label && <label className="input-label">{label}</label>}
      
      <div 
        className="input-field select-trigger" 
        onClick={() => setIsOpen(true)}
      >
        <span>{selectedLabel}</span>
        <ChevronDown size={16} className="select-arrow" />
      </div>

      {error && <span className="input-error">{error}</span>}

      {isOpen && (
        <div className="modal-overlay" onClick={() => setIsOpen(false)}>
          <div className="bottom-sheet" onClick={e => e.stopPropagation()}>
            <div className="bottom-sheet-header">
              <h3>Select {label}</h3>
            </div>
            <div className="bottom-sheet-options">
              {options.map(opt => (
                <div 
                  key={opt.value} 
                  className={`bottom-sheet-option ${value === opt.value ? 'selected' : ''}`}
                  onClick={() => {
                    onChange({ target: { value: opt.value } });
                    setIsOpen(false);
                  }}
                >
                  <span>{opt.label}</span>
                  {value === opt.value && <Check size={18} className="check-icon" />}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
