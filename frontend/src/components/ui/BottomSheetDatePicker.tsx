import { useState } from 'react';
import { Calendar, ChevronLeft, ChevronRight } from 'lucide-react';
import '../../styles/ui.css';

interface BottomSheetDatePickerProps {
  label?: string;
  value: string;
  onChange: (e: { target: { value: string } }) => void;
  error?: string;
}

export function BottomSheetDatePicker({ label, value, onChange, error }: BottomSheetDatePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  // Use current date if value is empty/invalid, otherwise parse value
  const [viewDate, setViewDate] = useState(value ? new Date(value) : new Date());

  const daysInMonth = (year: number, month: number) => new Date(year, month + 1, 0).getDate();
  const firstDayOfMonth = (year: number, month: number) => new Date(year, month, 1).getDay();

  const handleDateClick = (day: number) => {
    const newDate = new Date(viewDate.getFullYear(), viewDate.getMonth(), day);
    // Adjust for timezone offset to ensure YYYY-MM-DD is correct
    const offset = newDate.getTimezoneOffset();
    const adjustedDate = new Date(newDate.getTime() - (offset*60*1000));
    onChange({ target: { value: adjustedDate.toISOString().split('T')[0] } });
    setIsOpen(false);
  };

  const changeMonth = (delta: number) => {
    setViewDate(new Date(viewDate.getFullYear(), viewDate.getMonth() + delta, 1));
  };

  const renderCalendar = () => {
    const year = viewDate.getFullYear();
    const month = viewDate.getMonth();
    const days = daysInMonth(year, month);
    const firstDay = firstDayOfMonth(year, month);
    const blanks = Array(firstDay).fill(null);
    const dayArray = Array.from({ length: days }, (_, i) => i + 1);

    return (
      <div className="calendar-grid">
        {['S', 'M', 'T', 'W', 'T', 'F', 'S'].map(d => (
          <div key={d} className="calendar-day-header">{d}</div>
        ))}
        {blanks.map((_, i) => <div key={`blank-${i}`} />)}
        {dayArray.map(day => {
          const isSelected = value && 
            new Date(value).getDate() === day && 
            new Date(value).getMonth() === month && 
            new Date(value).getFullYear() === year;
            
          const isToday = 
            new Date().getDate() === day && 
            new Date().getMonth() === month && 
            new Date().getFullYear() === year;

          return (
            <div 
              key={day} 
              className={`calendar-day ${isSelected ? 'selected' : ''} ${isToday ? 'today' : ''}`}
              onClick={() => handleDateClick(day)}
            >
              {day}
            </div>
          );
        })}
      </div>
    );
  };

  const formatDateDisplay = (dateStr: string) => {
    if (!dateStr) return 'Select Date';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric' });
  };

  return (
    <div className="input-group">
      {label && <label className="input-label">{label}</label>}
      
      <div 
        className="input-field select-trigger" 
        onClick={() => setIsOpen(true)}
      >
        <span>{formatDateDisplay(value)}</span>
        <Calendar size={16} className="select-arrow" />
      </div>

      {error && <span className="input-error">{error}</span>}

      {isOpen && (
        <div className="modal-overlay" onClick={() => setIsOpen(false)}>
          <div className="bottom-sheet" onClick={e => e.stopPropagation()}>
            <div className="bottom-sheet-header calendar-header">
              <button type="button" onClick={() => changeMonth(-1)} className="month-nav-btn"><ChevronLeft size={20} /></button>
              <span>{viewDate.toLocaleDateString('en-US', { month: 'long', year: 'numeric' })}</span>
              <button type="button" onClick={() => changeMonth(1)} className="month-nav-btn"><ChevronRight size={20} /></button>
            </div>
            
            {renderCalendar()}
            
            <div className="calendar-actions">
              <button 
                type="button" 
                className="today-btn"
                onClick={() => {
                  const today = new Date();
                  const offset = today.getTimezoneOffset();
                  const adjustedDate = new Date(today.getTime() - (offset*60*1000));
                  onChange({ target: { value: adjustedDate.toISOString().split('T')[0] } });
                  setIsOpen(false);
                }}
              >
                Today
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
