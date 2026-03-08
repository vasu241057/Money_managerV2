import { useEffect, useMemo, useRef, useState } from 'react';
import '../styles/splash-screen.css';

interface SplashScreenProps {
  onFinish: () => void;
  minDuration?: number; // Minimum duration in ms
  isAppReady?: boolean;
}

export function SplashScreen({ onFinish, minDuration = 1000, isAppReady = true }: SplashScreenProps) {
  const [minDurationPassed, setMinDurationPassed] = useState(false);
  const finishScheduledRef = useRef(false);

  const shouldFadeOut = useMemo(
    () => minDurationPassed && isAppReady,
    [minDurationPassed, isAppReady],
  );

  useEffect(() => {
    const timer = setTimeout(() => {
      setMinDurationPassed(true);
    }, minDuration);

    return () => clearTimeout(timer);
  }, [minDuration]);

  useEffect(() => {
    if (!shouldFadeOut || finishScheduledRef.current) {
      return;
    }

    finishScheduledRef.current = true;
    const timer = setTimeout(onFinish, 500); // Wait for fade out animation
    return () => clearTimeout(timer);
  }, [shouldFadeOut, onFinish]);

  return (
    <div className={`splash-screen ${shouldFadeOut ? 'fade-out' : ''}`}>
      <div className="splash-content">
        <img src="/logo.png" alt="Money Manager" className="splash-logo" />
        <h1 className="splash-title">Money Manager</h1>
      </div>
      <div className="splash-footer">
        <p>Made by Vasu Khandelwal</p>
      </div>
    </div>
  );
}
