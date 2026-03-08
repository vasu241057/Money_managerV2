import { useEffect, useState } from 'react';
import '../styles/splash-screen.css';

interface SplashScreenProps {
  onFinish: () => void;
  minDuration?: number; // Minimum duration in ms
  isAppReady?: boolean;
}

export function SplashScreen({ onFinish, minDuration = 1000, isAppReady = true }: SplashScreenProps) {
  const [isVisible, setIsVisible] = useState(true);
  const [minDurationPassed, setMinDurationPassed] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => {
      setMinDurationPassed(true);
    }, minDuration);

    return () => clearTimeout(timer);
  }, [minDuration]);

  useEffect(() => {
    if (minDurationPassed && isAppReady) {
      setIsVisible(false);
      const timer = setTimeout(onFinish, 500); // Wait for fade out animation
      return () => clearTimeout(timer);
    }
  }, [minDurationPassed, isAppReady, onFinish]);

  return (
    <div className={`splash-screen ${!isVisible ? 'fade-out' : ''}`}>
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
