import styles from "./ErrorBanner.module.css";

export interface ErrorBannerProps {
  message: string;
  onRetry?: () => void;
}

export function ErrorBanner({ message, onRetry }: ErrorBannerProps) {
  return (
    <div className={styles.banner} role="alert" data-testid="error-banner">
      <span className={styles.message}>{message}</span>
      {onRetry && (
        <button className={styles.retryBtn} onClick={onRetry} data-testid="retry-btn">
          Retry
        </button>
      )}
    </div>
  );
}
