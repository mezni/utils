import styles from "./LoadingSpinner.module.css";

export interface LoadingSpinnerProps {
  message?: string;
}

export function LoadingSpinner({ message = "Loading..." }: LoadingSpinnerProps) {
  return (
    <div className={styles.spinner} role="status" data-testid="loading-spinner">
      <div className={styles.animation} />
      <span>{message}</span>
    </div>
  );
}
