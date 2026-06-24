import styles from "./EmptyState.module.css";

export interface EmptyStateProps {
  message?: string;
}

export function EmptyState({ message = "No charging stations found in this area." }: EmptyStateProps) {
  return (
    <div className={styles.container} data-testid="empty-state">
      <div className={styles.icon}>◇</div>
      <span>{message}</span>
    </div>
  );
}
