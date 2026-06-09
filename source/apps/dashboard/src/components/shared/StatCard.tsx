interface StatCardProps {
  label: string;
  value: number | string;
  icon?: React.ReactNode;
}

export function StatCard({ label, value, icon }: StatCardProps) {
  return (
    <div className="flex items-center gap-4 rounded-lg border border-default bg-card p-5 shadow-card">
      {icon && <div className="text-brand-primary">{icon}</div>}
      <div>
        <p className="text-sm text-muted">{label}</p>
        <p className="text-2xl font-bold text-main">{value}</p>
      </div>
    </div>
  );
}
