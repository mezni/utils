interface CardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: React.ReactNode;
  accent?: 'orange' | 'green' | 'blue';
}

const accentStyles = {
  orange: 'border-l-orange-500',
  green: 'border-l-green-500',
  blue: 'border-l-blue-500',
};

const glowStyles = {
  orange: 'hover:shadow-[0_0_24px_rgba(249,115,22,0.12)]',
  green: 'hover:shadow-[0_0_24px_rgba(34,197,94,0.12)]',
  blue: 'hover:shadow-[0_0_24px_rgba(59,130,246,0.12)]',
};

const iconBgStyles = {
  orange: 'bg-gradient-to-br from-orange-500/15 to-orange-500/5 text-orange-400',
  green: 'bg-gradient-to-br from-green-500/15 to-green-500/5 text-green-400',
  blue: 'bg-gradient-to-br from-blue-500/15 to-blue-500/5 text-blue-400',
};

export function Card({ title, value, subtitle, icon, accent = 'orange' }: CardProps) {
  return (
    <div className={`bg-surface border border-gray-800 border-l-2 ${accentStyles[accent]} rounded-xl p-6 ${glowStyles[accent]} transition-all duration-200 animate-slide-up group`}>
      <div className="flex items-start justify-between">
        <div className="space-y-1.5">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-widest">{title}</p>
          <p className="text-3xl font-bold font-mono text-foreground tabular-nums">{value}</p>
          {subtitle && <p className="text-xs text-gray-500">{subtitle}</p>}
        </div>
        <div className={`p-3 ${iconBgStyles[accent]} rounded-xl group-hover:scale-110 transition-transform duration-200`}>
          {icon}
        </div>
      </div>
    </div>
  );
}
