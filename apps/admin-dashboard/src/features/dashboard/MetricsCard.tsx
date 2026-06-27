import { motion } from 'framer-motion'

interface MetricsCardProps {
  label: string
  value: string | number
  change?: string
  changeType?: 'positive' | 'negative' | 'neutral'
  icon: React.ReactNode
}

export function MetricsCard({ label, value, change, changeType = 'neutral', icon }: MetricsCardProps) {
  const changeColor = {
    positive: 'text-primary',
    negative: 'text-destructive',
    neutral: 'text-foreground/50',
  }[changeType]

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.2 }}
      className="p-5 rounded-xl border border-border bg-muted/30 hover:bg-muted/50 transition-colors duration-150"
    >
      <div className="flex items-start justify-between mb-3">
        <span className="text-xs font-medium text-foreground/50 uppercase tracking-wider">
          {label}
        </span>
        <span className="text-foreground/40" aria-hidden="true">
          {icon}
        </span>
      </div>
      <div className="flex items-baseline gap-2">
        <span className="text-2xl font-bold font-heading text-foreground">
          {value}
        </span>
        {change && (
          <span className={`text-xs font-medium ${changeColor}`}>
            {change}
          </span>
        )}
      </div>
    </motion.div>
  )
}
