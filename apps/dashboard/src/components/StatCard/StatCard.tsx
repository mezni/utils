interface StatCardProps {
  value: number
  label: string
  trend?: 'up' | 'down' | 'neutral'
  trendValue?: number
}

export const StatCard = ({ value, label, trend, trendValue }: StatCardProps) => {
  const getTrendIcon = () => {
    if (trend === 'up') return '📈'
    if (trend === 'down') return '📉'
    return '➡️'
  }

  const getTrendColor = () => {
    if (trend === 'up') return 'text-status-successText'
    if (trend === 'down') return 'text-status-errorText'
    return 'text-text-muted'
  }

  return (
    <div className="bg-surface-panel rounded-lg p-6 border border-border-default">
      <div className="flex justify-between items-start mb-2">
        <div className="text-text-muted text-sm">{label}</div>
        {trend && trendValue && (
          <div className={`flex items-center gap-1 text-sm ${getTrendColor()}`}>
            <span>{getTrendIcon()}</span>
            <span>{trendValue}%</span>
          </div>
        )}
      </div>
      <div className="text-3xl font-bold text-text-primary">{value}</div>
    </div>
  )
}