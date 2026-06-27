import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'

interface DataPoint {
  date: string
  users: number
}

interface UserGrowthChartProps {
  data: DataPoint[]
}

export function UserGrowthChart({ data }: UserGrowthChartProps) {
  return (
    <div className="p-5 rounded-xl border border-border bg-muted/30">
      <h3 className="text-sm font-semibold text-foreground mb-4 font-heading">
        User Growth
      </h3>
      <div className="h-72" role="img" aria-label="User growth chart">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border)" />
            <XAxis
              dataKey="date"
              stroke="var(--color-foreground)"
              opacity={0.5}
              fontSize={12}
              tickLine={false}
            />
            <YAxis
              stroke="var(--color-foreground)"
              opacity={0.5}
              fontSize={12}
              tickLine={false}
              axisLine={false}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'var(--color-background)',
                border: '1px solid var(--color-border)',
                borderRadius: '8px',
                fontSize: '13px',
              }}
            />
            <Line
              type="monotone"
              dataKey="users"
              stroke="var(--color-primary)"
              strokeWidth={2}
              dot={{ fill: 'var(--color-primary)', strokeWidth: 0, r: 3 }}
              activeDot={{ r: 5, fill: 'var(--color-primary)' }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
