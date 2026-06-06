interface DataCardProps {
  title: string
  children: React.ReactNode
  actions?: React.ReactNode
}

export const DataCard = ({ title, children, actions }: DataCardProps) => {
  return (
    <div className="bg-surface-panel rounded-lg border border-border-default">
      <div className="flex justify-between items-center px-6 py-4 border-b border-border-default">
        <h3 className="text-lg font-semibold text-text-primary">{title}</h3>
        {actions && <div className="flex gap-2">{actions}</div>}
      </div>
      <div className="p-6">{children}</div>
    </div>
  )
}