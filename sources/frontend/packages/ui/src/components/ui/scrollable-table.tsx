import React from "react"

interface ScrollableTableProps {
  children: React.ReactNode
  minWidth?: number
}

export function ScrollableTable({ children, minWidth = 800 }: ScrollableTableProps) {
  return (
    <div style={{ overflowX: "auto", WebkitOverflowScrolling: "touch" }}>
      <div style={{ minWidth: `${minWidth}px` }}>
        {children}
      </div>
    </div>
  )
}
