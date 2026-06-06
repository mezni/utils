import React from 'react'
import type { SkeletonType } from '../../types'
import { neutral200 } from '../../tokens/colors'
import { radiusMd, radiusFull, radiusSm } from '../../tokens/radius'

interface SkeletonProps {
  type: SkeletonType
  width?: number | string
  height?: number | string
  animated?: boolean
}

const typeStyles: Record<SkeletonType, React.CSSProperties> = {
  block: { borderRadius: radiusMd },
  text: { borderRadius: radiusSm, height: 14 },
  circular: { borderRadius: radiusFull },
}

export function Skeleton({ type, width = '100%', height, animated = true }: SkeletonProps) {
  const resolvedWidth = typeof width === 'number' ? `${width}px` : width
  const resolvedHeight = type === 'text' && !height ? '14px' : typeof height === 'number' ? `${height}px` : height || '100%'

  return (
    <div
      className={`skeleton skeleton-${type}${animated ? ' skeleton-animated' : ''}`}
      style={{
        width: resolvedWidth,
        height: resolvedHeight,
        backgroundColor: neutral200,
        ...typeStyles[type],
        animation: animated ? 'skeleton-shimmer 1.5s ease-in-out infinite' : undefined,
      }}
      aria-busy="true"
      aria-label="Loading"
    />
  )
}
