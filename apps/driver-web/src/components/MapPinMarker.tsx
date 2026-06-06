interface MapPinMarkerProps {
  state: 'default' | 'selected' | 'unavailable'
  stationName: string
  hasAvailable: boolean
  onClick: () => void
  position: { top: string; left: string }
}

export default function MapPinMarker({ state, stationName, hasAvailable, onClick, position }: MapPinMarkerProps) {
  const colors = {
    default: hasAvailable ? 'bg-semantic-success' : 'bg-neutral-400',
    selected: 'bg-brand-primary',
    unavailable: 'bg-neutral-400',
  }

  const shadows = {
    default: hasAvailable ? 'shadow-[0_0_8px_rgba(34,197,94,0.5)]' : '',
    selected: 'shadow-[0_0_12px_rgba(59,130,246,0.6)]',
    unavailable: '',
  }

  return (
    <button
      onClick={onClick}
      className={`absolute h-4 w-4 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-white ${colors[state]} ${shadows[state]} transition-all hover:scale-125`}
      style={{ top: position.top, left: position.left }}
      aria-label={`${stationName}${hasAvailable ? ' - available' : ' - unavailable'}`}
    />
  )
}
