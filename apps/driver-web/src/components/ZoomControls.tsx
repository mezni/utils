interface ZoomControlsProps {
  onZoomIn: () => void
  onZoomOut: () => void
}

export default function ZoomControls({ onZoomIn, onZoomOut }: ZoomControlsProps) {
  return (
    <div className="absolute bottom-4 right-4 flex flex-col gap-0.5">
      <button
        onClick={onZoomIn}
        className="flex h-9 w-9 items-center justify-center rounded-t-lg bg-white text-lg font-bold text-neutral-700 shadow-md hover:bg-neutral-100 focus:outline-none focus:ring-2 focus:ring-brand-primary"
        aria-label="Zoom in"
      >
        +
      </button>
      <button
        onClick={onZoomOut}
        className="flex h-9 w-9 items-center justify-center rounded-b-lg bg-white text-lg font-bold text-neutral-700 shadow-md hover:bg-neutral-100 focus:outline-none focus:ring-2 focus:ring-brand-primary"
        aria-label="Zoom out"
      >
        &minus;
      </button>
    </div>
  )
}
