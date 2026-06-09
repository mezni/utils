interface ZoomControlsProps {
  onZoomIn: () => void;
  onZoomOut: () => void;
}

export function ZoomControls({ onZoomIn, onZoomOut }: ZoomControlsProps) {
  return (
    <div className="flex flex-col gap-1">
      <button
        onClick={onZoomIn}
        className="flex h-8 w-8 items-center justify-center rounded bg-white text-sm font-bold text-main shadow-card hover:bg-neutral-50"
        aria-label="Zoom in"
      >
        +
      </button>
      <button
        onClick={onZoomOut}
        className="flex h-8 w-8 items-center justify-center rounded bg-white text-sm font-bold text-main shadow-card hover:bg-neutral-50"
        aria-label="Zoom out"
      >
        &minus;
      </button>
    </div>
  );
}
