import { Button } from '@/components/ui/button';

interface ZoomControlsProps {
  onZoomIn: () => void;
  onZoomOut: () => void;
}

export function ZoomControls({ onZoomIn, onZoomOut }: ZoomControlsProps) {
  return (
    <div className="flex flex-col gap-1">
      <Button
        variant="outline"
        size="icon"
        onClick={onZoomIn}
        aria-label="Zoom in"
        className="h-8 w-8"
      >
        +
      </Button>
      <Button
        variant="outline"
        size="icon"
        onClick={onZoomOut}
        aria-label="Zoom out"
        className="h-8 w-8"
      >
        &minus;
      </Button>
    </div>
  );
}
