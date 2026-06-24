import { useState } from 'react';
import { Input } from '../ui/Input';

interface MapPickerProps {
  latitude: number;
  longitude: number;
  onLatChange: (v: number) => void;
  onLngChange: (v: number) => void;
}

export function MapPicker({ latitude, longitude, onLatChange, onLngChange }: MapPickerProps) {
  const [latStr, setLatStr] = useState(latitude.toString());
  const [lngStr, setLngStr] = useState(longitude.toString());

  return (
    <div className="space-y-3">
      <label className="block text-sm font-medium text-gray-300">Geospatial Coordinates</label>
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Latitude"
          type="number"
          step="0.000001"
          min="-90"
          max="90"
          value={latStr}
          onChange={(e) => { setLatStr(e.target.value); const v = parseFloat(e.target.value); if (!isNaN(v)) onLatChange(v); }}
          placeholder="48.8566"
          helperText="Decimal degrees (°)"
        />
        <Input
          label="Longitude"
          type="number"
          step="0.000001"
          min="-180"
          max="180"
          value={lngStr}
          onChange={(e) => { setLngStr(e.target.value); const v = parseFloat(e.target.value); if (!isNaN(v)) onLngChange(v); }}
          placeholder="2.3522"
          helperText="Decimal degrees (°)"
        />
      </div>
      <div className="h-40 bg-surfaceAlt border border-gray-800 rounded-xl flex items-center justify-center relative overflow-hidden">
        <div className="absolute inset-0 bg-grid-subtle opacity-30" />
        <div className="relative z-10 flex flex-col items-center gap-2">
          <div className="w-6 h-6 border-2 border-orange-500 rounded-full flex items-center justify-center bg-orange-500/10 shadow-lg shadow-orange-500/30">
            <div className="w-2 h-2 bg-orange-500 rounded-full" />
          </div>
          <span className="text-xs font-mono text-gray-400">
            {latitude.toFixed(4)}°, {longitude.toFixed(4)}°
          </span>
        </div>
        {/* crosshair lines */}
        <div className="absolute inset-0 pointer-events-none">
          <div className="absolute left-1/2 top-0 bottom-0 w-px bg-orange-500/20" />
          <div className="absolute top-1/2 left-0 right-0 h-px bg-orange-500/20" />
        </div>
      </div>
      <p className="text-[11px] text-gray-600">
        Interactive map integration requires a Mapbox/Google Maps API key. Coordinates entered manually above.
      </p>
    </div>
  );
}
