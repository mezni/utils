interface MapContainerProps {
  latitude: number;
  longitude: number;
  zoom?: number;
  children?: React.ReactNode;
}

export function MapContainer({
  children,
}: MapContainerProps) {
  return <div style={{ width: "100%", height: "100%", minHeight: 400 }}>{children}</div>;
}
