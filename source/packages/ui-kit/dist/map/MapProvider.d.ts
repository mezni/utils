import type { ReactNode } from "react";
export interface MapProviderProps {
    center: [number, number];
    zoom: number;
    children: ReactNode;
    onViewportChange?: (center: [number, number], zoom: number) => void;
}
export declare function MapProvider({ center, zoom, children, onViewportChange }: MapProviderProps): import("react").JSX.Element;
//# sourceMappingURL=MapProvider.d.ts.map