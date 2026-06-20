export interface Partner {
  id: string;
  name: string;
  networkType: "INDIVIDUAL" | "COMPANY";
  supportPhone: string;
  supportEmail: string;
  isVerified: boolean;
  stationCount: number;
  createdAt: string;
}

export interface Station {
  id: string;
  partnerId: string;
  partnerName: string;
  name: string;
  address: string;
  location: { lat: number; lng: number };
  chargerCount: number;
  createdAt: string;
}

export interface Charger {
  id: string;
  stationId: string;
  stationName: string;
  connectorType: string;
  currentType: string;
  status: "available" | "occupied" | "offline" | "unknown";
  powerKw: number;
  voltage: number;
  amperage: number;
  countAvailable: number;
  countTotal: number;
  createdAt: string;
}

export const mockPartners: Partner[] = [
  { id: "OPR-a1b2c3d4e5f6", name: "GreenCharge Networks", networkType: "COMPANY", supportPhone: "+33 1 23 45 67 89", supportEmail: "support@greencharge.fr", isVerified: true, stationCount: 24, createdAt: "2026-01-15T10:30:00Z" },
  { id: "OPR-b2c3d4e5f6a7", name: "Electra Mobility", networkType: "COMPANY", supportPhone: "+33 1 98 76 54 32", supportEmail: "hello@electra-mobility.com", isVerified: true, stationCount: 18, createdAt: "2026-02-20T14:00:00Z" },
  { id: "OPR-c3d4e5f6a7b8", name: "Jean Dupont", networkType: "INDIVIDUAL", supportPhone: "+33 6 12 34 56 78", supportEmail: "j.dupont@email.fr", isVerified: false, stationCount: 2, createdAt: "2026-03-10T09:15:00Z" },
  { id: "OPR-d4e5f6a7b8c9", name: "ChargePoint France", networkType: "COMPANY", supportPhone: "+33 1 40 50 60 70", supportEmail: "contact@chargepoint.fr", isVerified: true, stationCount: 47, createdAt: "2026-04-05T16:45:00Z" },
  { id: "OPR-e5f6a7b8c9d0", name: "Marie Martin", networkType: "INDIVIDUAL", supportPhone: "+33 6 98 76 54 32", supportEmail: "m.martin@email.fr", isVerified: false, stationCount: 1, createdAt: "2026-05-12T11:30:00Z" },
];

export const mockStations: Station[] = [
  { id: "STA-a1b2c3d4e5f6", partnerId: "OPR-a1b2c3d4e5f6", partnerName: "GreenCharge Networks", name: "Gare de Lyon", address: "20 Boulevard Diderot, 75012 Paris", location: { lat: 48.8448, lng: 2.3735 }, chargerCount: 6, createdAt: "2026-01-20T08:00:00Z" },
  { id: "STA-b2c3d4e5f6a7", partnerId: "OPR-a1b2c3d4e5f6", partnerName: "GreenCharge Networks", name: "Aéroport CDG Terminal 2", address: "Roissy-en-France, 95700", location: { lat: 49.0097, lng: 2.5479 }, chargerCount: 12, createdAt: "2026-02-01T10:00:00Z" },
  { id: "STA-c3d4e5f6a7b8", partnerId: "OPR-b2c3d4e5f6a7", partnerName: "Electra Mobility", name: "Centre Commercial Val d'Europe", address: "14 Cours de la Garonne, 77700 Serris", location: { lat: 48.8456, lng: 2.7805 }, chargerCount: 8, createdAt: "2026-02-25T09:30:00Z" },
  { id: "STA-d4e5f6a7b8c9", partnerId: "OPR-d4e5f6a7b8c9", partnerName: "ChargePoint France", name: "Gare Montparnasse", address: "17 Boulevard de Vaugirard, 75015 Paris", location: { lat: 48.8411, lng: 2.3190 }, chargerCount: 10, createdAt: "2026-04-10T14:00:00Z" },
  { id: "STA-e5f6a7b8c9d0", partnerId: "OPR-d4e5f6a7b8c9", partnerName: "ChargePoint France", name: "Lyon Part-Dieu", address: "5 Place Charles Béraudier, 69003 Lyon", location: { lat: 45.7605, lng: 4.8595 }, chargerCount: 4, createdAt: "2026-04-20T11:00:00Z" },
];

export const mockChargers: Charger[] = [
  { id: "CHG-a1b2c3d4e5f6", stationId: "STA-a1b2c3d4e5f6", stationName: "Gare de Lyon", connectorType: "CCS", currentType: "DC", status: "available", powerKw: 150, voltage: 800, amperage: 200, countAvailable: 2, countTotal: 3, createdAt: "2026-01-20T08:00:00Z" },
  { id: "CHG-b2c3d4e5f6a7", stationId: "STA-a1b2c3d4e5f6", stationName: "Gare de Lyon", connectorType: "Type2", currentType: "AC", status: "occupied", powerKw: 22, voltage: 400, amperage: 32, countAvailable: 0, countTotal: 2, createdAt: "2026-01-20T08:00:00Z" },
  { id: "CHG-c3d4e5f6a7b8", stationId: "STA-b2c3d4e5f6a7", stationName: "Aéroport CDG Terminal 2", connectorType: "CHAdeMO", currentType: "DC", status: "available", powerKw: 50, voltage: 500, amperage: 125, countAvailable: 3, countTotal: 4, createdAt: "2026-02-01T10:00:00Z" },
  { id: "CHG-d4e5f6a7b8c9", stationId: "STA-c3d4e5f6a7b8", stationName: "Centre Commercial Val d'Europe", connectorType: "CCS", currentType: "DC", status: "offline", powerKw: 350, voltage: 800, amperage: 500, countAvailable: 0, countTotal: 2, createdAt: "2026-02-25T09:30:00Z" },
  { id: "CHG-e5f6a7b8c9d0", stationId: "STA-d4e5f6a7b8c9", stationName: "Gare Montparnasse", connectorType: "Type2", currentType: "AC", status: "available", powerKw: 22, voltage: 400, amperage: 32, countAvailable: 4, countTotal: 6, createdAt: "2026-04-10T14:00:00Z" },
];

export const dashboardStats = {
  totalPartners: mockPartners.length,
  verifiedPartners: mockPartners.filter(p => p.isVerified).length,
  totalStations: mockStations.length,
  totalChargers: mockChargers.length,
  availableChargers: mockChargers.filter(c => c.status === "available").length,
  activeChargers: mockChargers.filter(c => c.status !== "offline").length,
};
