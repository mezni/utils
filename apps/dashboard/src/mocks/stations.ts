import { Station } from '../types'

export const mockStations: Station[] = [
  {
    id: 'STN-4B8N2P6Q9',
    name: 'Centre Urbain Nord',
    address: 'Avenue Habib Bourguiba, Tunis',
    latitude: 36.8008,
    longitude: 10.1859,
    partnerId: 'PRT-3A7K8L2M9',
    chargerCount: 4,
    status: 'available',
    availability: 75,
    reviews: 24,
    averageRating: 4.2
  },
  {
    id: 'STN-5C9O3R7S0',
    name: 'Lac 2 Parking',
    address: 'Boulevard du Lac 2, Tunis',
    latitude: 36.8221,
    longitude: 10.1788,
    partnerId: 'PRT-3A7K8L2M9',
    chargerCount: 3,
    status: 'available',
    availability: 66,
    reviews: 18,
    averageRating: 4.5
  },
  {
    id: 'STN-6D1P4S8T1',
    name: 'Carrefour Tunis',
    address: 'Rue du Lac, Tunis',
    latitude: 36.8340,
    longitude: 10.1897,
    partnerId: 'PRT-4B9N3O7P0',
    chargerCount: 4,
    status: 'in-use',
    availability: 50,
    reviews: 31,
    averageRating: 4.1
  },
  {
    id: 'STN-7E2Q5T9U2',
    name: 'Marsa Plaza',
    address: 'Rue de la Plage, Marsa',
    latitude: 36.8857,
    longitude: 10.3215,
    partnerId: 'PRT-5C0P4Q8R1',
    chargerCount: 3,
    status: 'available',
    availability: 100,
    reviews: 15,
    averageRating: 4.8
  },
  {
    id: 'STN-8F3R6U0V3',
    name: 'Ariana Mall',
    address: 'Route de La Marsa, Ariana',
    latitude: 36.8582,
    longitude: 10.1654,
    partnerId: 'PRT-5C0P4Q8R1',
    chargerCount: 2,
    status: 'maintenance',
    availability: 0,
    reviews: 8,
    averageRating: 3.9
  },
  {
    id: 'STN-9G4S7V1W4',
    name: 'Gammarth Center',
    address: 'Rue de Gammarth, Tunis',
    latitude: 36.9104,
    longitude: 10.2563,
    partnerId: 'PRT-3A7K8L2M9',
    chargerCount: 2,
    status: 'available',
    availability: 100,
    reviews: 12,
    averageRating: 4.4
  },
  {
    id: 'STN-0H5T8W2X5',
    name: 'Sidi Bou Said',
    address: 'Route de Sidi Bou Said',
    latitude: 36.8713,
    longitude: 10.3450,
    partnerId: 'PRT-6D1Q5R9S2',
    chargerCount: 1,
    status: 'in-use',
    availability: 0,
    reviews: 6,
    averageRating: 4.6
  },
  {
    id: 'STN-1I6U9X3Y6',
    name: 'La Marsa Port',
    address: 'Port de La Marsa',
    latitude: 36.8969,
    longitude: 10.3097,
    partnerId: 'PRT-4B9N3O7P0',
    chargerCount: 3,
    status: 'available',
    availability: 66,
    reviews: 22,
    averageRating: 4.3
  },
  {
    id: 'STN-2J7V0Y4Z7',
    name: 'Bardo Museum',
    address: 'Le Bardo, Tunis',
    latitude: 36.8092,
    longitude: 10.1385,
    partnerId: 'PRT-3A7K8L2M9',
    chargerCount: 2,
    status: 'available',
    availability: 100,
    reviews: 19,
    averageRating: 4.7
  },
  {
    id: 'STN-3K8W1Z5A8',
    name: 'Sfax Gare',
    address: 'Gare de Sfax',
    latitude: 34.7406,
    longitude: 10.7603,
    partnerId: 'PRT-5C0P4Q8R1',
    chargerCount: 2,
    status: 'maintenance',
    availability: 0,
    reviews: 9,
    averageRating: 4.0
  },
  {
    id: 'STN-4L9X2A6B9',
    name: 'Sousse Medina',
    address: 'Medina de Sousse',
    latitude: 35.8256,
    longitude: 10.6084,
    partnerId: 'PRT-7E2R6S0T3',
    chargerCount: 1,
    status: 'available',
    availability: 100,
    reviews: 5,
    averageRating: 4.9
  },
  {
    id: 'STN-5M0Y3B7C0',
    name: 'Bizerte Port',
    address: 'Port de Bizerte',
    latitude: 37.2745,
    longitude: 9.8736,
    partnerId: 'PRT-4B9N3O7P0',
    chargerCount: 3,
    status: 'available',
    availability: 100,
    reviews: 17,
    averageRating: 4.2
  },
  {
    id: 'STN-6N1Z4C8D1',
    name: 'Monastir Center',
    address: 'Avenue Habib Bourguiba, Monastir',
    latitude: 35.7792,
    longitude: 10.8265,
    partnerId: 'PRT-5C0P4Q8R1',
    chargerCount: 2,
    status: 'in-use',
    availability: 50,
    reviews: 11,
    averageRating: 4.5
  },
  {
    id: 'STN-7O2A5D9E2',
    name: 'Nabeul Center',
    address: 'Centre de Nabeul',
    latitude: 36.4538,
    longitude: 10.7345,
    partnerId: 'PRT-3A7K8L2M9',
    chargerCount: 3,
    status: 'available',
    availability: 100,
    reviews: 20,
    averageRating: 4.6
  },
  {
    id: 'STN-8P3B6E0F3',
    name: 'Zarzis Beach',
    address: 'Plage de Zarzis',
    latitude: 33.5196,
    longitude: 11.0254,
    partnerId: 'PRT-5C0P4Q8R1',
    chargerCount: 2,
    status: 'available',
    availability: 100,
    reviews: 7,
    averageRating: 4.8
  }
]

export const getStationsByPartner = (partnerId: string): Station[] => {
  return mockStations.filter(s => s.partnerId === partnerId)
}

export const getStationById = (id: string): Station | undefined => {
  return mockStations.find(s => s.id === id)
}

export const getStationsByStatus = (status: string): Station[] => {
  return mockStations.filter(s => s.status === status)
}