import { Partner } from '../types'

export const mockPartners: Partner[] = [
  {
    id: 'PRT-3A7K8L2M9',
    name: 'Tunisie Électricité',
    stationCount: 5,
    status: 'active',
    createdAt: '2024-01-15T08:30:00Z'
  },
  {
    id: 'PRT-4B9N3O7P0',
    name: 'EV Charge Tunisie',
    stationCount: 3,
    status: 'active',
    createdAt: '2024-02-20T10:15:00Z'
  },
  {
    id: 'PRT-5C0P4Q8R1',
    name: 'Green Energy Solutions',
    stationCount: 4,
    status: 'active',
    createdAt: '2024-03-10T14:22:00Z'
  },
  {
    id: 'PRT-6D1Q5R9S2',
    name: 'Charger Plus SA',
    stationCount: 2,
    status: 'inactive',
    createdAt: '2024-04-05T09:45:00Z'
  },
  {
    id: 'PRT-7E2R6S0T3',
    name: 'ElectroMobility Tunisie',
    stationCount: 1,
    status: 'pending',
    createdAt: '2024-05-12T16:30:00Z'
  }
]

export const getPartnerById = (id: string): Partner | undefined => {
  return mockPartners.find(p => p.id === id)
}

export const getPartnersByStatus = (status: string): Partner[] => {
  return mockPartners.filter(p => p.status === status)
}