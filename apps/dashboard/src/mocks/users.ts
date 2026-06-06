import { User } from '../types'

export const mockUsers: User[] = [
  {
    id: 'USR-6D1P4S8T1',
    name: 'Ahmed Ben Ali',
    email: 'ahmed.benali@example.tn',
    role: 'partner',
    status: 'active',
    partnerId: 'PRT-3A7K8L2M9',
    createdAt: '2024-02-10T14:22:00Z'
  },
  {
    id: 'USR-7E2Q5T9U2',
    name: 'Fatma Trabelsi',
    email: 'fatma.trabelsi@example.tn',
    role: 'admin',
    status: 'active',
    createdAt: '2024-01-05T09:30:00Z'
  },
  {
    id: 'USR-8F3R6U0V3',
    name: 'Mohamed Kacem',
    email: 'mohamed.kacem@example.tn',
    role: 'partner',
    status: 'active',
    partnerId: 'PRT-4B9N3O7P0',
    createdAt: '2024-02-15T11:45:00Z'
  },
  {
    id: 'USR-9G4S7V1W4',
    name: 'Sarra Ben Salem',
    email: 'sarra.bensalem@example.tn',
    role: 'admin',
    status: 'active',
    createdAt: '2024-01-10T10:00:00Z'
  },
  {
    id: 'USR-0H5T8W2X5',
    name: 'Oussama Saidi',
    email: 'oussama.saidi@example.tn',
    role: 'partner',
    status: 'inactive',
    partnerId: 'PRT-5C0P4Q8R1',
    createdAt: '2024-03-01T13:20:00Z'
  },
  {
    id: 'USR-1I6U9X3Y6',
    name: 'Leila Masmoudi',
    email: 'leila.masmoudi@example.tn',
    role: 'registered_driver',
    status: 'active',
    createdAt: '2024-04-10T15:30:00Z'
  },
  {
    id: 'USR-2J7V0Y4Z7',
    name: 'Karim Driss',
    email: 'karim.driss@example.tn',
    role: 'registered_driver',
    status: 'active',
    createdAt: '2024-04-15T16:45:00Z'
  },
  {
    id: 'USR-3K8W1Z5A8',
    name: 'Amira Bouazizi',
    email: 'amira.bouazizi@example.tn',
    role: 'admin',
    status: 'suspended',
    createdAt: '2024-01-20T11:10:00Z'
  },
  {
    id: 'USR-4L9X2A6B9',
    name: 'Nouri Gharbi',
    email: 'nouri.gharbi@example.tn',
    role: 'partner',
    status: 'active',
    partnerId: 'PRT-3A7K8L2M9',
    createdAt: '2024-02-25T14:00:00Z'
  },
  {
    id: 'USR-5M0Y3B7C0',
    name: 'Sana Jaziri',
    email: 'sana.jaziri@example.tn',
    role: 'registered_driver',
    status: 'active',
    createdAt: '2024-05-01T17:15:00Z'
  }
]

export const getUserById = (id: string): User | undefined => {
  return mockUsers.find(u => u.id === id)
}

export const getUsersByRole = (role: string): User[] => {
  return mockUsers.filter(u => u.role === role)
}

export const getUsersByStatus = (status: string): User[] => {
  return mockUsers.filter(u => u.status === status)
}