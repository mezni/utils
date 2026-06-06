import { Review } from '../types'

export const mockReviews: Review[] = [
  {
    id: 'REV-1A2B3C4D5',
    stationId: 'STN-4B8N2P6Q9',
    userId: 'USR-1I6U9X3Y6',
    rating: 5,
    text: 'محطة ممتازة ونظيفة جدا. شحن سريع وسهل.',
    date: '2024-03-05T16:45:00Z',
    language: 'ar'
  },
  {
    id: 'REV-2B3C4D5E6',
    stationId: 'STN-5C9O3R7S0',
    userId: 'USR-2J7V0Y4Z7',
    rating: 4,
    text: 'Good location, fast charging. Will come back.',
    date: '2024-03-10T14:30:00Z',
    language: 'en'
  },
  {
    id: 'REV-3C4D5E6F7',
    stationId: 'STN-6D1P4S8T1',
    userId: 'USR-3K8W1Z5A8',
    rating: 5,
    text: 'Excellente station, très rapide et bien située.',
    date: '2024-03-15T11:20:00Z',
    language: 'fr'
  },
  {
    id: 'REV-4D5E6F7G8',
    stationId: 'STN-7E2Q5T9U2',
    userId: 'USR-1I6U9X3Y6',
    rating: 4,
    text: 'جيدة لكن تحتاج تحديث.',
    date: '2024-03-20T15:50:00Z',
    language: 'ar'
  },
  {
    id: 'REV-5E6F7G8H9',
    stationId: 'STN-4B8N2P6Q9',
    userId: 'USR-2J7V0Y4Z7',
    rating: 3,
    text: 'Average experience. Could be better.',
    date: '2024-03-25T13:10:00Z',
    language: 'en'
  }
]

export const getReviewsByStation = (stationId: string): Review[] => {
  return mockReviews.filter(r => r.stationId === stationId)
}

export const getReviewsByUser = (userId: string): Review[] => {
  return mockReviews.filter(r => r.userId === userId)
}

export const getReviewById = (id: string): Review | undefined => {
  return mockReviews.find(r => r.id === id)
}