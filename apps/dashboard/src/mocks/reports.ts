import { Report } from '../types'

export const mockPartnerReports: Report[] = [
  {
    id: 'total_stations',
    label: 'إجمالي المحطات',
    value: 5,
    trend: 'up',
    trendValue: 20
  },
  {
    id: 'total_chargers',
    label: 'إجمالي الشواحن',
    value: 14,
    trend: 'up',
    trendValue: 16.7
  },
  {
    id: 'total_reviews',
    label: 'إجمالي التقييمات',
    value: 62,
    trend: 'up',
    trendValue: 12
  },
  {
    id: 'availability',
    label: 'نسبة التوفر',
    value: 71,
    trend: 'down',
    trendValue: 3
  }
]

export const mockAdminReports: Report[] = [
  {
    id: 'total_users',
    label: 'Total Users',
    value: 1247,
    trend: 'up',
    trendValue: 8.5
  },
  {
    id: 'total_partners',
    label: 'Total Partners',
    value: 5,
    trend: 'neutral'
  },
  {
    id: 'total_stations',
    label: 'Total Stations',
    value: 15,
    trend: 'up',
    trendValue: 25
  },
  {
    id: 'total_chargers',
    label: 'Total Chargers',
    value: 48,
    trend: 'up',
    trendValue: 33.3
  },
  {
    id: 'total_reviews',
    label: 'Total Reviews',
    value: 312,
    trend: 'up',
    trendValue: 15
  },
  {
    id: 'total_events',
    label: 'Total Events',
    value: 5432,
    trend: 'up',
    trendValue: 22.4
  }
]

export const getPartnerReports = (): Report[] => {
  return mockPartnerReports
}

export const getAdminReports = (): Report[] => {
  return mockAdminReports
}