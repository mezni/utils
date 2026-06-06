import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import ChargerRow from './ChargerRow'

describe('ChargerRow', () => {
  it('renders connector type label', () => {
    render(
      <ChargerRow
        charger={{
          id: 'CHG-001',
          stationId: 'STN-001',
          connectorType: 'CCS',
          powerKw: 50,
          availability: 'available',
          pricePerKwh: 0.55,
          lastMaintained: '2026-05-01',
        }}
      />,
    )
    expect(screen.getByText('CCS')).toBeInTheDocument()
  })

  it('renders power in kW', () => {
    render(
      <ChargerRow
        charger={{
          id: 'CHG-001',
          stationId: 'STN-001',
          connectorType: 'Type2',
          powerKw: 22,
          availability: 'available',
          pricePerKwh: 0.45,
          lastMaintained: '2026-05-01',
        }}
      />,
    )
    expect(screen.getByText('22 kW')).toBeInTheDocument()
  })

  it('renders price per kWh', () => {
    render(
      <ChargerRow
        charger={{
          id: 'CHG-001',
          stationId: 'STN-001',
          connectorType: 'Type2',
          powerKw: 22,
          availability: 'available',
          pricePerKwh: 0.45,
          lastMaintained: '2026-05-01',
        }}
      />,
    )
    expect(screen.getByText('0.45 TND/kWh')).toBeInTheDocument()
  })

  it('shows available status', () => {
    render(
      <ChargerRow
        charger={{
          id: 'CHG-001',
          stationId: 'STN-001',
          connectorType: 'Type2',
          powerKw: 22,
          availability: 'available',
          pricePerKwh: 0.45,
          lastMaintained: '2026-05-01',
        }}
      />,
    )
    expect(screen.getByText('Disponible')).toBeInTheDocument()
  })

  it('shows unavailable status', () => {
    render(
      <ChargerRow
        charger={{
          id: 'CHG-001',
          stationId: 'STN-001',
          connectorType: 'Type2',
          powerKw: 22,
          availability: 'unavailable',
          pricePerKwh: 0.45,
          lastMaintained: '2026-05-01',
        }}
      />,
    )
    expect(screen.getByText('Indisponible')).toBeInTheDocument()
  })

  it('renders all connector types', () => {
    const { rerender } = render(
      <ChargerRow charger={{ id: '1', stationId: 'S1', connectorType: 'Type2', powerKw: 22, availability: 'available', pricePerKwh: 0.4, lastMaintained: '2026-01-01' }} />,
    )
    expect(screen.getByText('Type 2')).toBeInTheDocument()

    rerender(
      <ChargerRow charger={{ id: '1', stationId: 'S1', connectorType: 'CHAdeMO', powerKw: 50, availability: 'available', pricePerKwh: 0.5, lastMaintained: '2026-01-01' }} />,
    )
    expect(screen.getByText('CHAdeMO')).toBeInTheDocument()
  })

  it('formats price to 2 decimal places', () => {
    render(
      <ChargerRow
        charger={{
          id: 'CHG-001',
          stationId: 'STN-001',
          connectorType: 'Type2',
          powerKw: 22,
          availability: 'available',
          pricePerKwh: 0.5,
          lastMaintained: '2026-05-01',
        }}
      />,
    )
    expect(screen.getByText('0.50 TND/kWh')).toBeInTheDocument()
  })
})
