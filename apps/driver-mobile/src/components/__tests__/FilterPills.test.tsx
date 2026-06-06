import React from 'react'
import { render } from '@testing-library/react-native'
import FilterPills from '../FilterPills'

describe('FilterPills', () => {
  it('renders filter buttons', () => {
    const { getByText } = render(
      <FilterPills
        selectedChargerType={null}
        onChargerTypeChange={() => {}}
        selectedAvailability={null}
        onAvailabilityChange={() => {}}
      />
    )
    expect(getByText(/ac/i)).toBeTruthy()
    expect(getByText(/dc/i)).toBeTruthy()
  })

  it('highlights selected charger type', () => {
    const { getByTestId } = render(
      <FilterPills
        selectedChargerType="ac"
        onChargerTypeChange={() => {}}
        selectedAvailability={null}
        onAvailabilityChange={() => {}}
      />
    )
    const acButton = getByTestId('filter-ac')
    expect(acButton.props.style.backgroundColor).toBeDefined()
  })

  it('calls onChargerTypeChange when filter selected', () => {
    const onChargerTypeChange = jest.fn()
    const { getByTestId } = render(
      <FilterPills
        selectedChargerType={null}
        onChargerTypeChange={onChargerTypeChange}
        selectedAvailability={null}
        onAvailabilityChange={() => {}}
      />
    )
    const acButton = getByTestId('filter-ac')
    fireEvent.press(acButton)
    expect(onChargerTypeChange).toHaveBeenCalledWith('ac')
  })

  it('renders availability filters', () => {
    const { getByText } = render(
      <FilterPills
        selectedChargerType={null}
        onChargerTypeChange={() => {}}
        selectedAvailability={null}
        onAvailabilityChange={() => {}}
      />
    )
    expect(getByText(/available/i)).toBeTruthy()
  })
})

import { fireEvent } from '@testing-library/react-native'
