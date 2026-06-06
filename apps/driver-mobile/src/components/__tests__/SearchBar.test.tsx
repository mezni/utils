import React from 'react'
import { render, screen, fireEvent } from '@testing-library/react-native'
import SearchBar from '../SearchBar'

describe('SearchBar', () => {
  it('renders search input', () => {
    const { getByPlaceholderText } = render(
      <SearchBar value="" onChange={() => {}} onSubmit={() => {}} />
    )
    expect(getByPlaceholderText(/search/i)).toBeTruthy()
  })

  it('calls onChange when input changes', () => {
    const onChange = jest.fn()
    const { getByPlaceholderText } = render(
      <SearchBar value="" onChange={onChange} onSubmit={() => {}} />
    )
    const input = getByPlaceholderText(/search/i)
    fireEvent.changeText(input, 'test')
    expect(onChange).toHaveBeenCalledWith('test')
  })

  it('calls onSubmit when submit is triggered', () => {
    const onSubmit = jest.fn()
    const { getByPlaceholderText } = render(
      <SearchBar value="" onChange={() => {}} onSubmit={onSubmit} />
    )
    const input = getByPlaceholderText(/search/i)
    fireEvent.changeText(input, 'test query')
    fireEvent.press(screen.getByRole('button'))
    expect(onSubmit).toHaveBeenCalledWith('test query')
  })

  it('displays current value', () => {
    const { getByPlaceholderText } = render(
      <SearchBar value="existing search" onChange={() => {}} onSubmit={() => {}} />
    )
    const input = getByPlaceholderText(/search/i)
    expect(input.props.value).toBe('existing search')
  })
})
