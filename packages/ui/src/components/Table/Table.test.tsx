import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { Table } from './Table'

describe('Table', () => {
  const columns = [
    { key: 'name', label: 'Name', sortable: true },
    { key: 'email', label: 'Email' },
  ]
  const data = [
    { name: 'John', email: 'john@test.com' },
    { name: 'Jane', email: 'jane@test.com' },
  ]

  it('renders column headers', () => {
    render(<Table columns={columns} data={data} />)
    expect(screen.getByText('Name')).toBeInTheDocument()
    expect(screen.getByText('Email')).toBeInTheDocument()
  })

  it('renders data rows', () => {
    render(<Table columns={columns} data={data} />)
    expect(screen.getByText('John')).toBeInTheDocument()
    expect(screen.getByText('jane@test.com')).toBeInTheDocument()
  })

  it('renders row actions', () => {
    render(<Table columns={columns} data={data} rowActions={[{ label: 'Edit', icon: <span>✎</span> }]} />)
    const icons = screen.getAllByText('✎')
    expect(icons).toHaveLength(2)
  })

  it('calls onRowAction when action clicked', () => {
    const onRowAction = vi.fn()
    render(
      <Table
        columns={columns}
        data={data}
        rowActions={[{ label: 'Edit', icon: <span>✎</span> }]}
        onRowAction={onRowAction}
      />,
    )
    const actionButtons = screen.getAllByRole('button')
    fireEvent.click(actionButtons[0])
    expect(onRowAction).toHaveBeenCalled()
  })

  it('renders empty state when no data', () => {
    render(<Table columns={columns} data={[]} />)
    expect(screen.getByText('No data')).toBeInTheDocument()
  })

  it('has proper table role', () => {
    render(<Table columns={columns} data={data} />)
    expect(screen.getByRole('table')).toBeInTheDocument()
  })
})
