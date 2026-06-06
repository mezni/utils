import { render, screen, fireEvent } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import MobileTopBar from './MobileTopBar'

describe('MobileTopBar', () => {
  it('renders brand name', () => {
    render(<MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} />)
    expect(screen.getByText('BorneMap')).toBeInTheDocument()
  })

  it('renders notification badge when count > 0', () => {
    render(<MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} notificationCount={5} />)
    expect(screen.getByText('5')).toBeInTheDocument()
  })

  it('hides badge when count is 0', () => {
    render(<MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} notificationCount={0} />)
    expect(screen.queryByText('0')).not.toBeInTheDocument()
  })

  it('shows hamburger icon when sidebar closed', () => {
    const { container } = render(<MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} />)
    const paths = container.querySelectorAll('path')
    const d = paths[0]?.getAttribute('d')
    expect(d).toContain('M4 6h16')
  })

  it('shows X icon when sidebar open', () => {
    const { container } = render(<MobileTopBar sidebarOpen={true} onToggleSidebar={() => {}} />)
    const paths = container.querySelectorAll('path')
    const d = paths[0]?.getAttribute('d')
    expect(d).toContain('M6 18L18 6')
  })

  it('calls onToggleSidebar on hamburger click', () => {
    const onToggle = vi.fn()
    render(<MobileTopBar sidebarOpen={false} onToggleSidebar={onToggle} />)
    fireEvent.click(screen.getByLabelText('Open menu'))
    expect(onToggle).toHaveBeenCalledTimes(1)
  })

  it('calls onToggleSidebar on X click', () => {
    const onToggle = vi.fn()
    render(<MobileTopBar sidebarOpen={true} onToggleSidebar={onToggle} />)
    fireEvent.click(screen.getByLabelText('Close menu'))
    expect(onToggle).toHaveBeenCalledTimes(1)
  })

  it('calls onNotificationClick when notification clicked', () => {
    const onNotif = vi.fn()
    render(<MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} onNotificationClick={onNotif} />)
    fireEvent.click(screen.getByLabelText('Notifications'))
    expect(onNotif).toHaveBeenCalledTimes(1)
  })

  it('has accessible hamburger label', () => {
    render(<MobileTopBar sidebarOpen={false} onToggleSidebar={() => {}} />)
    expect(screen.getByLabelText('Open menu')).toBeInTheDocument()
  })

  it('has accessible close label when open', () => {
    render(<MobileTopBar sidebarOpen={true} onToggleSidebar={() => {}} />)
    expect(screen.getByLabelText('Close menu')).toBeInTheDocument()
  })
})
