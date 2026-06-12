import { render } from '@testing-library/react-native';
import { Badge } from './Badge';

describe('Badge', () => {
  it('renders children text', () => {
    const { getByText } = render(<Badge>New</Badge>);
    expect(getByText('New')).toBeTruthy();
  });

  it('renders all variants', () => {
    const variants = ['default', 'success', 'warning', 'error', 'info'] as const;
    for (const variant of variants) {
      const { getByText, unmount } = render(<Badge variant={variant}>{variant}</Badge>);
      expect(getByText(variant)).toBeTruthy();
      unmount();
    }
  });

  it('renders all sizes', () => {
    const sizes = ['sm', 'md', 'lg'] as const;
    for (const size of sizes) {
      const { getByText, unmount } = render(<Badge size={size}>{size}</Badge>);
      expect(getByText(size)).toBeTruthy();
      unmount();
    }
  });
});
