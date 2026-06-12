import { render } from '@testing-library/react-native';
import { Skeleton } from './Skeleton';

describe('Skeleton', () => {
  it('renders text skeleton with correct number of lines', () => {
    const { UNSAFE_getAllByType } = render(<Skeleton shape="text" lines={3} />);
    expect(UNSAFE_getAllByType.bind(null, 'Animated.View')).toBeDefined();
  });

  it('renders rectangular skeleton', () => {
    const { UNSAFE_getAllByType } = render(
      <Skeleton shape="rectangular" width={100} height={200} />,
    );
    expect(UNSAFE_getAllByType.bind(null, 'Animated.View')).toBeDefined();
  });

  it('renders circular skeleton', () => {
    const { UNSAFE_getAllByType } = render(
      <Skeleton shape="circular" width={48} />,
    );
    expect(UNSAFE_getAllByType.bind(null, 'Animated.View')).toBeDefined();
  });
});
