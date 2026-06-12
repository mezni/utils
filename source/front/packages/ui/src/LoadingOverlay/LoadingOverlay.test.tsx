import { render, fireEvent } from '@testing-library/react-native';
import { LoadingOverlay } from './LoadingOverlay';

describe('LoadingOverlay', () => {
  it('renders nothing when not visible', () => {
    const { queryByText } = render(
      <LoadingOverlay visible={false} message="Loading..." />,
    );
    expect(queryByText('Loading...')).toBeNull();
  });

  it('renders message when visible', () => {
    const { getByText } = render(
      <LoadingOverlay visible message="Please wait..." />,
    );
    expect(getByText('Please wait...')).toBeTruthy();
  });

  it('renders cancel button when cancelable', () => {
    const onCancel = jest.fn();
    const { getByText } = render(
      <LoadingOverlay visible cancelable onCancel={onCancel} />,
    );
    fireEvent.press(getByText('Cancel'));
    expect(onCancel).toHaveBeenCalledTimes(1);
  });
});
