import { Skeleton as ShadcnSkeleton } from '@/components/ui/skeleton';

interface SkeletonProps {
  className?: string;
}

export function Skeleton({ className }: SkeletonProps) {
  return <ShadcnSkeleton className={className} />;
}
