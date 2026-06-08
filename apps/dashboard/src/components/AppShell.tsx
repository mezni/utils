import Sidebar from './Sidebar';

interface Props {
  children: React.ReactNode;
}

export default function AppShell({ children }: Props) {
  return (
    <div className="flex h-screen bg-gray-50">
      <Sidebar />
      <main className="flex-1 overflow-auto p-6">{children}</main>
    </div>
  );
}
