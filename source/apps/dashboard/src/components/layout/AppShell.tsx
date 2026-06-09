import { Outlet } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { TopBar } from './TopBar';
import { PageContent } from './PageContent';

const pageTitles: Record<string, string> = {
  '/': 'Overview',
  '/partners': 'Partners',
  '/stations': 'Stations',
  '/chargers': 'Chargers',
};

export function AppShell() {
  const path = window.location.pathname;
  const title = pageTitles[path] || 'Dashboard';

  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex flex-1 flex-col">
        <TopBar title={title} />
        <PageContent>
          <Outlet />
        </PageContent>
      </div>
    </div>
  );
}
