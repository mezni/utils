import { ReactNode } from "react";
import { Header } from "./Header";
import { Sidebar } from "./Sidebar";
import { ToastContainer } from "../ui/Toast";
import type { UseToastResult } from "../ui/Toast";
import type { SidebarItem } from "./Sidebar";

interface AppLayoutProps {
  title: string;
  sidebarItems: SidebarItem[];
  sidebarCollapsed: boolean;
  onSidebarToggle: () => void;
  children: ReactNode;
  toast: UseToastResult;
}

export function AppLayout({
  title,
  sidebarItems,
  sidebarCollapsed,
  onSidebarToggle,
  children,
  toast,
}: AppLayoutProps) {
  return (
    <div className="flex h-screen bg-gray-50">
      <Sidebar
        items={sidebarItems}
        collapsed={sidebarCollapsed}
        onToggle={onSidebarToggle}
      />

      <div className="flex-1 flex flex-col overflow-hidden">
        <Header title={title} />

        <main className="flex-1 overflow-y-auto bg-gray-50">
          <div className="container mx-auto px-6 py-6 max-w-7xl">
            {children}
          </div>
        </main>
      </div>

      <ToastContainer toasts={toast.toasts} />
    </div>
  );
}
