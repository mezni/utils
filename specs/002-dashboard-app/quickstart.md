# Dashboard App — Developer Quickstart

**Target Audience**: Developers implementing the Dashboard App

**Duration**: 15-30 minutes to have a working dev environment

---

## Prerequisites

Before you start, ensure you have:

- **Node.js 18+** installed (`node --version`)
- **npm or yarn** available
- **Git** configured
- **Backend running locally** (Sprint 1.1 backend at `http://localhost:8000`)
  - See AGENTS.md "Running MVP-1 Locally" section for backend setup

---

## 1. Clone/Navigate to Dashboard Directory

```bash
cd /home/dali/WORK/BorneMap/source/apps/dashboard
```

> If `dashboard/` directory doesn't exist yet, create it:
> ```bash
> mkdir -p source/apps/dashboard
> cd source/apps/dashboard
> ```

---

## 2. Initialize Project

If starting fresh, initialize with Vite:

```bash
npm create vite@latest . -- --template react-ts
```

Or use a TypeScript template directly:

```bash
npm create vite@latest dashboard --template react-ts
cd dashboard
```

---

## 3. Install Dependencies

```bash
npm install
```

Core dependencies:

```bash
npm install react react-dom react-router-dom axios
npm install -D typescript @types/react @types/react-dom @types/node
```

UI/Form libraries:

```bash
npm install @headlessui/react @hookform/resolvers
npm install react-hook-form
npm install classnames
```

Tailwind CSS (including base config from shared tokens):

```bash
npm install -D tailwindcss postcss autoprefixer
npm install -D @tailwindcss/forms
```

---

## 4. Configure Tailwind

Create `tailwind.config.js`:

```javascript
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Brand colors (from source/packages/ui/src/tokens/colors.ts)
        brand: {
          primary: "#007943",    // Main action color
          sageLight: "#EAF0E6",   // Active/selected states
          glow: "#00E676",        // Live markers (driver apps)
        },
        // Status colors
        status: {
          available: "#10B981",   // Green
          inUse: "#F59E0B",       // Amber
          maintenance: "#EF4444", // Red
        },
        // Surface colors
        surface: {
          background: "#F9FAFB",
          card: "#FFFFFF",
          sidebar: "#FFFFFF",
        },
        // Text colors
        text: {
          main: "#111827",
          muted: "#6B7280",
        },
      },
      spacing: {
        // Base 4px scale
        4: "0.25rem",   // 4px
        8: "0.5rem",    // 8px
        12: "0.75rem",  // 12px
        16: "1rem",     // 16px
        20: "1.25rem",  // 20px
        24: "1.5rem",   // 24px
        32: "2rem",     // 32px
        40: "2.5rem",   // 40px
        48: "3rem",     // 48px
        64: "4rem",     // 64px
        80: "5rem",     // 80px
        96: "6rem",     // 96px
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
}
```

Create `postcss.config.js`:

```javascript
export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
}
```

---

## 5. Environment Configuration

Create `.env.local`:

```bash
VITE_API_BASE_URL=http://localhost:8000
```

Reference in `src/services/api.ts`:

```typescript
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
```

---

## 6. Project Structure Setup

Create the directory structure from `plan.md`:

```bash
mkdir -p src/components/{AppShell,Common,Forms,Screens}
mkdir -p src/services
mkdir -p src/hooks
mkdir -p src/types
mkdir -p src/utils
mkdir -p tests/{unit,integration}
mkdir -p public
```

---

## 7. Create API Client

`src/services/api.ts`:

```typescript
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add response interceptor for error handling
api.interceptors.response.use(
  response => response,
  error => {
    if (!error.response) {
      // Network error - API unreachable
      console.error('API unreachable:', error);
    }
    return Promise.reject(error);
  }
);

export default api;
```

---

## 8. Create Type Definitions

`src/types/api.ts`:

```typescript
export interface Partner {
  id: string;
  name: string;
  created_at: string;
}

export interface Station {
  id: string;
  partner_id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
  charger_count: number;
  available_count: number;
  created_at: string;
  updated_at: string;
}

export type ChargerStatus = 'available' | 'in_use' | 'maintenance';

export interface Charger {
  id: string;
  station_id: string;
  connector_type: string;
  power_kw: number;
  status: ChargerStatus;
  created_at: string;
  updated_at: string;
}

export interface ApiResponse<T> {
  data: T;
}

export interface ApiListResponse<T> {
  data: T[];
}
```

---

## 9. Create First Data Hook

`src/hooks/usePartners.ts`:

```typescript
import { useState, useEffect } from 'react';
import api from '../services/api';
import { Partner, ApiListResponse } from '../types/api';

export function usePartners() {
  const [partners, setPartners] = useState<Partner[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetch = async () => {
    setLoading(true);
    try {
      const response = await api.get<ApiListResponse<Partner>>('/api/v1/partners');
      setPartners(response.data.data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Unknown error'));
    } finally {
      setLoading(false);
    }
  };

  const create = async (payload: { name: string }) => {
    const response = await api.post<ApiResponse<Partner>>('/api/v1/partners', payload);
    setPartners([...partners, response.data.data]);
    return response.data.data;
  };

  const update = async (id: string, payload: { name: string }) => {
    const response = await api.put<ApiResponse<Partner>>(`/api/v1/partners/${id}`, payload);
    setPartners(partners.map(p => p.id === id ? response.data.data : p));
    return response.data.data;
  };

  const delete_partner = async (id: string) => {
    await api.delete(`/api/v1/partners/${id}`);
    setPartners(partners.filter(p => p.id !== id));
  };

  useEffect(() => {
    fetch();
  }, []);

  return { partners, loading, error, fetch, create, update, delete: delete_partner };
}
```

---

## 10. Create AppShell Layout

`src/components/AppShell/Layout.tsx`:

```typescript
import React from 'react';
import Sidebar from './Sidebar';
import TopBar from './TopBar';

interface LayoutProps {
  children: React.ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  return (
    <div className="flex h-screen bg-surface-background">
      {/* Sidebar - fixed left */}
      <Sidebar />
      
      {/* Main content area */}
      <div className="flex-1 flex flex-col">
        {/* Top bar - fixed */}
        <TopBar />
        
        {/* Page content - scrollable */}
        <main className="flex-1 overflow-y-auto p-6">
          {children}
        </main>
      </div>
    </div>
  );
}
```

`src/components/AppShell/Sidebar.tsx`:

```typescript
import { Link, useLocation } from 'react-router-dom';

export default function Sidebar() {
  const location = useLocation();
  
  const navItems = [
    { path: '/', label: 'Overview' },
    { path: '/partners', label: 'Partners' },
    { path: '/stations', label: 'Stations' },
    { path: '/chargers', label: 'Chargers' },
  ];

  return (
    <aside className="w-64 bg-surface-sidebar border-r border-gray-200 flex flex-col">
      {/* Logo/Brand */}
      <div className="p-6 border-b border-gray-200">
        <h1 className="text-xl font-bold text-brand-primary">BorneMap</h1>
        <p className="text-xs text-text-muted">Admin</p>
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-4 space-y-2">
        {navItems.map(item => (
          <Link
            key={item.path}
            to={item.path}
            className={`block px-4 py-2 rounded ${
              location.pathname === item.path
                ? 'bg-brand-sageLight text-brand-primary font-semibold'
                : 'text-text-main hover:bg-gray-100'
            }`}
          >
            {item.label}
          </Link>
        ))}
      </nav>

      {/* Settings link */}
      <div className="p-4 border-t border-gray-200">
        <Link to="/settings" className="text-sm text-text-muted hover:text-brand-primary">
          Settings
        </Link>
      </div>
    </aside>
  );
}
```

`src/components/AppShell/TopBar.tsx`:

```typescript
export default function TopBar() {
  return (
    <header className="h-16 bg-white border-b border-gray-200 flex items-center px-6">
      <h2 className="text-lg font-semibold text-brand-primary">Dashboard</h2>
    </header>
  );
}
```

---

## 11. Create Root App Component

`src/App.tsx`:

```typescript
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Layout from './components/AppShell/Layout';
import Overview from './components/Screens/Overview';
import PartnersScreen from './components/Screens/PartnersScreen';
import StationsScreen from './components/Screens/StationsScreen';
import ChargersScreen from './components/Screens/ChargersScreen';

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/partners" element={<PartnersScreen />} />
          <Route path="/stations" element={<StationsScreen />} />
          <Route path="/chargers" element={<ChargersScreen />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}
```

`src/main.tsx`:

```typescript
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
```

`src/index.css`:

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

/* Custom global styles */
html, body {
  margin: 0;
  padding: 0;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}

body {
  background-color: #F9FAFB;
}
```

---

## 12. Create Placeholder Screens

Create empty screen components that will be filled in during implementation:

`src/components/Screens/Overview.tsx`:
```typescript
export default function Overview() {
  return <div className="p-6"><h1>Overview</h1></div>;
}
```

`src/components/Screens/PartnersScreen.tsx`:
```typescript
export default function PartnersScreen() {
  return <div className="p-6"><h1>Partners</h1></div>;
}
```

`src/components/Screens/StationsScreen.tsx`:
```typescript
export default function StationsScreen() {
  return <div className="p-6"><h1>Stations</h1></div>;
}
```

`src/components/Screens/ChargersScreen.tsx`:
```typescript
export default function ChargersScreen() {
  return <div className="p-6"><h1>Chargers</h1></div>;
}
```

---

## 13. Run Development Server

```bash
npm run dev
```

You should see output like:

```
  VITE v5.0.0  ready in 456 ms

  ➜  Local:   http://localhost:5173/
  ➜  press h to show help
```

Visit `http://localhost:5173/` in your browser.

---

## 14. First Test: Create a Partner

**Prerequisites**:
- Backend running at `http://localhost:8000`
- Dashboard running at `http://localhost:5173`

**Steps**:

1. Navigate to `/partners` screen
2. Click "Create Partner" button
3. Enter name: "Test Partner"
4. Click "Save"
5. Check that partner appears in the table

**Debug if needed**:
- Open browser DevTools (F12)
- Check Network tab for API request to `POST /api/v1/partners`
- Check Console for any errors
- Verify backend is running: `curl http://localhost:8000/api/v1/health`

---

## 15. Building for Production

```bash
npm run build
```

Output will be in `dist/` directory, ready for deployment.

---

## Common Issues & Fixes

### "API unreachable"
- Verify backend is running: `curl http://localhost:8000/api/v1/health`
- Check `VITE_API_BASE_URL` in `.env.local`
- Ensure CORS is not blocking (backend should allow it)

### "Cannot find module '@'"
- Check `vite.config.ts` has alias configured:
  ```typescript
  alias: {
    '@': fileURLToPath(new URL('./src', import.meta.url))
  }
  ```

### Tailwind styles not loading
- Verify `tailwind.config.js` content paths are correct
- Rebuild: `npm run dev` (restart dev server)
- Clear Vite cache: `rm -rf .vite` and restart

### TypeScript errors
- Run `npm install` again to ensure all types are installed
- Check `tsconfig.json` is configured correctly

---

## Next Steps

1. **Implement Partners Screen** (simplest CRUD)
   - DataTable component
   - Create/Edit modals
   - Delete confirmation
   
2. **Implement Common Components**
   - Modal wrapper
   - DataTable
   - ErrorState, EmptyState
   - StatusBadge
   
3. **Implement Stations Screen**
   - Form with coordinate validation
   - Partner filter dropdown
   
4. **Implement Chargers Screen**
   - Status badge colors
   - Charger form with status enum
   
5. **Implement Overview Screen**
   - StatCards with real counts
   - Summary table

---

## Reference

- **Backend API**: `docs/api/bornemap-service.md`
- **Data Model**: `specs/002-dashboard-app/data-model.md`
- **API Contracts**: `specs/002-dashboard-app/contracts/api-integration.md`
- **Design Tokens**: `source/packages/ui/src/tokens/colors.ts`
- **Implementation Plan**: `specs/002-dashboard-app/plan.md`

---

**Ready to build!** 🚀
