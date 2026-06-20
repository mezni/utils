import { BrowserRouter, Routes, Route } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import Dashboard from "./pages/Dashboard";
import Partners from "./pages/Partners";
import Stations from "./pages/Stations";
import Chargers from "./pages/Chargers";

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex min-h-screen">
        <Sidebar />
        <main className="flex-1 ml-64 p-8">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/partners" element={<Partners />} />
            <Route path="/stations" element={<Stations />} />
            <Route path="/chargers" element={<Chargers />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
