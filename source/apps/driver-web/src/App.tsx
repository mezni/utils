import { Routes, Route } from 'react-router-dom';
import { MapPage } from './pages/MapPage';
import { StationDetailPage } from './pages/StationDetailPage';

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<MapPage />} />
      <Route path="/stations/:id" element={<StationDetailPage />} />
    </Routes>
  );
}
