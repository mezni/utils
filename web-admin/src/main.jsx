import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import App from './App';
import LoginPage from './pages/LoginPage';
import DashboardPage from './pages/DashboardPage';
import StationsPage from './pages/StationsPage';
import PartnersPage from './pages/PartnersPage';

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/" element={<App />}>
          <Route index element={<DashboardPage />} />
          <Route path="stations" element={<StationsPage />} />
          <Route path="partners" element={<PartnersPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
);
