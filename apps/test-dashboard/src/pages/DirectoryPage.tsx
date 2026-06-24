import { useState, useMemo, useCallback } from 'react';
import { AppShell } from '../components/layout/AppShell';
import { Sidebar } from '../components/layout/Sidebar';
import { ContextBar, ContextSnippet } from '../components/layout/ContextBar';
import { BreadcrumbNav } from '../components/layout/BreadcrumbNav';
import { HyperTable, colId, colBadge, colTelemetry, type HyperColumn } from '../components/data/HyperTable';
import { StatePill } from '../components/data/StatePill';
import { TelemetryMini } from '../components/data/TelemetryMini';
import { Button } from '../components/ui/Button';
import { Badge } from '../components/ui/Badge';
import { SlideDrawer } from '../components/forms/SlideDrawer';
import { ProvisionWizard } from '../components/forms/ProvisionWizard';
import { MapPicker } from '../components/forms/MapPicker';
import { OcppConfigFields } from '../components/forms/OcppConfigFields';
import { HardwareProfileFields } from '../components/forms/HardwareProfileFields';
import { FinancialSplitFields } from '../components/forms/FinancialSplitFields';
import { GridLimitCalculator } from '../components/forms/GridLimitCalculator';
import { Input } from '../components/ui/Input';
import { Modal } from '../components/ui/Modal';
import { DependencyCheckModal } from '../components/safety/DependencyCheckModal';
import { UnbindArchiveFlow } from '../components/safety/UnbindArchiveFlow';
import { FailsafeConfirm } from '../components/safety/FailsafeConfirm';
import { TwoStepApproval } from '../components/safety/TwoStepApproval';
import {
  getPartners, getPartner, getStationsForPartner, getStation,
  getChargersForStation, getCharger,
  createPartner, updatePartner, deletePartner,
  createStation, updateStation, deleteStation,
  createCharger, updateCharger, unbindCharger, softDeleteCharger,
} from '../data/mock';
import type { Partner } from '../types/partner';
import type { Station } from '../types/station';
import type { Charger } from '../types/charger';
import type { EntityStatus } from '../types/common';

/* ─── Column defs ─── */

const partnerColumns: HyperColumn<Partner>[] = [
  { key: 'id', header: 'ID', width: 'w-24', render: (p) => <span className="font-mono text-[11px] text-gray-500">{p.id}</span>, hideOnMobile: true },
  { key: 'name', header: 'Partner', width: 'flex-[2]', render: (p) => <span className="font-medium text-gray-200">{p.name}</span> },
  colBadge<Partner>('w-24'),
  { key: 'stations', header: 'Stations', width: 'w-20', render: (p) => <span className="font-mono text-xs text-gray-400 tabular-nums">{p.station_count}</span> },
  { key: 'chargers', header: 'Chargers', width: 'w-20', render: (p) => <span className="font-mono text-xs text-gray-400 tabular-nums">{p.charger_count}</span> },
  { key: 'power', header: 'Total kW', width: 'w-24', render: (p) => <span className="font-mono text-xs text-orange-400 tabular-nums">{p.total_power_kw.toLocaleString()}</span>, hideOnMobile: true },
  colTelemetry<Partner>('w-48'),
];

const stationColumns: HyperColumn<Station>[] = [
  colId<Station>('w-20'),
  { key: 'name', header: 'Station', width: 'flex-[2]', render: (s) => <span className="font-medium text-gray-200">{s.name}</span> },
  colBadge<Station>('w-24'),
  { key: 'load', header: 'Load', width: 'w-28', render: (s) => (
    <div className="flex items-center gap-2">
      <span className="font-mono text-xs text-orange-400 tabular-nums">{s.current_load_kw} kW</span>
      <div className="w-12 h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${s.current_load_kw / s.grid_limit_kw > 0.8 ? 'bg-yellow-500' : 'bg-green-500'}`}
          style={{ width: `${Math.min((s.current_load_kw / s.grid_limit_kw) * 100, 100)}%` }} />
      </div>
    </div>
  ), hideOnMobile: true },
  { key: 'chargers', header: 'Active', width: 'w-16', render: (s) => (
    <span className="font-mono text-xs text-gray-400 tabular-nums">{s.chargers_active}/{s.charger_count}</span>
  ) },
  colTelemetry<Station>('w-48'),
];

const chargerColumns: HyperColumn<Charger>[] = [
  { key: 'chargeBox', header: 'ChargeBox ID', width: 'w-36', render: (c) => <span className="font-mono text-xs text-gray-400">{c.charge_box_id}</span> },
  { key: 'model', header: 'Model', width: 'w-32', render: (c) => <span className="text-xs text-gray-300">{c.manufacturer} {c.model}</span> },
  colBadge<Charger>('w-24'),
  { key: 'power', header: 'Power', width: 'w-20', render: (c) => (
    <span className="font-mono text-xs tabular-nums">
      <span className="text-orange-400">{c.power_rating_kw}</span>
      <span className="text-gray-600">kW</span>
    </span>
  ) },
  { key: 'session', header: 'Session', width: 'w-20', render: (c) => c.charger_state === 'CHARGING' ? (
    <span className="font-mono text-xs text-blue-400 tabular-nums">{c.session_energy_kwh.toFixed(1)} kWh</span>
  ) : <span className="text-xs text-gray-600">—</span> },
  { key: 'ocpp', header: 'OCPP', width: 'w-16', render: (c) => <span className="text-xs text-gray-500">{c.ocpp_version}</span>, hideOnMobile: true },
  { key: 'revenue', header: 'RevShare', width: 'w-20', render: (c) => <span className="font-mono text-xs text-gray-400 tabular-nums">{c.revenue_share_pct.toFixed(1)}%</span>, hideOnMobile: true },
  colTelemetry<Charger>('w-44'),
];

/* ─── Page ─── */

export function DirectoryPage() {
  /* Navigation state */
  const [selectedPartnerId, setSelectedPartnerId] = useState<string | null>(null);
  const [selectedStationId, setSelectedStationId] = useState<string | null>(null);
  const [selectedChargerId, setSelectedChargerId] = useState<string | null>(null);
  const [contextBarOpen, setContextBarOpen] = useState(true);

  /* Entity data */
  const partners = useMemo(() => getPartners(), []);
  const selectedPartner = useMemo(() => selectedPartnerId ? getPartner(selectedPartnerId) ?? null : null, [selectedPartnerId]);
  const stations = useMemo(() => selectedPartnerId ? getStationsForPartner(selectedPartnerId) : [], [selectedPartnerId]);
  const selectedStation = useMemo(() => selectedStationId ? getStation(selectedStationId) ?? null : null, [selectedStationId]);
  const chargers = useMemo(() => selectedStationId ? getChargersForStation(selectedStationId) : [], [selectedStationId]);
  const selectedCharger = useMemo(() => selectedChargerId ? getCharger(selectedChargerId) ?? null : null, [selectedChargerId]);

  /* Create/Edit state */
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerMode, setDrawerMode] = useState<'create' | 'edit'>('create');
  const [drawerEntity, setDrawerEntity] = useState<'partner' | 'station' | 'charger'>('partner');

  const [wizardOpen, setWizardOpen] = useState(false);
  const [wizardStep, setWizardStep] = useState(0);

  /* Safety modals */
  const [dependencyCheck, setDependencyCheck] = useState<{ type: 'Partner' | 'Station'; id: string; name: string } | null>(null);
  const [unbindFlow, setUnbindFlow] = useState<Charger | null>(null);
  const [failsafe, setFailsafe] = useState<{ action: string; id: string; matchString: string } | null>(null);
  const [twoStepApproval, setTwoStepApproval] = useState<{ command: string; id: string } | null>(null);

  /* Form state */
  const [form, setForm] = useState<Record<string, any>>({});
  const [formErrors, setFormErrors] = useState<Record<string, string>>({});
  const [wizardForm, setWizardForm] = useState<Record<string, any>>({});

  const updateForm = useCallback((field: string, value: any) => {
    setForm(prev => ({ ...prev, [field]: value }));
    setFormErrors(prev => { const { [field]: _, ...rest } = prev; return rest; });
  }, []);

  /* ─── Handlers ─── */

  const openCreate = (entity: 'partner' | 'station' | 'charger') => {
    setDrawerMode('create');
    setDrawerEntity(entity);
    setForm({});
    setFormErrors({});
    setDrawerOpen(true);
  };

  const openEdit = (entity: 'partner' | 'station' | 'charger', data: any) => {
    setDrawerMode('edit');
    setDrawerEntity(entity);
    setForm({ ...data });
    setFormErrors({});
    setDrawerOpen(true);
  };

  const handleDrawerSave = () => {
    if (drawerEntity === 'partner') {
      if (!form.name?.trim()) { setFormErrors({ name: 'Name is required' }); return; }
      if (drawerMode === 'create') createPartner({ name: form.name });
      else if (selectedPartnerId) updatePartner(selectedPartnerId, form);
    } else if (drawerEntity === 'station') {
      if (!form.name?.trim()) { setFormErrors({ name: 'Name is required' }); return; }
      if (drawerMode === 'create' && selectedPartnerId) {
        createStation({ name: form.name, location: form.location || '', partner_id: selectedPartnerId });
      } else if (selectedStationId) {
        updateStation(selectedStationId, form);
      }
    } else if (drawerEntity === 'charger') {
      if (!form.charge_box_id?.trim()) { setFormErrors({ chargeBoxId: 'ChargeBox ID is required' }); return; }
      if (drawerMode === 'create' && selectedStationId) {
        createCharger({ station_id: selectedStationId, charge_box_id: form.charge_box_id });
      } else if (selectedChargerId) {
        updateCharger(selectedChargerId, form);
      }
    }
    setDrawerOpen(false);
  };

  /* Delete handlers */
  const handleDeletePartner = (p: Partner) => {
    const activeStations = stations.filter(s => s.status === 'ACTIVE');
    if (activeStations.length > 0) {
      setDependencyCheck({ type: 'Partner', id: p.id, name: p.name });
    } else {
      const success = deletePartner(p.id);
      if (success) { setSelectedPartnerId(null); setSelectedStationId(null); setSelectedChargerId(null); }
    }
  };

  const handleDeleteStation = (s: Station) => {
    const activeChargers = chargers.filter(c => c.status === 'ACTIVE');
    if (activeChargers.length > 0) {
      setDependencyCheck({ type: 'Station', id: s.id, name: s.name });
    } else {
      deleteStation(s.id);
      setSelectedStationId(null);
      setSelectedChargerId(null);
    }
  };

  /* Wizard */
  const handleWizardComplete = () => {
    // create charger with wizard data
    if (selectedStationId && wizardForm.chargeBoxId) {
      createCharger({ station_id: selectedStationId, charge_box_id: wizardForm.chargeBoxId });
    }
    setWizardOpen(false);
    setWizardStep(0);
    setWizardForm({});
  };

  /* ─── Sidebar items ─── */

  const partnerSidebarItems = partners.map(p => ({
    id: p.id,
    label: p.name,
    count: p.station_count,
    icon: <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" /></svg>,
    active: selectedPartnerId === p.id,
    onClick: () => { setSelectedPartnerId(p.id); setSelectedStationId(null); setSelectedChargerId(null); },
  }));

  /* ─── Context crumbs ─── */

  const crumbs = [];
  if (selectedPartner) crumbs.push({ label: selectedPartner.name, onClick: () => { setSelectedStationId(null); setSelectedChargerId(null); } });
  if (selectedStation) crumbs.push({ label: selectedStation.name, onClick: () => setSelectedChargerId(null) });
  if (selectedCharger) crumbs.push({ label: selectedCharger.charge_box_id });

  /* ─── Render ─── */

  return (
    <>
      <AppShell
        sidebar={
          <Sidebar
            title="Partners"
            items={partnerSidebarItems}
          />
        }
        contextBar={
          selectedPartner ? (
            <ContextBar
              title={selectedPartner.name}
              subtitle={`${stations.length} stations · ${selectedPartner.charger_count} chargers`}
              onClose={() => { setSelectedStationId(null); setSelectedChargerId(null); }}
            >
              <div className="flex items-center justify-between">
                <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Stations</span>
                <Button size="sm" variant="ghost" onClick={() => openCreate('station')}>+ Add</Button>
              </div>
              {stations.length === 0 ? (
                <p className="text-xs text-gray-600 py-8 text-center">No stations. Create one to get started.</p>
              ) : (
                <div className="space-y-1">
                  {stations.map(s => (
                    <button
                      key={s.id}
                      onClick={() => setSelectedStationId(s.id)}
                      className={`w-full text-left px-3 py-2.5 rounded-lg transition-all text-sm
                        ${selectedStationId === s.id ? 'bg-orange-500/10 border border-orange-500/20' : 'hover:bg-gray-800/50 border border-transparent'}`}
                    >
                      <div className="flex items-center justify-between">
                        <span className={`font-medium truncate ${selectedStationId === s.id ? 'text-orange-300' : 'text-gray-300'}`}>{s.name}</span>
                        <StatePill status={s.status} size="sm" />
                      </div>
                      <div className="flex items-center gap-3 mt-1 text-[11px] text-gray-600">
                        <span>{s.charger_count} chargers</span>
                        <span>{s.current_load_kw} / {s.grid_limit_kw} kW</span>
                      </div>
                    </button>
                  ))}
                </div>
              )}
              {selectedPartner && (
                <>
                  <div className="pt-3 border-t border-gray-800 space-y-2">
                    <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Partner Details</span>
                    <ContextSnippet label="External ID" value={selectedPartner.external_id} mono />
                    <ContextSnippet label="Tax ID" value={selectedPartner.tax_id} mono />
                    <ContextSnippet label="Email" value={selectedPartner.email} />
                    <ContextSnippet label="Phone" value={selectedPartner.phone} />
                  </div>
                  <div className="flex gap-2 pt-2">
                    <Button size="sm" variant="secondary" onClick={() => openEdit('partner', selectedPartner)} className="flex-1">Edit</Button>
                    <Button size="sm" variant="danger" onClick={() => handleDeletePartner(selectedPartner)} className="flex-1">Delete</Button>
                  </div>
                </>
              )}
            </ContextBar>
          ) : (
            <ContextBar title="Select a Partner" subtitle="Choose from the sidebar to view stations">
              <div className="flex flex-col items-center justify-center py-12 text-center gap-3">
                <svg className="w-10 h-10 text-gray-700" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857" />
                </svg>
                <p className="text-xs text-gray-600">Select a partner to view their stations, chargers, and telemetry data.</p>
              </div>
            </ContextBar>
          )
        }
        contextBarOpen={contextBarOpen}
        onToggleContextBar={() => setContextBarOpen(!contextBarOpen)}
        main={
          <div className="space-y-6 animate-fade-in">
            <BreadcrumbNav crumbs={crumbs} />

            {/* Top header area */}
            <div className="flex items-start justify-between">
              <div>
                <h1 className="text-xl font-bold text-white font-mono tracking-tight">
                  {selectedCharger ? selectedCharger.charge_box_id
                    : selectedStation ? selectedStation.name
                    : selectedPartner ? selectedPartner.name
                    : 'Asset Directory'}
                </h1>
                <p className="text-sm text-gray-500 mt-0.5">
                  {selectedCharger ? `${selectedCharger.manufacturer} ${selectedCharger.model} · ${selectedCharger.power_rating_kw}kW`
                    : selectedStation ? `${selectedStation.charger_count} chargers · ${selectedStation.total_power_kw}kW capacity`
                    : selectedPartner ? `${selectedPartner.station_count} stations · ${selectedPartner.charger_count} chargers`
                    : 'Select a partner to begin drilling down'}
                </p>
              </div>
              <div className="flex items-center gap-2">
                {selectedPartner && !selectedStation && (
                  <Button size="sm" variant="secondary" onClick={() => openCreate('station')}>+ Station</Button>
                )}
                {selectedPartner && (
                  <Button size="sm" variant="secondary" onClick={() => openCreate('partner')}>+ Partner</Button>
                )}
              </div>
            </div>

            {/* Charger Detail View */}
            {selectedCharger && (
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-5 animate-slide-up">
                {/* Main panel */}
                <div className="lg:col-span-2 space-y-5">
                  {/* Header card */}
                  <div className="bg-surface border border-gray-800 rounded-xl p-5 space-y-4">
                    <div className="flex items-start justify-between">
                      <div className="flex items-center gap-3">
                        <div className={`p-2 rounded-xl ${
                          selectedCharger.charger_state === 'IDLE' || selectedCharger.charger_state === 'CHARGING'
                            ? 'bg-green-500/15 text-green-400'
                            : selectedCharger.charger_state === 'FAULTED'
                              ? 'bg-red-500/15 text-red-400'
                              : 'bg-gray-500/15 text-gray-400'
                        }`}>
                          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                          </svg>
                        </div>
                        <div>
                          <p className="text-lg font-semibold text-white font-mono">{selectedCharger.charge_box_id}</p>
                          <p className="text-xs text-gray-500">{selectedCharger.manufacturer} {selectedCharger.model} · SN: {selectedCharger.serial_number}</p>
                        </div>
                      </div>
                      <StatePill status={selectedCharger.status as EntityStatus} pulse />
                    </div>

                    {/* Connectors */}
                    <div>
                      <p className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Connectors</p>
                      <div className="grid grid-cols-3 gap-2">
                        {selectedCharger.connectors.map((conn) => (
                          <div key={conn.id} className={`p-3 rounded-lg border text-xs ${
                            conn.session_active ? 'bg-blue-500/5 border-blue-500/20' : 'bg-surfaceAlt border-gray-800'
                          }`}>
                            <div className="flex items-center justify-between mb-1">
                              <span className="font-mono text-gray-400">{conn.id}</span>
                              <Badge status={conn.session_active ? 'CHARGING' : conn.status as EntityStatus} />
                            </div>
                            <p className="text-gray-500 font-mono">{conn.type}</p>
                            <p className="text-gray-400 font-mono tabular-nums mt-1">
                              {conn.power_current_kw > 0 ? (
                                <span className="text-blue-400">{conn.power_current_kw.toFixed(0)}</span>
                              ) : (
                                <span className="text-gray-600">0</span>
                              )}
                              <span className="text-gray-600"> / {conn.power_rated_kw} kW</span>
                            </p>
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Session info */}
                    {selectedCharger.charger_state === 'CHARGING' && selectedCharger.session_id && (
                      <div className="p-3 bg-blue-500/5 border border-blue-500/20 rounded-xl flex items-center gap-3">
                        <div className="w-2 h-2 bg-blue-400 rounded-full animate-pulse-dot" />
                        <div>
                          <p className="text-xs font-medium text-blue-400">Active Session</p>
                          <p className="text-xs text-gray-500 font-mono">
                            {selectedCharger.session_id} · {selectedCharger.session_energy_kwh.toFixed(2)} kWh
                          </p>
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Charger table */}
                  <div>
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="text-sm font-semibold text-gray-300">All Chargers at {selectedStation?.name}</h3>
                      <div className="flex gap-2">
                        <Button size="sm" variant="ghost" onClick={() => openCreate('charger')}>+ Charger</Button>
                        <Button size="sm" variant="ghost" onClick={() => setWizardOpen(true)}>Wizard</Button>
                      </div>
                    </div>
                    <HyperTable
                      columns={chargerColumns}
                      data={chargers}
                      onRowClick={(c) => setSelectedChargerId(c.id)}
                      rowActions={(c) => [
                        { label: 'Edit', icon: <EditIcon />, onClick: () => openEdit('charger', c) },
                        { label: 'Remote Reboot', icon: <RebootIcon />, onClick: () => setFailsafe({ action: 'Remote Reboot', id: c.id, matchString: c.charge_box_id }), variant: 'default' },
                        { label: 'Unbind & Archive', icon: <UnlinkIcon />, onClick: () => setUnbindFlow(c), variant: 'danger' },
                        { label: 'Soft Delete', icon: <TrashIcon />, onClick: () => { softDeleteCharger(c.id); }, variant: 'danger' },
                      ]}
                      emptyMessage="No chargers at this station"
                    />
                  </div>
                </div>

                {/* Right detail panel */}
                <div className="space-y-4">
                  <div className="bg-surface border border-gray-800 rounded-xl p-5 space-y-4">
                    <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Telemetry</h4>
                    <TelemetryMini telemetry={selectedCharger.telemetry} />
                    <div className="grid grid-cols-2 gap-3 text-xs">
                      <div><span className="text-gray-600">Temperature</span><p className="font-mono text-gray-300 mt-0.5">{selectedCharger.telemetry.temperature_c.toFixed(1)}°C</p></div>
                      <div><span className="text-gray-600">Sessions</span><p className="font-mono text-gray-300 mt-0.5 tabular-nums">{selectedCharger.telemetry.session_count}</p></div>
                      <div><span className="text-gray-600">Energy Total</span><p className="font-mono text-gray-300 mt-0.5 tabular-nums">{selectedCharger.telemetry.energy_total_kwh.toFixed(0)} kWh</p></div>
                      <div><span className="text-gray-600">Last Seen</span><p className="font-mono text-gray-300 mt-0.5 text-[10px]">{new Date(selectedCharger.telemetry.last_seen).toLocaleTimeString()}</p></div>
                    </div>
                  </div>

                  <div className="bg-surface border border-gray-800 rounded-xl p-5 space-y-3">
                    <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Configuration</h4>
                    <div className="space-y-2 text-xs">
                      <InfoRow label="OCPP" value={`${selectedCharger.ocpp_version}`} />
                      <InfoRow label="Firmware" value={selectedCharger.firmware_version} />
                      <InfoRow label="OCPI Visible" value={selectedCharger.ocpi_visible ? 'Yes' : 'No'} />
                      <InfoRow label="Energy Rate" value={`€${selectedCharger.energy_rate_per_kwh.toFixed(3)}/kWh`} />
                      <InfoRow label="Rev Share" value={`${selectedCharger.revenue_share_pct.toFixed(1)}%`} />
                      <InfoRow label="Tariff ID" value={selectedCharger.tariff_id} mono />
                    </div>
                  </div>

                  <div className="flex gap-2">
                    <Button size="sm" variant="secondary" onClick={() => openEdit('charger', selectedCharger)} className="flex-1">Edit</Button>
                    <Button size="sm" variant="danger" onClick={() => setUnbindFlow(selectedCharger)} className="flex-1">Unbind</Button>
                  </div>
                </div>
              </div>
            )}

            {/* Station View (no charger selected) */}
            {selectedStation && !selectedCharger && (
              <div className="animate-slide-up">
                {/* Station KPIs */}
                <div className="grid grid-cols-4 gap-4 mb-6">
                  {[
                    { label: 'Chargers', value: `${selectedStation.chargers_active}/${selectedStation.charger_count}`, color: 'text-orange-400' },
                    { label: 'Load', value: `${selectedStation.current_load_kw} kW`, color: 'text-blue-400' },
                    { label: 'Capacity', value: `${selectedStation.total_power_kw} kW`, color: 'text-green-400' },
                    { label: 'Grid Limit', value: `${selectedStation.grid_limit_kw} kW`, color: 'text-yellow-400' },
                  ].map(kpi => (
                    <div key={kpi.label} className="bg-surface border border-gray-800 rounded-xl p-4">
                      <p className="text-xs text-gray-600">{kpi.label}</p>
                      <p className={`text-lg font-bold font-mono tabular-nums mt-1 ${kpi.color}`}>{kpi.value}</p>
                    </div>
                  ))}
                </div>

                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-sm font-semibold text-gray-300">Chargers</h3>
                  <div className="flex gap-2">
                    <Button size="sm" variant="ghost" onClick={() => openCreate('charger')}>+ Charger</Button>
                    <Button size="sm" variant="ghost" onClick={() => setWizardOpen(true)}>Provision Wizard</Button>
                  </div>
                </div>
                <HyperTable
                  columns={chargerColumns}
                  data={chargers}
                  onRowClick={(c) => setSelectedChargerId(c.id)}
                  rowActions={(c) => [
                    { label: 'Edit', icon: <EditIcon />, onClick: () => openEdit('charger', c) },
                    { label: 'Remote Reboot', icon: <RebootIcon />, onClick: () => setFailsafe({ action: 'Remote Reboot', id: c.id, matchString: c.charge_box_id }), variant: 'default' },
                    { label: 'Unbind & Archive', icon: <UnlinkIcon />, onClick: () => setUnbindFlow(c), variant: 'danger' },
                    { label: 'Soft Delete', icon: <TrashIcon />, onClick: () => { softDeleteCharger(c.id); }, variant: 'danger' },
                  ]}
                  emptyMessage="No chargers at this station. Create one or use the provision wizard."
                />
              </div>
            )}

            {/* Partner View (no station selected) */}
            {selectedPartner && !selectedStation && (
              <div className="animate-slide-up">
                <HyperTable
                  columns={stationColumns}
                  data={stations}
                  onRowClick={(s) => setSelectedStationId(s.id)}
                  rowActions={(s) => [
                    { label: 'Edit', icon: <EditIcon />, onClick: () => openEdit('station', s) },
                    { label: 'Delete', icon: <TrashIcon />, onClick: () => handleDeleteStation(s), variant: 'danger' },
                  ]}
                  emptyMessage="No stations for this partner"
                />
              </div>
            )}

            {/* Welcome (nothing selected) */}
            {!selectedPartner && (
              <div className="flex flex-col items-center justify-center py-24 gap-6 text-center animate-fade-in">
                <div className="p-5 bg-surface rounded-2xl border border-gray-800">
                  <svg className="w-16 h-16 text-orange-500/30" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
                  </svg>
                </div>
                <div>
                  <h2 className="text-xl font-bold text-white font-mono">EV Infrastructure Directory</h2>
                  <p className="text-sm text-gray-500 mt-2 max-w-md">
                    Select a partner from the sidebar to explore their stations and chargers.
                    Use the context panel to maintain hierarchical awareness.
                  </p>
                </div>
                <Button onClick={() => openCreate('partner')}>+ Create Partner</Button>
              </div>
            )}
          </div>
        }
      />

      {/* ─── Slide Drawer (Create/Edit) ─── */}
      <SlideDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        title={drawerMode === 'create' ? `Create ${drawerEntity.charAt(0).toUpperCase() + drawerEntity.slice(1)}` : `Edit ${drawerEntity.charAt(0).toUpperCase() + drawerEntity.slice(1)}`}
        subtitle={drawerMode === 'create' ? `Add a new ${drawerEntity} to the network` : `Modify ${drawerEntity} configuration`}
        width="max-w-lg"
        footer={
          <>
            <Button variant="ghost" onClick={() => setDrawerOpen(false)}>Cancel</Button>
            <Button onClick={handleDrawerSave} variant="primary">{drawerMode === 'create' ? 'Create' : 'Save Changes'}</Button>
          </>
        }
      >
        {drawerEntity === 'partner' && (
          <div className="space-y-4">
            <Input label="Organization Name" value={form.name || ''} onChange={(e) => updateForm('name', e.target.value)} placeholder="e.g. GreenCharge Networks" error={formErrors.name} />
            <Input label="Email" type="email" value={form.email || ''} onChange={(e) => updateForm('email', e.target.value)} placeholder="ops@example.com" />
            <Input label="Phone" value={form.phone || ''} onChange={(e) => updateForm('phone', e.target.value)} placeholder="+49 30 1234567" />
            <Input label="Address" value={form.address || ''} onChange={(e) => updateForm('address', e.target.value)} placeholder="Street, City, Country" />
            <Input label="Tax ID" value={form.tax_id || ''} onChange={(e) => updateForm('tax_id', e.target.value)} placeholder="DE-321654987" className="font-mono text-xs" />
          </div>
        )}
        {drawerEntity === 'station' && (
          <div className="space-y-4">
            <Input label="Station Name" value={form.name || ''} onChange={(e) => updateForm('name', e.target.value)} placeholder="e.g. Berlin Hauptbahnhof Hub" error={formErrors.name} />
            <MapPicker
              latitude={form.latitude ?? 51.5}
              longitude={form.longitude ?? -0.12}
              onLatChange={(v) => updateForm('latitude', v)}
              onLngChange={(v) => updateForm('longitude', v)}
            />
            <Input label="Address" value={form.address || ''} onChange={(e) => updateForm('address', e.target.value)} />
            <Input label="Timezone" value={form.timezone || 'Europe/Berlin'} onChange={(e) => updateForm('timezone', e.target.value)} />
            <GridLimitCalculator
              gridLimitKw={form.grid_limit_kw ?? 1000}
              totalChargerPowerKw={chargers.reduce((s, c) => s + c.power_rating_kw, 0)}
              onChange={(v) => updateForm('grid_limit_kw', v)}
            />
          </div>
        )}
        {drawerEntity === 'charger' && (
          <div className="space-y-5">
            <OcppConfigFields
              chargeBoxId={form.charge_box_id || ''}
              ocppVersion={form.ocpp_version || '2.0.1'}
              serialNumber={form.serial_number || ''}
              onChange={(f, v) => updateForm(f, v)}
              errors={formErrors}
            />
            <HardwareProfileFields
              manufacturer={form.manufacturer || ''}
              model={form.model || ''}
              powerRatingKw={form.power_rating_kw ?? 350}
              maxConnectors={form.max_connectors ?? 3}
              connectorTypes={form.connector_types || ['CCS2', 'TYPE2']}
              onChange={(f, v) => updateForm(f, v)}
            />
            <FinancialSplitFields
              revenueSharePct={form.revenue_share_pct ?? 5}
              payoutAddress={form.payout_address || ''}
              tariffId={form.tariff_id || 'TARIFF-STANDARD'}
              energyRatePerKwh={form.energy_rate_per_kwh ?? 0.35}
              onChange={(f, v) => updateForm(f, v)}
            />
          </div>
        )}
      </SlideDrawer>

      {/* ─── Provision Wizard ─── */}
      {wizardOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/70 backdrop-blur-sm animate-fade-in">
          <div className="w-full max-w-2xl bg-surface border border-gray-800 rounded-2xl shadow-2xl p-6 max-h-[90vh] overflow-y-auto">
            <ProvisionWizard
              steps={[
                { id: 'identity', label: 'Identity', description: 'OCPP & serial', icon: <span>1</span> },
                { id: 'hardware', label: 'Hardware', description: 'Power & connectors', icon: <span>2</span> },
                { id: 'financial', label: 'Financial', description: 'Revenue & tariffs', icon: <span>3</span> },
                { id: 'review', label: 'Review', description: 'Final checks', icon: <span>4</span> },
              ]}
              currentStep={wizardStep}
              onStepChange={setWizardStep}
              onComplete={handleWizardComplete}
              onCancel={() => { setWizardOpen(false); setWizardStep(0); setWizardForm({}); }}
              canProceed={true}
              completeLabel="Provision Charger"
            >
              {wizardStep === 0 && (
                <div className="space-y-4">
                  <p className="text-sm text-gray-400 mb-2">Enter the OCPP identity and hardware serial for the new charger.</p>
                  <OcppConfigFields
                    chargeBoxId={wizardForm.chargeBoxId || ''}
                    ocppVersion={wizardForm.ocppVersion || '2.0.1'}
                    serialNumber={wizardForm.serialNumber || ''}
                    onChange={(f, v) => setWizardForm((prev: any) => ({ ...prev, [f]: v }))}
                  />
                </div>
              )}
              {wizardStep === 1 && (
                <div className="space-y-4">
                  <p className="text-sm text-gray-400 mb-2">Configure the hardware profile, power rating, and connector layout.</p>
                  <HardwareProfileFields
                    manufacturer={wizardForm.manufacturer || 'ABB'}
                    model={wizardForm.model || 'Terra 350'}
                    powerRatingKw={wizardForm.powerRatingKw ?? 350}
                    maxConnectors={wizardForm.maxConnectors ?? 3}
                    connectorTypes={wizardForm.connectorTypes || ['CCS2', 'TYPE2']}
                    onChange={(f, v) => setWizardForm((prev: any) => ({ ...prev, [f]: v }))}
                  />
                </div>
              )}
              {wizardStep === 2 && (
                <div className="space-y-4">
                  <p className="text-sm text-gray-400 mb-2">Configure revenue sharing, payout routing, and tariff information.</p>
                  <FinancialSplitFields
                    revenueSharePct={wizardForm.revenueSharePct ?? 5}
                    payoutAddress={wizardForm.payoutAddress || ''}
                    tariffId={wizardForm.tariffId || 'TARIFF-STANDARD'}
                    energyRatePerKwh={wizardForm.energyRatePerKwh ?? 0.35}
                    onChange={(f, v) => setWizardForm((prev: any) => ({ ...prev, [f]: v }))}
                  />
                </div>
              )}
              {wizardStep === 3 && (
                <div className="space-y-4">
                  <p className="text-sm font-medium text-green-400 flex items-center gap-2">
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                    Ready to provision
                  </p>
                  <div className="bg-surfaceAlt rounded-xl p-4 space-y-2 text-sm">
                    <ReviewRow label="ChargeBox ID" value={wizardForm.chargeBoxId || '—'} />
                    <ReviewRow label="OCPP" value={wizardForm.ocppVersion || '—'} />
                    <ReviewRow label="Serial" value={wizardForm.serialNumber || '—'} />
                    <ReviewRow label="Manufacturer" value={wizardForm.manufacturer || '—'} />
                    <ReviewRow label="Model" value={wizardForm.model || '—'} />
                    <ReviewRow label="Power Rating" value={wizardForm.powerRatingKw ? `${wizardForm.powerRatingKw} kW` : '—'} />
                    <ReviewRow label="Connector Types" value={(wizardForm.connectorTypes as string[] || []).join(', ')} />
                    <ReviewRow label="Revenue Share" value={wizardForm.revenueSharePct ? `${wizardForm.revenueSharePct}%` : '—'} />
                    <ReviewRow label="Energy Rate" value={wizardForm.energyRatePerKwh ? `€${wizardForm.energyRatePerKwh}/kWh` : '—'} />
                  </div>
                  <p className="text-xs text-gray-600">
                    This will be provisioned at station <strong className="text-gray-300">{selectedStation?.name || '—'}</strong>
                  </p>
                </div>
              )}
            </ProvisionWizard>
          </div>
        </div>
      )}

      {/* ─── Safety Modals ─── */}
      <DependencyCheckModal
        open={!!dependencyCheck}
        onClose={() => setDependencyCheck(null)}
        entityType={dependencyCheck?.type || 'Partner'}
        entityName={dependencyCheck?.name || ''}
        dependentCount={dependencyCheck?.type === 'Partner' ? stations.filter(s => s.status === 'ACTIVE').length : chargers.filter(c => c.status === 'ACTIVE').length}
        dependentType={dependencyCheck?.type === 'Partner' ? 'stations' : 'chargers'}
        onReassign={() => { setDependencyCheck(null); /* would open reassign UI */ }}
        onForceDelete={() => {
          if (dependencyCheck?.type === 'Partner') {
            if (dependencyCheck.id) deletePartner(dependencyCheck.id);
            setSelectedPartnerId(null); setSelectedStationId(null); setSelectedChargerId(null);
          } else {
            if (dependencyCheck?.id) deleteStation(dependencyCheck.id);
            setSelectedStationId(null); setSelectedChargerId(null);
          }
          setDependencyCheck(null);
        }}
      />

      {unbindFlow && (
        <UnbindArchiveFlow
          open={true}
          onClose={() => setUnbindFlow(null)}
          chargerId={unbindFlow.id}
          chargerState={unbindFlow.charger_state}
          chargeBoxId={unbindFlow.charge_box_id}
          stationName={selectedStation?.name || ''}
          sessionActive={unbindFlow.charger_state === 'CHARGING'}
          onConfirm={() => {
            unbindCharger(unbindFlow.id);
            setUnbindFlow(null);
          }}
        />
      )}

      {failsafe && (
        <FailsafeConfirm
          open={true}
          onClose={() => setFailsafe(null)}
          title={failsafe.action}
          entityName="Charger"
          entityId={failsafe.id}
          destructiveAction={failsafe.action}
          consequence={`This will remotely reboot the charger ${failsafe.matchString}. Any active charging sessions will be interrupted.`}
          matchString={failsafe.matchString}
          onConfirm={() => {
            // execute reboot
            setFailsafe(null);
          }}
          requireApproval
        />
      )}

      {twoStepApproval && (
        <TwoStepApproval
          open={true}
          onClose={() => setTwoStepApproval(null)}
          title="Authorize Destructive Operation"
          command={twoStepApproval.command}
          targetName="Charger"
          targetId={twoStepApproval.id}
          onApproved={() => { setTwoStepApproval(null); }}
          onRejected={() => { setTwoStepApproval(null); }}
        />
      )}
    </>
  );
}

/* ─── Helpers ─── */

function InfoRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex justify-between">
      <span className="text-gray-600">{label}</span>
      <span className={`text-gray-300 ${mono ? 'font-mono text-[11px]' : ''}`}>{value}</span>
    </div>
  );
}

function ReviewRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between">
      <span className="text-gray-500">{label}</span>
      <span className="text-gray-200 font-mono text-xs">{value}</span>
    </div>
  );
}

/* ─── Icons ─── */

function EditIcon() { return <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" /></svg>; }
function RebootIcon() { return <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>; }
function UnlinkIcon() { return <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" /></svg>; }
function TrashIcon() { return <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>; }
