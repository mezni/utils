import { useState, useEffect } from "react";
import {
  Dialog,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import type { AdminChargerDto, CreateChargerRequest, UpdateChargerRequest } from "@bornemap/domain-types";

interface ChargerFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (data: CreateChargerRequest | UpdateChargerRequest) => void;
  charger?: AdminChargerDto | null;
  isLoading?: boolean;
}

export function ChargerFormDialog({
  open,
  onOpenChange,
  onSubmit,
  charger,
  isLoading,
}: ChargerFormDialogProps) {
  const isEdit = !!charger;
  const [stationId, setStationId] = useState("");
  const [connectorTypeId, setConnectorTypeId] = useState("1");
  const [statusId, setStatusId] = useState("1");
  const [currentTypeId, setCurrentTypeId] = useState("1");
  const [powerKw, setPowerKw] = useState("");
  const [voltage, setVoltage] = useState("");
  const [amperage, setAmperage] = useState("");
  const [countAvailable, setCountAvailable] = useState("1");
  const [countTotal, setCountTotal] = useState("1");

  useEffect(() => {
    if (charger) {
      setStationId(charger.station_id);
      setConnectorTypeId(String(charger.connector_type_id));
      setStatusId(String(charger.status_id));
      setCurrentTypeId(String(charger.current_type_id));
      setPowerKw(charger.power_kw ? String(charger.power_kw) : "");
      setVoltage(charger.voltage ? String(charger.voltage) : "");
      setAmperage(charger.amperage ? String(charger.amperage) : "");
      setCountAvailable(String(charger.count_available));
      setCountTotal(String(charger.count_total));
    } else {
      setStationId("");
      setConnectorTypeId("1");
      setStatusId("1");
      setCurrentTypeId("1");
      setPowerKw("");
      setVoltage("");
      setAmperage("");
      setCountAvailable("1");
      setCountTotal("1");
    }
  }, [charger, open]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const payload = isEdit
      ? {
          ...(connectorTypeId ? { connector_type_id: parseInt(connectorTypeId) } : {}),
          ...(statusId ? { status_id: parseInt(statusId) } : {}),
          ...(currentTypeId ? { current_type_id: parseInt(currentTypeId) } : {}),
          ...(powerKw ? { power_kw: parseFloat(powerKw) } : {}),
          ...(voltage ? { voltage: parseInt(voltage) } : {}),
          ...(amperage ? { amperage: parseInt(amperage) } : {}),
          ...(countAvailable ? { count_available: parseInt(countAvailable) } : {}),
          ...(countTotal ? { count_total: parseInt(countTotal) } : {}),
        }
      : {
          station_id: stationId,
          connector_type_id: parseInt(connectorTypeId),
          status_id: parseInt(statusId),
          current_type_id: parseInt(currentTypeId),
          ...(powerKw ? { power_kw: parseFloat(powerKw) } : {}),
          ...(voltage ? { voltage: parseInt(voltage) } : {}),
          ...(amperage ? { amperage: parseInt(amperage) } : {}),
          ...(countAvailable ? { count_available: parseInt(countAvailable) } : {}),
          ...(countTotal ? { count_total: parseInt(countTotal) } : {}),
        };
    onSubmit(payload);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogHeader>
        <DialogTitle>{isEdit ? "Edit Charger" : "Create Charger"}</DialogTitle>
        <DialogDescription>
          {isEdit ? "Update charger details." : "Add a new charger."}
        </DialogDescription>
      </DialogHeader>
      <form onSubmit={handleSubmit}>
        <div className="space-y-4">
          {!isEdit && (
            <div className="space-y-2">
              <Label htmlFor="stationId">Station ID *</Label>
              <Input id="stationId" value={stationId} onChange={(e) => setStationId(e.target.value)} required placeholder="STA-xxxxxxxxxxxx" />
            </div>
          )}
          <div className="grid grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label htmlFor="connectorType">Connector</Label>
              <Select
                id="connectorType"
                value={connectorTypeId}
                onChange={(e) => setConnectorTypeId(e.target.value)}
                options={[
                  { value: "1", label: "Type 2" },
                  { value: "2", label: "CCS" },
                  { value: "3", label: "CHAdeMO" },
                  { value: "4", label: "Type 1" },
                  { value: "5", label: "GB/T" },
                ]}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="status">Status</Label>
              <Select
                id="status"
                value={statusId}
                onChange={(e) => setStatusId(e.target.value)}
                options={[
                  { value: "1", label: "Active" },
                  { value: "2", label: "Inactive" },
                  { value: "3", label: "Maintenance" },
                  { value: "4", label: "Offline" },
                ]}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="currentType">Current</Label>
              <Select
                id="currentType"
                value={currentTypeId}
                onChange={(e) => setCurrentTypeId(e.target.value)}
                options={[
                  { value: "1", label: "AC" },
                  { value: "2", label: "DC" },
                ]}
              />
            </div>
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div className="space-y-2">
              <Label htmlFor="powerKw">Power (kW)</Label>
              <Input id="powerKw" type="number" step="any" value={powerKw} onChange={(e) => setPowerKw(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="voltage">Voltage (V)</Label>
              <Input id="voltage" type="number" value={voltage} onChange={(e) => setVoltage(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="amperage">Amperage (A)</Label>
              <Input id="amperage" type="number" value={amperage} onChange={(e) => setAmperage(e.target.value)} />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="countAvailable">Available</Label>
              <Input id="countAvailable" type="number" value={countAvailable} onChange={(e) => setCountAvailable(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="countTotal">Total</Label>
              <Input id="countTotal" type="number" value={countTotal} onChange={(e) => setCountTotal(e.target.value)} />
            </div>
          </div>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button type="submit" disabled={isLoading}>
            {isLoading ? "Saving..." : isEdit ? "Update" : "Create"}
          </Button>
        </DialogFooter>
      </form>
    </Dialog>
  );
}
