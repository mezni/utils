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
import type { AdminStationDto, CreateStationRequest, UpdateStationRequest } from "@bornemap/domain-types";

interface StationFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (data: CreateStationRequest | UpdateStationRequest) => void;
  station?: AdminStationDto | null;
  isLoading?: boolean;
}

export function StationFormDialog({
  open,
  onOpenChange,
  onSubmit,
  station,
  isLoading,
}: StationFormDialogProps) {
  const isEdit = !!station;
  const [name, setName] = useState("");
  const [address, setAddress] = useState("");
  const [lat, setLat] = useState("36.8");
  const [lon, setLon] = useState("10.18");
  const [partnerId, setPartnerId] = useState("");

  useEffect(() => {
    if (station) {
      setName(station.name);
      setAddress(station.address || "");
      setLat(String(station.lat));
      setLon(String(station.lon));
      setPartnerId(station.partner_id || "");
    } else {
      setName("");
      setAddress("");
      setLat("36.8");
      setLon("10.18");
      setPartnerId("");
    }
  }, [station, open]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const payload = isEdit
      ? {
          ...(name ? { name } : {}),
          ...(address ? { address } : {}),
          ...(lat ? { lat: parseFloat(lat) } : {}),
          ...(lon ? { lon: parseFloat(lon) } : {}),
          ...(partnerId ? { partner_id: partnerId } : {}),
        }
      : {
          name,
          lat: parseFloat(lat),
          lon: parseFloat(lon),
          ...(address ? { address } : {}),
          ...(partnerId ? { partner_id: partnerId } : {}),
        };
    onSubmit(payload);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogHeader>
        <DialogTitle>{isEdit ? "Edit Station" : "Create Station"}</DialogTitle>
        <DialogDescription>
          {isEdit ? "Update station details." : "Add a new charging station."}
        </DialogDescription>
      </DialogHeader>
      <form onSubmit={handleSubmit}>
        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="name">Name *</Label>
            <Input id="name" value={name} onChange={(e) => setName(e.target.value)} required placeholder="Station name" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="address">Address</Label>
            <Input id="address" value={address} onChange={(e) => setAddress(e.target.value)} placeholder="Street address" />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="lat">Latitude *</Label>
              <Input id="lat" type="number" step="any" value={lat} onChange={(e) => setLat(e.target.value)} required />
            </div>
            <div className="space-y-2">
              <Label htmlFor="lon">Longitude *</Label>
              <Input id="lon" type="number" step="any" value={lon} onChange={(e) => setLon(e.target.value)} required />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="partnerId">Partner ID</Label>
            <Input id="partnerId" value={partnerId} onChange={(e) => setPartnerId(e.target.value)} placeholder="OPR-xxxxxxxxxxxx" />
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
