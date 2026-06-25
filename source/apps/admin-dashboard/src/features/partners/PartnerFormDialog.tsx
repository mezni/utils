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
import type { AdminPartnerDto, CreatePartnerRequest, UpdatePartnerRequest } from "@bornemap/domain-types";

interface PartnerFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (data: CreatePartnerRequest | UpdatePartnerRequest) => void;
  partner?: AdminPartnerDto | null;
  isLoading?: boolean;
}

export function PartnerFormDialog({
  open,
  onOpenChange,
  onSubmit,
  partner,
  isLoading,
}: PartnerFormDialogProps) {
  const isEdit = !!partner;
  const [name, setName] = useState("");
  const [partnerType, setPartnerType] = useState("");
  const [supportPhone, setSupportPhone] = useState("");
  const [supportEmail, setSupportEmail] = useState("");

  useEffect(() => {
    if (partner) {
      setName(partner.name);
      setPartnerType(partner.partner_type || "");
      setSupportPhone(partner.support_phone || "");
      setSupportEmail(partner.support_email || "");
    } else {
      setName("");
      setPartnerType("");
      setSupportPhone("");
      setSupportEmail("");
    }
  }, [partner, open]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const payload: CreatePartnerRequest | UpdatePartnerRequest = isEdit
      ? {
          ...(name ? { name } : {}),
          ...(partnerType ? { partner_type: partnerType } : {}),
          ...(supportPhone ? { support_phone: supportPhone } : {}),
          ...(supportEmail ? { support_email: supportEmail } : {}),
        }
      : {
          name,
          ...(partnerType ? { partner_type: partnerType } : {}),
          ...(supportPhone ? { support_phone: supportPhone } : {}),
          ...(supportEmail ? { support_email: supportEmail } : {}),
        };
    onSubmit(payload);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogHeader>
        <DialogTitle>{isEdit ? "Edit Partner" : "Create Partner"}</DialogTitle>
        <DialogDescription>
          {isEdit ? "Update partner details." : "Add a new charging partner."}
        </DialogDescription>
      </DialogHeader>
      <form onSubmit={handleSubmit}>
        <div className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="name">Name *</Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              placeholder="Partner name"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="type">Type</Label>
            <Select
              id="type"
              value={partnerType}
              onChange={(e) => setPartnerType(e.target.value)}
              options={[
                { value: "INDIVIDUAL", label: "Individual" },
                { value: "COMPANY", label: "Company" },
              ]}
              placeholder="Select type"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="phone">Support Phone</Label>
            <Input
              id="phone"
              value={supportPhone}
              onChange={(e) => setSupportPhone(e.target.value)}
              placeholder="+216 XX XXX XXX"
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="email">Support Email</Label>
            <Input
              id="email"
              type="email"
              value={supportEmail}
              onChange={(e) => setSupportEmail(e.target.value)}
              placeholder="support@example.com"
            />
          </div>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button type="submit" disabled={isLoading}>
            {isLoading ? "Saving..." : isEdit ? "Update" : "Create"}
          </Button>
        </DialogFooter>
      </form>
    </Dialog>
  );
}
