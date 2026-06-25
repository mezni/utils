import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { PageHeader } from "@/components/common/PageHeader";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface LookupTable {
  title: string;
  description: string;
  entries: { id: number; name: string }[];
}

const lookupTables: LookupTable[] = [
  {
    title: "Access Types",
    description: "Types of access for charging stations.",
    entries: [
      { id: 1, name: "Public" },
      { id: 2, name: "Private" },
      { id: 3, name: "Restricted" },
    ],
  },
  {
    title: "Data Sources",
    description: "Sources of station data.",
    entries: [
      { id: 1, name: "OpenStreetMap" },
      { id: 2, name: "Manual" },
      { id: 3, name: "Partner API" },
    ],
  },
  {
    title: "Connector Types",
    description: "EV connector standards.",
    entries: [
      { id: 1, name: "Type 2" },
      { id: 2, name: "CCS" },
      { id: 3, name: "CHAdeMO" },
      { id: 4, name: "Type 1" },
      { id: 5, name: "GB/T" },
    ],
  },
  {
    title: "Current Types",
    description: "Electrical current types.",
    entries: [
      { id: 1, name: "AC" },
      { id: 2, name: "DC" },
    ],
  },
  {
    title: "Connector Statuses",
    description: "Operational statuses for chargers.",
    entries: [
      { id: 1, name: "Active" },
      { id: 2, name: "Inactive" },
      { id: 3, name: "Maintenance" },
      { id: 4, name: "Offline" },
    ],
  },
];

function LookupTableSection({ table }: { table: LookupTable }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">{table.title}</CardTitle>
        <p className="text-sm text-muted-foreground">{table.description}</p>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-16">ID</TableHead>
              <TableHead>Name</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {table.entries.map((entry) => (
              <TableRow key={entry.id}>
                <TableCell className="font-mono text-xs">{entry.id}</TableCell>
                <TableCell>
                  <Badge variant="outline">{entry.name}</Badge>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

export function SettingsPage() {
  return (
    <div>
      <PageHeader
        title="Settings"
        description="Lookup table reference data."
      />
      <div className="grid gap-6 md:grid-cols-2">
        {lookupTables.map((table) => (
          <LookupTableSection key={table.title} table={table} />
        ))}
      </div>
    </div>
  );
}
