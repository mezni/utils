import { formatId } from "@bornemap/shared-types";
import type { EventEnvelope } from "@bornemap/event-taxonomy";
import type { ErrorEnvelope } from "@bornemap/api-contracts";
import type { SuccessEnvelope } from "@bornemap/api-contracts";
import type { PaginationMeta } from "@bornemap/api-contracts";

const APP_NAME = "partner-dashboard";
const sampleId = formatId("PRT", "01JAN1234567890");

function App() {
  const verifyEvent: EventEnvelope | null = null;
  const verifyError: ErrorEnvelope | null = null;
  const verifySuccess: SuccessEnvelope | null = null;
  const verifyMeta: PaginationMeta | null = null;
  return (
    <h1>
      {APP_NAME} | {sampleId}
      {verifyEvent !== null ? "E" : ""}
      {verifyError !== null ? "E" : ""}
      {verifySuccess !== null ? "S" : ""}
      {verifyMeta !== null ? "M" : ""}
    </h1>
  );
}

export default App;
