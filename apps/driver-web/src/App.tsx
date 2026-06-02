import { formatId, type EntityPrefix } from "@bornemap/shared-types";
import type { EventEnvelope } from "@bornemap/event-taxonomy";

const APP_NAME = "driver-web";
const sampleId = formatId("USR" as EntityPrefix, "01JAN1234567890");

function App() {
  // verify cross-stack import resolves
  const _verify: EventEnvelope | null = null;
  return <h1>{APP_NAME} | {sampleId} {_verify !== null ? "?" : ""}</h1>;
}

export default App;
