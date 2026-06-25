const DEFAULT_BASE = "";
async function request(path, init) {
    const res = await fetch(`${DEFAULT_BASE}${path}`, {
        headers: { "Content-Type": "application/json" },
        ...init,
    });
    if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `API error: ${res.status} ${res.statusText}`);
    }
    if (res.status === 204)
        return undefined;
    return res.json();
}
export async function listStations(params) {
    const qs = new URLSearchParams();
    if (params?.page)
        qs.set("page", String(params.page));
    if (params?.per_page)
        qs.set("per_page", String(params.per_page));
    if (params?.partner_id)
        qs.set("partner_id", params.partner_id);
    const q = qs.toString();
    return request(`/api/v1/stations${q ? `?${q}` : ""}`);
}
export async function getStation(id) {
    return request(`/api/v1/stations/${encodeURIComponent(id)}`);
}
export async function createStation(data) {
    return request("/api/v1/stations", {
        method: "POST",
        body: JSON.stringify(data),
    });
}
export async function updateStation(id, data) {
    return request(`/api/v1/stations/${encodeURIComponent(id)}`, {
        method: "PUT",
        body: JSON.stringify(data),
    });
}
export async function deleteStation(id) {
    return request(`/api/v1/stations/${encodeURIComponent(id)}`, { method: "DELETE" });
}
//# sourceMappingURL=stations.js.map