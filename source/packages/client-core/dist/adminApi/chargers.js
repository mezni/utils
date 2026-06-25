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
export async function listChargers(params) {
    const qs = new URLSearchParams();
    if (params?.page)
        qs.set("page", String(params.page));
    if (params?.per_page)
        qs.set("per_page", String(params.per_page));
    if (params?.station_id)
        qs.set("station_id", params.station_id);
    const q = qs.toString();
    return request(`/api/v1/chargers${q ? `?${q}` : ""}`);
}
export async function getCharger(id) {
    return request(`/api/v1/chargers/${encodeURIComponent(id)}`);
}
export async function createCharger(data) {
    return request("/api/v1/chargers", {
        method: "POST",
        body: JSON.stringify(data),
    });
}
export async function updateCharger(id, data) {
    return request(`/api/v1/chargers/${encodeURIComponent(id)}`, {
        method: "PUT",
        body: JSON.stringify(data),
    });
}
export async function deleteCharger(id) {
    return request(`/api/v1/chargers/${encodeURIComponent(id)}`, { method: "DELETE" });
}
//# sourceMappingURL=chargers.js.map