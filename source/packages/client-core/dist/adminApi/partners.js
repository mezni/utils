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
export async function listPartners(params) {
    const qs = new URLSearchParams();
    if (params?.page)
        qs.set("page", String(params.page));
    if (params?.per_page)
        qs.set("per_page", String(params.per_page));
    if (params?.search)
        qs.set("search", params.search);
    const q = qs.toString();
    return request(`/api/v1/partners${q ? `?${q}` : ""}`);
}
export async function getPartner(id) {
    return request(`/api/v1/partners/${encodeURIComponent(id)}`);
}
export async function createPartner(data) {
    return request("/api/v1/partners", {
        method: "POST",
        body: JSON.stringify(data),
    });
}
export async function updatePartner(id, data) {
    return request(`/api/v1/partners/${encodeURIComponent(id)}`, {
        method: "PUT",
        body: JSON.stringify(data),
    });
}
export async function deletePartner(id) {
    return request(`/api/v1/partners/${encodeURIComponent(id)}`, { method: "DELETE" });
}
//# sourceMappingURL=partners.js.map