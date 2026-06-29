export interface Station {
  id: string;
  partner_id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
  status: "active" | "inactive" | "maintenance" | "offline";
  created_at: string;
  updated_at: string;
}

export interface CreateStationInput {
  partner_id: string;
  name: string;
  address: string;
  latitude: number;
  longitude: number;
}

export interface UpdateStationInput {
  name: string;
  address: string;
  latitude: number;
  longitude: number;
  status: Station["status"];
}

export const stationsApi = {
  list: async (): Promise<Station[]> => {
    // Mock data - replace with actual API calls
    return [
      {
        id: "STN_12345678",
        partner_id: "PRT_12345678",
        name: "Downtown Tunis",
        address: "123 Avenue Habib Bourguiba, Tunis",
        latitude: 36.8065,
        longitude: 10.1815,
        status: "active",
        created_at: "2024-01-15T10:00:00Z",
        updated_at: "2024-01-20T14:30:00Z",
      },
      {
        id: "STN_23456789",
        partner_id: "PRT_12345678",
        name: "La Marsa",
        address: "45 Avenue Carthage, La Marsa",
        latitude: 36.8625,
        longitude: 10.3256,
        status: "active",
        created_at: "2024-02-01T09:00:00Z",
        updated_at: "2024-02-10T16:45:00Z",
      },
      {
        id: "STN_34567890",
        partner_id: "PRT_23456789",
        name: "Sousse Center",
        address: "78 Avenue Habib Bourguiba, Sousse",
        latitude: 35.8256,
        longitude: 10.6372,
        status: "maintenance",
        created_at: "2024-02-15T11:00:00Z",
        updated_at: "2024-02-25T10:15:00Z",
      },
    ];
  },

  listByPartner: async (partnerId: string): Promise<Station[]> => {
    // Mock API call
    const allStations = await stationsApi.list();
    return allStations.filter(s => s.partner_id === partnerId);
  },

  create: async (input: CreateStationInput): Promise<Station> => {
    // Mock API call
    const newStation: Station = {
      id: `STN_${Math.random().toString(36).substr(2, 8).toUpperCase()}`,
      partner_id: input.partner_id,
      name: input.name,
      address: input.address,
      latitude: input.latitude,
      longitude: input.longitude,
      status: "active",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    return newStation;
  },

  update: async (id: string, input: UpdateStationInput): Promise<Station> => {
    // Mock API call
    const updatedStation: Station = {
      id,
      partner_id: "PRT_12345678", // This should come from the original data
      name: input.name,
      address: input.address,
      latitude: input.latitude,
      longitude: input.longitude,
      status: input.status,
      created_at: "2024-01-15T10:00:00Z",
      updated_at: new Date().toISOString(),
    };
    return updatedStation;
  },

  delete: async (id: string): Promise<void> => {
    // Mock API call
    return Promise.resolve();
  },

  findById: async (id: string): Promise<Station | null> => {
    // Mock API call
    const stations = await stationsApi.list();
    return stations.find(s => s.id === id) || null;
  },
};