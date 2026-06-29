export interface Partner {
  id: string;
  name: string;
  created_at: string;
  updated_at: string;
}

export interface CreatePartnerInput {
  name: string;
}

export interface UpdatePartnerInput {
  name: string;
}

export const partnersApi = {
  list: async (): Promise<Partner[]> => {
    // Mock data - replace with actual API calls
    return [
      {
        id: "PRT_12345678",
        name: "Tesla Tunisia",
        created_at: "2024-01-15T10:00:00Z",
        updated_at: "2024-01-20T14:30:00Z",
      },
      {
        id: "PRT_23456789",
        name: "Ionity",
        created_at: "2024-02-01T09:00:00Z",
        updated_at: "2024-02-10T16:45:00Z",
      },
      {
        id: "PRT_34567890",
        name: "TotalEnergies",
        created_at: "2024-02-15T11:00:00Z",
        updated_at: "2024-02-25T10:15:00Z",
      },
    ];
  },

  create: async (input: CreatePartnerInput): Promise<Partner> => {
    // Mock API call
    const newPartner: Partner = {
      id: `PRT_${Math.random().toString(36).substr(2, 8).toUpperCase()}`,
      name: input.name,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    return newPartner;
  },

  update: async (id: string, input: UpdatePartnerInput): Promise<Partner> => {
    // Mock API call
    const updatedPartner: Partner = {
      id,
      name: input.name,
      created_at: "2024-01-15T10:00:00Z",
      updated_at: new Date().toISOString(),
    };
    return updatedPartner;
  },

  delete: async (id: string): Promise<void> => {
    // Mock API call
    return Promise.resolve();
  },

  findById: async (id: string): Promise<Partner | null> => {
    // Mock API call
    const partners = await partnersApi.list();
    return partners.find(p => p.id === id) || null;
  },
};
