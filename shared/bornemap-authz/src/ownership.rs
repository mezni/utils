use std::fmt;

/// Types of resource owners
#[derive(Debug, Clone, PartialEq)]
pub enum Owner {
    User(uuid::Uuid),
    Partner(uuid::Uuid),
    System,
}

impl fmt::Display for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Owner::User(user_id) => write!(f, "User({})", user_id),
            Owner::Partner(partner_id) => write!(f, "Partner({})", partner_id),
            Owner::System => write!(f, "System"),
        }
    }
}

/// Resource ownership types
#[derive(Debug, Clone, PartialEq)]
pub enum ResourceOwnership {
    /// Resource is owned by a specific owner
    Owner(Owner),
    /// Resource is shared among multiple users/partners
    Shared,
}

impl fmt::Display for ResourceOwnership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceOwnership::Owner(owner) => write!(f, "Owned by {}", owner),
            ResourceOwnership::Shared => write!(f, "Shared"),
        }
    }
}

/// Resource types that can be owned
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceType {
    Station,
    Charger,
    Pricing,
    OpeningHours,
    Partner,
    User,
    System,
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceType::Station => write!(f, "Station"),
            ResourceType::Charger => write!(f, "Charger"),
            ResourceType::Pricing => write!(f, "Pricing"),
            ResourceType::OpeningHours => write!(f, "OpeningHours"),
            ResourceType::Partner => write!(f, "Partner"),
            ResourceType::User => write!(f, "User"),
            ResourceType::System => write!(f, "System"),
        }
    }
}

impl ResourceType {
    /// Get the default owner for a resource type
    pub fn default_owner(&self) -> Owner {
        match self {
            ResourceType::User | ResourceType::Station | ResourceType::Charger => {
                // These are typically owned by users or partners
                // In a real implementation, this would be determined dynamically
                Owner::System  // Placeholder
            },
            ResourceType::Pricing | ResourceType::OpeningHours => {
                // These are typically system-owned
                Owner::System
            },
            ResourceType::Partner => {
                // Partners are owned by system
                Owner::System
            },
            ResourceType::System => {
                // System resources are owned by system
                Owner::System
            },
        }
    }

    /// Check if a resource type can be owned by users
    pub fn user_ownable(&self) -> bool {
        matches!(self, ResourceType::Station | ResourceType::Charger)
    }

    /// Check if a resource type can be owned by partners
    pub fn partner_ownable(&self) -> bool {
        matches!(self, ResourceType::Pricing | ResourceType::OpeningHours)
    }

    /// Check if a resource type is system-owned
    pub fn system_owned(&self) -> bool {
        matches!(self, ResourceType::System | ResourceType::Partner)
    }
}

/// Resource identifier combining type and ID
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceId {
    pub resource_type: ResourceType,
    pub id: String,
}

impl ResourceId {
    pub fn new(resource_type: ResourceType, id: String) -> Self {
        Self {
            resource_type,
            id,
        }
    }

    pub fn station(id: String) -> Self {
        Self::new(ResourceType::Station, id)
    }

    pub fn charger(id: String) -> Self {
        Self::new(ResourceType::Charger, id)
    }

    pub fn pricing(id: String) -> Self {
        Self::new(ResourceType::Pricing, id)
    }

    pub fn opening_hours(id: String) -> Self {
        Self::new(ResourceType::OpeningHours, id)
    }

    pub fn partner(id: String) -> Self {
        Self::new(ResourceType::Partner, id)
    }

    pub fn user(id: String) -> Self {
        Self::new(ResourceType::User, id)
    }

    pub fn system(id: String) -> Self {
        Self::new(ResourceType::System, id)
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.resource_type, self.id)
    }
}

/// Ownership service for managing resource ownership
pub struct OwnershipService {
    ownership_map: std::collections::HashMap<ResourceId, ResourceOwnership>,
}

impl OwnershipService {
    pub fn new() -> Self {
        Self {
            ownership_map: std::collections::HashMap::new(),
        }
    }

    /// Set ownership for a resource
    pub fn set_ownership(&mut self, resource_id: ResourceId, ownership: ResourceOwnership) {
        self.ownership_map.insert(resource_id, ownership);
    }

    /// Get ownership for a resource
    pub fn get_ownership(&self, resource_id: &ResourceId) -> Option<&ResourceOwnership> {
        self.ownership_map.get(resource_id)
    }

    /// Check if a user owns a resource
    pub fn user_owns_resource(&self, user_id: uuid::Uuid, resource_id: &ResourceId) -> bool {
        match self.get_ownership(resource_id) {
            Some(ResourceOwnership::Owner(owner)) => {
                match owner {
                    Owner::User(resource_user_id) => resource_user_id == &user_id,
                    Owner::Partner(_) => false, // Partners don't own user resources
                    Owner::System => false, // System doesn't own user resources
                }
            },
            Some(ResourceOwnership::Shared) => true,
            None => false,
        }
    }

    /// Check if a partner owns a resource
    pub fn partner_owns_resource(&self, partner_id: uuid::Uuid, resource_id: &ResourceId) -> bool {
        match self.get_ownership(resource_id) {
            Some(ResourceOwnership::Owner(owner)) => {
                match owner {
                    Owner::User(_) => false, // Users don't own partner resources
                    Owner::Partner(resource_partner_id) => resource_partner_id == &partner_id,
                    Owner::System => false, // System doesn't own partner resources
                }
            },
            Some(ResourceOwnership::Shared) => true,
            None => false,
        }
    }

    /// Check if a resource is shared
    pub fn is_shared_resource(&self, resource_id: &ResourceId) -> bool {
        matches!(self.get_ownership(resource_id), Some(ResourceOwnership::Shared))
    }

    /// Check if a resource is system-owned
    pub fn is_system_owned(&self, resource_id: &ResourceId) -> bool {
        match self.get_ownership(resource_id) {
            Some(ResourceOwnership::Owner(Owner::System)) => true,
            _ => false,
        }
    }

    /// Remove ownership for a resource
    pub fn remove_ownership(&mut self, resource_id: &ResourceId) {
        self.ownership_map.remove(resource_id);
    }

    /// Clear all ownership
    pub fn clear(&mut self) {
        self.ownership_map.clear();
    }
}

impl Default for OwnershipService {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating common ownership patterns
pub struct OwnershipFactory;

impl OwnershipFactory {
    /// Create user-owned resource
    pub fn user_owned(user_id: uuid::Uuid) -> ResourceOwnership {
        ResourceOwnership::Owner(Owner::User(user_id))
    }

    /// Create partner-owned resource
    pub fn partner_owned(partner_id: uuid::Uuid) -> ResourceOwnership {
        ResourceOwnership::Owner(Owner::Partner(partner_id))
    }

    /// Create system-owned resource
    pub fn system_owned() -> ResourceOwnership {
        ResourceOwnership::Owner(Owner::System)
    }

    /// Create shared resource
    pub fn shared() -> ResourceOwnership {
        ResourceOwnership::Shared
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_type_default_owner() {
        assert_eq!(ResourceType::Station.default_owner(), Owner::System);
        assert_eq!(ResourceType::Charger.default_owner(), Owner::System);
        assert_eq!(ResourceType::Pricing.default_owner(), Owner::System);
        assert_eq!(ResourceType::OpeningHours.default_owner(), Owner::System);
        assert_eq!(ResourceType::Partner.default_owner(), Owner::System);
        assert_eq!(ResourceType::User.default_owner(), Owner::System);
        assert_eq!(ResourceType::System.default_owner(), Owner::System);
    }

    #[test]
    fn resource_type_ownable() {
        assert!(ResourceType::Station.user_ownable());
        assert!(ResourceType::Charger.user_ownable());
        assert!(!ResourceType::Pricing.user_ownable());
        assert!(!ResourceType::OpeningHours.user_ownable());

        assert!(!ResourceType::Station.partner_ownable());
        assert!(!ResourceType::Charger.partner_ownable());
        assert!(ResourceType::Pricing.partner_ownable());
        assert!(ResourceType::OpeningHours.partner_ownable());

        assert!(!ResourceType::Station.system_owned());
        assert!(!ResourceType::Charger.system_owned());
        assert!(ResourceType::System.system_owned());
        assert!(ResourceType::Partner.system_owned());
    }

    #[test]
    fn resource_id_creation() {
        let station_id = ResourceId::station("station-123".to_string());
        assert_eq!(station_id.resource_type, ResourceType::Station);
        assert_eq!(station_id.id, "station-123");

        let charger_id = ResourceId::charger("charger-456".to_string());
        assert_eq!(charger_id.resource_type, ResourceType::Charger);
        assert_eq!(charger_id.id, "charger-456");
    }

    #[test]
    fn ownership_service_user_ownership() {
        let mut service = OwnershipService::new();
        let user_id = uuid::Uuid::new_v4();
        let station_id = ResourceId::station("station-123".to_string());

        // Set user ownership
        service.set_ownership(station_id.clone(), OwnershipFactory::user_owned(user_id));

        // Check ownership
        assert!(service.user_owns_resource(user_id, &station_id));
        assert!(!service.user_owns_resource(uuid::Uuid::new_v4(), &station_id));
        assert!(!service.partner_owns_resource(uuid::Uuid::new_v4(), &station_id));
    }

    #[test]
    fn ownership_service_partner_ownership() {
        let mut service = OwnershipService::new();
        let partner_id = uuid::Uuid::new_v4();
        let pricing_id = ResourceId::pricing("pricing-123".to_string());

        // Set partner ownership
        service.set_ownership(pricing_id.clone(), OwnershipFactory::partner_owned(partner_id));

        // Check ownership
        assert!(service.partner_owns_resource(partner_id, &pricing_id));
        assert!(!service.partner_owns_resource(uuid::Uuid::new_v4(), &pricing_id));
        assert!(!service.user_owns_resource(uuid::Uuid::new_v4(), &pricing_id));
    }

    #[test]
    fn ownership_service_shared_resource() {
        let mut service = OwnershipService::new();
        let station_id = ResourceId::station("station-123".to_string());

        // Set shared ownership
        service.set_ownership(station_id.clone(), OwnershipFactory::shared());

        // Check ownership
        assert!(service.is_shared_resource(&station_id));
        assert!(service.user_owns_resource(uuid::Uuid::new_v4(), &station_id));
        assert!(service.partner_owns_resource(uuid::Uuid::new_v4(), &station_id));
    }

    #[test]
    fn ownership_service_system_owned() {
        let mut service = OwnershipService::new();
        let system_id = ResourceId::system("system-123".to_string());

        // Set system ownership
        service.set_ownership(system_id.clone(), OwnershipFactory::system_owned());

        // Check ownership
        assert!(service.is_system_owned(&system_id));
        assert!(!service.user_owns_resource(uuid::Uuid::new_v4(), &system_id));
        assert!(!service.partner_owns_resource(uuid::Uuid::new_v4(), &system_id));
    }

    #[test]
    fn ownership_factory() {
        let user_id = uuid::Uuid::new_v4();
        let partner_id = uuid::Uuid::new_v4();

        let user_ownership = OwnershipFactory::user_owned(user_id);
        let partner_ownership = OwnershipFactory::partner_owned(partner_id);
        let system_ownership = OwnershipFactory::system_owned();
        let shared_ownership = OwnershipFactory::shared();

        match user_ownership {
            ResourceOwnership::Owner(Owner::User(id)) => assert_eq!(id, user_id),
            _ => panic!("Expected user ownership"),
        }

        match partner_ownership {
            ResourceOwnership::Owner(Owner::Partner(id)) => assert_eq!(id, partner_id),
            _ => panic!("Expected partner ownership"),
        }

        match system_ownership {
            ResourceOwnership::Owner(Owner::System) => (),
            _ => panic!("Expected system ownership"),
        }

        match shared_ownership {
            ResourceOwnership::Shared => (),
            _ => panic!("Expected shared ownership"),
        }
    }
}