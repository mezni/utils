use shared_jwt::JwtService;

pub struct JwtInfrastructure {
    jwt_service: JwtService,
}

impl JwtInfrastructure {
    pub fn new(secret: &str) -> Result<Self, jsonwebtoken::errors::Error> {
        let jwt_service = JwtService::new(secret)?;

        Ok(JwtInfrastructure { jwt_service })
    }

    pub fn get_jwt_service(&self) -> &JwtService {
        &self.jwt_service
    }
}