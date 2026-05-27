use nanoid::nanoid;

pub type StationId = String;
pub type UserId = String;
pub type PartnerId = String;

pub fn generate_id(prefix: &str) -> String {
    format!("{}_{}", prefix, nanoid!(12))
}
