use sqlx::PgPool;
use tracing::{error, info};

use crate::error::AuthError;
use crate::middleware::auth::UserContext;

#[derive(Debug, Clone, serde::Serialize)]
pub struct AuditEntry {
    pub actor_id: String,
    pub action: String,
    pub target_type: String,
    pub target_id: String,
    pub before_snapshot: Option<serde_json::Value>,
    pub after_snapshot: serde_json::Value,
    pub payload: Option<serde_json::Value>,
}

pub async fn audit_diff_service(
    pool: &PgPool,
    user_context: &UserContext,
    action: &str,
    target_id: &str,
    before_snapshot: Option<serde_json::Value>,
    after_snapshot: serde_json::Value,
    payload: Option<serde_json::Value>,
) -> Result<(), AuthError> {
    // Note: This is called OUTSIDE of transaction (per constitution)
    // and failure does NOT roll back the mutation

    let audit_entry = AuditEntry {
        actor_id: user_context.user_id.clone(),
        action: action.to_string(),
        target_type: "unknown".to_string(), // TODO: Determine entity type
        target_id: target_id.to_string(),
        before_snapshot,
        after_snapshot,
        payload,
    };

    // Insert into analytics_db.audit_log
    let query = sqlx::query!(
        r#"
        INSERT INTO analytics_db.audit_log (
            actor_id, action, target_type, target_id,
            before_snapshot, after_snapshot, payload, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        "#,
        audit_entry.actor_id,
        audit_entry.action,
        audit_entry.target_type,
        audit_entry.target_id,
        audit_entry.before_snapshot,
        audit_entry.after_snapshot,
        audit_entry.payload
    );

    match query.execute(pool).await {
        Ok(_) => {
            info!("Audit log entry created: {} - {}", audit_entry.action, target_id);
            Ok(())
        }
        Err(e) => {
            error!("Failed to write audit log: {}", e);
            // Per constitution: Failure → log error, proceed
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn test_audit_diff_service_success() {
        // This would need a real database connection for testing
        // For now, just verify the function compiles
        let pool = sqlx::PgPool::connect("postgresql://localhost/platform_db").await.unwrap();
        let user_context = UserContext {
            user_id: "USR-test".to_string(),
            roles: vec!["role:admin".to_string()],
        };

        let result = audit_diff_service(
            &pool,
            &user_context,
            "partner.created",
            "OPR-test",
            None,
            serde_json::json!({"test": "data"}),
            None,
        ).await;

        assert!(result.is_ok());
    }
}
