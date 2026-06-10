use sqlx::postgres::PgPoolOptions;

fn get_pool() -> Option<sqlx::PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy(&url)
        .ok()
}

fn skip_msg() {
    eprintln!("skipping: set DATABASE_URL to run this test");
}

async fn insert_partner(
    pool: &sqlx::PgPool,
    id: &str,
    name: &str,
    is_verified: bool,
    is_live: bool,
    is_active: bool,
) {
    sqlx::query(
        r#"
        INSERT INTO "ev-platform".partner (id, name, type, is_verified, is_live, is_active, created_at, created_by, updated_at, updated_by)
        VALUES ($1, $2, 'business', $3, $4, $5, NOW(), 'test', NOW(), 'test')
        ON CONFLICT (id) DO UPDATE SET
            is_verified = EXCLUDED.is_verified,
            is_live = EXCLUDED.is_live,
            is_active = EXCLUDED.is_active,
            name = EXCLUDED.name
        "#,
    )
    .bind(id)
    .bind(name)
    .bind(is_verified)
    .bind(is_live)
    .bind(is_active)
    .execute(pool)
    .await
    .expect("failed to upsert test partner");
}

async fn update_partner_flag(
    pool: &sqlx::PgPool,
    id: &str,
    flag_name: &str,
    value: bool,
) {
    let sql = format!(
        r#"UPDATE "ev-platform".partner SET {} = $1, updated_at = NOW() WHERE id = $2"#,
        flag_name
    );
    sqlx::query(&sql)
        .bind(value)
        .bind(id)
        .execute(pool)
        .await
        .expect("failed to update partner flag");
}

async fn insert_station(pool: &sqlx::PgPool, id: &str, partner_id: &str, name: &str) {
    sqlx::query(
        r#"
        INSERT INTO "ev-platform".station (id, partner_id, name, latitude, longitude, created_at, created_by, updated_at, updated_by)
        VALUES ($1, $2, $3, 36.0, 10.0, NOW(), 'test', NOW(), 'test')
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(id)
    .bind(partner_id)
    .bind(name)
    .execute(pool)
    .await
    .expect("failed to insert test station");
}

async fn station_is_visible(pool: &sqlx::PgPool, station_id: &str) -> bool {
    let rows = sqlx::query_as::<_, (String,)>(
        r#"
        SELECT s.id
        FROM "ev-platform".station s
        JOIN "ev-platform".partner p ON s.partner_id = p.id
        WHERE p.is_verified = true AND p.is_live = true AND p.is_active = true
          AND s.id = $1
        "#,
    )
    .bind(station_id)
    .fetch_all(pool)
    .await
    .expect("visibility query failed");
    !rows.is_empty()
}

async fn cleanup(pool: &sqlx::PgPool, station_ids: &[&str], partner_ids: &[&str]) {
    for sid in station_ids {
        sqlx::query(r#"DELETE FROM "ev-platform".station WHERE id = $1"#)
            .bind(sid)
            .execute(pool)
            .await
            .ok();
    }
    for pid in partner_ids {
        sqlx::query(r#"DELETE FROM "ev-platform".partner WHERE id = $1"#)
            .bind(pid)
            .execute(pool)
            .await
            .ok();
    }
}

/// Verify that a partner flag set to `false` hides its stations.
async fn assert_flag_hides(flag_name: &str, flag_label: &str) {
    let pool = match get_pool() {
        Some(p) => p,
        None => {
            skip_msg();
            return;
        }
    };

    let pid = format!("TEST_PRT_HIDE_{}", flag_label);
    let sid = format!("TEST_STN_HIDE_{}", flag_label);

    // Create partner with all flags visible, then flip the test flag to false
    insert_partner(&pool, &pid, &format!("Hide {}", flag_label), true, true, true).await;
    update_partner_flag(&pool, &pid, flag_name, false).await;
    insert_station(&pool, &sid, &pid, &format!("Station {}", flag_label)).await;

    assert!(
        !station_is_visible(&pool, &sid).await,
        "station of partner with {}=false should be hidden",
        flag_name
    );

    cleanup(&pool, &[&sid], &[&pid]).await;
}

#[tokio::test]
async fn test_is_active_false_hides_stations() {
    assert_flag_hides("is_active", "active").await;
}

#[tokio::test]
async fn test_is_verified_false_hides_stations() {
    assert_flag_hides("is_verified", "verified").await;
}

#[tokio::test]
async fn test_is_live_false_hides_stations() {
    assert_flag_hides("is_live", "live").await;
}

#[tokio::test]
async fn test_all_flags_true_shows_stations() {
    let pool = match get_pool() {
        Some(p) => p,
        None => {
            skip_msg();
            return;
        }
    };

    let pid = "TEST_PRT_SHOW_ALLTRUE";
    let sid = "TEST_STN_SHOW_ALLTRUE";

    insert_partner(&pool, pid, "All True", true, true, true).await;
    insert_station(&pool, sid, pid, "Visible Station").await;

    assert!(
        station_is_visible(&pool, sid).await,
        "station with all partner flags=true should be visible"
    );

    cleanup(&pool, &[sid], &[pid]).await;
}
