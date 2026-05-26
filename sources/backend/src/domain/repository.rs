use crate::utils::pagination::Cursor;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, QueryBuilder};

pub struct SoftDeleteFilter;

impl SoftDeleteFilter {
    pub fn where_not_deleted() -> &'static str {
        " WHERE deleted_at IS NULL"
    }
}

pub struct TestFilter;

impl TestFilter {
    pub fn and_include_test(include_test: bool) -> &'static str {
        if include_test {
            ""
        } else {
            " AND is_test = FALSE"
        }
    }
}

pub fn apply_cursor_pagination<'a>(
    qb: &mut QueryBuilder<'a, Postgres>,
    cursor: Option<Cursor>,
    fetch_limit: i64,
) {
    if let Some(c) = cursor {
        qb.push(" AND (created_at, id) > (");
        qb.push_bind(c.created_at);
        qb.push(", ");
        qb.push_bind(c.id);
        qb.push(")");
    }
    qb.push(" ORDER BY created_at ASC, id ASC LIMIT ");
    qb.push_bind(fetch_limit);
}

pub fn paginate<T, F>(items: Vec<T>, limit: i64, cursor_fn: F) -> (Vec<T>, Option<String>, bool)
where
    F: Fn(&T) -> (DateTime<Utc>, String),
{
    let _fetch_limit = limit + 1;
    let has_more = items.len() > limit as usize;
    let mut data: Vec<T> = items.into_iter().collect();

    if has_more {
        data.pop();
    }

    let next_cursor = if has_more {
        data.last().map(|item| {
            let (created_at, id) = cursor_fn(item);
            Cursor { created_at, id }.encode()
        })
    } else {
        None
    };

    (data, next_cursor, has_more)
}
