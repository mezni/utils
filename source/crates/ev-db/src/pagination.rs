/// A generic paginated response containing a slice of data with total count
/// and page metadata.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Paginated<T> {
    /// The items for the current page.
    pub data: Vec<T>,
    /// Total number of items across all pages.
    pub total: u64,
    /// Current page number (1-indexed).
    pub page: u32,
    /// Number of items per page.
    pub page_size: u32,
    /// Total number of pages computed as `ceil(total / page_size)`.
    /// When `total` is 0, `total_pages` is 0.
    pub total_pages: u32,
}

impl<T> Paginated<T> {
    /// Creates a new `Paginated` response.
    ///
    /// # Panics
    ///
    /// Panics if `page` is 0 or `page_size` is 0.
    ///
    /// # Example
    ///
    /// ```
    /// use ev_db::Paginated;
    ///
    /// let paginated = Paginated::new(vec!["a", "b"], 100, 1, 20);
    /// assert_eq!(paginated.total_pages, 5);
    /// ```
    pub fn new(data: Vec<T>, total: u64, page: u32, page_size: u32) -> Self {
        assert!(page > 0, "page must be at least 1");
        assert!(page_size > 0, "page_size must be at least 1");
        let total_pages = if total == 0 {
            0
        } else {
            total.div_ceil(page_size as u64) as u32
        };
        Self {
            data,
            total,
            page,
            page_size,
            total_pages,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_items_returns_zero_total_pages() {
        let data: Vec<i32> = vec![];
        let p = Paginated::new(data, 0, 1, 20);
        assert_eq!(p.total_pages, 0);
        assert_eq!(p.total, 0);
        assert!(p.data.is_empty());
    }

    #[test]
    fn exact_multiple_computes_correct_pages() {
        let p = Paginated::new(vec![1; 20], 100, 1, 20);
        assert_eq!(p.total_pages, 5);
    }

    #[test]
    fn remainder_computes_ceiling() {
        let p = Paginated::new(vec![1; 20], 101, 1, 20);
        assert_eq!(p.total_pages, 6);
    }

    #[test]
    fn single_item_single_page() {
        let p = Paginated::new(vec![1], 1, 1, 20);
        assert_eq!(p.total_pages, 1);
    }

    #[test]
    fn second_page_metadata_correct() {
        let p = Paginated::new(vec![1; 5], 25, 2, 10);
        assert_eq!(p.page, 2);
        assert_eq!(p.page_size, 10);
        assert_eq!(p.total_pages, 3);
    }

    #[test]
    #[should_panic(expected = "page must be at least 1")]
    fn zero_page_panics() {
        let data: Vec<i32> = vec![];
        Paginated::new(data, 0, 0, 20);
    }

    #[test]
    #[should_panic(expected = "page_size must be at least 1")]
    fn zero_page_size_panics() {
        let data: Vec<i32> = vec![];
        Paginated::new(data, 0, 1, 0);
    }
}
