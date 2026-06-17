use driver_service::middleware::validation::{validate_coordinates, validate_radius_m, validate_max_results};

#[test]
fn test_coordinate_validation_valid() {
    assert!(validate_coordinates(36.8, 10.18).is_none());
    assert!(validate_coordinates(-90.0, -180.0).is_none());
    assert!(validate_coordinates(90.0, 180.0).is_none());
    assert!(validate_coordinates(0.0, 0.0).is_none());
}

#[test]
fn test_coordinate_validation_invalid_latitude() {
    let err = validate_coordinates(91.0, 10.0);
    assert!(err.is_some());
    assert_eq!(err.unwrap().error.code, "GEO_001");

    let err = validate_coordinates(-91.0, 10.0);
    assert!(err.is_some());
}

#[test]
fn test_coordinate_validation_invalid_longitude() {
    let err = validate_coordinates(36.0, 181.0);
    assert!(err.is_some());
    assert_eq!(err.unwrap().error.code, "GEO_001");

    let err = validate_coordinates(36.0, -181.0);
    assert!(err.is_some());
}

#[test]
fn test_radius_validation_valid() {
    assert!(validate_radius_m(Some(1)).is_none());
    assert!(validate_radius_m(Some(50000)).is_none());
    assert!(validate_radius_m(Some(5000)).is_none());
    assert!(validate_radius_m(None).is_none()); // defaults to 5000
}

#[test]
fn test_radius_validation_too_small() {
    let err = validate_radius_m(Some(0));
    assert!(err.is_some());
    assert_eq!(err.unwrap().error.code, "GEO_002");
}

#[test]
fn test_radius_validation_too_large() {
    let err = validate_radius_m(Some(50001));
    assert!(err.is_some());
    assert_eq!(err.unwrap().error.code, "GEO_002");
}

#[test]
fn test_max_results_validation_valid() {
    assert!(validate_max_results(Some(1)).is_none());
    assert!(validate_max_results(Some(100)).is_none());
    assert!(validate_max_results(Some(50)).is_none());
    assert!(validate_max_results(None).is_none());
}

#[test]
fn test_max_results_validation_too_small() {
    let err = validate_max_results(Some(0));
    assert!(err.is_some());
    assert_eq!(err.unwrap().error.code, "GEO_003");
}

#[test]
fn test_max_results_validation_too_large() {
    let err = validate_max_results(Some(101));
    assert!(err.is_some());
    assert_eq!(err.unwrap().error.code, "GEO_003");
}
