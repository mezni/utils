use deltalake::{
    kernel::{DataType, PrimitiveType, StructField, StructType},
    DeltaOps, DeltaTable, DeltaTableError,
};

pub fn table_schema() -> StructType {
    StructType::new(vec![
        StructField::new("session_id", DataType::Primitive(PrimitiveType::String), false),
        StructField::new("timestamp", DataType::Primitive(PrimitiveType::String), false),
    ])
}

pub async fn get_delta_table(
    path: &str,
    delta_schema: StructType,
    partitions: Option<Vec<&str>>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = match deltalake::open_table(path).await {
        Ok(table) => table,
        Err(DeltaTableError::InvalidTableLocation(_)) => {
            DeltaOps::try_from_uri(path)
                .await?
                .create()
                .with_columns(delta_schema.fields().cloned().collect::<Vec<_>>())
                .with_partition_columns(partitions.unwrap_or_default())
                .await?
        }
        _ => panic!("unexpected err while reading delta_table"),
    };

    Ok(table)
}

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    let table1_path = "./delta_lake/table1";
    let table1_schema = table_schema();

    let _trips_table = get_delta_table(
        table1_path,
        table1_schema,
        None,  
    )
    .await?;

    Ok(())
}