pub mod mongo {
    use mongodb::{Client, options::ClientOptions};

    pub async fn connect(mongo_url: &str) -> Result<Client, mongodb::error::Error> {
        let client_options = ClientOptions::parse(mongo_url).await?;
        let client = Client::with_options(client_options)?;
        tracing::info!("Connected to MongoDB");
        Ok(client)
    }
}

pub mod rabbit {
    use lapin::{Connection, ConnectionProperties};

    pub async fn connect(rabbitmq_url: &str) -> Result<Connection, lapin::Error> {
        let conn = Connection::connect(rabbitmq_url, ConnectionProperties::default()).await?;
        tracing::info!("Connected to RabbitMQ");
        Ok(conn)
    }
}
