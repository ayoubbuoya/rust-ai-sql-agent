use anyhow::Error;
use async_trait::async_trait;

#[async_trait]
pub trait Chain {
    async fn initialize() -> Result<Box<dyn Chain + Send>, Error>
    where
        Self: Sized;

    async fn run(&self, input: String) -> Result<String, Error>;
}
