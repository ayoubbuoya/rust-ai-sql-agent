use std::env;

use crate::chain::Chain;
use anyhow::Error;
use async_trait::async_trait;
use dotenvy::dotenv;
use ollama_rs::generation::completion::request::GenerationRequest;
use ollama_rs::Ollama;
use sqlx::postgres::PgPool;
use sqlx::{Column, Row};

pub struct TextToSqlChain {
    client: Ollama,
    db: PgPool,
}

#[async_trait]
impl Chain for TextToSqlChain {
    async fn initialize() -> Result<Box<dyn Chain + Send>, Error>
    where
        Self: Sized,
    {
        dotenv().ok();

        let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPool::connect(&db_url).await?;

        Ok(Box::new(TextToSqlChain {
            client: Ollama::default(),
            db: pool,
        }))
    }

    async fn run(&self, input: String) -> Result<String, Error> {
        let prompt = self.construct_prompt(input).await?;

        let request = GenerationRequest::new(String::from("llama3.2:latest"), prompt);

        let sql_query = self
            .client
            .generate(request)
            .await
            .expect("Failed to generate sql query")
            .response;

        println!("Sql Generated: {:?}", sql_query);

        let data = self
            .query(sql_query)
            .await
            .expect("Failed to execute query");

        Ok(data)
    }
}

impl TextToSqlChain {
    async fn construct_prompt(&self, input: String) -> Result<String, Error> {
        let db_info = self
            .get_db_info()
            .await
            .expect("Failed to get database info");

        Ok(format!("Provided this schema : {} \n Generate executable SQL query that answers this question: {}. Only return the SQL query.", db_info, input))
    }

    async fn get_db_info(&self) -> Result<String, Error> {
        let tables_query =
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';";
        let rows = sqlx::query(tables_query).fetch_all(&self.db).await?;

        let mut tables_info = Vec::new();

        for row in rows {
            let table_name: String = row.get("table_name");
            let columns_query = format!("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}';", table_name);
            let columns_rows = sqlx::query(&columns_query).fetch_all(&self.db).await?;

            let columns: Vec<String> = columns_rows
                .iter()
                .map(|col_row| col_row.get("column_name"))
                .collect();

            tables_info.push(format!("Table: {}, Columns: {:?}", table_name, columns));
        }

        Ok(tables_info.join(", "))
    }

    async fn query(&self, generated_query: String) -> Result<String, Error> {
        let rows = sqlx::query(&generated_query).fetch_all(&self.db).await?;

        let mut result_string = String::new();

        for row in rows {
            let mut row_string = String::new();

            for (index, column) in row.columns().iter().enumerate() {
                let column_name = column.name();

                let value: Option<String> = row.try_get(index).unwrap_or(None);

                row_string.push_str(&format!("{}: {:?}, ", column_name, value));
            }

            if row_string.ends_with(", ") {
                row_string.truncate(row_string.len() - 2);
            }

            result_string.push_str(&format!("{{ {} }}", row_string));
        }

        Ok(result_string)
    }
}
