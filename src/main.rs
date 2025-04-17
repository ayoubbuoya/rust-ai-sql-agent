use std::io::{stdin, stdout, Write};

use chain::Chain;
use text_to_sql_chain::TextToSqlChain;

mod chain;
mod text_to_sql_chain;

#[tokio::main]
async fn main() {
    let processor = TextToSqlChain::initialize().await.unwrap();
    let mut input = String::new();

    print!("How can I help you?: ");

    stdout().flush().unwrap();

    stdin().read_line(&mut input).expect("Failed to read line");

    let result = processor.run(input).await.expect("Failed to execute run");

    println!("Response {:?}", result);
}
