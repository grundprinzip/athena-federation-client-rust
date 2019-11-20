extern crate rust_lambda_fed;

use rlf::models::*;
use rlf::{Configuration, Planner};
use rust_lambda_fed as rlf;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;
use env_logger;

#[derive(Debug, Clone)]
pub struct TestConfig {
    config: rlf::Configuration,
}

impl TestConfig {
    pub fn new(arn: String) -> Self {
        TestConfig {
            config: Configuration::new(arn),
        }
    }
}

fn setup() -> TestConfig {
    let _ = env_logger::builder().is_test(true).try_init();
    let c = TestConfig::new("arn:aws:lambda:us-east-1:269293906241:function:cwtest".to_owned());
    return c;
}

#[test]
fn test_list_schemas() {
    let c = setup();
    let mut p = Planner::new(c.config.clone());
    let schemas = p.list_schemas();
    assert!(!schemas.schemas.is_empty());
}

#[test]
fn test_list_tables() {
    println!("YEs");
    let c = setup();
    let mut p = Planner::new(c.config.clone());
    let schema_response = p.list_schemas();
    println!("YEs");
    for schema in &schema_response.schemas {
        let tables = p.list_tables("".to_owned(), schema.clone());
        for t in &tables.tables {
            debug!("{:?}", t);
        }
    }
}

#[test]
fn test_get_table() {
    let c = setup();
    let mut p = Planner::new(c.config.clone());
    dbg!(p.get_table(
        "".to_owned(),
        "/aws/lambda/cwtest".to_owned(),
        "2019/11/16/[$latest]05346b61111b4ad696d94ba60e4734b6".to_owned(),
    ));
}

#[test]
fn test_get_table_layout() {
    let c = setup();
    let mut p = Planner::new(c.config.clone());
    let mut val = dbg!(p.get_table(
        "".to_owned(),
        "/aws/lambda/cwtest".to_owned(),
        "2019/11/16/[$latest]05346b61111b4ad696d94ba60e4734b6".to_owned(),
    ));

    let schema = val.schema.get_schema().unwrap();
    let s = dbg!(schema.metadata()).get("partitionCols");

    p.get_table_layout(
        val.catalog_name,
        val.table_name,
        Constraints::default(),
        val.schema,
        vec![s.unwrap().clone()],
    );
}
