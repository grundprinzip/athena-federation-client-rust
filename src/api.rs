use super::models;
use super::requests;
use bytes::{Buf, Bytes, IntoBuf};
use rusoto_lambda::{InvocationRequest, InvocationResponse, Lambda, LambdaClient};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::default::Default;
use std::str;

#[derive(Default, Debug, Clone)]
pub struct Configuration {
    record_lambda: String,
    metadata_lambda: String,
    region: String,
}

impl Configuration {
    pub fn new(lambda: String) -> Configuration {
        Configuration {
            record_lambda: lambda.clone(),
            metadata_lambda: lambda.clone(),
            region: "us-east-1".to_string(),
        }
    }
}

/// The Planner class is responsible to resolve the metadata for each federation call.
/// The first step is to check for tables and extract the table layout, once the table
/// layout is fetched, we can extract the splits and based on the splits execute the
/// ReadRecordRequests for each Split.
pub struct Planner {
    config: Configuration,
    client: LambdaClient,
}

impl Planner {
    /// Instantiates a new Planner object configured with a Configuration
    /// object.
    pub fn new(c: Configuration) -> Self {
        let r = c.region.as_str().parse().unwrap();
        Planner {
            config: c,
            client: LambdaClient::new(r),
        }
    }

    /// Generic invoke method to handle the request serialization and invocation.
    /// The return value is automatically inferred and populated based on
    /// the caller.
    fn invoke<T>(&mut self, body: String) -> T
    where
        T: DeserializeOwned,
    {
        // Setup the request
        let mut lambda_fun = InvocationRequest::default();
        lambda_fun.function_name = self.config.metadata_lambda.clone();

        // COnvert body to Bytes
        lambda_fun.payload = Some(Bytes::from(body));
        trace!("Invoking lambda function: {}", lambda_fun.function_name);
        let result_future = self.client.invoke(lambda_fun);
        let result = result_future.sync().unwrap();

        // print the body
        let payload = result.payload.unwrap();
        trace!("{}", std::str::from_utf8(&payload).unwrap());
        let reader = payload.into_buf().reader();
        trace!("Result: {:?}", reader);
        return serde_json::from_reader(reader).unwrap();
    }

    /// For a given catalog name, list all schemas inside the catalog
    pub fn list_schemas(&mut self) -> requests::ListSchemasResponse {
        let req = requests::ListSchemasRequest::default();

        // Request should be converted to JSON
        let body = serde_json::to_string(&req).unwrap();
        let res: requests::ListSchemasResponse = self.invoke(body);
        trace!("{:?}", res);
        return res;
    }

    pub fn list_tables(
        &mut self,
        catalog_name: String,
        schema_name: String,
    ) -> requests::ListTablesResponse {
        let req = requests::ListTablesRequest::new(&"".to_owned(), &catalog_name, &schema_name);
        let body = serde_json::to_string(&req).unwrap();
        let res: requests::ListTablesResponse = self.invoke(body);
        trace!("{:?}", res);
        return res;
    }

    pub fn get_table(
        &mut self,
        catalog_name: String,
        schema_name: String,
        table_name: String,
    ) -> requests::GetTableResponse {
        let req = requests::GetTableRequest::new(catalog_name, schema_name, table_name);
        let body = serde_json::to_string(&req).unwrap();
        let res: requests::GetTableResponse = self.invoke(body);
        trace!("{:?}", res);
        return res;
    }

    pub fn get_table_layout(
        &mut self,
        catalog_name: String,
        table_name: models::TableName,
        constraints: models::Constraints,
        schema: models::Schema,
        partition_cols: Vec<String>,
    ) -> requests::GetTableLayoutResponse {
        let query_id = "".to_string();
        let req = requests::GetTableLayoutRequest::new(
            query_id,
            catalog_name,
            table_name,
            constraints,
            schema,
            partition_cols,
        );
        let body = serde_json::to_string(&req).unwrap();
        let res: requests::GetTableLayoutResponse = self.invoke(body);
        trace!("{:?}", res);
        return res;
    }

    pub fn get_splits() {
        //let _req = requests::GetSplitsRequest::default();
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn default_test() {
        let c = Configuration::default();
        assert!(c.record_lambda.is_empty());
        assert!(c.metadata_lambda.is_empty());
    }

    #[test]
    fn test_config_setup() {
        let c = Configuration::new("this-is-my-arn".to_string());
        assert_eq!("this-is-my-arn".to_string(), c.record_lambda);
        assert_eq!(c.metadata_lambda, c.record_lambda);
    }
}
