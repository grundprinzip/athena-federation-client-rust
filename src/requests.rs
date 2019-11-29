// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::default::Default;

// Include the model classes
use super::models::*;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadRecordRequest {
    catalog_name: String,
    query_id: String,
    identity: FederatedIdentity,
    table_name: TableName,
    schema: Schema,
    split: Split,
    constraints: Constraints,
    max_block_size: i64,
    max_inline_block_size: i64,
    request_type: String,
}

impl Default for ReadRecordRequest {
    fn default() -> Self {
        ReadRecordRequest {
            catalog_name: String::new(),
            query_id: String::new(),
            identity: FederatedIdentity::default(),
            table_name: TableName::default(),
            schema: Schema::default(),
            split: Split::default(),
            constraints: Constraints::default(),
            max_block_size: 16000000,
            max_inline_block_size: 5242880,
            request_type: String::from("READ_RECORDS"),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasRequest {
    identity: FederatedIdentity,
    query_id: String,
    catalog_name: String,
    #[serde(rename(serialize = "@type"))]
    class_type: String,
}

impl Default for ListSchemasRequest {
    fn default() -> Self {
        ListSchemasRequest {
            identity: FederatedIdentity::default(),
            query_id: String::new(),
            catalog_name: String::new(),
            class_type: "ListSchemasRequest".to_owned(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasResponse {
    pub catalog_name: String,
    request_type: String,
    pub schemas: Vec<String>,
    #[serde(rename(deserialize = "@type"))]
    class_type: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesRequest {
    identity: FederatedIdentity,
    query_id: String,
    catalog_name: String,
    schema_name: String,
    #[serde(rename(serialize = "@type"))]
    class_type: String,
}

impl Default for ListTablesRequest {
    fn default() -> Self {
        ListTablesRequest {
            identity: FederatedIdentity::default(),
            query_id: String::new(),
            catalog_name: String::new(),
            schema_name: String::new(),
            class_type: "ListTablesRequest".to_owned(),
        }
    }
}

impl ListTablesRequest {
    pub fn new(query_id: &String, catalog_name: &String, schema: &String) -> Self {
        ListTablesRequest {
            identity: FederatedIdentity::default(),
            query_id: query_id.clone(),
            catalog_name: catalog_name.clone(),
            schema_name: schema.clone(),
            class_type: "ListTablesRequest".to_owned(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    #[serde(rename(deserialize = "@type"))]
    class_type: String,

    pub catalog_name: String,
    pub tables: Vec<TableName>,
    request_type: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTableRequest {
    identity: FederatedIdentity,
    query_id: String,
    catalog_name: String,
    table_name: TableName,

    #[serde(rename(serialize = "@type"))]
    class_type: String,
}

impl GetTableRequest {
    pub fn new(catalog_name: String, schema_name: String, table_name: String) -> Self {
        GetTableRequest {
            identity: FederatedIdentity::default(),
            catalog_name: catalog_name,
            query_id: String::new(),
            table_name: TableName::new(schema_name, table_name),
            class_type: "GetTableRequest".to_owned(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetTableResponse {
    #[serde(rename(deserialize = "@type"))]
    class_type: String,

    pub catalog_name: String,
    pub table_name: TableName,
    pub schema: Schema,
    request_type: String,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTableLayoutRequest {
    identity: FederatedIdentity,
    query_id: String,
    catalog_name: String,
    table_name: TableName,
    constraints: Constraints,
    schema: Schema,
    // Is a set
    partition_cols: Vec<String>,

    #[serde(rename(serialize = "@type"))]
    class_type: String,
}

impl GetTableLayoutRequest {
    pub fn new(
        query_id: String,
        catalog_name: String,
        table_name: TableName,
        constraints: Constraints,
        schema: Schema,
        partition_cols: Vec<String>,
    ) -> Self {
        let identity = FederatedIdentity::default();
        GetTableLayoutRequest {
            identity,
            query_id,
            catalog_name,
            table_name,
            constraints,
            schema,
            partition_cols,
            class_type: "GetTableLayoutRequest".to_string(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTableLayoutResponse {
    #[serde(rename(deserialize = "@type"))]
    class_type: String,
    request_type: String,

    pub catalog_name: String,
    pub table_name: TableName,
    pub partitions: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetSplitsRequest {
    identity: FederatedIdentity,
    query_id: String,
    catalog_name: String,
    table_name: TableName,
    partitions: Block,
    partition_cols: Vec<String>,
    constraints: Constraints,
    continuation_token: String,
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn json_serializer() {
        let d = FederatedIdentity::default();
        let serialized = serde_json::to_string(&d).unwrap();
        println!("{}", serialized);
        assert!(!serialized.is_empty());

        let r = ReadRecordRequest::default();
        let r_serialized = serde_json::to_string(&r).unwrap();
        println!("{}", r_serialized);
        assert!(!r_serialized.is_empty());

        let mut r = ListSchemasRequest::default();
        r.query_id = "myqueryid".to_owned();
        println!("{}", serde_json::to_string(&r).unwrap());

        let tn = TableName::new("Martin".to_string(), "Grund".to_string());
        let cols = vec!["Col1".to_string()];
        let schema_str_new = "/////0ABAAAQAAAAAAAKAA4ABgANAAgACgAAAAAAAwAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAABEAAAAAQAAAAwAAAAIAAwACAAEAAgAAAAIAAAAFAAAAAoAAABsb2dfc3RyZWFtAAANAAAAcGFydGl0aW9uQ29scwAAAAMAAACMAAAAOAAAAAQAAACS////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAID///8HAAAAbWVzc2FnZQDC////FAAAABQAAAAcAAAAAAACASAAAAAAAAAAAAAAAAgADAAIAAcACAAAAAAAAAFAAAAABAAAAHRpbWUAABIAGAAUABMAEgAMAAAACAAEABIAAAAUAAAAFAAAABgAAAAAAAUBFAAAAAAAAAAAAAAABAAEAAQAAAAKAAAAbG9nX3N0cmVhbQAAAAAAAA==".to_owned();
        let mut schema_new = Schema::new_from_string(schema_str_new);

        let gtl = GetTableLayoutRequest::new(
            "query_id".to_string(),
            "catalog_name".to_string(),
            tn,
            Constraints::default(),
            schema_new,
            cols,
        );

        println!("{}", serde_json::to_string(&gtl).unwrap());
    }

    #[test]
    fn json_get_split_request() {
        let json = r#"{
                "identity":{"id":"UNKNOWN_ID","principal":"UNKNOWN_PRINCIPAL","account":"UNKNOWN_ACCOUNT"},
                "queryId":"query_id",
                "catalogName":"catalog_name",
                "tableName":{"schemaName":"Martin","tableName":"Grund"},
                "partitions": {
                    "schema": "/////xABAAAQAAAAAAAKAA4ABgANAAgACgAAAAAAAwAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAAIAAAAAAAAAAMAAACcAAAAPAAAAAQAAACC////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAHD///8JAAAAbG9nX2dyb3VwAAAAtv///xQAAAAUAAAAHAAAAAAAAgEgAAAAAAAAAAAAAAAIAAwACAAHAAgAAAAAAAABQAAAABAAAABsb2dfc3RyZWFtX2J5dGVzAAASABgAFAATABIADAAAAAgABAASAAAAFAAAABQAAAAYAAAAAAAFARQAAAAAAAAAAAAAAAQABAAEAAAACgAAAGxvZ19zdHJlYW0AAA==",
                    "records": "/////wgBAAAUAAAAAAAAAAwAFgAOABUAEAAEAAwAAACAAAAAAAAAAAAAAwAQAAAAAAMKABgADAAIAAQACgAAABQAAACYAAAAAQAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAEAAAAAAAAAA0AAAAAAAAAEgAAAAAAAAAAQAAAAAAAABQAAAAAAAAAAgAAAAAAAAAWAAAAAAAAAABAAAAAAAAAGAAAAAAAAAACAAAAAAAAABoAAAAAAAAABIAAAAAAAAAAAAAAAMAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAA0AAAAMjAxOS8xMS8xNi9bJExBVEVTVF0wNTM0NmI2MTExMWI0YWQ2OTZkOTRiYTYwZTQ3MzRiNgAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAEgAAAC9hd3MvbGFtYmRhL2N3dGVzdAAAAAAAAA==", 
                    "aId": "52fb8f5f-e2d0-4345-84d4-5f651bee361b"
                },
                "partitionCols": [
                    "log_stream"
                ],
                "constraints":{"summary":{}},
                "continuationToken" : "abc"
            }"#;

        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let req: GetSplitsRequest = serde_json::from_str(&json).unwrap();

        let new_val = serde_json::to_value(req).unwrap();
        assert_eq!(val, new_val);
    }
}
