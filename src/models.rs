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

use base64;
use std::collections::HashMap;
use std::default::Default;

use arrow;
use arrow::ipc;
use arrow::ipc::file::reader as rr;
use arrow::ipc::gen::Message::MessageHeader;
use arrow::record_batch::RecordBatch;

use serde;
use serde::de;
use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use serde_json;
use serde_json::Value;

use std::sync::Arc;

/// Base class referring to the federated identity. This is normally populated
/// by Athena using the Access Key and the account number.
//#[derive(Debug)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FederatedIdentity {
    id: String,
    principal: String,
    account: String,
}

impl Default for FederatedIdentity {
    /// Creates a default value for Federated Identity
    fn default() -> Self {
        FederatedIdentity {
            id: String::from("UNKNOWN_ID"),
            principal: String::from("UNKNOWN_PRINCIPAL"),
            account: String::from("UNKNOWN_ACCOUNT"),
        }
    }
}

/// Value struct storing information about the table name.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TableName {
    schema_name: String,
    table_name: String,
}

impl TableName {
    pub fn new(s: String, t: String) -> Self {
        TableName {
            schema_name: s,
            table_name: t,
        }
    }
}

/// This is a value container for an Arrow schema object.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    schema: String,

    #[serde(skip_serializing)]
    arrow_schema: Option<arrow::datatypes::Schema>,
}

impl Schema {
    pub fn new_from_string(str: String) -> Self {
        Schema {
            schema: str,
            arrow_schema: None,
        }
    }

    /// Returns the arrow Schema object for the column. If the schema has not yet
    /// been decoded, it will decode it from the binary string representation.
    /// TOOD(magrund) We should implement a deserialize_with function instesad of this wrapper.
    pub fn get_schema(&mut self) -> Option<arrow::datatypes::Schema> {
        trace!("Deserializing schema");
        if self.arrow_schema.is_some() {
            return self.arrow_schema.clone();
        }

        let res = base64::decode(&self.schema);
        if res.is_err() {
            error!("Could not deserialize base64 Schema string");
            return None;
        }
        let schema_str_decoded = res.unwrap();
        // Now, try to decode the Arrow object. If we have a message written by
        // Arrow 0.15.0 and up, we might have to add additional 4 bytes padding.

        let mut fbs = ipc::get_size_prefixed_root_as_message(&schema_str_decoded);
        if fbs.header_type() == MessageHeader::NONE {
            fbs = ipc::get_size_prefixed_root_as_message(&schema_str_decoded[4..]);
        }

        if fbs.header_type() != MessageHeader::Schema {
            error!("Could not parse Schema flatbuffer message");
            return None;
        }

        let schema_fbs = fbs.header_as_schema().unwrap();
        self.arrow_schema = Some(ipc::convert::fb_to_schema(schema_fbs));
        self.arrow_schema.clone()
    }
}

/// This struct represents the block as transmitted over the wire from
/// the SDK. This struct is used to cache the serialized representation
/// to make it easy to send it back to the lambda functions.
#[derive(Debug)]
struct SerializedBlock {
    schema: String,
    records: String,
    a_id: String,
}

/// This is a value container for an Arrow schema object.
#[derive(Debug)]
pub struct Block {
    /// Holds a RecordBatch of Arrow values.
    records: RecordBatch,
    /// This member caches the serialized representation of
    /// the decoded values stored in records.
    serialized: SerializedBlock,
}

impl Block {
    /// Initializes the block with the decoded value and the
    /// encoded members of the Block.
    fn new(records: RecordBatch, schema_str: String, records_str: String, a_id: String) -> Self {
        let serialized = SerializedBlock {
            schema: schema_str,
            records: records_str,
            a_id: a_id,
        };
        Block {
            records,
            serialized,
        }
    }
}

/// Helper convert a serde_json::Value as a String into a binary value.
fn decode_value(v: Option<&Value>) -> Option<Vec<u8>> {
    if let Some(Value::String(v)) = v {
        if let Ok(decoded) = base64::decode(&v) {
            Some(decoded)
        } else {
            None
        }
    } else {
        None
    }
}

impl<'de> Deserialize<'de> for Block {
    /// Custom implementation to deserialize a Block from a given JSON record. First, we
    /// extract the JSON string values from the known keys, then we convert them to binary
    /// by base64 decoding them. Finally, we extract the Schema and RecordBatch messages and
    /// deserialize them into the Arrow Array types.
    fn deserialize<D>(deserializer: D) -> Result<Block, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let helper: Value = Deserialize::deserialize(deserializer)?;
        let tuple = (
            decode_value(helper.get("schema")),
            decode_value(helper.get("records")),
        );

        match tuple {
            (Some(schema), Some(records)) => {
                // We have to make sure that this is safe
                // TODO(magrund)
                let fbs_schema = ipc::get_size_prefixed_root_as_message(&schema[4..]);
                let fbs_records = ipc::get_size_prefixed_root_as_message(&records[4..]);

                if fbs_schema.header_type() == MessageHeader::Schema {
                    if let Some(fbs_schema) = fbs_schema.header_as_schema() {
                        let ss = ipc::convert::fb_to_schema(fbs_schema);

                        if fbs_records.header_type() == MessageHeader::RecordBatch {
                            let body_length = fbs_records.bodyLength();

                            if let Some(fbs_records) = fbs_records.header_as_record_batch() {
                                // Read fom the record batch
                                let x = rr::read_record_batch(
                                    &records[records.len() - body_length as usize..],
                                    fbs_records,
                                    Arc::new(ss),
                                );
                                if let Ok(Some(x)) = x {
                                    return Ok(Block::new(
                                        x,
                                        helper.get("schema").unwrap().as_str().unwrap().to_string(),
                                        helper
                                            .get("records")
                                            .unwrap()
                                            .as_str()
                                            .unwrap()
                                            .to_string(),
                                        helper.get("aId").unwrap().as_str().unwrap().to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
                return Err(D::Error::custom("Missing field `schema` in Block"));
            }
            (_, _) => return Err(D::Error::custom("Missing field `schema` in Block")),
        }
    }
}

impl Serialize for Block {
    /// Custom implementation to deserialize a Block from a given JSON record. First, we
    /// extract the JSON string values from the known keys, then we convert them to binary
    /// by base64 decoding them. Finally, we extract the Schema and RecordBatch messages and
    /// deserialize them into the Arrow Array types.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("Block", 3)?;
        s.serialize_field("schema", &self.serialized.schema)?;
        s.serialize_field("records", &self.serialized.records)?;
        s.serialize_field("aId", &self.serialized.a_id)?;
        s.end()
    }
}

/// A `SpillLocation` contains the metadata to be passed to the
/// lambda function where to spill values if the result becomes larger
/// than a certain threshold value configured in the request.
///
/// The knowledge about the `SpillLocation` is used for the caller to
/// fetch values from S3 instead of fetching them as an inline result
/// from the invocation of the lambda function.
#[derive(Debug, Serialize, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpillLocation {
    bucket: String,
    key: String,
    directory: bool,
    #[serde(rename = "@type", default = "SpillLocation::class_type_def")]
    class_type: String,
}

impl SpillLocation {
    fn class_type_def() -> String {
        "S3SpillLocation".to_string()
    }
}

/// Value struct containing information about the encryption key used
/// by the lambda function to encrypt the results in S3.
#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionKey {}

/// A `Split` is a work unit used in the distribution of requests.
#[derive(Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Split {
    spill_location: SpillLocation,
    properties: HashMap<String, String>,
    encryption_key: Option<EncryptionKey>,
}

impl Split {
    /// Creates a new Split with the given parameters. The default initialization
    /// will simply initialize the spill location for the Split. The actual split
    /// is defined via the properties of the Split.
    fn create(bucket: String, key: String) -> Split {
        let spill_loc = SpillLocation {
            bucket: bucket,
            key: key,
            directory: true,
            class_type: SpillLocation::class_type_def(),
        };
        Split {
            spill_location: spill_loc,
            properties: HashMap::new(),
            encryption_key: None,
        }
    }
}

/// Constraints are a complicated piece of technology that was
/// inherited by Presto. and we don't have a good way yet to
/// deal with it.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Constraints {
    summary: HashMap<String, String>,
}

impl Default for Constraints {
    /// Creates a default initialized instance of the constraints map.
    fn default() -> Self {
        Constraints {
            summary: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn defaults_test() {
        let d = FederatedIdentity::default();
        assert_eq!(String::from("UNKNOWN_ID"), d.id);
        assert_eq!(String::from("UNKNOWN_ACCOUNT"), d.account);
        assert_eq!(String::from("UNKNOWN_PRINCIPAL"), d.principal);

        let tn = TableName::default();
        assert!(tn.table_name.is_empty());
        assert!(tn.schema_name.is_empty());

        let split = Split::create(
            String::from("magrund-ops"),
            String::from("federation-spill"),
        );
        assert!(split.spill_location.directory);
        assert!(split.properties.is_empty());
    }

    #[test]
    fn test_schema_deserializing() {
        init();
        let schema_str_new = "/////0ABAAAQAAAAAAAKAA4ABgANAAgACgAAAAAAAwAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAABEAAAAAQAAAAwAAAAIAAwACAAEAAgAAAAIAAAAFAAAAAoAAABsb2dfc3RyZWFtAAANAAAAcGFydGl0aW9uQ29scwAAAAMAAACMAAAAOAAAAAQAAACS////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAID///8HAAAAbWVzc2FnZQDC////FAAAABQAAAAcAAAAAAACASAAAAAAAAAAAAAAAAgADAAIAAcACAAAAAAAAAFAAAAABAAAAHRpbWUAABIAGAAUABMAEgAMAAAACAAEABIAAAAUAAAAFAAAABgAAAAAAAUBFAAAAAAAAAAAAAAABAAEAAQAAAAKAAAAbG9nX3N0cmVhbQAAAAAAAA==".to_owned();
        let schema_str_old = "PAEAABAAAAAAAAoADgAGAA0ACAAKAAAAAAADABAAAAAAAQoADAAAAAgABAAKAAAACAAAAEQAAAABAAAADAAAAAgADAAIAAQACAAAAAgAAAAUAAAACgAAAGxvZ19zdHJlYW0AAA0AAABwYXJ0aXRpb25Db2xzAAAAAwAAAIwAAAA4AAAABAAAAJL///8UAAAAFAAAABQAAAAAAAUBEAAAAAAAAAAAAAAAgP///wcAAABtZXNzYWdlAML///8UAAAAFAAAABwAAAAAAAIBIAAAAAAAAAAAAAAACAAMAAgABwAIAAAAAAAAAUAAAAAEAAAAdGltZQAAEgAYABQAEwASAAwAAAAIAAQAEgAAABQAAAAUAAAAGAAAAAAABQEUAAAAAAAAAAAAAAAEAAQABAAAAAoAAABsb2dfc3RyZWFtAAA=".to_owned();

        let mut schema_new = Schema::new_from_string(schema_str_new);
        assert!(schema_new.arrow_schema.is_none());
        let the_schema = schema_new.get_schema();
        assert!(the_schema.is_some());

        let mut schema_old = Schema::new_from_string(schema_str_old);
        assert!(schema_old.arrow_schema.is_none());
        assert!(schema_old.get_schema().is_some());
    }

    #[test]
    fn test_block_deserializing() {
        let json = r#"{
            "schema": "/////xABAAAQAAAAAAAKAA4ABgANAAgACgAAAAAAAwAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAAIAAAAAAAAAAMAAACcAAAAPAAAAAQAAACC////FAAAABQAAAAUAAAAAAAFARAAAAAAAAAAAAAAAHD///8JAAAAbG9nX2dyb3VwAAAAtv///xQAAAAUAAAAHAAAAAAAAgEgAAAAAAAAAAAAAAAIAAwACAAHAAgAAAAAAAABQAAAABAAAABsb2dfc3RyZWFtX2J5dGVzAAASABgAFAATABIADAAAAAgABAASAAAAFAAAABQAAAAYAAAAAAAFARQAAAAAAAAAAAAAAAQABAAEAAAACgAAAGxvZ19zdHJlYW0AAA==",
            "records": "/////wgBAAAUAAAAAAAAAAwAFgAOABUAEAAEAAwAAACAAAAAAAAAAAAAAwAQAAAAAAMKABgADAAIAAQACgAAABQAAACYAAAAAQAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAQAAAAAAAAAIAAAAAAAAAAgAAAAAAAAAEAAAAAAAAAA0AAAAAAAAAEgAAAAAAAAAAQAAAAAAAABQAAAAAAAAAAgAAAAAAAAAWAAAAAAAAAABAAAAAAAAAGAAAAAAAAAACAAAAAAAAABoAAAAAAAAABIAAAAAAAAAAAAAAAMAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAA0AAAAMjAxOS8xMS8xNi9bJExBVEVTVF0wNTM0NmI2MTExMWI0YWQ2OTZkOTRiYTYwZTQ3MzRiNgAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAEgAAAC9hd3MvbGFtYmRhL2N3dGVzdAAAAAAAAA==", 
            "aId": "52fb8f5f-e2d0-4345-84d4-5f651bee361b"
            }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        assert_eq!(1, block.records.num_rows());
        assert_eq!(3, block.records.num_columns());
    }

    #[test]
    fn test_spill_location() {
        let json = r#"
        {
            "@type": "S3SpillLocation",
            "bucket": "magrund-ath-fed",
            "key": "athena-spill//e8300bd6-0737-4dfc-9af3-552fe160054f",
            "directory": true
        }
        "#;

        let sl_val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let sl: SpillLocation = serde_json::from_str(json).unwrap();
        assert_eq!("magrund-ath-fed".to_string(), sl.bucket);
        let val: serde_json::Value = serde_json::to_value(sl).unwrap();
        assert_eq!(sl_val, val);
    }

    #[test]
    fn test_split_serde() {
        let json = r#"{
            "spillLocation": {
                "@type": "S3SpillLocation",
                "bucket": "magrund-ath-fed",
                "key": "athena-spill//e8300bd6-0737-4dfc-9af3-552fe160054f",
                "directory": true
            },
            "encryptionKey": null,
            "properties": {
                "log_group": "/aws/lambda/cwtest",
                "log_stream_bytes": "0",
                "log_stream": "2019/11/16/[$LATEST]05346b61111b4ad696d94ba60e4734b6"
            }
        }"#;

        let sl_val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let sl: Split = serde_json::from_str(json).unwrap();
        //assert_eq!("magrund-ath-fed".to_string(), sl.bucket);
        let val: serde_json::Value = serde_json::to_value(sl).unwrap();
        assert_eq!(sl_val, val);
    }
}
