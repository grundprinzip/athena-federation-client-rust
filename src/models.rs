use std::collections::HashMap;


/// Base class referring to the federated identity. This is normally populated
/// by Athena using the Access Key and the account number.
//#[derive(Debug)]
#[derive(Debug)]
pub struct FederatedIdentity {
    id: String,
    principal: String,
    account: String,
}

impl FederatedIdentity {
    /// Creates a default value for Federated Identity
    fn default() -> FederatedIdentity {
        FederatedIdentity {
            id: String::from("UNKNOWN_ID"),
            principal: String::from("UNKNOWN_PRINCIPAL"),
            account: String::from("UNKNOWN_ACCOUNT"),
        }
    }
}

/// Value struct storing information about the table name.
#[derive(Debug)]
pub struct TableName {
    schema_name: String,
    table_name: String,
}

impl TableName {
    fn default() -> TableName {
        TableName {
            schema_name: String::from(""),
            table_name: String::from(""),
        }
    }
}

/// This is a value container for an Arrow schema object.
#[derive(Debug)]
pub struct Schema {}

#[derive(Debug)]
pub struct SpillLocation {
    bucket: String,
    key: String,
    directory: bool,
}

#[derive(Debug)]
pub struct Split {
    spill_location: SpillLocation,
    encrypted: bool,
    properties: HashMap<String, String>,
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
        };
        Split {
            spill_location: spill_loc,
            encrypted: false,
            properties: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct Constraints {
    summary: HashMap<String, String>,
}

impl Constraints {
    /// Creates a default initialized instance of the constraints map.
    fn default() -> Constraints {
        Constraints {
            summary: HashMap::new(),
        }
    }
}

#[derive(Debug)]
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

#[cfg(test)]
mod test {

    use super::*;

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
        assert!(!split.encrypted);
        assert!(split.spill_location.directory);
        assert!(split.properties.is_empty());
    }
}
