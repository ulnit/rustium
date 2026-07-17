use md2::Md2;
use md5::Md5;
use regex::{Regex, RegexBuilder};
use rustium_config::{ColumnHashAlgorithm, ColumnHashVersion, ColumnTransformRule};
use rustium_core::{ChangeEvent, DataValue, Error, Result, Row};
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512, Sha512_224, Sha512_256};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};

use crate::schema_history::{PostgresColumnType, TableSchema};

#[derive(Debug, Clone)]
pub(crate) struct ColumnTransformer {
    rules: Vec<CompiledRule>,
}

#[derive(Debug, Clone)]
struct CompiledRule {
    selectors: Vec<Regex>,
    operation: TransformOperation,
}

#[derive(Debug, Clone)]
enum TransformOperation {
    Truncate(usize),
    Mask(String),
    Hash {
        algorithm: ColumnHashAlgorithm,
        salt: Vec<u8>,
        version: ColumnHashVersion,
    },
}

impl ColumnTransformer {
    pub(crate) fn new(rules: &[ColumnTransformRule]) -> Result<Self> {
        let mut ordered = rules.iter().enumerate().collect::<Vec<_>>();
        ordered.sort_by_key(|(index, rule)| (rule.priority(), *index));
        let rules = ordered
            .into_iter()
            .map(|(_, rule)| {
                let selectors = rule
                    .columns()
                    .iter()
                    .map(|selector| compile_selector(selector))
                    .collect::<Result<Vec<_>>>()?;
                let operation = match rule {
                    ColumnTransformRule::Truncate { length, .. } => {
                        TransformOperation::Truncate(*length as usize)
                    }
                    ColumnTransformRule::Mask { length, .. } => {
                        TransformOperation::Mask("*".repeat(*length as usize))
                    }
                    ColumnTransformRule::Hash {
                        algorithm,
                        salt,
                        version,
                        ..
                    } => TransformOperation::Hash {
                        algorithm: *algorithm,
                        salt: salt.as_bytes().to_vec(),
                        version: *version,
                    },
                };
                Ok(CompiledRule {
                    selectors,
                    operation,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { rules })
    }

    pub(crate) fn transform_event(&self, event: &mut ChangeEvent, schema: &TableSchema) {
        if let Some(before) = &mut event.before {
            self.transform_row(before, schema);
        }
        if let Some(after) = &mut event.after {
            self.transform_row(after, schema);
        }
    }

    pub(crate) fn transform_row(&self, row: &mut Row, schema: &TableSchema) {
        for field in &schema.event_schema.fields {
            let Some(value) = row.get_mut(&field.name) else {
                continue;
            };
            *value = self.transform_value(schema, &field.name, value.clone());
        }
    }

    pub(crate) fn transform_value(
        &self,
        schema: &TableSchema,
        column: &str,
        value: DataValue,
    ) -> DataValue {
        let Some(column_type) = schema
            .column_types
            .iter()
            .find(|column_type| column_type.name == column)
        else {
            return value;
        };
        let Some(field) = schema
            .event_schema
            .fields
            .iter()
            .find(|field| field.name == column)
        else {
            return value;
        };
        let qualified_name = format!("{}.{}.{}", schema.schema, schema.table, column);
        let Some(rule) = self.rules.iter().find(|rule| {
            rule.selectors
                .iter()
                .any(|selector| selector.is_match(&qualified_name))
        }) else {
            return value;
        };
        match &rule.operation {
            TransformOperation::Truncate(length) if is_character_type(&field.type_name) => {
                match value {
                    DataValue::String(value) => {
                        DataValue::String(value.chars().take(*length).collect())
                    }
                    value => value,
                }
            }
            TransformOperation::Truncate(length) => match value {
                DataValue::Bytes(mut value) => {
                    value.truncate(*length);
                    DataValue::Bytes(value)
                }
                value => value,
            },
            TransformOperation::Mask(mask) if is_character_type(&field.type_name) => {
                DataValue::String(mask.clone())
            }
            TransformOperation::Hash {
                algorithm,
                salt,
                version,
            } if is_character_type(&field.type_name) => match value {
                DataValue::String(value) => {
                    let mut hash = hash_string(*algorithm, salt, *version, &value);
                    if let Some(length) = declared_character_length(&field.type_name, column_type) {
                        hash.truncate(length.min(hash.len()));
                    }
                    DataValue::String(hash)
                }
                value => value,
            },
            TransformOperation::Mask(_) | TransformOperation::Hash { .. } => value,
        }
    }
}

fn compile_selector(selector: &str) -> Result<Regex> {
    RegexBuilder::new(&format!("^(?:{selector})$"))
        .case_insensitive(true)
        .build()
        .map_err(|error| {
            Error::Configuration(format!(
                "invalid PostgreSQL column transformation selector {selector:?}: {error}"
            ))
        })
}

fn is_character_type(type_name: &str) -> bool {
    let type_name = type_name.trim();
    if type_name.ends_with("[]") {
        return false;
    }
    let base = type_name
        .split('(')
        .next()
        .unwrap_or(type_name)
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or(type_name)
        .trim()
        .trim_matches('"')
        .to_ascii_lowercase();
    matches!(
        base.as_str(),
        "char" | "character" | "bpchar" | "varchar" | "character varying" | "text" | "name"
    )
}

fn declared_character_length(type_name: &str, column_type: &PostgresColumnType) -> Option<usize> {
    if let Some((_, suffix)) = type_name.split_once('(')
        && let Some(length) = suffix.split_once(')').map(|(length, _)| length.trim())
        && let Ok(length) = length.parse::<usize>()
    {
        return Some(length);
    }
    match column_type.type_oid {
        18 => Some(1),
        1042 | 1043 if column_type.type_modifier >= 4 => {
            usize::try_from(column_type.type_modifier - 4).ok()
        }
        _ => None,
    }
}

fn hash_string(
    algorithm: ColumnHashAlgorithm,
    salt: &[u8],
    version: ColumnHashVersion,
    value: &str,
) -> String {
    let serialized;
    let bytes = match version {
        ColumnHashVersion::V1 => {
            serialized = java_serialized_string(value);
            serialized.as_slice()
        }
        ColumnHashVersion::V2 => value.as_bytes(),
    };
    match algorithm {
        ColumnHashAlgorithm::Md2 => digest::<Md2>(salt, bytes),
        ColumnHashAlgorithm::Md5 => digest::<Md5>(salt, bytes),
        ColumnHashAlgorithm::Sha1 => digest::<Sha1>(salt, bytes),
        ColumnHashAlgorithm::Sha224 => digest::<Sha224>(salt, bytes),
        ColumnHashAlgorithm::Sha256 => digest::<Sha256>(salt, bytes),
        ColumnHashAlgorithm::Sha384 => digest::<Sha384>(salt, bytes),
        ColumnHashAlgorithm::Sha512 => digest::<Sha512>(salt, bytes),
        ColumnHashAlgorithm::Sha512_224 => digest::<Sha512_224>(salt, bytes),
        ColumnHashAlgorithm::Sha512_256 => digest::<Sha512_256>(salt, bytes),
        ColumnHashAlgorithm::Sha3_224 => digest::<Sha3_224>(salt, bytes),
        ColumnHashAlgorithm::Sha3_256 => digest::<Sha3_256>(salt, bytes),
        ColumnHashAlgorithm::Sha3_384 => digest::<Sha3_384>(salt, bytes),
        ColumnHashAlgorithm::Sha3_512 => digest::<Sha3_512>(salt, bytes),
    }
}

fn digest<D: Digest>(salt: &[u8], value: &[u8]) -> String {
    let mut digest = D::new();
    digest.update(salt);
    digest.update(value);
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn java_serialized_string(value: &str) -> Vec<u8> {
    let mut modified_utf8 = Vec::with_capacity(value.len());
    for code_unit in value.encode_utf16() {
        match code_unit {
            0 => modified_utf8.extend_from_slice(&[0xc0, 0x80]),
            0x0001..=0x007f => modified_utf8.push(code_unit as u8),
            0x0080..=0x07ff => {
                modified_utf8.push((0xc0 | (code_unit >> 6)) as u8);
                modified_utf8.push((0x80 | (code_unit & 0x3f)) as u8);
            }
            _ => {
                modified_utf8.push((0xe0 | (code_unit >> 12)) as u8);
                modified_utf8.push((0x80 | ((code_unit >> 6) & 0x3f)) as u8);
                modified_utf8.push((0x80 | (code_unit & 0x3f)) as u8);
            }
        }
    }

    let mut serialized = Vec::with_capacity(modified_utf8.len() + 13);
    serialized.extend_from_slice(&[0xac, 0xed, 0x00, 0x05]);
    if let Ok(length) = u16::try_from(modified_utf8.len()) {
        serialized.push(0x74);
        serialized.extend_from_slice(&length.to_be_bytes());
    } else {
        serialized.push(0x7c);
        serialized.extend_from_slice(&(modified_utf8.len() as u64).to_be_bytes());
    }
    serialized.extend_from_slice(&modified_utf8);
    serialized
}

#[cfg(test)]
mod tests {
    use rustium_core::{EventSchema, FieldSchema};

    use super::*;

    fn test_schema(type_name: &str, type_oid: u32, type_modifier: i32) -> TableSchema {
        TableSchema {
            schema: "public".into(),
            table: "customers".into(),
            event_schema: EventSchema {
                name: "test.public.customers.Envelope".into(),
                version: 1,
                fields: vec![FieldSchema {
                    name: "secret".into(),
                    type_name: type_name.into(),
                    optional: true,
                    primary_key: false,
                }],
            },
            column_types: vec![PostgresColumnType {
                name: "secret".into(),
                type_oid,
                type_modifier,
            }],
            opaque_columns: Vec::new(),
        }
    }

    fn test_transformer(rule: ColumnTransformRule) -> ColumnTransformer {
        ColumnTransformer::new(&[rule]).unwrap()
    }

    #[test]
    fn reproduces_debezium_hash_versions_and_declared_length() {
        let schema = test_schema("character varying(20)", 1043, 24);
        let v1 = test_transformer(ColumnTransformRule::Hash {
            algorithm: ColumnHashAlgorithm::Sha256,
            salt: "CzQMA0cB5K".into(),
            version: ColumnHashVersion::V1,
            columns: vec![r"PUBLIC\.CUSTOMERS\.SECRET".into()],
        });
        assert_eq!(
            v1.transform_value(&schema, "secret", DataValue::String("test".into())),
            DataValue::String("8e68c68edbbac316dfe2".into())
        );
        assert_eq!(
            v1.transform_value(&schema, "secret", DataValue::String("hello".into())),
            DataValue::String("b4d39ab0d198fb4cac8b".into())
        );

        let unbounded = test_schema("text", 25, -1);
        let v1 = test_transformer(ColumnTransformRule::Hash {
            algorithm: ColumnHashAlgorithm::Sha256,
            salt: "salt123".into(),
            version: ColumnHashVersion::V1,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            v1.transform_value(
                &unbounded,
                "secret",
                DataValue::String("12345678901234567890".into())
            ),
            DataValue::String(
                "5944c66655670e4ce234df8529d452ba1cae10a641b9cd1583abf62585b8515a".into()
            )
        );
        let v2 = test_transformer(ColumnTransformRule::Hash {
            algorithm: ColumnHashAlgorithm::Sha256,
            salt: "salt123".into(),
            version: ColumnHashVersion::V2,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            v2.transform_value(
                &unbounded,
                "secret",
                DataValue::String("12345678901234567890".into())
            ),
            DataValue::String(
                "b65875d34a3dedf070f3a012970bf3b5da424560d7be3d2c23b986b525d2d7f3".into()
            )
        );
    }

    #[test]
    fn applies_debezium_priority_and_null_semantics() {
        let rules = vec![
            ColumnTransformRule::Hash {
                algorithm: ColumnHashAlgorithm::Sha256,
                salt: "salt".into(),
                version: ColumnHashVersion::V2,
                columns: vec![r"public\.customers\.secret".into()],
            },
            ColumnTransformRule::Mask {
                length: 4,
                columns: vec![r"public\.customers\.secret".into()],
            },
            ColumnTransformRule::Truncate {
                length: 2,
                columns: vec![r"public\.customers\.secret".into()],
            },
        ];
        let transformer = ColumnTransformer::new(&rules).unwrap();
        let schema = test_schema("text", 25, -1);
        assert_eq!(
            transformer.transform_value(&schema, "secret", DataValue::String("value".into())),
            DataValue::String("va".into())
        );

        let mask = test_transformer(ColumnTransformRule::Mask {
            length: 3,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            mask.transform_value(&schema, "secret", DataValue::Null),
            DataValue::String("***".into())
        );
        let hash = test_transformer(ColumnTransformRule::Hash {
            algorithm: ColumnHashAlgorithm::Sha256,
            salt: "salt".into(),
            version: ColumnHashVersion::V1,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            hash.transform_value(&schema, "secret", DataValue::Null),
            DataValue::Null
        );
    }

    #[test]
    fn truncates_unicode_by_characters_and_ignores_non_character_columns() {
        let transformer = test_transformer(ColumnTransformRule::Truncate {
            length: 2,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            transformer.transform_value(
                &test_schema("text", 25, -1),
                "secret",
                DataValue::String("A界C".into())
            ),
            DataValue::String("A界".into())
        );
        assert_eq!(
            transformer.transform_value(
                &test_schema("integer", 23, -1),
                "secret",
                DataValue::String("123".into())
            ),
            DataValue::String("123".into())
        );
        assert_eq!(
            transformer.transform_value(
                &test_schema("bytea", 17, -1),
                "secret",
                DataValue::Bytes(vec![1, 2, 3])
            ),
            DataValue::Bytes(vec![1, 2])
        );
    }
}
