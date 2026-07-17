//! Shared Debezium-compatible column transformations.

use std::collections::HashMap;

use md2::Md2;
use md5::Md5;
use regex::{Regex, RegexBuilder};
use rustium_config::{ColumnHashAlgorithm, ColumnHashVersion, ColumnTransformRule};
use rustium_core::{ChangeEvent, DataValue, Error, EventSchema, Result, Row};
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512, Sha512_224, Sha512_256};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};

#[derive(Debug, Clone)]
pub struct ColumnTransformer {
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
    pub fn new(rules: &[ColumnTransformRule]) -> Result<Self> {
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

    pub fn transform_event(
        &self,
        event: &mut ChangeEvent,
        namespace: &str,
        table: &str,
        declared_lengths: &HashMap<String, usize>,
    ) {
        if let Some(before) = &mut event.before {
            self.transform_row(before, namespace, table, &event.schema, declared_lengths);
        }
        if let Some(after) = &mut event.after {
            self.transform_row(after, namespace, table, &event.schema, declared_lengths);
        }
    }

    pub fn transform_row(
        &self,
        row: &mut Row,
        namespace: &str,
        table: &str,
        schema: &EventSchema,
        declared_lengths: &HashMap<String, usize>,
    ) {
        for field in &schema.fields {
            let Some(value) = row.get_mut(&field.name) else {
                continue;
            };
            *value = self.transform_value(
                namespace,
                table,
                schema,
                &field.name,
                value.clone(),
                declared_lengths.get(&field.name).copied(),
            );
        }
    }

    pub fn transform_value(
        &self,
        namespace: &str,
        table: &str,
        schema: &EventSchema,
        column: &str,
        value: DataValue,
        declared_length: Option<usize>,
    ) -> DataValue {
        let Some(field) = schema.fields.iter().find(|field| field.name == column) else {
            return value;
        };
        let qualified_name = format!("{namespace}.{table}.{column}");
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
                    if let Some(length) =
                        declared_length.or_else(|| declared_character_length(&field.type_name))
                    {
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
                "invalid column transformation selector {selector:?}: {error}"
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
        "char"
            | "character"
            | "bpchar"
            | "varchar"
            | "character varying"
            | "text"
            | "tinytext"
            | "mediumtext"
            | "longtext"
            | "name"
    )
}

fn declared_character_length(type_name: &str) -> Option<usize> {
    let (_, suffix) = type_name.split_once('(')?;
    suffix
        .split_once(')')
        .map(|(length, _)| length.trim())
        .and_then(|length| length.parse::<usize>().ok())
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
    use super::*;
    use rustium_core::{EventSchema, FieldSchema};

    fn schema(type_name: &str) -> EventSchema {
        EventSchema {
            name: "test.public.customers.Envelope".into(),
            version: 1,
            fields: vec![FieldSchema {
                name: "secret".into(),
                type_name: type_name.into(),
                optional: true,
                primary_key: false,
            }],
        }
    }

    fn transformer(rule: ColumnTransformRule) -> ColumnTransformer {
        ColumnTransformer::new(&[rule]).unwrap()
    }

    #[test]
    fn reproduces_debezium_hash_fixtures() {
        let event_schema = schema("varchar(20)");
        let v1 = transformer(ColumnTransformRule::Hash {
            algorithm: ColumnHashAlgorithm::Sha256,
            salt: "CzQMA0cB5K".into(),
            version: ColumnHashVersion::V1,
            columns: vec![r"PUBLIC\.CUSTOMERS\.SECRET".into()],
        });
        assert_eq!(
            v1.transform_value(
                "public",
                "customers",
                &event_schema,
                "secret",
                DataValue::String("test".into()),
                None
            ),
            DataValue::String("8e68c68edbbac316dfe2".into())
        );
        let v2 = transformer(ColumnTransformRule::Hash {
            algorithm: ColumnHashAlgorithm::Sha256,
            salt: "salt123".into(),
            version: ColumnHashVersion::V2,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            v2.transform_value(
                "public",
                "customers",
                &schema("text"),
                "secret",
                DataValue::String("12345678901234567890".into()),
                None
            ),
            DataValue::String(
                "b65875d34a3dedf070f3a012970bf3b5da424560d7be3d2c23b986b525d2d7f3".into()
            )
        );
    }

    #[test]
    fn applies_priority_null_and_binary_semantics() {
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
        let combined = ColumnTransformer::new(&rules).unwrap();
        let event_schema = schema("text");
        assert_eq!(
            combined.transform_value(
                "public",
                "customers",
                &event_schema,
                "secret",
                DataValue::String("value".into()),
                None
            ),
            DataValue::String("va".into())
        );
        let mask = transformer(ColumnTransformRule::Mask {
            length: 3,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            mask.transform_value(
                "public",
                "customers",
                &event_schema,
                "secret",
                DataValue::Null,
                None
            ),
            DataValue::String("***".into())
        );
        let truncate = transformer(ColumnTransformRule::Truncate {
            length: 2,
            columns: vec![r"public\.customers\.secret".into()],
        });
        assert_eq!(
            truncate.transform_value(
                "public",
                "customers",
                &schema("blob"),
                "secret",
                DataValue::Bytes(vec![1, 2, 3]),
                None
            ),
            DataValue::Bytes(vec![1, 2])
        );
    }
}
