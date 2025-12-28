//! Schema definitions for `RowBinary` encoding.

use crate::{
    error::{Error, Result},
    types::{TypeDesc, parse_type_desc},
    value::Value,
};

/// Column descriptor used by `RowBinary` readers and writers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// Column name.
    pub name: String,
    /// Column type.
    pub ty: TypeDesc,
}

/// Schema containing ordered fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    /// Creates a schema from fields.
    #[must_use]
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Returns the ordered field list.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Returns the number of fields.
    #[must_use]
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Reports whether the schema is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Creates a schema from name/type pairs.
    pub fn from_names_and_types<I, S>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (S, TypeDesc)>,
        S: Into<String>,
    {
        let fields = pairs
            .into_iter()
            .map(|(name, ty)| Field {
                name: name.into(),
                ty,
            })
            .collect();
        Self { fields }
    }

    /// Parses a schema from name/type strings.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error`] when parsing type names fails.
    pub fn from_type_strings(pairs: &[(&str, &str)]) -> Result<Self> {
        let mut fields = Vec::with_capacity(pairs.len());
        for (name, ty) in pairs {
            fields.push(Field {
                name: (*name).to_string(),
                ty: parse_type_desc(ty)?,
            });
        }
        Ok(Self { fields })
    }
}

/// A single `RowBinary` row.
pub type Row = Vec<Value>;

pub(crate) fn expand_schema_for_writing(schema: &Schema) -> Schema {
    let mut fields = Vec::new();
    for field in &schema.fields {
        match &field.ty {
            TypeDesc::Nested(items) => {
                for item in items {
                    let name = item.name.as_deref().unwrap_or("");
                    fields.push(Field {
                        name: format!("{}.{}", field.name, name),
                        ty: TypeDesc::Array(Box::new(item.ty.clone())),
                    });
                }
            }
            _ => fields.push(field.clone()),
        }
    }
    Schema::new(fields)
}

pub(crate) fn ensure_nested_names(schema: &Schema) -> Result<()> {
    for field in &schema.fields {
        if let TypeDesc::Nested(items) = &field.ty {
            for item in items {
                if item.name.as_deref().unwrap_or("").is_empty() {
                    return Err(Error::InvalidValue(
                        "Nested fields must have names when writing",
                    ));
                }
            }
        }
    }
    Ok(())
}
