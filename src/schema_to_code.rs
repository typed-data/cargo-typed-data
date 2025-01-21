use std::path::Path;
use std::sync::Arc;
use std::vec;

use codegen::Scope;

use anyhow::{Context, Result};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema;

#[derive(Clone, Debug)]
struct ParsedField {
    name: String,
    type_: String,
}

impl ParsedField {
    fn new(name: String, type_: String) -> Self {
        Self { name, type_ }
    }
}
#[derive(Clone, Debug)]
struct ParsedStruct {
    pub name: String,
    pub fields: Vec<ParsedField>,
}

impl ParsedStruct {
    fn new(name: String) -> Self {
        Self {
            name,
            fields: vec![],
        }
    }

    fn add_field(&mut self, field: ParsedField) {
        self.fields.push(field);
    }

    fn generate_rust(structs: &[ParsedStruct]) -> String {
        let mut scope = Scope::new();
        for entry in structs {
            let struct_ = scope.new_struct(&entry.name)
                .vis("pub")
                .derive("Debug")
                .derive("Clone")
                .derive("PartialEq")
                .derive("Eq")
                .derive("ArrowField")
                .derive("ArrowSerialize")
                .derive("ArrowDeserialize");
            for field in &entry.fields {
                struct_.field(&field.name, &field.type_).vis("pub");
            }
        }
        scope.to_string()
    }
    
    fn create(
        name: String,
        fields: &[Arc<schema::types::Type>],
    ) -> Vec<ParsedStruct> {
        let mut out = vec![];
        let mut struct_ = ParsedStruct::new(name);
        for field in fields {
            let basic_info = field.get_basic_info();
            let parsed_field = if field.is_group() {
                let type_ = basic_info.name();
                let new_struct = capitalize_first_letter(type_);
                let fields = field.get_fields();
                let mut structs = Self::create(new_struct.clone(), fields);

                out.append(&mut structs);
                ParsedField::new(basic_info.name().into(), new_struct.into())
            } else if field.is_primitive() {
                let type_ = if let Some(logical_type) = basic_info.logical_type() {
                    match logical_type {
                        parquet::basic::LogicalType::String => "String".to_string(),
                        parquet::basic::LogicalType::Integer {
                            bit_width,
                            is_signed,
                        } => match (bit_width, is_signed) {
                            (8, true) => "i8",
                            (8, false) => "u8",
                            (16, true) => "i16",
                            (16, false) => "u16",
                            (32, true) => "i32",
                            (32, false) => "u32",
                            (64, true) => "i64",
                            (64, false) => "u64",
                            _ => panic!("Unsupported int type"),
                        }
                        .to_string(),
                        parquet::basic::LogicalType::Float16 => "f16".to_string(),
                        _ => panic!("Unsupported logical type"),
                    }
                } else {
                    match field.get_physical_type() {
                        parquet::basic::Type::BOOLEAN => "bool",
                        parquet::basic::Type::INT32 => "i32",
                        parquet::basic::Type::INT64 => "i64",
                        parquet::basic::Type::INT96 => "i96",
                        parquet::basic::Type::FLOAT => "f32",
                        parquet::basic::Type::DOUBLE => "f64",
                        parquet::basic::Type::BYTE_ARRAY => "Vec<u8>",
                        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => "Vec<u8>",
                        _ => panic!("Unsupported physical type"),
                    }
                    .to_string()
                };
                ParsedField::new(basic_info.name().into(), type_)
            } else {
                panic!("Unsupported type");
            };
            struct_.add_field(parsed_field);
        }
        out.push(struct_);
        out
    }
}


pub fn from_file<P: AsRef<Path>>(file_name: P) -> Result<()> {
    let test_parquet = SerializedFileReader::new(std::fs::File::open(file_name.as_ref())?)
        .with_context(|| format!("Parquet error: {:?}", file_name.as_ref()))?;
    let schema = test_parquet.metadata().file_metadata().schema_descr();
    let name = capitalize_first_letter(schema.name());
    let done = ParsedStruct::create(name, schema.root_schema().get_fields());
    println!("{}{}", "use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};", ParsedStruct::generate_rust(done.as_slice()));
    Ok(())
}

fn capitalize_first_letter(type_: &str) -> String {
    type_.chars().next().unwrap().to_uppercase().to_string() + &type_[1..]
}
