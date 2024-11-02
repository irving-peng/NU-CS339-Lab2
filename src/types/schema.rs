use crate::types::field::Field;
use core::ops::Deref;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int,
    Float,
    Text,
    Invalid,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "bool"),
            DataType::Int => write!(f, "int"),
            DataType::Float => write!(f, "float"),
            DataType::Text => write!(f, "varchar"),
            DataType::Invalid => write!(f, "invalid"),
        }
    }
}

impl DataType {
    pub fn from_string(data_type: &str) -> DataType {
        match data_type {
            "Bool" => DataType::Bool,
            "Int" => DataType::Int,
            "Float" => DataType::Float,
            "Text" => DataType::Text,
            "Invalid" => DataType::Invalid,
            "Null" => DataType::Invalid,
            _ => panic!("Unknown data type"),
        }
    }

    // not for use with strings
    pub fn length_bytes(&self) -> u16 {
        match self {
            DataType::Bool => 1,
            DataType::Int => 4,
            DataType::Float => 4,
            DataType::Text => 0,
            DataType::Invalid => 0,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    /// Column name. Can't be empty.
    name: String,
    /// Column datatype.
    data_type: DataType,
    /// Whether the column allows null values. Not legal for primary keys.
    nullable: bool,
    /// The column's default value. If None, the user must specify an explicit
    /// value. Must match the column datatype. Nullable columns require a
    /// default (often Null), and Null is only a valid default when nullable.,
    default: Option<Field>,
    /// 0 for varchar / bound of MAX_STRING_LENGTH
    max_str_len: u16,
    /// For fixed length fields: The offset in bytes of the field from the start of the field data
    /// For variable length fields: The index of the offset, rather than the offset itself.
    ///
    /// See `[crate::Row::to_bytes()]` for more detail about the data layout.
    stored_offset: u16,
}

impl Column {
    pub fn new(
        column_name: &str,
        dt: DataType,
        nullable: bool,
        default: Option<Field>,
        max_str_chars: Option<u16>,
    ) -> Column {
        Column {
            name: column_name.to_string(),
            data_type: dt,
            nullable,
            default: match default {
                Some(expr) => Some(expr),
                None if nullable => Some(Field::Null),
                None => None,
            },
            max_str_len: max_str_chars.unwrap_or(0),
            stored_offset: 0,
        }
    }

    pub fn builder() -> ColumnBuilder {
        ColumnBuilder::new()
    }

    pub fn to_string(&self) -> String {
        let base = format!("{}:{}", self.name, self.data_type.to_string());
        if self.data_type == DataType::Text {
            format!("{}({})", base, self.max_str_len)
        } else {
            base
        }
    }

    pub fn set_data_type(&mut self, data_type: DataType) {
        self.data_type = data_type;
    }

    pub fn get_data_type(&self) -> DataType {
        self.data_type
    }

    pub fn set_name(&mut self, column_name: &str) {
        self.name = column_name.to_string();
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn default(&self) -> Option<&Field> {
        self.default.as_ref()
    }

    pub fn length_bytes(&self) -> u16 {
        self.data_type.length_bytes() + self.max_str_len
    }

    pub fn stored_offset(&self) -> u16 {
        self.stored_offset
    }

    pub fn get_max_str_len(&self) -> u16 {
        self.max_str_len
    }
}

pub struct ColumnBuilder {
    name: Option<String>,
    data_type: Option<DataType>,
    nullable: Option<bool>,
    default: Option<Field>,
    max_str_len: Option<u16>,
}

impl ColumnBuilder {
    fn new() -> Self {
        Self {
            name: None,
            data_type: None,
            nullable: None,
            default: None,
            max_str_len: None,
        }
    }

    pub fn name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn data_type(mut self, data_type: DataType) -> Self {
        self.data_type = Some(data_type);
        self
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = Some(nullable);
        self
    }

    pub fn default(mut self, default: Field) -> Self {
        assert!(self.data_type.is_some());
        assert_eq!(default.get_type(), self.data_type.unwrap());
        self.default = Some(default);
        self
    }

    pub fn max_str_len(mut self, max_str_len: u16) -> Self {
        self.max_str_len = Some(max_str_len);
        self
    }

    pub fn build(self) -> Column {
        let nullable = self.nullable.unwrap_or(false);
        Column {
            name: self.name.expect("name must be specified before building."),
            data_type: self
                .data_type
                .expect("data_type must be specified before building."),
            nullable,
            default: match self.default {
                Some(expr) => Some(expr),
                None if nullable => Some(Field::Null),
                None => None,
            },
            max_str_len: self.max_str_len.unwrap_or(0),
            stored_offset: 0,
        }
    }
}

impl From<DataType> for Column {
    fn from(dt: DataType) -> Column {
        Column {
            name: "".to_string(),
            data_type: dt,
            nullable: false,
            default: None,
            max_str_len: 0,
            stored_offset: 0,
        }
    }
}

impl From<(DataType, u16)> for Column {
    fn from((dt, str_len): (DataType, u16)) -> Column {
        Column {
            name: "".to_string(),
            data_type: dt,
            nullable: false,
            default: None,
            max_str_len: str_len,
            stored_offset: 0,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    /// The name of the table
    name: String,
    /// The number of bytes of the fixed fields serialized in memory
    fixed_field_size_bytes: u16,
    /// The column definitions of the table
    columns: Vec<Column>,
}

impl Table {
    pub fn new(table_name: &str) -> Table {
        Table {
            name: table_name.to_string(),
            fixed_field_size_bytes: 0,
            columns: Vec::new(),
        }
    }

    pub fn builder() -> TableBuilder {
        TableBuilder::default()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_name(mut self, table_name: &str) {
        self.name = table_name.to_string();
    }

    pub fn add_column(&mut self, column: &Column) {
        let data_type = column.get_data_type();
        let mut to_push = column.clone();

        if data_type == DataType::Text {
            to_push.stored_offset = self.variable_length_fields() as u16;
            self.columns.push(to_push);
        } else {
            // fixed-length field
            to_push.stored_offset = self.fixed_field_size_bytes;
            self.columns.push(to_push);
            self.fixed_field_size_bytes += data_type.length_bytes();
        }
    }
    pub fn with_columns(&mut self, columns: Vec<Column>) {
        for column in columns {
            self.add_column(&column);
        }
    }

    pub fn get_column(&self, index: usize) -> &Column {
        &self.columns[index]
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn to_string(&self) -> String {
        let mut result = format!("{}(", self.name);
        if self.columns.is_empty() {
            return result + ")";
        }

        result.push_str(&self.columns[0].to_string());

        for i in 1..self.columns.len() {
            result.push_str(", ");
            result.push_str(&self.columns[i].to_string());
        }
        result + ")"
    }

    pub fn col_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column_name(&self, index: usize) -> String {
        self.columns[index].get_name()
    }

    pub fn get_field_type(&self, index: usize) -> DataType {
        self.columns[index].get_data_type()
    }

    // if a field exists return its offset in the schema
    // otherwise return None
    pub fn field_name_to_index(&self, field_name: Option<&String>) -> Option<usize> {
        let f = field_name?;

        for (i, column) in self.columns.iter().enumerate() {
            if column.get_name() == *f {
                return Some(i);
            }
        }
        None
    }

    // max possible size for tuple
    pub fn size(&self) -> u16 {
        let mut size = 0;
        for column in &self.columns {
            size += column.length_bytes();
        }
        size
    }

    pub fn fixed_field_size_bytes(&self) -> u16 {
        self.fixed_field_size_bytes
    }

    // return the count of variable length fields.
    pub fn variable_length_fields(&self) -> usize {
        self.columns
            .iter()
            .filter(|&col| col.get_data_type() == DataType::Text)
            .count()
    }

    pub fn merge(d1: &Table, d2: &Table) -> Table {
        let mut schema = Table::new("");
        schema.columns.append(&mut d1.columns.clone());
        schema.columns.append(&mut d2.columns.clone());

        schema.fixed_field_size_bytes = 0;
        for i in 0..schema.col_count() {
            if schema.columns[i].data_type != DataType::Text {
                schema.columns[i].stored_offset = schema.fixed_field_size_bytes;
                schema.fixed_field_size_bytes += schema.columns[i].data_type.length_bytes();
            }
        }
        schema
    }
}

// set up anonymous columns by type.
impl From<DataType> for Table {
    fn from(dt: DataType) -> Table {
        let mut schema = Table::new("");
        schema.add_column(&Column::from(dt));
        schema
    }
}

impl From<Vec<DataType>> for Table {
    fn from(dt: Vec<DataType>) -> Table {
        let mut schema = Table::new("");
        for d in dt {
            schema.add_column(&Column::from(d));
        }
        schema
    }
}

impl From<(Table, Table)> for Table {
    fn from((schema1, schema2): (Table, Table)) -> Table {
        let mut dst = Table::new("");
        for col in schema1.columns.iter() {
            dst.add_column(col);
        }

        for col in schema2.columns.iter() {
            dst.add_column(col);
        }

        dst
    }
}

impl Deref for Table {
    type Target = Vec<Column>;

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

#[derive(Default)]
pub struct TableBuilder {
    name: Option<String>,
    columns: Vec<Column>,
}

impl TableBuilder {
    pub fn name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn column(
        &mut self,
        column_name: &str,
        dt: DataType,
        nullable: bool,
        default: Option<Field>,
        max_str_chars: Option<u16>,
    ) -> &mut Self {
        self.columns.push(Column::new(
            column_name,
            dt,
            nullable,
            default,
            max_str_chars,
        ));
        self
    }

    pub fn column_from_definition(&mut self, column_definition: Column) -> &mut Self {
        self.columns.push(column_definition);
        self
    }

    pub fn columns(&mut self, columns: Vec<Column>) -> &mut Self {
        self.columns.extend(columns);
        self
    }

    pub fn build(&mut self) -> Table {
        let name = self
            .name
            .clone()
            .expect("Cannot build a Table without a `name`.");
        let mut table_definition = Table::new(&name);
        self.columns
            .iter()
            .for_each(|column| table_definition.add_column(column));
        table_definition
    }

    pub fn build_with_handle(&mut self) -> Arc<Table> {
        Arc::new(self.build())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::utility;
    use rand::Rng;

    use super::*;

    #[test]
    pub fn test_column_declaration() {
        let mut test1 = Column::builder()
            .name("column1".to_string())
            .data_type(DataType::Int)
            .build();
        let mut test2 = Column::builder()
            .name("column2".to_string())
            .data_type(DataType::Text)
            .max_str_len(7)
            .build();

        assert_eq!(test1.to_string(), "column1:int");
        assert_eq!(test2.to_string(), "column2:varchar(7)");

        // test updating the fields
        test1.set_data_type(DataType::Bool);
        test2.set_name("c2");

        assert_eq!(test1.to_string(), "column1:bool");
        assert_eq!(test2.to_string(), "c2:varchar(7)");
    }

    #[test]
    pub fn test_get_type() {
        let td = utility::create_table_definition(1, "d1").clone();
        assert_eq!(td.get_field_type(0), DataType::Int);
    }

    #[test]
    pub fn test_column_size() {
        let schema = Table::builder()
            .name("test_table")
            .column("column1", DataType::Int, false, None, None)
            .column("column2", DataType::Text, false, None, Some(10))
            .column("column3", DataType::Float, false, None, None)
            .column("column4", DataType::Bool, false, None, None)
            .build();

        assert_eq!(schema.get_column(0).length_bytes(), 4);
        assert_eq!(schema.get_column(1).length_bytes(), 10);
        assert_eq!(schema.get_column(2).length_bytes(), 4);
        assert_eq!(schema.get_column(3).length_bytes(), 1);
        assert_eq!(schema.size(), 19);
    }

    #[test]
    pub fn test_field_count() {
        let schema = Table::builder()
            .name("test_table")
            .column("column1", DataType::Int, false, None, None)
            .column("column2", DataType::Text, false, None, None)
            .column("column3", DataType::Float, false, None, None)
            .build();

        assert_eq!(schema.col_count(), 3);

        let schema = utility::create_table_definition(5, "five");
        assert_eq!(schema.col_count(), 5);
        let mut rng = rand::thread_rng();

        for _i in 0..5 {
            let r: usize = rng.gen_range(0..1000);
            let td = utility::create_table_definition(r, "rand");
            assert_eq!(td.col_count(), r);
        }
    }

    #[test]
    pub fn test_table_iterator() {
        let schema = Table::builder()
            .name("test_table")
            .column("column1", DataType::Int, false, None, None)
            .column("column2", DataType::Text, false, None, Some(10))
            .column("column3", DataType::Float, false, None, None)
            .build();

        let mut count = 0;

        for _col in schema.iter() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    fn check_combined_field_names(d1: &Table, d2: &Table, merged: &Table) -> bool {
        if (d1.col_count() + d2.col_count()) != merged.col_count() {
            return false;
        }

        for i in 0..d1.col_count() {
            if d1.get_column_name(i) != merged.get_column_name(i) {
                return false;
            }
        }

        for i in 0..d2.col_count() {
            if d2.get_column_name(i) != merged.get_column_name(i + d1.col_count()) {
                return false;
            }
        }
        return true;
    }

    #[test]
    pub fn test_name_to_idx() {
        let lens: [usize; 3] = [1, 2, 1000];
        let prefix: String = "test".to_string();
        let foo: String = "foo".to_string();

        for len in lens.iter() {
            let td = utility::create_table_definition(*len, &prefix);
            for i in 0..*len {
                let name = format!("{}{}", prefix, i);
                let idx = td.field_name_to_index(Some(&name)).unwrap();
                assert_eq!(idx, i);
            }

            assert_eq!(td.field_name_to_index(Some(&foo)), None);

            if td.field_name_to_index(None) != None {
                panic!("None is not a valid field name!");
            }

            let td = utility::create_table_definition_by_data_type(*len, DataType::Int);
            assert_eq!(td.field_name_to_index(Some(&prefix)), None);
        }

        let td = utility::create_table_definition(3, "test");
        assert_eq!(td.to_string(), "test(test0:int, test1:int, test2:int)");
    }

    #[test]
    pub fn test_combine_schemas() {
        let td1 = utility::create_table_definition(1, "d1");
        let td2 = utility::create_table_definition(2, "d2");
        let td3 = Table::merge(&td1, &td2);

        assert_eq!(td3.col_count(), 3);
        let i_type = DataType::from_string("Int");

        assert_eq!(td3.size(), 3 * i_type.length_bytes());

        for i in 0..3 {
            assert_eq!(td3.get_field_type(i), DataType::Int);
        }

        assert!(check_combined_field_names(&td1, &td2, &td3));

        // check td2 + td1
        let td3 = Table::merge(&td2, &td1);
        assert_eq!(td3.col_count(), 3);
        assert_eq!(td3.size(), 3 * DataType::Int.length_bytes());
        for i in 0..3 {
            assert_eq!(td3.get_field_type(i), DataType::Int);
        }

        assert!(check_combined_field_names(&td2, &td1, &td3));

        // test td2 + td2
        let td3 = Table::merge(&td2, &td2);
        assert_eq!(td3.col_count(), 4);
        assert_eq!(td3.size(), 4 * DataType::Int.length_bytes());
        for i in 0..4 {
            assert_eq!(td3.get_field_type(i), DataType::Int);
        }
        assert!(check_combined_field_names(&td2, &td2, &td3));
    }

    #[test]
    pub fn test_empty_schema() {
        let td = Table::new("empty");
        assert_eq!(td.col_count(), 0);
        assert_eq!(td.size(), 0);
        assert_eq!(td.to_string(), "empty()");
    }

    #[test]
    pub fn test_equals() {
        let single_int1 = utility::create_table_definition_by_data_type(1, DataType::Int);
        let single_int2 = utility::create_table_definition_by_data_type(1, DataType::Int);

        let type_vec = vec![DataType::Int, DataType::Text];
        let int_string1 = Table::from(type_vec.clone());
        let int_string2 = Table::from(type_vec.clone());

        assert_eq!(*single_int1, *single_int2);
        assert_eq!(int_string1, int_string2);

        assert_ne!(*single_int1, *Table::from(vec![DataType::Bool]));
        assert_ne!(*single_int1, *int_string1);
        assert_ne!(*single_int1, *int_string2);
        assert_ne!(*single_int2, *int_string2);
    }

    #[test]
    pub fn test_storage_offset() {
        let schema = Table::builder()
            .name("test_table")
            .column("column1", DataType::Int, false, None, None)
            .column("column2", DataType::Text, false, None, Some(30))
            .column("column3", DataType::Float, false, None, None)
            .column("column4", DataType::Text, false, None, Some(20))
            .column("column5", DataType::Text, false, None, Some(10))
            .column("column6", DataType::Bool, false, None, None)
            .column("column7", DataType::Int, false, None, None)
            .build();

        assert_eq!(schema.get_column(0).stored_offset, 0);
        assert_eq!(schema.get_column(1).stored_offset, 0);
        assert_eq!(schema.get_column(2).stored_offset, 4);
        assert_eq!(schema.get_column(3).stored_offset, 1);
        assert_eq!(schema.get_column(4).stored_offset, 2);
        assert_eq!(schema.get_column(5).stored_offset, 8);
        assert_eq!(schema.get_column(6).stored_offset, 9);
    }
}
