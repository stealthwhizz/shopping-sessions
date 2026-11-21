//! Minimal Parquet v1: required scalar columns, optional UTF-8, PLAIN/uncompressed.
//! Keeping this deliberately narrow avoids an Arrow runtime and compression dependencies.
use std::{fs::{self, File}, io::{self, Write}, path::Path};

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 { out.push(value as u8 | 128); value >>= 7; }
    out.push(value as u8);
}

#[derive(Default)]
struct Compact { bytes: Vec<u8>, last: i16 }
impl Compact {
    fn field(&mut self, id: i16, kind: u8) {
        let delta = id - self.last;
        if (1..=15).contains(&delta) { self.bytes.push((delta as u8) << 4 | kind); }
        else { self.bytes.push(kind); varint(&mut self.bytes, ((id << 1) ^ (id >> 15)) as u16 as u64); }
        self.last = id;
    }
    fn int(&mut self, id: i16, value: i64, kind: u8) {
        self.field(id, kind); varint(&mut self.bytes, ((value << 1) ^ (value >> 63)) as u64);
    }
    fn i32(&mut self, id: i16, value: i32) { self.int(id, value as i64, 5); }
    fn i64(&mut self, id: i16, value: i64) { self.int(id, value, 6); }
    fn binary(&mut self, id: i16, value: &[u8]) {
        self.field(id, 8); varint(&mut self.bytes, value.len() as u64); self.bytes.extend_from_slice(value);
    }
    fn list(&mut self, id: i16, kind: u8, len: usize) {
        self.field(id, 9);
        if len < 15 { self.bytes.push((len as u8) << 4 | kind); }
        else { self.bytes.push(0xf0 | kind); varint(&mut self.bytes, len as u64); }
    }
    fn structure(&mut self, id: i16, child: Compact) { self.field(id, 12); self.bytes.extend(child.finish()); }
    fn finish(mut self) -> Vec<u8> { self.bytes.push(0); self.bytes }
}

pub struct Column {
    name: &'static str,
    physical: i32,
    converted: Option<i32>,
    optional: bool,
    data: Vec<u8>,
}
impl Column {
    pub fn utf8_plain(name: &'static str, data: Vec<u8>) -> Self {
        Self { name, physical: 6, converted: Some(0), optional: false, data }
    }
    pub fn integer(name: &'static str, values: impl Iterator<Item=i64> + Clone) -> Self {
        let narrow = values.clone().all(|value| i32::try_from(value).is_ok());
        let mut data = Vec::with_capacity(values.size_hint().0 * if narrow { 4 } else { 8 });
        if narrow {
            for value in values { data.extend_from_slice(&(value as i32).to_le_bytes()); }
        } else {
            for value in values { data.extend_from_slice(&value.to_le_bytes()); }
        }
        Self { name, physical: if narrow { 1 } else { 2 }, converted: None, optional: false, data }
    }
    pub fn date(name: &'static str, values: impl Iterator<Item=i32>) -> Self {
        let mut data = Vec::with_capacity(values.size_hint().0 * 4);
        for value in values { data.extend_from_slice(&value.to_le_bytes()); }
        Self { name, physical: 1, converted: Some(6), optional: false, data }
    }
    pub fn string<'a>(name: &'static str, optional: bool, values: impl Iterator<Item=Option<&'a [u8]>> + Clone) -> Self {
        let mut data = Vec::with_capacity(values.size_hint().0 * 12);
        if optional {
            // DATA_PAGE v1 prefixes the RLE definition-level stream with its byte length.
            data.extend_from_slice(&[0;4]);
            let mut current = false;
            let mut run = 0u64;
            for value in values.clone() {
                let present = value.is_some();
                if run > 0 && present != current {
                    varint(&mut data, run << 1); data.push(u8::from(current)); run = 0;
                }
                current = present; run += 1;
            }
            if run > 0 { varint(&mut data, run << 1); data.push(u8::from(current)); }
            let length = (data.len()-4) as u32;
            data[..4].copy_from_slice(&length.to_le_bytes());
        }
        for value in values {
            if let Some(value) = value {
                data.extend_from_slice(&(value.len() as u32).to_le_bytes());
                data.extend_from_slice(value);
            } else { assert!(optional, "null in required string column"); }
        }
        Self { name, physical: 6, converted: Some(0), optional, data }
    }
}

pub fn write(path: &Path, part: usize, rows: usize, columns: Vec<Column>) -> io::Result<()> {
    let _phase = crate::profile::Span::worker(crate::profile::Phase::ParquetWrite);
    fs::create_dir_all(path)?;
    let mut file = File::create(path.join(format!("part-{part:05}.parquet")))?;
    file.write_all(b"PAR1")?;
    let mut offset = 4i64;
    let mut schemas = Vec::with_capacity(columns.len()+1);
    let mut root = Compact::default();
    root.binary(4, b"schema"); root.i32(5, columns.len() as i32);
    schemas.push(root.finish());
    let mut column_chunks = Vec::new();
    let mut total_size = 0;
    for column in columns {
        let mut schema = Compact::default();
        schema.i32(1, column.physical);
        schema.i32(3, i32::from(column.optional));
        schema.binary(4, column.name.as_bytes());
        if let Some(converted) = column.converted { schema.i32(6, converted); }
        schemas.push(schema.finish());
        if rows == 0 { continue; }
        let size = i32::try_from(column.data.len()).map_err(|_| io::Error::other("Parquet page exceeds 2 GiB"))?;
        let count = i32::try_from(rows).map_err(|_| io::Error::other("Parquet page exceeds i32 rows"))?;
        let mut data_header = Compact::default();
        data_header.i32(1, count); data_header.i32(2, 0); data_header.i32(3, 3); data_header.i32(4, 3);
        let mut page = Compact::default();
        page.i32(1, 0); page.i32(2, size); page.i32(3, size); page.structure(5, data_header);
        let header = page.finish();
        let length = (header.len() + column.data.len()) as i64;
        file.write_all(&header)?; file.write_all(&column.data)?;
        let mut metadata = Compact::default();
        metadata.i32(1, column.physical);
        metadata.list(2, 5, 2); varint(&mut metadata.bytes, 0); varint(&mut metadata.bytes, 6);
        metadata.list(3, 8, 1); varint(&mut metadata.bytes, column.name.len() as u64); metadata.bytes.extend_from_slice(column.name.as_bytes());
        metadata.i32(4, 0); metadata.i64(5, rows as i64); metadata.i64(6, length); metadata.i64(7, length); metadata.i64(9, offset);
        let mut chunk = Compact::default();
        chunk.i64(2, offset); chunk.structure(3, metadata);
        column_chunks.push(chunk.finish());
        offset += length; total_size += length;
    }
    let mut metadata = Compact::default();
    metadata.i32(1, 1);
    metadata.list(2, 12, schemas.len());
    for schema in schemas { metadata.bytes.extend(schema); }
    metadata.i64(3, rows as i64);
    metadata.list(4, 12, usize::from(rows > 0));
    if rows > 0 {
        let mut group = Compact::default();
        group.list(1, 12, column_chunks.len());
        for chunk in column_chunks { group.bytes.extend(chunk); }
        group.i64(2, total_size); group.i64(3, rows as i64);
        metadata.bytes.extend(group.finish());
    }
    metadata.binary(6, b"shopping-sessions");
    let footer = metadata.finish();
    file.write_all(&footer)?;
    file.write_all(&(footer.len() as u32).to_le_bytes())?;
    file.write_all(b"PAR1")?;
    file.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integer_width_selection_never_truncates_signed_values() {
        for values in [
            [i32::MIN as i64, 0, i32::MAX as i64],
            [i32::MIN as i64-1, 0, i32::MAX as i64+1],
        ] {
            let column = Column::integer("values", values.into_iter());
            let decoded: Vec<i64> = match column.physical {
                1 => column.data.chunks_exact(4).map(|b| i32::from_le_bytes(b.try_into().unwrap()) as i64).collect(),
                2 => column.data.chunks_exact(8).map(|b| i64::from_le_bytes(b.try_into().unwrap())).collect(),
                other => panic!("unexpected integer physical type {other}"),
            };
            assert_eq!(decoded, values);
        }
    }
}
