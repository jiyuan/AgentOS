use super::hybrid::{
    hash_embedding, memory_backend_error, metadata_embedding, searchable_record_text,
    stable_hash_u64, vector_json, SemanticIndex, SemanticSearchHit,
};
use super::{memory_sqlite_error, MemoryError, SqliteStore};
use agentos_interfaces::memory::Record;
use agentos_proto::{Namespace, RecordId};
use async_trait::async_trait;
use rusqlite::params;
use std::sync::{Arc, OnceLock};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteVecConfig {
    pub table: Arc<str>,
    pub vector_dimensions: usize,
}

impl Default for SqliteVecConfig {
    fn default() -> Self {
        Self {
            table: Arc::from("memory_records_vec"),
            vector_dimensions: 384,
        }
    }
}

pub struct SqliteVecSemanticIndex {
    store: Arc<SqliteStore>,
    config: SqliteVecConfig,
}

impl SqliteVecSemanticIndex {
    pub fn new(store: Arc<SqliteStore>, config: SqliteVecConfig) -> Result<Self, MemoryError> {
        validate_table_name(&config.table)?;
        if config.vector_dimensions == 0 {
            return Err(memory_backend_error(
                "sqlite_vec vector_dimensions must be greater than 0",
            ));
        }
        let index = Self { store, config };
        index.init_schema()?;
        Ok(index)
    }

    pub fn register_auto_extension() -> Result<(), MemoryError> {
        static REGISTER: OnceLock<Result<(), Arc<str>>> = OnceLock::new();
        match REGISTER.get_or_init(|| {
            register_sqlite_vec_auto_extension().map_err(|err| Arc::from(err.to_string()))
        }) {
            Ok(()) => Ok(()),
            Err(err) => Err(MemoryError::Backend(Arc::clone(err))),
        }
    }

    fn init_schema(&self) -> Result<(), MemoryError> {
        let conn = self.store.memory_conn()?;
        conn.execute_batch(&format!(
            "CREATE VIRTUAL TABLE IF NOT EXISTS {} USING vec0(\
             record_id TEXT, \
             namespace TEXT partition key, \
             embedding float[{}]\
             );",
            self.config.table, self.config.vector_dimensions
        ))
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    fn vector_for_record(&self, record: &Record) -> Vec<f32> {
        metadata_embedding(record).unwrap_or_else(|| {
            hash_embedding(
                &searchable_record_text(record),
                self.config.vector_dimensions,
            )
        })
    }

    fn query_vector(&self, query: &str) -> Vec<f32> {
        hash_embedding(query, self.config.vector_dimensions)
    }
}

#[async_trait]
impl SemanticIndex for SqliteVecSemanticIndex {
    async fn upsert(&self, record: &Record) -> Result<(), MemoryError> {
        let Some(record_id) = &record.id else {
            return Err(memory_backend_error(
                "sqlite_vec upsert requires a stable memory record id",
            ));
        };
        let row_id = vector_row_id(record_id);
        let vector = vector_json(&self.vector_for_record(record));
        let conn = self.store.memory_conn()?;
        conn.execute(
            &format!("DELETE FROM {} WHERE rowid = ?1", self.config.table),
            params![row_id],
        )
        .map_err(memory_sqlite_error)?;
        conn.execute(
            &format!(
                "INSERT INTO {} (rowid, record_id, namespace, embedding) \
                 VALUES (?1, ?2, ?3, ?4)",
                self.config.table
            ),
            params![
                row_id,
                record_id.as_str(),
                record.namespace.as_str(),
                vector
            ],
        )
        .map_err(memory_sqlite_error)?;
        Ok(())
    }

    async fn search(
        &self,
        namespace: &Namespace,
        query: &str,
        limit: usize,
    ) -> Result<Vec<SemanticSearchHit>, MemoryError> {
        if limit == 0 || query.trim().is_empty() {
            return Ok(Vec::new());
        }

        let vector = vector_json(&self.query_vector(query));
        let conn = self.store.memory_conn()?;
        let mut stmt = conn
            .prepare(&format!(
                "SELECT record_id, distance \
                 FROM {} \
                 WHERE embedding MATCH ?1 \
                   AND k = ?2 \
                   AND namespace = ?3 \
                 ORDER BY distance ASC",
                self.config.table
            ))
            .map_err(memory_sqlite_error)?;
        let rows = stmt
            .query_map(params![vector, limit as i64, namespace.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?))
            })
            .map_err(memory_sqlite_error)?;
        let mut hits = Vec::new();
        for row in rows {
            let (record_id, distance) = row.map_err(memory_sqlite_error)?;
            hits.push(SemanticSearchHit {
                record_id: RecordId::new(record_id),
                score: -distance,
            });
        }
        Ok(hits)
    }

    async fn delete(
        &self,
        _namespace: &Namespace,
        record_ids: &[RecordId],
    ) -> Result<(), MemoryError> {
        if record_ids.is_empty() {
            return Ok(());
        }
        let conn = self.store.memory_conn()?;
        for record_id in record_ids {
            conn.execute(
                &format!("DELETE FROM {} WHERE rowid = ?1", self.config.table),
                params![vector_row_id(record_id)],
            )
            .map_err(memory_sqlite_error)?;
        }
        Ok(())
    }
}

fn register_sqlite_vec_auto_extension() -> Result<(), MemoryError> {
    use rusqlite::auto_extension::{register_auto_extension, RawAutoExtension};

    let raw_ext: RawAutoExtension =
        unsafe { std::mem::transmute(::sqlite_vec::sqlite3_vec_init as *const () as usize) };
    unsafe { register_auto_extension(raw_ext) }.map_err(memory_sqlite_error)?;
    Ok(())
}

fn validate_table_name(table: &str) -> Result<(), MemoryError> {
    if table.is_empty()
        || !table
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || table.chars().next().is_some_and(|ch| ch.is_ascii_digit())
    {
        return Err(memory_backend_error(
            "sqlite_vec table must be a non-empty identifier containing only letters, digits, or '_' and must not start with a digit",
        ));
    }
    Ok(())
}

fn vector_row_id(record_id: &RecordId) -> i64 {
    let row_id = (stable_hash_u64(record_id.as_str()) & 0x7fff_ffff_ffff_ffff) as i64;
    if row_id == 0 {
        1
    } else {
        row_id
    }
}
