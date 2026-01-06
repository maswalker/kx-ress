use alloy_primitives::{map::B256HashSet, B256};
use itertools::Itertools;
use metrics::Label;
use reth_db::{
    create_db, database_metrics::DatabaseMetrics, mdbx::DatabaseArguments, Database, DatabaseEnv,
    DatabaseError,
};
use reth_db_api::{
    cursor::DbCursorRO,
    transaction::{DbTx, DbTxMut},
};
use reth_primitives::Bytecode;
use std::{path::Path, sync::Arc};
use tracing::*;

mod tables {
    use alloy_primitives::B256;
    use reth_db::{tables, TableSet, TableType, TableViewer};
    use reth_db_api::table::TableInfo;
    use reth_primitives::Bytecode;
    use std::fmt;

    tables! {
        table Bytecodes {
            type Key = B256;
            type Value = Bytecode;
        }
    }
}
use tables::{Bytecodes, Tables};

#[derive(Clone, Debug)]
pub struct RessDatabase {
    database: Arc<DatabaseEnv>,
}

impl RessDatabase {
    pub fn new<P: AsRef<Path>>(path: P) -> eyre::Result<Self> {
        Self::new_with_args(
            path,
            DatabaseArguments::default()
                .with_growth_step(Some(1024 * 1024 * 1024))
                .with_geometry_max_size(Some(64 * 1024 * 1024 * 1024)),
        )
    }

    pub fn new_with_args<P: AsRef<Path>>(path: P, args: DatabaseArguments) -> eyre::Result<Self> {
        let mut database = Arc::new(create_db(path, args)?);
        Arc::get_mut(&mut database).unwrap().create_tables()?;
        Ok(Self { database })
    }

    pub fn bytecode_exists(&self, code_hash: B256) -> Result<bool, DatabaseError> {
        Ok(self.get_bytecode(code_hash)?.is_some())
    }

    pub fn get_bytecode(&self, code_hash: B256) -> Result<Option<Bytecode>, DatabaseError> {
        self.database.tx()?.get::<Bytecodes>(code_hash)
    }

    pub fn insert_bytecode(
        &self,
        code_hash: B256,
        bytecode: Bytecode,
    ) -> Result<(), DatabaseError> {
        let tx_mut = self.database.tx_mut()?;
        tx_mut.put::<Bytecodes>(code_hash, bytecode)?;
        tx_mut.commit()?;
        Ok(())
    }

    pub fn missing_code_hashes(
        &self,
        code_hashes: B256HashSet,
    ) -> Result<B256HashSet, DatabaseError> {
        let mut missing = B256HashSet::default();
        let tx = self.database.tx()?;
        let mut cursor = tx.cursor_read::<Bytecodes>()?;
        for code_hash in code_hashes.into_iter().sorted_unstable() {
            if cursor.seek_exact(code_hash)?.is_none() {
                missing.insert(code_hash);
            }
        }
        Ok(missing)
    }

    #[allow(clippy::type_complexity)]
    fn collect_metrics(&self) -> Result<Vec<(&'static str, f64, Vec<Label>)>, DatabaseError> {
        self.database
            .view(|tx| -> reth_db::mdbx::Result<_> {
                let table = Tables::Bytecodes.name();
                let table_label = Label::new("table", table);
                let table_db = tx.inner.open_db(Some(table))?;
                let stats = tx.inner.db_stat(&table_db)?;

                let page_size = stats.page_size() as usize;
                let leaf_pages = stats.leaf_pages();
                let branch_pages = stats.branch_pages();
                let overflow_pages = stats.overflow_pages();
                let num_pages = leaf_pages + branch_pages + overflow_pages;
                let table_size = page_size * num_pages;
                let entries = stats.entries();

                let metrics = Vec::from([
                    ("db.table_size", table_size as f64, Vec::from([table_label.clone()])),
                    (
                        "db.table_pages",
                        leaf_pages as f64,
                        Vec::from([table_label.clone(), Label::new("type", "leaf")]),
                    ),
                    (
                        "db.table_pages",
                        branch_pages as f64,
                        Vec::from([table_label.clone(), Label::new("type", "branch")]),
                    ),
                    (
                        "db.table_pages",
                        overflow_pages as f64,
                        Vec::from([table_label.clone(), Label::new("type", "overflow")]),
                    ),
                    ("db.table_entries", entries as f64, Vec::from([table_label])),
                ]);
                Ok(metrics)
            })?
            .map_err(|e| DatabaseError::Read(e.into()))
    }
}

impl DatabaseMetrics for RessDatabase {
    fn report_metrics(&self) {
        for (name, value, labels) in self.gauge_metrics() {
            metrics::gauge!(name, labels).set(value);
        }
    }

    fn gauge_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        self.collect_metrics()
            .inspect_err(
                |error| error!(target: "ress::db", ?error, "Error collecting database metrics"),
            )
            .unwrap_or_default()
    }
}
