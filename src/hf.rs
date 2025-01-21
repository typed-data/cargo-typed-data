use std::path::PathBuf;
use anyhow::Result;

use hf_hub::{api::sync::Api, Repo, RepoType};
use std::collections::HashMap;
use once_cell::sync::Lazy;

/// define coordinates for datasets on hugging face hub.
#[derive(Clone)]
pub struct DatasetCoords {
    name: String,
    revision: String,
    file: String,
}

/// Known dataset coordinates.
static DATASET_COORDS: Lazy<HashMap<&str, DatasetCoords>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert("cifar10", DatasetCoords {
        name: "cifar10".to_string(),
        revision: "refs/convert/parquet".to_string(),
        file: "plain_text/test/0000.parquet".to_string(),
    });
    m
});

/// Download the representative Parquet file for a dataset from the Hugging Face Hub.
pub fn from_hf(dataset: &str) -> Result<(PathBuf, DatasetCoords)> {
    let coords = DATASET_COORDS.get(dataset).ok_or_else(|| anyhow::anyhow!("Dataset not found"))?;
    let api = Api::new()?;
    let repo = Repo::with_revision(
        coords.name.clone(),
        RepoType::Dataset,
        coords.revision.clone(),
    );
    let repo = api.repo(repo);
    let test_parquet_filename = repo
        .get(&coords.file)?;
    Ok((test_parquet_filename, (*coords).clone()))
}