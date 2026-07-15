use std::{
    fs::OpenOptions,
    io::{ErrorKind, Read},
    path::Path,
};

use rustium_core::{Error, Result};

pub(crate) async fn read_and_clear(path: &str) -> Result<Vec<String>> {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || read_and_clear_blocking(Path::new(&path)))
        .await
        .map_err(|error| Error::Source(format!("PostgreSQL signal file task failed: {error}")))?
}

fn read_and_clear_blocking(path: &Path) -> Result<Vec<String>> {
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(signal_file_error(path, error)),
    };
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|error| signal_file_error(path, error))?;
    if contents.is_empty() {
        return Ok(Vec::new());
    }
    file.set_len(0)
        .map_err(|error| signal_file_error(path, error))?;
    file.sync_data()
        .map_err(|error| signal_file_error(path, error))?;
    Ok(contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn signal_file_error(path: &Path, error: std::io::Error) -> Error {
    Error::Source(format!(
        "failed to read PostgreSQL signal file {:?}: {error}",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_json_lines_and_clears_the_signal_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("signals.jsonl");
        std::fs::write(&path, " first \n\nsecond\r\n").unwrap();

        assert_eq!(
            read_and_clear(path.to_str().unwrap()).await.unwrap(),
            ["first", "second"]
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
        assert!(
            read_and_clear(path.to_str().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            read_and_clear(directory.path().join("missing").to_str().unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }
}
