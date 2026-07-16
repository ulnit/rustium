use std::{fs::OpenOptions, io::ErrorKind, path::Path};

use rustium_core::{Error, Result};

pub(crate) async fn read_and_clear(path: &str) -> Result<Vec<String>> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || read_and_clear_blocking(Path::new(&path)))
        .await
        .map_err(|error| Error::Source(format!("SQL Server signal file task failed: {error}")))?
}

fn read_and_clear_blocking(path: &Path) -> Result<Vec<String>> {
    let mut file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(Error::Source(format!(
                "failed to read SQL Server signal file {:?}: {error}",
                path.display()
            )));
        }
    };
    let mut contents = String::new();
    std::io::Read::read_to_string(&mut file, &mut contents).map_err(|error| {
        Error::Source(format!(
            "failed to read SQL Server signal file {:?}: {error}",
            path.display()
        ))
    })?;
    if contents.is_empty() {
        return Ok(Vec::new());
    }
    file.set_len(0).map_err(|error| {
        Error::Source(format!(
            "failed to clear SQL Server signal file {:?}: {error}",
            path.display()
        ))
    })?;
    file.sync_data().map_err(|error| {
        Error::Source(format!(
            "failed to sync SQL Server signal file {:?}: {error}",
            path.display()
        ))
    })?;
    Ok(contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
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
    }
}
