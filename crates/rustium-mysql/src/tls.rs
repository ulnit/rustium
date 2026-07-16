use std::{collections::HashSet, fs, io::Cursor, path::PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jks::KeyStore as JksKeyStore;
use mysql_async::{ClientIdentity, SslOpts};
use p12_keystore::{KeyStore as Pkcs12KeyStore, KeyStoreEntry as Pkcs12Entry, Pkcs12ImportPolicy};
use rustium_config::MySqlSourceConfig;
use rustium_core::{Error, Result};

const JKS_MAGIC: [u8; 4] = [0xfe, 0xed, 0xfe, 0xed];

pub(crate) fn ssl_options(
    config: &MySqlSourceConfig,
    skip_domain_validation: bool,
    accept_invalid_certs: bool,
) -> Result<SslOpts> {
    let mut options = SslOpts::default();
    if let Some(ca) = &config.ssl_ca {
        options = options.with_root_certs(vec![PathBuf::from(ca).into()]);
    } else if let Some(truststore) = &config.ssl_truststore {
        let certificates = load_truststore(
            truststore,
            config
                .ssl_truststore_password
                .as_deref()
                .unwrap_or_default(),
        )?;
        options =
            options.with_root_certs(certificates.into_iter().map(Into::into).collect::<Vec<_>>());
    }

    if let (Some(cert), Some(key)) = (&config.ssl_cert, &config.ssl_key) {
        options = options.with_client_identity(Some(ClientIdentity::new(
            PathBuf::from(cert).into(),
            PathBuf::from(key).into(),
        )));
    } else if let Some(keystore) = &config.ssl_keystore {
        let (certificate_chain, private_key) = load_keystore(
            keystore,
            config.ssl_keystore_password.as_deref().unwrap_or_default(),
        )?;
        options = options.with_client_identity(Some(ClientIdentity::new(
            certificate_chain.into(),
            private_key.into(),
        )));
    }

    Ok(options
        .with_danger_skip_domain_validation(skip_domain_validation)
        .with_danger_accept_invalid_certs(accept_invalid_certs))
}

fn load_truststore(path: &str, password: &str) -> Result<Vec<Vec<u8>>> {
    let bytes = read_store(path)?;
    let mut certificates = Vec::new();
    let mut seen = HashSet::new();
    if bytes.starts_with(&JKS_MAGIC) {
        let key_store = load_jks(path, &bytes, password)?;
        for alias in key_store.aliases() {
            if key_store.is_trusted_certificate_entry(&alias) {
                let entry = key_store
                    .get_trusted_certificate_entry(&alias)
                    .map_err(|error| store_error(path, error))?;
                push_unique_certificate(&mut certificates, &mut seen, entry.certificate.content);
            } else if key_store.is_private_key_entry(&alias) {
                let entry = key_store
                    .get_raw_private_key_entry(&alias)
                    .map_err(|error| store_error(path, error))?;
                for certificate in entry.certificate_chain {
                    push_unique_certificate(&mut certificates, &mut seen, certificate.content);
                }
            }
        }
    } else if is_pkcs12(&bytes) {
        let key_store = load_pkcs12(path, &bytes, password, Pkcs12ImportPolicy::Raw)?;
        for (_, entry) in key_store.entries() {
            match entry {
                Pkcs12Entry::Certificate(certificate) => push_unique_certificate(
                    &mut certificates,
                    &mut seen,
                    certificate.as_der().to_vec(),
                ),
                Pkcs12Entry::PrivateKeyChain(chain) => {
                    for certificate in chain.certs() {
                        push_unique_certificate(
                            &mut certificates,
                            &mut seen,
                            certificate.as_der().to_vec(),
                        );
                    }
                }
                Pkcs12Entry::Secret(_) => {}
            }
        }
    } else {
        return Err(unsupported_store_format(path));
    }
    if certificates.is_empty() {
        return Err(Error::Configuration(format!(
            "MySQL TLS truststore {path:?} contains no certificates"
        )));
    }
    Ok(certificates)
}

fn load_keystore(path: &str, password: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let bytes = read_store(path)?;
    if bytes.starts_with(&JKS_MAGIC) {
        load_jks_keystore(path, &bytes, password)
    } else if is_pkcs12(&bytes) {
        load_pkcs12_keystore(path, &bytes, password)
    } else {
        Err(unsupported_store_format(path))
    }
}

fn load_jks_keystore(path: &str, bytes: &[u8], password: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let key_store = load_jks(path, bytes, password)?;
    let aliases = key_store
        .aliases()
        .into_iter()
        .filter(|alias| key_store.is_private_key_entry(alias))
        .collect::<Vec<_>>();
    let [alias] = aliases.as_slice() else {
        return Err(Error::Configuration(format!(
            "MySQL TLS keystore {path:?} must contain exactly one private-key entry"
        )));
    };
    let entry = key_store
        .get_private_key_entry(alias, password.as_bytes())
        .map_err(|error| store_error(path, error))?;
    if entry.certificate_chain.is_empty() {
        return Err(Error::Configuration(format!(
            "MySQL TLS keystore {path:?} private-key entry has no certificate chain"
        )));
    }

    let mut certificate_chain = Vec::new();
    for certificate in entry.certificate_chain {
        append_pem(&mut certificate_chain, "CERTIFICATE", &certificate.content);
    }
    let private_key = pem_block("PRIVATE KEY", &entry.private_key);
    Ok((certificate_chain, private_key))
}

fn load_pkcs12_keystore(path: &str, bytes: &[u8], password: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let key_store = load_pkcs12(path, bytes, password, Pkcs12ImportPolicy::Strict)?;
    let key_chains = key_store
        .entries()
        .filter_map(|(_, entry)| match entry {
            Pkcs12Entry::PrivateKeyChain(chain) => Some(chain),
            Pkcs12Entry::Certificate(_) | Pkcs12Entry::Secret(_) => None,
        })
        .collect::<Vec<_>>();
    let [key_chain] = key_chains.as_slice() else {
        return Err(Error::Configuration(format!(
            "MySQL TLS keystore {path:?} must contain exactly one private-key entry"
        )));
    };
    if key_chain.certs().is_empty() {
        return Err(Error::Configuration(format!(
            "MySQL TLS keystore {path:?} private-key entry has no certificate chain"
        )));
    }

    let mut certificate_chain = Vec::new();
    for certificate in key_chain.certs() {
        append_pem(&mut certificate_chain, "CERTIFICATE", certificate.as_der());
    }
    let private_key = pem_block("PRIVATE KEY", key_chain.key().as_der());
    Ok((certificate_chain, private_key))
}

fn read_store(path: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path).map_err(|error| {
        Error::Configuration(format!("could not read MySQL TLS store {path:?}: {error}"))
    })?;
    if bytes.is_empty() {
        return Err(Error::Configuration(format!(
            "MySQL TLS store {path:?} is empty"
        )));
    }
    Ok(bytes)
}

fn load_jks(path: &str, bytes: &[u8], password: &str) -> Result<JksKeyStore> {
    let mut key_store = JksKeyStore::new();
    key_store
        .load(Cursor::new(bytes), password.as_bytes())
        .map_err(|error| store_error(path, error))?;
    Ok(key_store)
}

fn load_pkcs12(
    path: &str,
    bytes: &[u8],
    password: &str,
    policy: Pkcs12ImportPolicy,
) -> Result<Pkcs12KeyStore> {
    Pkcs12KeyStore::from_pkcs12(bytes, password, policy).map_err(|error| store_error(path, error))
}

fn is_pkcs12(bytes: &[u8]) -> bool {
    bytes.first() == Some(&0x30)
}

fn unsupported_store_format(path: &str) -> Error {
    Error::Configuration(format!(
        "unsupported MySQL TLS store format for {path:?}; expected JKS or PKCS#12"
    ))
}

fn push_unique_certificate(
    certificates: &mut Vec<Vec<u8>>,
    seen: &mut HashSet<Vec<u8>>,
    certificate: Vec<u8>,
) {
    if seen.insert(certificate.clone()) {
        certificates.push(certificate);
    }
}

fn store_error(path: &str, error: impl std::fmt::Display) -> Error {
    Error::Configuration(format!(
        "could not decode MySQL TLS store {path:?}: {error}"
    ))
}

fn pem_block(label: &str, bytes: &[u8]) -> Vec<u8> {
    let encoded = BASE64.encode(bytes);
    let mut pem = Vec::with_capacity(encoded.len() + label.len() * 2 + 32);
    pem.extend_from_slice(format!("-----BEGIN {label}-----\n").as_bytes());
    for chunk in encoded.as_bytes().chunks(64) {
        pem.extend_from_slice(chunk);
        pem.push(b'\n');
    }
    pem.extend_from_slice(format!("-----END {label}-----\n").as_bytes());
    pem
}

fn append_pem(output: &mut Vec<u8>, label: &str, bytes: &[u8]) {
    output.extend_from_slice(&pem_block(label, bytes));
}

#[cfg(test)]
mod tests {
    use super::*;
    use jks::{Certificate, PrivateKeyEntry, TrustedCertificateEntry};
    use p12_keystore::{
        Certificate as Pkcs12Certificate, KeyStoreEntry, PrivateKey, PrivateKeyChain,
    };
    use std::time::SystemTime;
    use tempfile::tempdir;

    const PASSWORD: &str = "changeit";
    const PRIVATE_KEY_DER: &str =
        "MC4CAQAwBQYDK2VwBCIEIJdc1qHD3QcFsrziY3HgJb3WACC8/IpfVpNgetPQIZrG";
    const CERTIFICATE_DER: &str = "MIIBWDCCAQqgAwIBAgIUJHqHxlYUeJLW/OvjdnQXBwy/eWswBQYDK2VwMBYxFDASBgNVBAMMC2V4YW1wbGUuY29tMB4XDTI2MDYxOTA2MjA1OFoXDTI4MDUxOTA2MjA1OFowFjEUMBIGA1UEAwwLZXhhbXBsZS5jb20wKjAFBgMrZXADIQBZeSuQJmHBhe6U7x4GDBUMK4INE8VxqP311K/ejllcnaNqMGgwCQYDVR0TBAIwADAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwCwYDVR0PBAQDAgOIMBMGA1UdJQQMMAoGCCsGAQUFBwMBMB0GA1UdDgQWBBSw1lXZ6Gnm3DuRtrFmcWOZEYxdqjAFBgMrZXADQQCicT1reAXWy/i58EABJ2n2zNYdeKP1jyvjlUwzm81sZbNfeaqYNjJoYAK1EBiCW0PGfFIuS++1od7w56YgV+EO";

    fn materials() -> (Vec<u8>, Vec<u8>) {
        (
            BASE64.decode(CERTIFICATE_DER).unwrap(),
            BASE64.decode(PRIVATE_KEY_DER).unwrap(),
        )
    }

    fn jks_private_key_entry(certificate: &[u8], private_key: &[u8]) -> PrivateKeyEntry {
        PrivateKeyEntry {
            creation_time: SystemTime::UNIX_EPOCH,
            private_key: private_key.to_vec(),
            certificate_chain: vec![Certificate {
                cert_type: "X.509".into(),
                content: certificate.to_vec(),
            }],
        }
    }

    #[test]
    fn converts_jks_keystore_and_truststore_to_pem_materials() {
        let directory = tempdir().unwrap();
        let keystore_path = directory.path().join("client.jks");
        let truststore_path = directory.path().join("trust.jks");
        let password = PASSWORD.as_bytes();
        let (certificate, private_key) = materials();

        let mut keystore = JksKeyStore::new();
        keystore
            .set_private_key_entry(
                "client",
                jks_private_key_entry(&certificate, &private_key),
                password,
            )
            .unwrap();
        keystore
            .store(fs::File::create(&keystore_path).unwrap(), password)
            .unwrap();

        let mut truststore = JksKeyStore::new();
        truststore
            .set_trusted_certificate_entry(
                "ca",
                TrustedCertificateEntry {
                    creation_time: SystemTime::UNIX_EPOCH,
                    certificate: Certificate {
                        cert_type: "X.509".into(),
                        content: certificate.clone(),
                    },
                },
            )
            .unwrap();
        truststore
            .store(fs::File::create(&truststore_path).unwrap(), password)
            .unwrap();

        let (chain, key) = load_keystore(keystore_path.to_str().unwrap(), PASSWORD).unwrap();
        assert!(
            String::from_utf8(chain)
                .unwrap()
                .contains("BEGIN CERTIFICATE")
        );
        assert!(
            String::from_utf8(key)
                .unwrap()
                .contains("BEGIN PRIVATE KEY")
        );
        assert_eq!(
            load_truststore(truststore_path.to_str().unwrap(), PASSWORD).unwrap(),
            vec![certificate]
        );
    }

    #[test]
    fn converts_modern_pkcs12_keystore_and_truststore_to_pem_materials() {
        let directory = tempdir().unwrap();
        let keystore_path = directory.path().join("client.p12");
        let truststore_path = directory.path().join("trust.p12");
        let (certificate_der, private_key_der) = materials();
        let certificate = Pkcs12Certificate::from_der(&certificate_der).unwrap();
        let private_key = PrivateKey::from_der(&private_key_der).unwrap();

        let mut keystore = Pkcs12KeyStore::new();
        keystore.add_entry(
            "client",
            KeyStoreEntry::PrivateKeyChain(PrivateKeyChain::new(
                "client",
                private_key,
                [certificate.clone()],
            )),
        );
        fs::write(&keystore_path, keystore.writer(PASSWORD).write().unwrap()).unwrap();

        let mut truststore = Pkcs12KeyStore::new();
        truststore.add_entry("ca", KeyStoreEntry::Certificate(certificate));
        fs::write(
            &truststore_path,
            truststore.writer(PASSWORD).write().unwrap(),
        )
        .unwrap();

        let (chain, key) = load_keystore(keystore_path.to_str().unwrap(), PASSWORD).unwrap();
        assert!(
            String::from_utf8(chain)
                .unwrap()
                .contains("BEGIN CERTIFICATE")
        );
        assert!(
            String::from_utf8(key)
                .unwrap()
                .contains("BEGIN PRIVATE KEY")
        );
        assert_eq!(
            load_truststore(truststore_path.to_str().unwrap(), PASSWORD).unwrap(),
            vec![certificate_der]
        );
        assert!(load_keystore(keystore_path.to_str().unwrap(), "wrong-password").is_err());
    }

    #[test]
    fn rejects_empty_and_multi_key_stores() {
        let directory = tempdir().unwrap();
        let empty_path = directory.path().join("empty.jks");
        let multi_key_path = directory.path().join("multi.jks");
        let (certificate, private_key) = materials();

        let empty_store = JksKeyStore::new();
        empty_store
            .store(fs::File::create(&empty_path).unwrap(), PASSWORD.as_bytes())
            .unwrap();
        let truststore_error = load_truststore(empty_path.to_str().unwrap(), PASSWORD).unwrap_err();
        assert!(
            truststore_error
                .to_string()
                .contains("contains no certificates")
        );
        let keystore_error = load_keystore(empty_path.to_str().unwrap(), PASSWORD).unwrap_err();
        assert!(
            keystore_error
                .to_string()
                .contains("exactly one private-key")
        );

        let mut multi_key_store = JksKeyStore::new();
        for alias in ["client-one", "client-two"] {
            multi_key_store
                .set_private_key_entry(
                    alias,
                    jks_private_key_entry(&certificate, &private_key),
                    PASSWORD.as_bytes(),
                )
                .unwrap();
        }
        multi_key_store
            .store(
                fs::File::create(&multi_key_path).unwrap(),
                PASSWORD.as_bytes(),
            )
            .unwrap();
        let error = load_keystore(multi_key_path.to_str().unwrap(), PASSWORD).unwrap_err();
        assert!(error.to_string().contains("exactly one private-key"));
    }

    #[test]
    fn rejects_empty_files_and_unknown_store_formats() {
        let directory = tempdir().unwrap();
        let empty_path = directory.path().join("empty.p12");
        let unknown_path = directory.path().join("unknown.store");
        fs::write(&empty_path, []).unwrap();
        fs::write(&unknown_path, b"not-a-store").unwrap();

        let empty_error = load_keystore(empty_path.to_str().unwrap(), PASSWORD).unwrap_err();
        assert!(empty_error.to_string().contains("is empty"));
        let format_error = load_keystore(unknown_path.to_str().unwrap(), PASSWORD).unwrap_err();
        assert!(format_error.to_string().contains("expected JKS or PKCS#12"));
    }
}
