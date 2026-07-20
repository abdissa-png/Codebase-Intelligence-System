//! Corrupt wal.json must fail closed (no silent empty WAL).

use std::fs;

#[test]
fn durable_wal_open_rejects_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("wal.json");
    fs::write(&wal_path, b"{not valid json").unwrap();
    let err = cis_wal::DurableMutationLog::open(&wal_path).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn runtime_panics_on_corrupt_existing_wal() {
    let dir = tempfile::tempdir().unwrap();
    let cis = dir.path().join(".cis");
    fs::create_dir_all(&cis).unwrap();
    fs::write(cis.join("wal.json"), b"CORRUPT").unwrap();

    std::env::remove_var("CIS_WAL_MEMORY");
    std::env::set_var("CIS_SKIP_WORKSPACE_LOAD", "1");

    let repo = dir.path().to_string_lossy().into_owned();
    let result = std::panic::catch_unwind(|| {
        let _ = cis_core::CisMcpRuntime::new_dev(&repo);
    });
    assert!(
        result.is_err(),
        "opening runtime with corrupt wal.json must panic / fail closed"
    );
}
