use std::fs;
use std::path::Path;

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn every_rust_example_is_documented_with_a_run_command() {
    let root = manifest_dir();
    let examples_dir = root.join("examples");
    let readme = fs::read_to_string(examples_dir.join("README.md"))
        .expect("examples/README.md should be readable");

    let mut examples = fs::read_dir(&examples_dir)
        .expect("examples directory should be readable")
        .map(|entry| entry.expect("example entry should be readable").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .map(|path| {
            path.file_stem()
                .expect("example should have a file stem")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    examples.sort();

    assert!(!examples.is_empty(), "expected at least one Rust example");

    for example in examples {
        let file_name = format!("{example}.rs");
        assert!(
            readme.contains(&format!("`{file_name}`")),
            "examples/README.md should document `{file_name}`"
        );
        assert!(
            readme.contains(&format!("cargo run --example {example}")),
            "examples/README.md should include a run command for `{file_name}`"
        );
    }
}

#[test]
fn verify_script_runs_integration_tests_and_examples() {
    let verify = fs::read_to_string(manifest_dir().join("scripts/verify.sh"))
        .expect("scripts/verify.sh should be readable");

    for required_command in [
        "cargo test --lib",
        "cargo test --tests",
        "cargo test --no-default-features --lib",
        "cargo test --no-default-features --tests",
        "cargo build --examples",
    ] {
        assert!(
            verify.contains(required_command),
            "scripts/verify.sh should include `{required_command}`"
        );
    }
}

#[test]
fn custom_extensions_server_keeps_process_alive_until_shutdown_signal() {
    let source = fs::read_to_string(manifest_dir().join("examples/custom_extensions_example.rs"))
        .expect("custom extensions example should be readable");

    assert!(
        source.contains("tokio::signal::ctrl_c().await?"),
        "custom extensions server should keep running after server.start() until Ctrl+C"
    );
    assert!(
        source.contains("server.stop().await?"),
        "custom extensions server should stop gracefully after receiving Ctrl+C"
    );
}
