use std::fs;
use std::path::Path;

fn manifest_path(relative: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn test_pre_commit_config_exists_and_valid() {
    let path = manifest_path(".pre-commit-config.yaml");
    assert!(path.exists(), ".pre-commit-config.yaml must exist");

    let content = fs::read_to_string(&path).expect("Failed to read .pre-commit-config.yaml");
    assert!(!content.trim().is_empty());
    assert!(
        content.contains("gitleaks"),
        "Pre-commit must include Gitleaks hook"
    );
    assert!(
        content.contains("cargo-fmt") || content.contains("cargo fmt"),
        "Pre-commit must include cargo fmt"
    );
    assert!(
        content.contains("cargo-clippy") || content.contains("cargo clippy"),
        "Pre-commit must include cargo clippy"
    );
    assert!(
        content.contains("cargo-test") || content.contains("cargo test"),
        "Pre-commit must include cargo test"
    );
    assert!(
        content.contains("repos:"),
        "Must be a valid pre-commit config with repos block"
    );
}

#[test]
fn test_deny_toml_exists_and_configured() {
    let path = manifest_path("deny.toml");
    assert!(path.exists(), "deny.toml must exist");

    let content = fs::read_to_string(&path).expect("Failed to read deny.toml");
    let parsed: toml::Value = toml::from_str(&content).expect("deny.toml must be valid TOML");

    assert!(parsed.get("advisories").is_some());
    assert!(parsed.get("licenses").is_some());
    assert!(parsed.get("bans").is_some());
    assert!(parsed.get("sources").is_some());

    let licenses = parsed.get("licenses").unwrap();
    let allow_list = licenses
        .get("allow")
        .and_then(|v| v.as_array())
        .expect("licenses.allow must be an array");

    let allowed_strings: Vec<&str> = allow_list.iter().filter_map(|v| v.as_str()).collect();
    assert!(allowed_strings.contains(&"MIT"));
    assert!(allowed_strings.contains(&"Apache-2.0"));
    assert!(allowed_strings.contains(&"Unicode-3.0"));
}

#[test]
fn test_dual_shell_audit_scripts_parity() {
    let bash_script = manifest_path("scripts/audit-and-scan.sh");
    let ps_script = manifest_path("scripts/audit-and-scan.ps1");

    assert!(bash_script.exists(), "scripts/audit-and-scan.sh must exist");
    assert!(ps_script.exists(), "scripts/audit-and-scan.ps1 must exist");

    let bash_content = fs::read_to_string(&bash_script).unwrap();
    let ps_content = fs::read_to_string(&ps_script).unwrap();

    // Check shebang and robustness in bash script
    assert!(
        bash_content.starts_with("#!/usr/bin/env bash"),
        "Bash audit script must have valid shebang"
    );
    assert!(
        bash_content.contains("set -euo pipefail"),
        "Bash audit script must enable strict error handling"
    );

    // Check shared stages
    for required_cmd in &[
        "cargo fmt",
        "cargo clippy",
        "cargo test",
        "cargo audit",
        "cargo deny",
        "gitleaks",
    ] {
        assert!(
            bash_content.contains(required_cmd),
            "Bash audit script missing check: {required_cmd}"
        );
        assert!(
            ps_content.contains(required_cmd),
            "PowerShell audit script missing check: {required_cmd}"
        );
    }
}

#[test]
fn test_readme_documentation_completeness() {
    let readme_path = manifest_path("README.md");
    assert!(readme_path.exists(), "README.md must exist");

    let readme = fs::read_to_string(&readme_path).expect("Failed to read README.md");

    // Must have C4 architecture or Mermaid flow
    assert!(
        readme.contains("```mermaid"),
        "README.md must contain Mermaid architecture diagram"
    );
    assert!(
        readme.contains("stream-replay-rs"),
        "README.md must document stream-replay-rs"
    );

    // Must document CLI flags and presets
    assert!(
        readme.contains("--speed"),
        "README.md must document --speed flag"
    );
    assert!(
        readme.contains("10x"),
        "README.md must document 10x default speed"
    );
    assert!(
        readme.contains("--burst"),
        "README.md must document --burst flag"
    );
    assert!(
        readme.contains("--progress"),
        "README.md must document --progress flag"
    );
    assert!(
        readme.contains("orders/intake"),
        "README.md must document orders/intake topic"
    );
    assert!(
        readme.contains("svc_stream_replay"),
        "README.md must document svc_stream_replay M2M account"
    );

    // Must document all supported sinks
    assert!(
        readme.contains("kafka") || readme.contains("Kafka"),
        "README must document Kafka sink"
    );
    assert!(
        readme.contains("nats") || readme.contains("NATS"),
        "README must document NATS sink"
    );
    assert!(
        readme.contains("fluvio") || readme.contains("Fluvio"),
        "README must document Fluvio sink"
    );
    assert!(
        readme.contains("iceoryx2"),
        "README must document iceoryx2 sink"
    );

    // Must document library API & Feature flags
    assert!(
        readme.contains("ReplayEngine") || readme.contains("ReplayConfigBuilder"),
        "README must document programmatic library API"
    );
    assert!(
        readme.contains("Cargo Features") || readme.contains("features"),
        "README must document Cargo features"
    );

    // Must document quickstart & local verification
    assert!(
        readme.contains("audit-and-scan"),
        "README.md must document audit-and-scan scripts"
    );
    assert!(
        readme.contains("pre-commit"),
        "README.md must document pre-commit setup"
    );
}

#[test]
fn test_no_github_actions_workflows() {
    let workflows_path = manifest_path(".github/workflows");
    assert!(
        !workflows_path.exists(),
        "Per AD-13, no GitHub Actions workflows should exist; enforce shift-left local DevSecOps."
    );
}
