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
    for flag in [
        "--speed",
        "--rate",
        "--event-time",
        "--timestamp-unit",
        "--shift-column",
        "--shift-timestamps",
        "--progress",
        "--loop-id-salt",
    ] {
        assert!(readme.contains(flag), "README.md must document {flag}");
    }
    assert!(
        readme.contains("10x"),
        "README.md must document the 10x speed format"
    );
    for removed in ["--burst", "--auth-url", "--svc-account", "--svc-secret"] {
        assert!(
            !readme.contains(removed),
            "README.md must not document the removed flag {removed}"
        );
    }

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

/// Text that must not appear anywhere in the published crate. The words are assembled at
/// run time so that this file does not contain them itself.
fn forbidden_terms() -> Vec<String> {
    vec![
        ["fer", "rous"].concat(),
        ["ol", "ist"].concat(),
        ["kan", "idm"].concat(),
        ["bench", "mark"].concat(),
    ]
}

fn collect_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

#[test]
fn test_repository_is_generic_and_carries_no_platform_references() {
    let mut files = Vec::new();
    for dir in ["src", "tests", "scripts", "config"] {
        collect_files(&manifest_path(dir), &mut files);
    }
    for file in [
        "Cargo.toml",
        "README.md",
        "deny.toml",
        ".pre-commit-config.yaml",
    ] {
        files.push(manifest_path(file));
    }

    let terms = forbidden_terms();
    for file in files {
        let Ok(content) = fs::read_to_string(&file) else {
            continue; // binary fixture
        };
        let lower = content.to_lowercase();
        for term in &terms {
            assert!(
                !lower.contains(term.as_str()),
                "{} contains a forbidden reference",
                file.display()
            );
        }
    }
}

#[test]
fn test_no_bundled_datasets_or_dev_broker_files() {
    assert!(!manifest_path("data").exists(), "no bundled dataset");
    assert!(!manifest_path("docker-compose.dev.yml").exists());
}

#[test]
fn test_cargo_manifest_is_ready_for_crates_io() {
    let content = fs::read_to_string(manifest_path("Cargo.toml")).unwrap();
    let parsed: toml::Value = toml::from_str(&content).unwrap();
    let package = parsed.get("package").unwrap();
    for key in [
        "name",
        "version",
        "description",
        "license",
        "repository",
        "readme",
        "keywords",
        "categories",
    ] {
        assert!(
            package.get(key).is_some(),
            "Cargo.toml [package] needs {key}"
        );
    }
    assert_eq!(package["name"].as_str(), Some("stream-replay-rs"));
    assert!(package["keywords"].as_array().unwrap().len() <= 5);
}
