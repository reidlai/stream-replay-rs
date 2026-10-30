#!/usr/bin/env bash
# ==============================================================================
# stream-replay-rs Tier 1 DevSecOps Local Audit & Scan Gate (POSIX/Bash)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRATE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${CRATE_DIR}"

echo "======================================================================"
echo " [Tier 1 Security Gate] stream-replay-rs: Code Quality & Security Scan"
echo "======================================================================"

# 1. Format Gate
echo "--> [1/6] Checking code formatting (cargo fmt)..."
if ! cargo fmt --all -- --check; then
    echo "ERROR: cargo fmt failed. Please run 'cargo fmt --all' to reformat code." >&2
    exit 1
fi

# 2. Clippy Linter Gate
echo "--> [2/6] Running static analysis (cargo clippy)..."
if ! cargo clippy --all-targets -- -D warnings; then
    echo "ERROR: cargo clippy detected warnings or errors. Build halted." >&2
    exit 1
fi

# 3. Unit & Integration Test Gate
echo "--> [3/6] Running unit and integration tests (cargo test)..."
if ! cargo test --all-targets; then
    echo "ERROR: cargo test failed. Fix failing tests before committing." >&2
    exit 1
fi

# 4. Dependency Vulnerability Audit
echo "--> [4/6] Checking dependency vulnerabilities (cargo audit)..."
if command -v cargo-audit >/dev/null 2>&1; then
    if ! cargo audit; then
        echo "ERROR: cargo audit found security vulnerabilities in dependencies." >&2
        exit 1
    fi
else
    echo "    [WARN] cargo-audit is not installed. Run 'cargo install cargo-audit' to enable."
fi

# 5. Supply-Chain & License Assurance
echo "--> [5/6] Checking license and supply chain compliance (cargo deny)..."
if command -v cargo-deny >/dev/null 2>&1; then
    if ! cargo deny check; then
        echo "ERROR: cargo deny detected banned licenses or advisories." >&2
        exit 1
    fi
else
    echo "    [WARN] cargo-deny is not installed. Run 'cargo install cargo-deny' to enable."
fi

# 6. Secret Detection Gate
echo "--> [6/6] Checking for hardcoded secrets (gitleaks)..."
if command -v gitleaks >/dev/null 2>&1; then
    if ! gitleaks detect --no-git --source . --exclude-path target --verbose; then
        echo "ERROR: gitleaks detected potential secrets or credentials in working tree." >&2
        exit 1
    fi
else
    echo "    [WARN] gitleaks is not installed. Skipping local secret scan."
fi

echo "======================================================================"
echo " [SUCCESS] stream-replay-rs passed all Tier 1 DevSecOps quality gates!"
echo "======================================================================"
exit 0
