<#
.SYNOPSIS
    stream-replay-rs Tier 1 DevSecOps Local Audit & Scan Gate (PowerShell)
.DESCRIPTION
    Executes shift-left security scans, code formatting, clippy linter,
    unit/integration tests, and supply-chain audits with full POSIX parity.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
if (-not $ScriptDir) {
    $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
$CrateDir = Split-Path -Parent $ScriptDir
Set-Location $CrateDir

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host " [Tier 1 Security Gate] stream-replay-rs: Code Quality & Security Scan" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

# 1. Format Gate
Write-Host "--> [1/6] Checking code formatting (cargo fmt)..." -ForegroundColor Yellow
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo fmt failed. Please run 'cargo fmt --all' to reformat code."
    exit 1
}

# 2. Clippy Linter Gate
Write-Host "--> [2/6] Running static analysis (cargo clippy)..." -ForegroundColor Yellow
cargo clippy --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo clippy detected warnings or errors. Build halted."
    exit 1
}

# 3. Unit & Integration Test Gate
Write-Host "--> [3/6] Running unit and integration tests (cargo test)..." -ForegroundColor Yellow
cargo test --all-targets
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo test failed. Fix failing tests before committing."
    exit 1
}

# 4. Dependency Vulnerability Audit
Write-Host "--> [4/6] Checking dependency vulnerabilities (cargo audit)..." -ForegroundColor Yellow
$cargoAudit = Get-Command "cargo-audit" -ErrorAction SilentlyContinue
if ($cargoAudit) {
    cargo audit
    if ($LASTEXITCODE -ne 0) {
        Write-Error "cargo audit found security vulnerabilities in dependencies."
        exit 1
    }
} else {
    Write-Host "    [WARN] cargo-audit is not installed. Run 'cargo install cargo-audit' to enable." -ForegroundColor DarkYellow
}

# 5. Supply-Chain & License Assurance
Write-Host "--> [5/6] Checking license and supply chain compliance (cargo deny)..." -ForegroundColor Yellow
$cargoDeny = Get-Command "cargo-deny" -ErrorAction SilentlyContinue
if ($cargoDeny) {
    cargo deny check
    if ($LASTEXITCODE -ne 0) {
        Write-Error "cargo deny detected banned licenses or advisories."
        exit 1
    }
} else {
    Write-Host "    [WARN] cargo-deny is not installed. Run 'cargo install cargo-deny' to enable." -ForegroundColor DarkYellow
}

# 6. Secret Detection Gate
Write-Host "--> [6/6] Checking for hardcoded secrets (gitleaks)..." -ForegroundColor Yellow
$gitleaks = Get-Command "gitleaks" -ErrorAction SilentlyContinue
if ($gitleaks) {
    gitleaks detect --no-git --source . --exclude-path target --verbose
    if ($LASTEXITCODE -ne 0) {
        Write-Error "gitleaks detected potential secrets or credentials in working tree."
        exit 1
    }
} else {
    Write-Host "    [WARN] gitleaks is not installed. Skipping local secret scan." -ForegroundColor DarkYellow
}

Write-Host "======================================================================" -ForegroundColor Green
Write-Host " [SUCCESS] stream-replay-rs passed all Tier 1 DevSecOps quality gates!" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
exit 0
