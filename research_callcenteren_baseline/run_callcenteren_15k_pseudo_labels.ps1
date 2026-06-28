param(
    [int]$TargetTotal = 15000,
    [int]$BatchSize = 5,
    [double]$Sleep = 4.2,
    [int]$MaxTranscriptChars = 1600,
    [string]$Model = "gemma-4-31b-it",
    [string]$ApiKeyEnv = "GEMINI_API_KEY",
    [switch]$PrepareOnly,
    [switch]$SkipPrepare
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
$CandidateCsv = Join-Path $ScriptDir "output\callcenteren_15k_candidate.csv"
$CandidateSummary = Join-Path $ScriptDir "output\callcenteren_15k_candidate_summary.json"
$PrepareScript = Join-Path $ScriptDir "prepare_callcenteren_15k_candidates.py"
$BatchScript = Join-Path $ScriptDir "batch_pseudo_label_call_codes.py"

Set-Location $RepoRoot
$env:PYTHONUTF8 = "1"

if (-not $SkipPrepare) {
    python $PrepareScript `
        --target-size $TargetTotal `
        --output-csv $CandidateCsv `
        --summary-json $CandidateSummary
}

if ($PrepareOnly) {
    Write-Host "PrepareOnly complete. Candidate CSV: $CandidateCsv"
    exit 0
}

$ApiKeyValue = [Environment]::GetEnvironmentVariable($ApiKeyEnv, "Process")
if ([string]::IsNullOrWhiteSpace($ApiKeyValue)) {
    throw "Missing environment variable $ApiKeyEnv. Set it first, for example: `$env:$ApiKeyEnv='YOUR_KEY_HERE'"
}

python $BatchScript `
    --input $CandidateCsv `
    --target-total $TargetTotal `
    --batch-size $BatchSize `
    --sleep $Sleep `
    --max-transcript-chars $MaxTranscriptChars `
    --model $Model `
    --api-key-env $ApiKeyEnv
