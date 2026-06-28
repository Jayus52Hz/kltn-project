# CallCenterEN Baseline Research Workflow

This folder contains the research workflow for using CallCenterEN as the second
main dataset branch in a multi-source Hybrid Data Lakehouse. The AGI Telesales
dataset and CallCenterEN are processed as separate branches and compared through
dataset profiling, label distribution, and BoW model experiments.

CallCenterEN `call_code` values are pseudo-labels. They are accepted as trusted
labels for the CallCenterEN branch experiments, but the test splits of the two
datasets remain separate to avoid train/test leakage.

## Inputs

Expected files:

```text
master_data/raw/transcript_batch*.json
92k-real-world-call-center-scripts-english/prepared_subset/baseline_analysis_sample.csv
92k-real-world-call-center-scripts-english/prepared_subset/auxiliary_training_candidate.csv
NLP model/train.csv
```

## Step 1: Dataset Baseline Analysis

Run from the repository root:

```powershell
python .\research_callcenteren_baseline\analyze_dataset_baseline.py
```

Outputs:

```text
research_callcenteren_baseline/output/dataset_comparison_summary.json
research_callcenteren_baseline/output/dataset_comparison_table.csv
research_callcenteren_baseline/output/call_code_distribution.csv
research_callcenteren_baseline/output/report_dataset_baseline_section.md
```

Use the Markdown report section as the basis for revising the thesis report.

## Step 2: Pilot AI Pseudo-Labeling

Do not write the API key into any source file. Set it only as an environment
variable in your current PowerShell session:

```powershell
$env:GEMINI_API_KEY="YOUR_KEY_HERE"
python .\research_callcenteren_baseline\gemini_pseudo_label_call_codes.py --limit 50 --dry-run
python .\research_callcenteren_baseline\gemini_pseudo_label_call_codes.py --limit 50
```

Outputs:

```text
research_callcenteren_baseline/output/pseudo_labels_gemini.jsonl
research_callcenteren_baseline/output/pseudo_labels_gemini.csv
```

Recommended experiment order:

1. `--limit 50 --dry-run` to inspect prompts without calling the API.
2. `--limit 50` to verify API behavior.
3. Manually review 30-50 outputs.
4. Increase to `--limit 300` for a pilot pseudo-label quality report.
5. Only expand toward 2,000 samples if the pilot labels are usable.

If the API returns `HTTP Error 429: Too Many Requests`, stop the run and wait for
quota/rate limits to reset. Then retry with a smaller `--limit`, a larger
`--sleep`, or a different available Gemini model:

```powershell
python .\research_callcenteren_baseline\gemini_pseudo_label_call_codes.py --limit 20 --sleep 10
```

For Google AI Studio Gemma 4 31B with a 15 RPM limit:

```powershell
$env:GEMINI_API_KEY="YOUR_KEY_HERE"
python .\research_callcenteren_baseline\gemini_pseudo_label_call_codes.py --provider gemini --model gemma-4-31b-it --limit 20 --sleep 4.2
```

The script ignores model `thought` parts and parses only the final JSON response.

For larger runs, use the resumable batch script. It appends to the existing CSV,
skips already labeled `text_hash` values, and can be rerun after transient HTTP
500 errors:

```powershell
$env:GEMINI_API_KEY="YOUR_KEY_HERE"
python .\research_callcenteren_baseline\batch_pseudo_label_call_codes.py --target-total 300 --batch-size 5 --sleep 4.2 --max-transcript-chars 1600
```

The completed pilot currently contains 300 pseudo-labeled rows. To expand later:

```powershell
python .\research_callcenteren_baseline\batch_pseudo_label_call_codes.py --target-total 1000 --batch-size 5 --sleep 4.2 --max-transcript-chars 1600
python .\research_callcenteren_baseline\batch_pseudo_label_call_codes.py --target-total 2000 --batch-size 5 --sleep 4.2 --max-transcript-chars 1600
```

## Step 2b: 15k CallCenterEN Pseudo-Labeling

The old `auxiliary_training_candidate.csv` has only 2,000 rows. For a 15k run,
first prepare a larger candidate file from all local CallCenterEN ZIP archives:

```powershell
python .\research_callcenteren_baseline\prepare_callcenteren_15k_candidates.py
```

Outputs:

```text
research_callcenteren_baseline/output/callcenteren_15k_candidate.csv
research_callcenteren_baseline/output/callcenteren_15k_candidate_summary.json
```

Then run resumable Gemini pseudo-labeling. The script appends to the existing
`pseudo_labels_gemini.csv`, skips already labeled `text_hash` values, and stops
when the CSV reaches 15,000 rows:

```powershell
$env:GEMINI_API_KEY="YOUR_KEY_HERE"
python .\research_callcenteren_baseline\batch_pseudo_label_call_codes.py `
  --input .\research_callcenteren_baseline\output\callcenteren_15k_candidate.csv `
  --target-total 15000 `
  --batch-size 5 `
  --sleep 4.2 `
  --max-transcript-chars 1600 `
  --model gemma-4-31b-it
```

Or use the wrapper:

```powershell
$env:GEMINI_API_KEY="YOUR_KEY_HERE"
powershell -ExecutionPolicy Bypass -File .\research_callcenteren_baseline\run_callcenteren_15k_pseudo_labels.ps1
```

Prepare only, without calling Gemini:

```powershell
powershell -ExecutionPolicy Bypass -File .\research_callcenteren_baseline\run_callcenteren_15k_pseudo_labels.ps1 -PrepareOnly
```

At 15 RPM and `--batch-size 5`, reaching 15k labels takes roughly 3.5-5 hours
from scratch depending on API latency. Since the current pilot already has 300
rows, the remaining run labels about 14,700 new rows.

Run the BoW auxiliary experiment after pseudo-labeling:

```powershell
python .\research_callcenteren_baseline\train_auxiliary_bow_experiment.py
```

## Step 3: Prepare CallCenterEN as a Main Dataset Branch

After stopping or completing pseudo-label generation, inspect quality:

```powershell
python .\research_callcenteren_baseline\inspect_pseudo_label_quality.py
```

Create deterministic train/valid/test splits:

```powershell
python .\research_callcenteren_baseline\prepare_callcenteren_splits.py
```

Outputs:

```text
research_callcenteren_baseline/output/callcenteren_labeled.csv
research_callcenteren_baseline/output/callcenteren_train.csv
research_callcenteren_baseline/output/callcenteren_valid.csv
research_callcenteren_baseline/output/callcenteren_test.csv
research_callcenteren_baseline/output/callcenteren_split_summary.json
research_callcenteren_baseline/output/callcenteren_split_label_distribution.csv
```

Smoke test without overwriting full outputs:

```powershell
python .\research_callcenteren_baseline\prepare_callcenteren_splits.py `
  --limit 300 `
  --output-dir .\research_callcenteren_baseline\output\smoke
```

## Step 4: Multi-Source BoW Experiments

Run a lightweight smoke test:

```powershell
python .\research_callcenteren_baseline\run_multisource_bow_experiments.py `
  --split-dir .\research_callcenteren_baseline\output\smoke `
  --output-dir .\research_callcenteren_baseline\output\smoke\multisource_bow `
  --limit-train 80 `
  --limit-eval 40 `
  --max-features 5000
```

Run the full experiment after pseudo-labeling is frozen:

```powershell
python .\research_callcenteren_baseline\run_multisource_bow_experiments.py `
  --split-dir .\research_callcenteren_baseline\output `
  --output-dir .\research_callcenteren_baseline\output\multisource_bow `
  --save-models
```

The experiment reports:

```text
M0: primary -> primary
M1: primary -> CallCenterEN
M2: CallCenterEN -> CallCenterEN
M3: CallCenterEN -> primary
M4: primary + CallCenterEN -> each test set
```

## Step 4b: Fine-Tune a Separate CallCenterEN Model

The final direction keeps the primary AGI Telesales model and CallCenterEN model
separate. Do not use the combined model as the serving model. Fine-tune and
apply a CallCenterEN-specific classifier with:

```powershell
python .\research_callcenteren_baseline\finetune_callcenteren_bow.py `
  --output-dir .\research_callcenteren_baseline\output\callcenteren_finetuned_max4 `
  --max-labels 4
```

Outputs:

```text
research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_finetune_report.md
research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_best_finetuned_model.pkl
research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_15k_with_model_callcodes.csv
research_callcenteren_baseline/output/callcenteren_finetuned_max4/callcenteren_15k_model_callcode_summary.json
```

The 15k schema CSV is the full CallCenterEN branch input for Lakehouse. It keeps
existing Gemini pseudo-labels when present and adds `model_call_code` for every
candidate row.

## Step 5: Lakehouse Integration

The Docker Compose stack mounts `research_callcenteren_baseline/output` into
Spark/Airflow at:

```text
/opt/spark/work-dir/callcenteren-output
```

After `callcenteren_finetuned_max4/callcenteren_15k_with_model_callcodes.csv` exists,
the Airflow DAG can run the `callcenteren_external_branch` task to create:

```text
lakehouse.bronze_external.callcenteren_raw
lakehouse.silver_external.callcenteren_clean
lakehouse.silver_external.callcenteren_labeled
lakehouse.gold_external.callcenteren_call_analytics
lakehouse.gold.dataset_profile_comparison
lakehouse.gold.call_code_distribution_comparison
lakehouse.gold.model_experiment_comparison
```

## Academic Positioning

Use this wording consistently:

```text
The project is extended into a multi-source Hybrid Data Lakehouse. The AGI
Telesales dataset and CallCenterEN are modeled as two main dataset branches;
the Gold comparison layer evaluates transcript structure, call duration, PII
signals, call_code distribution, and NLP model behavior across both sources.
```

Do not call Gemini-generated labels original human ground truth. Use one of:

```text
weak labels
pseudo-labels
AI-assisted labels
auxiliary labels
```
