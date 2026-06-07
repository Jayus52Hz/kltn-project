# CallCenterEN Baseline Research Workflow

This folder contains the research workflow for using CallCenterEN as:

1. an external real-world baseline to justify the thesis dataset design; and
2. an auxiliary corpus for pseudo-label or domain-adaptive model experiments.

The primary thesis dataset remains `master_data/*.json`. CallCenterEN must not be
treated as the main dataset and should not be mixed into the primary test set.

## Inputs

Expected files:

```text
master_data/transcript_batch*.json
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

Run the BoW auxiliary experiment after pseudo-labeling:

```powershell
python .\research_callcenteren_baseline\train_auxiliary_bow_experiment.py
```

## Academic Positioning

Use this wording consistently:

```text
CallCenterEN is used as an external real-world reference baseline and auxiliary
training corpus. The proposed telesales dataset remains the primary dataset
because it contains task-specific business entities and call_code labels.
```

Do not call Gemini-generated labels ground truth. Use one of:

```text
weak labels
pseudo-labels
AI-assisted labels
auxiliary labels
```
