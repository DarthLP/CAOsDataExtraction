# CAO Data Extraction Pipeline - Reorganized Structure

## 📁 New Directory Layout

The project has been reorganized into a clean ETL (Extract, Transform, Load) structure:

```
CAOsDataExtraction/
├── conf/
│   └── config.yaml          # Centralized configuration
├── docs/
│   ├── REORG-LOG.md         # Reorganization log
│   ├── fields_prompt*.md    # LLM prompt templates
│   └── gemini_info.txt      # API documentation
├── inputs/
│   ├── excel/               # Excel input files
│   └── pdfs/                # PDF input files
├── monitoring/
│   ├── monitoring_3_1.py    # Performance monitoring
│   └── performance_logs/    # Log files
├── outputs/
│   ├── analysis/            # Analysis results
│   ├── comparison/          # Comparison results
│   ├── excel/               # Excel output files
│   ├── json/                # JSON output files
│   │   ├── old_flow/        # p3_llmExtraction output
│   │   └── new_flow/        # p3_1_llmExtraction output
│   └── logs/                # Log files
├── pipelines/
│   ├── __init__.py
│   ├── p0_webscraping.py    # Web scraping
│   ├── p1_inputExcel.py     # Excel processing
│   ├── p2_extract.py        # PDF extraction
│   ├── p3_llmExtraction.py  # LLM extraction (old flow)
│   ├── p3_1_llmExtraction.py # LLM extraction (new flow)
│   ├── p4_analysis.py       # Data analysis
│   └── p5_run.py            # Main pipeline runner
├── schema/                  # Data schemas
├── scripts/
│   ├── backups/             # Backup files
│   ├── check_imports_and_syntax.py
│   └── rewrite_imports_ast.py
├── utils/
│   └── OUTPUT_*.py          # Utility scripts
├── run_pipeline.py          # Main entry point
└── README.md
```

## 🚀 Quick Start

1. **Activate environment:**
   ```bash
   conda activate caos-extract
   ```

2. **Run the complete pipeline:**
   ```bash
   python run_pipeline.py
   ```

3. **Run individual steps:**
   ```bash
   python -m pipelines.p0_webscraping
   python -m pipelines.p1_inputExcel
   python -m pipelines.p2_extract
   python -m pipelines.p3_llmExtraction    # Old flow
   python -m pipelines.p3_1_llmExtraction  # New flow
   python -m pipelines.p4_analysis
   python -m pipelines.p5_run
   ```

## 🔧 Configuration

All paths are centralized in `conf/config.yaml`:

```yaml
paths:
  inputs_pdfs: inputs/pdfs/input_pdfs
  inputs_excel: inputs/excel/inputExcel
  outputs_json: outputs/json
  outputs_excel: outputs/excel
  outputs_logs: outputs/logs
  outputs_analysis: outputs/analysis
  outputs_comparison: outputs/comparison
  monitoring: monitoring
  utils: utils
  docs: docs
  scripts: scripts
```

## 📊 Output Structure

- **Old LLM Flow:** `outputs/json/old_flow/` (from p3_llmExtraction.py)
- **New LLM Flow:** `outputs/json/new_flow/` (from p3_1_llmExtraction.py)
- **Analysis Results:** `outputs/analysis/`
- **Comparison Results:** `outputs/comparison/`
- **Excel Outputs:** `outputs/excel/`
- **Logs:** `outputs/logs/`

## 🔄 Migration from Old Structure

### File Locations:
- **llmExtracted_json/ → outputs/json/** (merged with output_json)
- **output_json/ → outputs/json/**
- **input_pdfs/ → inputs/pdfs/input_pdfs/**
- **inputExcel/ → inputs/excel/inputExcel/**
- **results/ → outputs/excel/**
- **comparison_results/ → outputs/comparison/**
- **analysis_output/ → outputs/analysis/**

### Script Names:
- **0_webscrapping.py → pipelines/p0_webscraping.py** (corrected spelling)
- **1_inputExcel.py → pipelines/p1_inputExcel.py**
- **2_extract.py → pipelines/p2_extract.py**
- **3_llmExtraction.py → pipelines/p3_llmExtraction.py**
- **3_1_llmExtraction.py → pipelines/p3_1_llmExtraction.py**
- **4_analysis.py → pipelines/p4_analysis.py**
- **5_run.py → pipelines/p5_run.py**

## 🔍 How to Revert

If you need to revert to the old structure:

1. **Checkout the backup branch:**
   ```bash
   git checkout pre-reorg-backup
   ```

2. **Or use the tagged commit:**
   ```bash
   git checkout pre-reorg-v1.0
   ```

## 📝 Notes

- All LLM prompts and prompt templates remain unchanged
- Configuration is now centralized in `conf/config.yaml`
- Both LLM extraction flows are preserved in separate directories
- All imports have been updated to reflect the new structure
- Backup files are stored in `scripts/backups/`

## 🐛 Troubleshooting

1. **Import errors:** Ensure you're in the project root directory
2. **Missing files:** Check `conf/config.yaml` for correct paths
3. **Permission errors:** Ensure write permissions for output directories
4. **API errors:** Check your Gemini API keys and quotas

For detailed reorganization history, see `docs/REORG-LOG.md`.
