# Math Genealogy

Code for scraping the Math Genealogy Project website (https://genealogy.math.ndsu.nodak.edu).

## Requirements

 - Python 3.11+
 - uv (recommended) or pip
 - aiohttp
 - beautifulsoup4

## Setup

This project uses [uv](https://github.com/astral-sh/uv) for dependency management. If you don't have uv installed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

The project dependencies are defined in `pyproject.toml` and will be automatically managed by uv.

## Usage

### Basic Usage

Run the scraper to fetch new data from the Math Genealogy Project:

```bash
uv run fetch.py
```

This will:
- Auto-discover the most recent metadata from `output/` directory
- Load existing data from `output/data.json`
- Scan for new records starting from the last valid ID
- Stop after 50 consecutive 404s (indicating end of database)
- Save results to the `output/` directory

### Debug Mode

To test the scraper with a limited number of entities:

```bash
uv run fetch.py --limit 10
```

### Advanced Options

```bash
# Specify custom input files
uv run fetch.py --data-file output/data.json --metadata-file output/metadata_20241024_143000.json

# Adjust concurrent workers (default: 5)
uv run fetch.py --workers 10

# Adjust batch size for scanning (default: 200)
uv run fetch.py --batch-size 500

# Change 404 threshold before stopping (default: 50)
uv run fetch.py --404-threshold 100

# Start from a specific ID
uv run fetch.py --start-id 342338

# Combine options for testing
uv run fetch.py --start-id 342330 --limit 5
```

### Get Help

```bash
uv run fetch.py --help
```

## Output Format

All output files are saved to the `output/` directory:

### data.json

The main output file containing the scraped data in graph format with separate nodes and edges:

```json
{
  "nodes": [
    {
      "id": 2,
      "name": "Archie Higdon",
      "school": "Iowa State University",
      "country": "UnitedStates",
      "year": 1936,
      "subject": "74—Mechanics of deformable solids"
    }
  ],
  "edges": [
    {
      "advisor_id": 66052,
      "student_id": 159681
    }
  ]
}
```

Fields that are not found are set to `null`.

### metadata_TIMESTAMP.json

Timestamped metadata files track each scraper run:

```json
{
  "timestamp": "20241024_143829",
  "id_min": 1,
  "last_valid_id": 342337,
  "total_nodes": 212870,
  "total_edges": 228129,
  "new_records_this_run": 15,
  "bad_ids": [206, 323, 415, ...],
  "errors_count": 0
}
```

### errors_TIMESTAMP.txt

Timestamped error logs (one error per line in CSV format):

```
id,error_message
```

## How It Works

1. **Load existing data**: Reads `output/data.json` and the most recent `output/metadata_*.json`
2. **Determine scan range**: Starts from `last_valid_id + 1` from the metadata
3. **Scan for new records**: Fetches IDs in batches (default: 200 at a time)
4. **Stop condition**: Stops after 50 consecutive 404 errors
5. **Deduplicate**: Removes duplicate nodes (by ID) and edges (by advisor-student pair)
6. **Save results**: Writes to `output/data.json` and creates timestamped metadata/error files

## Rate Limiting

To be respectful to the Math Genealogy Project servers, this program rate-limits itself to 5 concurrent workers by default. You can adjust this with the `--workers` flag, but please be considerate:

```bash
# Increase workers (use responsibly!)
uv run fetch.py --workers 10
```

## Running as a Cron Job

This scraper is designed to run periodically (e.g., weekly) via cron:

```bash
# Edit your crontab
crontab -e

# Add a line to run weekly on Sundays at 2am
0 2 * * 0 cd /path/to/math-genealogy-scraper && /path/to/uv run fetch.py
```

Each run will:
- Load the previous data
- Scan for new records from where it left off
- Merge new data with existing data
- Create timestamped metadata for tracking
