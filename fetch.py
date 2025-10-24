import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Set, Optional

import aiohttp

from parse import parse

# Configuration
CONCURRENT_WORKERS = 5
BATCH_SIZE = 200  # How many IDs to check ahead when looking for new records
CONSECUTIVE_404_THRESHOLD = 50  # Stop after this many consecutive 404s
OUTPUT_DIR = Path('output')  # Directory for all output files

# Global state
errors = {}
nodes = []  # List of node dicts
edges = []  # List of edge dicts
bad_ids: Set[int] = set()
valid_ids: Set[int] = set()  # Track valid IDs found in this run
last_valid_id = 0  # Track the highest ID that was valid (not 404)


def load_existing_data(data_file: Optional[str] = None, metadata_file: Optional[str] = None):
    """Load the existing data and metadata files."""
    global nodes, edges, bad_ids, last_valid_id

    # Load data.json (always the same file)
    if data_file:
        data_path = Path(data_file)
    else:
        data_path = OUTPUT_DIR / 'data.json'

    if data_path.exists():
        print(f'Loading existing data from {data_path}')
        try:
            with open(data_path, 'r') as infile:
                data = json.load(infile)
                nodes = data.get('nodes', [])
                edges = data.get('edges', [])
            print(f'Found {len(nodes)} existing nodes and {len(edges)} existing edges')
        except Exception as e:
            print(f'Error loading data: {e}')
            nodes = []
            edges = []
    else:
        print('No existing data found')
        nodes = []
        edges = []

    # Find the most recent metadata.json file (these have timestamps)
    if metadata_file:
        metadata_files = [Path(metadata_file)]
    else:
        metadata_files = sorted(OUTPUT_DIR.glob('metadata*.json'), key=lambda p: p.stat().st_mtime, reverse=True)

    metadata = None
    if metadata_files:
        latest_metadata = metadata_files[0]
        print(f'Loading metadata from {latest_metadata}')
        try:
            with open(latest_metadata, 'r') as infile:
                metadata = json.load(infile)
        except Exception as e:
            print(f'Error loading metadata: {e}')

    if metadata:
        bad_ids = set(metadata.get('bad_ids', []))
        last_valid_id = metadata.get('last_valid_id', metadata.get('id_max', 0))
        print(f'Last valid ID from previous run: {last_valid_id}')
        print(f'Loaded {len(bad_ids)} known bad IDs')
    else:
        bad_ids = set()
        last_valid_id = 0
        print('No metadata found, starting from scratch')

    return metadata


def get_id_range(metadata, start_id_override=None):
    """Determine the range of IDs to scan."""
    existing = set(node['id'] for node in nodes)

    if metadata:
        # Start from the beginning but skip existing records
        id_min = metadata.get('id_min', 1)
        # Start scanning from last valid ID and go forward
        id_start = last_valid_id + 1 if last_valid_id > 0 else id_min
    else:
        # No metadata, start from 1
        id_min = 1
        id_start = 1

    # Allow command-line override of start ID
    if start_id_override is not None:
        id_start = start_id_override
        print(f'Overriding start ID to {id_start} from command line')

    # We'll scan dynamically, so return a reasonable upper bound
    # The actual scanning will stop when we hit consecutive 404s
    id_max_estimate = id_start + BATCH_SIZE

    return id_min, id_start, id_max_estimate, existing


sem = asyncio.BoundedSemaphore(CONCURRENT_WORKERS)


async def fetch(session, url):
    """Fetch a URL with timeout."""
    async with asyncio.timeout(10):
        async with session.get(url) as response:
            return await response.text(), response.status


async def fetch_by_id(session, mgp_id):
    """Fetch and parse a single mathematician record by ID."""
    global last_valid_id

    async with sem:
        url = f'https://genealogy.math.ndsu.nodak.edu/id.php?id={mgp_id}'

        try:
            raw_html, status = await fetch(session, url)
            print(f'Fetching id={mgp_id} (status={status})')
        except Exception as e:
            print(f'Error fetching id={mgp_id}: {e}')
            errors[mgp_id] = str(e)
            return False

        # Check for 404 (non-existent ID)
        if 'You have specified an ID that does not exist in the database.' in raw_html:
            print(f'  → Bad ID (404): {mgp_id}')
            bad_ids.add(mgp_id)
            return False

        # This is a valid ID (exists in database)
        valid_ids.add(mgp_id)
        if mgp_id > last_valid_id:
            last_valid_id = mgp_id

        # Try to parse the record
        try:
            node, node_edges = parse(mgp_id, raw_html)
            nodes.append(node)
            edges.extend(node_edges)
            print(f'  → Successfully parsed id={mgp_id}: {len(node_edges)} edges')
            return True
        except Exception as e:
            print(f'  → Failed to parse id={mgp_id}: {e}')
            errors[mgp_id] = str(e)
            return False


async def scan_range(session, start_id, end_id, existing, limit=None):
    """Scan a range of IDs, skipping those already processed."""
    ids_to_scan = [
        i for i in range(start_id, end_id)
        if i not in existing and i not in bad_ids
    ]

    # If we have a limit, only scan enough IDs to potentially hit the limit
    # Add some buffer for 404s (scan 3x the remaining needed)
    if limit:
        remaining = limit - len(valid_ids)
        if remaining > 0:
            # Scan more than needed to account for 404s
            ids_to_scan = ids_to_scan[:remaining * 3]

    tasks = [fetch_by_id(session, i) for i in ids_to_scan]

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def main(args):
    """Main scanning loop that dynamically discovers the ID range."""
    metadata = load_existing_data(args.data_file, args.metadata_file)
    id_min, id_start, id_max_estimate, existing = get_id_range(metadata, args.start_id)

    print(f'\nStarting scan from ID {id_start}')
    print(f'Skipping {len(existing)} known records')
    print(f'Using {CONCURRENT_WORKERS} concurrent workers')
    if args.limit:
        print(f'Limiting to {args.limit} new entities (debug mode)')
    print()

    async with aiohttp.ClientSession() as session:
        current_id = id_start
        consecutive_404s = 0

        # First, fill in any gaps in existing data (skip if we have a limit)
        if existing and not args.limit:
            max_existing = max(existing)
            if current_id < max_existing:
                print(f'Scanning for gaps between {current_id} and {max_existing}...')
                await scan_range(session, current_id, max_existing, existing, args.limit)
                current_id = max_existing + 1

        # Now scan forward to discover new records
        print(f'\nScanning forward from ID {current_id} to discover new records...')
        print(f'Will stop after {CONSECUTIVE_404_THRESHOLD} consecutive 404s\n')

        while consecutive_404s < CONSECUTIVE_404_THRESHOLD:
            # Check if we've hit the limit
            if args.limit and len(valid_ids) >= args.limit:
                print(f'\n✓ Reached limit of {args.limit} new entities. Stopping scan.')
                break

            batch_end = current_id + BATCH_SIZE

            # Scan this batch
            batch_start_len = len(valid_ids)
            await scan_range(session, current_id, batch_end, existing, args.limit)
            batch_new_valid = len(valid_ids) - batch_start_len

            # Count consecutive 404s in this batch
            batch_404s = 0
            for i in range(current_id, batch_end):
                if i in bad_ids:
                    batch_404s += 1
                    consecutive_404s += 1
                elif i in valid_ids or i in existing:
                    consecutive_404s = 0  # Reset counter on valid ID
                    batch_404s = 0

            print(f'\nBatch {current_id}-{batch_end}: {batch_new_valid} valid, {batch_404s} 404s, {consecutive_404s} consecutive 404s')
            if args.limit:
                print(f'Progress: {len(valid_ids)}/{args.limit} new entities')

            if consecutive_404s >= CONSECUTIVE_404_THRESHOLD:
                print(f'\nReached {CONSECUTIVE_404_THRESHOLD} consecutive 404s. Stopping scan.')
                break

            current_id = batch_end


def save_results(args):
    """Save data and metadata."""
    # Ensure output directory exists
    OUTPUT_DIR.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    print('\nSaving results...')

    # Save errors with timestamp
    errors_file = OUTPUT_DIR / f'errors_{timestamp}.txt'
    with open(errors_file, 'w') as outfile:
        for mgp_id, error in errors.items():
            outfile.write(f'{mgp_id},{error}\n')
    print(f'  → Errors: {errors_file}')

    # Deduplicate nodes and edges
    # For nodes: keep unique by id (last one wins if duplicates)
    node_dict = {node['id']: node for node in nodes}
    unique_nodes = list(node_dict.values())

    # For edges: deduplicate by (advisor_id, student_id) tuple
    edge_set = {(edge['advisor_id'], edge['student_id']) for edge in edges}
    unique_edges = [{'advisor_id': aid, 'student_id': sid} for aid, sid in edge_set]

    print(f'  → Deduplicated: {len(nodes)} → {len(unique_nodes)} nodes')
    print(f'  → Deduplicated: {len(edges)} → {len(unique_edges)} edges')

    # Save data (always to data.json, no timestamp)
    data_file = OUTPUT_DIR / 'data.json'
    with open(data_file, 'w') as outfile:
        json.dump({
            'nodes': unique_nodes,
            'edges': unique_edges
        }, outfile, indent=2)
    print(f'  → Data: {data_file} ({len(unique_nodes)} nodes, {len(unique_edges)} edges)')

    # Save metadata with timestamp
    metadata_file = OUTPUT_DIR / f'metadata_{timestamp}.json'
    metadata = {
        'timestamp': timestamp,
        'id_min': 1,
        'last_valid_id': last_valid_id,
        'total_nodes': len(unique_nodes),
        'total_edges': len(unique_edges),
        'new_records_this_run': len(valid_ids),
        'bad_ids': sorted(list(bad_ids)),
        'errors_count': len(errors)
    }

    with open(metadata_file, 'w') as outfile:
        json.dump(metadata, outfile, indent=2)
    print(f'  → Metadata: {metadata_file}')

    print(f'\n✓ Successfully saved {len(unique_nodes)} total nodes and {len(unique_edges)} total edges')
    print(f'✓ Found {len(valid_ids)} new valid IDs in this run')
    print(f'✓ Last valid ID: {last_valid_id}')
    print(f'✓ Total bad IDs: {len(bad_ids)}')


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Scrape the Mathematics Genealogy Project database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (auto-discovers most recent files from output/ directory)
  python fetch.py

  # Debug mode: process only 10 new entities
  python fetch.py --limit 10

  # Specify input files
  python fetch.py --data-file output/data.json --metadata-file output/metadata_20241024_143000.json

  # Adjust scanning parameters
  python fetch.py --workers 10 --batch-size 500 --404-threshold 100

  # Start from a specific ID and limit to 5 new entities (good for testing)
  python fetch.py --start-id 342338 --limit 5

Note: All output files (data.json, metadata_*.json, errors_*.txt) are saved to the 'output/' directory.
        """
    )

    parser.add_argument(
        '--data-file',
        type=str,
        help='Path to existing data.json file (default: output/data.json)'
    )

    parser.add_argument(
        '--metadata-file',
        type=str,
        help='Path to existing metadata.json file (default: auto-discover most recent from output/)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='Number of concurrent workers (default: 5)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=200,
        help='Number of IDs to check per batch (default: 200)'
    )

    parser.add_argument(
        '--404-threshold',
        type=int,
        default=50,
        dest='threshold_404',
        help='Stop after this many consecutive 404s (default: 50)'
    )

    parser.add_argument(
        '--start-id',
        type=int,
        help='Override starting ID (default: last_valid_id + 1 from metadata)'
    )

    parser.add_argument(
        '--limit',
        type=int,
        help='Limit processing to N new entities (useful for debugging)'
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    # Apply configuration from arguments
    CONCURRENT_WORKERS = args.workers
    BATCH_SIZE = args.batch_size
    CONSECUTIVE_404_THRESHOLD = args.threshold_404

    # Update semaphore with new worker count
    sem = asyncio.BoundedSemaphore(CONCURRENT_WORKERS)

    # Run the scraper
    asyncio.run(main(args))
    save_results(args)
    print('\nDone!')
