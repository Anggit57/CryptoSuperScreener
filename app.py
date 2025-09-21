import logging
import asyncio
from collections import defaultdict
from tqdm import tqdm
from flask import Flask, render_template, request, jsonify, session
import ccxt.async_support as ccxt_async

app = Flask(__name__)

MAX_TRIES = 3  # Define a maximum number of retries for each pair

# Setup basic configuration for logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

async def process_in_batches(task_func, items, batch_size, MAX_TRIES=10):
    results = []
    errors = defaultdict(int)
    pending_items = items[:]
    total_batches = (len(items) + batch_size - 1) // batch_size

    progress_bar = tqdm(total=total_batches)

    while pending_items:
        batch = pending_items[:batch_size]
        pending_items = pending_items[batch_size:]

        tasks = [task_func(item) for item in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for item, result in zip(batch, batch_results):
            if isinstance(result, Exception):
                errors[item] += 1
                if errors[item] < MAX_TRIES:
                    logging.error(f"Error processing {item}: {result}, retrying ({errors[item]}/{MAX_TRIES})...")
                    pending_items.append(item)
                else:
                    logging.error(f"Max retries exceeded for {item}: {result}")
            else:
                results.append((item, result[1]))

        progress_bar.update(1)

    progress_bar.close()
    return results

@app.route('/get_enriched_ohlcv', methods=['POST'])
async def get_enriched_ohlcv():
    data = request.get_json()
    pairs = data.get('pairs', [])
    timeframe = data.get('timeframe', '5m')
    if timeframe not in ['1m', '5m', '15m', '30m', '1h', '4h', '1d']:
        return jsonify({'error': 'Invalid timeframe'}), 400

    length = data.get('length', 120)

    if not pairs:
        return jsonify({'error': 'No pairs provided'}), 400

    enriched_data = {}

    async def fetch_and_process(pair):
        async with ccxt_async.kraken() as exchange:
            ohlcv_df = await fetch_ohlcv_data_async(pair, exchange, timeframe, length)
            enriched_df = add_custom_properties(ohlcv_df)
            return pair, enriched_df.to_dict('records')

    try:
        # Adjust batch_size as needed
        results = await process_in_batches(fetch_and_process, pairs, batch_size=40)

        for pair, ohlcv_data in results:
            enriched_data[pair] = ohlcv_data

    except Exception as e:
        return jsonify({'error': f"{str(e)} and enriched_data {enriched_data} and results = {results}"}), 400

    session['enriched_data'] = enriched_data
    return jsonify({'message': 'Data fetched and enriched successfully IN SESSION.'}), 200
