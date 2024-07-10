import json
import os
import tempfile
import time

import cups
import requests
from kafka import KafkaConsumer


def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    return local_filename


def print_document(file_path, printer_name, position, quantity=1, double_sided=False, color=False):
    conn = cups.Connection()

    printers = conn.getPrinters()
    if printer_name not in printers:
        print(f'Printer {printer_name} not found')
        return

    options = {
        'copies': str(quantity),
        'sides': 'two-sided-long-edge' if double_sided else 'one-sided',
        'ColorModel': 'Color' if color else 'GrayScale'
    }

    if position == 'landscape':
        options['orientation-requested'] = '4'

    try:
        job_id = conn.printFile(printer_name, file_path, "Print Job", options)
        print(f'Print Job submitted. JOB ID: {job_id}')
    except cups.IPPError as e:
        print(f'Error submitting print job: {e}')


def process_print_job(message):
    printer_name = 'HP_LaserJet_Professional_M1136_MFP'

    try:
        data = json.loads(message)
        file_url = data['file_url']
        quantity = data['quantity']
        double_sided = data['double_sided']
        color = data['color']
        position = data['position']

        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            local_filename = tmp_file.name

        download_file(file_url, local_filename)

        print_document(local_filename, printer_name, position, quantity, double_sided, color)

        os.unlink(local_filename)  # this will delete the file without warnings

    except Exception as e:
        print(f'Error processing print job: {e}')


def main():
    consumer = KafkaConsumer(
        'PrintJobs',
        bootstrap_servers=[os.getenv('BOOTSTRAP_SERVERS')],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='print_group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print('starting to commit messages')

    try:
        for message in consumer:
            print(f'Received message: {message.valu}')
            process_print_job(message)
            time.sleep(1)  # small delay to avoid overwhelming the printer
    except KeyboardInterrupt:
        print('Stopping the consumer...')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
