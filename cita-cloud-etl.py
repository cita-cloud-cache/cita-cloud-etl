#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright Rivtower Technologies LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import yaml
import json
import subprocess
import time
import os
from progress.bar import IncrementalBar

class BlockRow:
    height = 0
    block_hash = ''
    prev_hash = ''
    proposal_hash = ''
    round = 0
    signature = ''
    validators = ''
    proposer = ''
    state_root = ''
    timestamp = 0
    transaction_root = ''
    tx_count = 0
    version = 0

class TxRow:
    height = 0
    index = 0
    tx_hash = ''
    data = ''
    nonce = ''
    quota = 0
    to = ''
    valid_until_block = 0
    value = ''
    version = 0
    sender = ''
    signature = ''

class UtxoRow:
    height = 0
    index = 0
    tx_hash = ''
    lock_id = 0
    output = ''
    pre_tx_hash = ''
    version = 0
    sender = ''
    signature = ''

class ReceiptRow:
    height = 0
    index = 0
    contract_addr = ''
    cumulative_quota_used = ''
    quota_used = ''
    error_msg = ''
    logs_bloom = ''
    tx_hash = ''

class EventRow:
    address = ''
    topics = ''
    data = ''
    height = 0
    log_index = 0
    tx_log_index = 0
    tx_hash = ''

class SystemConfigRow:
    height = 0
    admin = ''
    block_interval = 0
    block_limit = 0
    chain_id = ''
    emergency_brake = False
    quota_limit = 0
    validators = ''
    version = 0


get_block_number = 'cldi -c default get block-number'
parse_proof_fmt = 'cldi -c default rpc parse-proof --consensus OVERLORD {}'
get_receipt_fmt = 'cldi -c default get receipt {}'
get_tx_fmt = 'cldi -c default get tx {}'
get_block_fmt = 'cldi -c default get block {}'
get_block_hash_fmt = 'cldi -c default get block-hash {}'
get_system_config_fmt = 'cldi -c default get system-config {}'
save_default_ctx_fmt = 'cldi -r {} -e {} -u default context save default'

support_source_type = ["cldi"]
support_sink_type = ["csv", "doris"]


with open('config.yaml',encoding='utf-8') as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)
    print("config: ", config)

if config['source']['type'] not in support_source_type:
    print('unsupport source type! now support: {}'.format(support_source_type))
    exit(1)

if config['sink']['type'] not in support_sink_type:
    print('unsupport sink type! now support: {}'.format(support_sink_type))
    exit(1)

# check range
start_block_number = int(config['range']['start'])
end_block_number = int(config['range']['end'])

# progress
progress_file_name = 'progress{}-{}'.format(start_block_number, end_block_number)
if os.path.exists(progress_file_name):
    with open(progress_file_name, 'r') as progress_file:
        progress = int(progress_file.read())
        if progress == end_block_number:
            print('already done!')
            exit(0)
        else:
            real_start_block_number = progress
else:
    real_start_block_number = start_block_number
print('start from block {}'.format(real_start_block_number))
bar = IncrementalBar('etl progress', max = end_block_number - real_start_block_number)

# ready for output
if config['sink']['type'] == 'csv':
    dir = config['sink']['path']
    block_f = open('{}/block{}-{}.csv'.format(dir, start_block_number, end_block_number), 'a')
    tx_f = open('{}/tx{}-{}.csv'.format(dir, start_block_number, end_block_number), 'a')
    utxo_f = open('{}/utxo{}-{}.csv'.format(dir, start_block_number, end_block_number), 'a')
    receipt_f = open('{}/receipt{}-{}.csv'.format(dir, start_block_number, end_block_number), 'a')
    event_f = open('{}/event{}-{}.csv'.format(dir, start_block_number, end_block_number), 'a')
    system_config_f = open('{}/system_config{}-{}.csv'.format(dir, start_block_number, end_block_number), 'a')

if config['source']['type'] == 'cldi':
    # set default context
    subprocess.run(save_default_ctx_fmt.format("{}:{}".format(config['source']['endpoint'], config['source']['rpc_port']), "{}:{}".format(config['source']['endpoint'], config['source']['executor_port'])), shell=True)

    current_block_number = int(subprocess.check_output(get_block_number, shell=True).decode('utf-8').strip())

    for h in range(real_start_block_number, end_block_number):
        if h > current_block_number:
            print('wait for new block...')
            while True:
                time.sleep(1)
                current_block_number = int(subprocess.check_output(get_block_number, shell=True).decode('utf-8').strip())
                if h <= current_block_number:
                    break
        # update progress
        bar.next()
        # set progress ahead
        with open(progress_file_name, 'w') as progress_file:
            progress_file.write(str(h))
        # sleep duration
        time.sleep(int(config['range']['duration']))
        # init row struct
        block_row = BlockRow()
        tx_rows = []
        utxo_rows = []
        receipt_rows = []
        event_rows = []
        system_config_row = SystemConfigRow()        
        # get block hash
        block_hash = subprocess.check_output(get_block_hash_fmt.format(h), shell=True).decode('utf-8').strip()
        # get block
        block = json.loads(subprocess.check_output(get_block_fmt.format(h), shell=True).decode('utf-8').strip())
        # record block info
        block_row.height = h
        block_row.block_hash = block_hash
        block_row.prev_hash = block['prev_hash']
        block_row.proposer = block['proposer']
        block_row.state_root = block['state_root']
        block_row.timestamp = int(block['timestamp'])
        block_row.transaction_root = block['transaction_root']
        block_row.tx_count = int(block['tx_count'])
        block_row.version = int(block['version'])
        # parse proof
        if block['proof'] != '0x':
            proof = json.loads(subprocess.check_output(parse_proof_fmt.format(block['proof']), shell=True).decode('utf-8').strip())
            block_row.proposal_hash = proof['proposal_hash']
            block_row.round = int(proof['round'])
            block_row.signature = proof['signature']
            block_row.validators = proof['address_bitmap']

        is_system_config_changed = False
        for tx_hash in block['tx_hashes']:
            # get tx
            tx = json.loads(subprocess.check_output(get_tx_fmt.format(tx_hash), shell=True).decode('utf-8').strip())
            # parse tx
            if tx['type'] == 'Utxo':
                utxo_row = UtxoRow()
                utxo_row.height = h
                utxo_row.index = int(tx['index'])
                utxo_row.tx_hash = tx_hash
                utxo_row.lock_id = int(tx['transaction']['transaction']['lock_id'])
                utxo_row.output = tx['transaction']['transaction']['output']
                utxo_row.pre_tx_hash = tx['transaction']['transaction']['pre_tx_hash']
                utxo_row.version = int(tx['transaction']['transaction']['version'])
                utxo_row.sender = tx['transaction']['witnesses'][0]['sender']
                utxo_row.signature = tx['transaction']['witnesses'][0]['signature']
                utxo_rows.append(utxo_row)
                is_system_config_changed = True
            elif tx['type'] == 'Normal':
                tx_row = TxRow()
                tx_row.height = h
                tx_row.index = int(tx['index'])
                tx_row.tx_hash = tx_hash
                tx_row.data = tx['transaction']['transaction']['data']
                tx_row.nonce = tx['transaction']['transaction']['nonce']
                tx_row.quota = int(tx['transaction']['transaction']['quota'])
                tx_row.to = tx['transaction']['transaction']['to']
                tx_row.valid_until_block = int(tx['transaction']['transaction']['valid_until_block'])
                tx_row.value = tx['transaction']['transaction']['value']
                tx_row.version = int(tx['transaction']['transaction']['version'])
                tx_row.sender = tx['transaction']['witness']['sender']
                tx_row.signature = tx['transaction']['witness']['signature']
                tx_rows.append(tx_row)
                # get receipt
                receipt = json.loads(subprocess.check_output(get_receipt_fmt.format(tx_hash), shell=True).decode('utf-8').strip())
                # parse receipt
                receipt_row = ReceiptRow()
                receipt_row.height = h
                receipt_row.index = receipt['tx_index']
                receipt_row.contract_addr = receipt['contract_addr']
                receipt_row.cumulative_quota_used = receipt['cumulative_quota_used']
                receipt_row.quota_used = receipt['quota_used']
                receipt_row.error_msg = receipt['error_msg']
                receipt_row.logs_bloom = receipt['logs_bloom']
                receipt_row.tx_hash = tx_hash
                receipt_rows.append(receipt_row)
                # parse event
                for event in receipt['logs']:
                    event_row = EventRow()
                    event_row.address = event['address']
                    event_row.topics = ';'.join(event['topics'])
                    event_row.data = event['data']
                    event_row.height = h
                    event_row.log_index = event['log_index']
                    event_row.tx_log_index = event['tx_log_index']
                    event_row.tx_hash = tx_hash
                    event_rows.append(event_row)
            else:
                print('unsupport tx type: {}'.format(tx['type']))
                exit(1)
        if is_system_config_changed:
            system_config = json.loads(subprocess.check_output(get_system_config_fmt.format(h), shell=True).decode('utf-8').strip())
            system_config_row.height = h
            system_config_row.admin = system_config['admin']
            system_config_row.block_interval = int(system_config['block_interval'])
            system_config_row.block_limit = int(system_config['block_limit'])
            system_config_row.chain_id = system_config['chain_id']
            system_config_row.emergency_brake = bool(system_config['emergency_brake'])
            system_config_row.quota_limit = int(system_config['quota_limit'])
            system_config_row.validators = ';'.join(system_config['validators'])
            system_config_row.version = int(system_config['version'])

        if config['sink']['type'] == 'csv':
            # write to csv
            block_f.write('{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(block_row.height, block_row.block_hash, block_row.prev_hash, block_row.proposal_hash, block_row.round, block_row.signature, block_row.validators, block_row.proposer, block_row.state_root, block_row.timestamp, block_row.transaction_root, block_row.tx_count, block_row.version))
            for tx_row in tx_rows:
                tx_f.write('{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(tx_row.height, tx_row.index, tx_row.tx_hash, tx_row.data, tx_row.nonce, tx_row.quota, tx_row.to, tx_row.valid_until_block, tx_row.value, tx_row.version, tx_row.sender, tx_row.signature))
            for utxo_row in utxo_rows:
                utxo_f.write('{},{},{},{},{},{},{},{},{}\n'.format(utxo_row.height, utxo_row.index, utxo_row.tx_hash, utxo_row.lock_id, utxo_row.output, utxo_row.pre_tx_hash, utxo_row.version, utxo_row.sender, utxo_row.signature))
            for receipt_row in receipt_rows:
                receipt_f.write('{},{},{},{},{},{},{},{}\n'.format(receipt_row.height, receipt_row.index, receipt_row.contract_addr, receipt_row.cumulative_quota_used, receipt_row.quota_used, receipt_row.error_msg, receipt_row.logs_bloom, receipt_row.tx_hash))
            for event_row in event_rows:
                event_f.write('{},{},{},{},{},{},{}\n'.format(event_row.address, event_row.topics, event_row.data, event_row.height, event_row.log_index, event_row.tx_log_index, event_row.tx_hash))
            if is_system_config_changed:
                system_config_f.write('{},{},{},{},{},{},{},{},{}\n'.format(system_config_row.height, system_config_row.admin, system_config_row.block_interval, system_config_row.block_limit, system_config_row.chain_id, system_config_row.emergency_brake, system_config_row.quota_limit, system_config_row.validators, system_config_row.version))
        elif config['sink']['type'] == 'doris':
            # write to json
            with open('block-{}.json'.format(h), 'w') as json_file:
                json_file.write(json.dumps(block_row.__dict__))
                json_file.write('\n')
            upload_block_cmd = 'curl -v --location-trusted -u {}:{} -H "format: json" -T block-{}.json {}/api/{}/blocks/_stream_load'.format(config['sink']['user'], config['sink']['password'], h, config['sink']['endpoint'], config['sink']['database'])
            ret = subprocess.run(upload_block_cmd, shell=True)
            if ret.returncode != 0:
                print("run cmd failed!")
                exit(-1)
            if tx_rows.__len__() > 0:
                with open('txs-{}.json'.format(h), 'w') as json_file:
                    for tx_row in tx_rows:
                        json_file.write(json.dumps(tx_row.__dict__))
                        json_file.write('\n')
                upload_txs_cmd = 'curl -v --location-trusted -u {}:{} -H "format: json" -H "read_json_by_line: true" -T txs-{}.json {}/api/{}/txs/_stream_load'.format(config['sink']['user'], config['sink']['password'], h, config['sink']['endpoint'], config['sink']['database'])
                ret = subprocess.run(upload_txs_cmd, shell=True)
                if ret.returncode != 0:
                    print("run cmd failed!")
                    exit(-1)
            if utxo_rows.__len__() > 0:
                with open('utxos-{}.json'.format(h), 'w') as json_file:
                    for utxo_row in utxo_rows:
                        json_file.write(json.dumps(utxo_row.__dict__))
                        json_file.write('\n')
                upload_utxos_cmd = 'curl -v --location-trusted -u {}:{} -H "format: json" -H "read_json_by_line: true" -T utxos-{}.json {}/api/{}/utxos/_stream_load'.format(config['sink']['user'], config['sink']['password'], h, config['sink']['endpoint'], config['sink']['database'])
                ret = subprocess.run(upload_utxos_cmd, shell=True)
                if ret.returncode != 0:
                    print("run cmd failed!")
                    exit(-1)
            if receipt_rows.__len__() > 0:
                with open('receipts-{}.json'.format(h), 'w') as json_file:
                    for receipt_row in receipt_rows:
                        json_file.write(json.dumps(receipt_row.__dict__))
                        json_file.write('\n')
                upload_receipts_cmd = 'curl -v --location-trusted -u {}:{} -H "format: json" -H "read_json_by_line: true" -T receipts-{}.json {}/api/{}/receipts/_stream_load'.format(config['sink']['user'], config['sink']['password'], h, config['sink']['endpoint'], config['sink']['database'])
                ret = subprocess.run(upload_receipts_cmd, shell=True)
                if ret.returncode != 0:
                    print("run cmd failed!")
                    exit(-1)
            if event_rows.__len__() > 0:
                with open('events-{}.json'.format(h), 'w') as json_file:
                    for event_row in event_rows:
                        json_file.write(json.dumps(event_row.__dict__))
                        json_file.write('\n')
                upload_events_cmd = 'curl -v --location-trusted -u {}:{} -H "format: json" -H "read_json_by_line: true" -T events-{}.json {}/api/{}/logs/_stream_load'.format(config['sink']['user'], config['sink']['password'], h, config['sink']['endpoint'], config['sink']['database'])
                ret = subprocess.run(upload_events_cmd, shell=True)
                if ret.returncode != 0:
                    print("run cmd failed!")
                    exit(-1)
            if is_system_config_changed:
                with open('system_config-{}.json'.format(h), 'w') as json_file:
                    json_file.write(json.dumps(system_config_row.__dict__))
                    json_file.write('\n')
                upload_system_config_cmd = 'curl -v --location-trusted -u {}:{} -H "format: json" -T system_config-{}.json {}/api/{}/system_config/_stream_load'.format(config['sink']['user'], config['sink']['password'], h, config['sink']['endpoint'], config['sink']['database'])
                ret = subprocess.run(upload_system_config_cmd, shell=True)
                if ret.returncode != 0:
                    print("run cmd failed!")
                    exit(-1)
bar.finish()
