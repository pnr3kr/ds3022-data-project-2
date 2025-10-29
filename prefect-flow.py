from typing import Dict, Optional
import os
import time

import boto3
import requests
from prefect import flow, task, get_run_logger


SCATTER_ENDPOINT = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"


@task(log_prints=True)
def seed_queue_task(uvaid: str) -> str:
	# POST and return the sqs_url
	logger = get_run_logger()
	url = f"{SCATTER_ENDPOINT}/{uvaid}"
	logger.info("Seeding queue via scatter API: %s", url)
	resp = requests.post(url, timeout=15)
	resp.raise_for_status()
	payload = resp.json()
	logger.info("scatter response: %s", payload)
	sqs_url = payload.get("sqs_url")
	if not sqs_url:
		raise RuntimeError(f"scatter API response missing 'sqs_url': {payload}")
	return sqs_url

@task(log_prints=True)
def wait_for_seed_task(queue_url: str, expected: int = 21, poll_interval: int = 10, timeout: int = 600):
	# Poll queue attributes until approximate total >= expected or timeout.
	logger = get_run_logger()
	sqs = boto3.client("sqs")
	start = time.time()
	while True:
		attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=[
			'ApproximateNumberOfMessages',
			'ApproximateNumberOfMessagesNotVisible',
			'ApproximateNumberOfMessagesDelayed'
		])
		a = attrs.get('Attributes', {})
		counts = {k: int(a.get(k, '0')) for k in a}
		total = sum(counts.values())
		logger.info("Queue attrs: %s total(approx)=%d", counts, total)
		if total >= expected:
			logger.info("Queue seeded (approx total >= %d).", expected)
			return True
		if time.time() - start > timeout:
			logger.warning("Timeout waiting for queue to be seeded (waited %s seconds).", timeout)
			return False
		time.sleep(poll_interval)


@task(log_prints=True)
def collect_messages_task(queue_url: str, expected_count: int = 21, wait_time: int = 10) -> Dict[int, str]:
	# Receive messages until we've collected expected_count fragments.

	# Each message is deleted after processing. Returns a dict mapping order_no->word.

	logger = get_run_logger()
	sqs = boto3.client('sqs')
	collected: Dict[int, str] = {}

	# Persist parsed fragments to a local sqlite DB to satisfy 'persistent storage'
	# requirement. The DB is placed in /tmp by default so it is not part of
	# the repository and will not be committed.
	import sqlite3
	db_path = os.getenv('DP2_DB_PATH', '/tmp/dp2_messages.db')
	conn = sqlite3.connect(db_path)
	cur = conn.cursor()
	cur.execute('''
		CREATE TABLE IF NOT EXISTS fragments (
			order_no INTEGER PRIMARY KEY,
			word TEXT NOT NULL,
			received_at REAL
		)
	''')
	conn.commit()

	logger.info("Begin collecting messages from %s expecting %d fragments", queue_url, expected_count)
	while len(collected) < expected_count:
		try:
			resp = sqs.receive_message(
				QueueUrl=queue_url,
				MaxNumberOfMessages=10,
				WaitTimeSeconds=wait_time,
				MessageAttributeNames=['All']
			)
		except Exception as e:
			logger.error("receive_message error: %s", e)
			time.sleep(5)
			continue

		messages = resp.get('Messages', [])
		if not messages:
			logger.info("No visible messages returned; will retry...")
			time.sleep(2)
			continue

		for m in messages:
			attrs = m.get('MessageAttributes') or {}
			order_attr = attrs.get('order_no')
			word_attr = attrs.get('word')
			if not order_attr or not word_attr:
				logger.warning("Message missing expected attributes; skipping MessageId=%s", m.get('MessageId'))
				# safer to delete messages lacking expected attributes to avoid clogging the queue
				try:
					sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m['ReceiptHandle'])
				except Exception as e:
					logger.error("Failed to delete malformed message: %s", e)
				continue

			try:
				order_no = int(order_attr.get('StringValue'))
			except Exception:
				logger.exception("Invalid order_no value; skipping: %s", order_attr)
				try:
					sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m['ReceiptHandle'])
				except Exception:
					pass
				continue

			word = word_attr.get('StringValue')
			# persist to sqlite (idempotent upsert)
			try:
				cur.execute('INSERT OR REPLACE INTO fragments(order_no, word, received_at) VALUES (?, ?, ?)',
							(order_no, word, time.time()))
				conn.commit()
			except Exception as e:
				logger.error("Failed to persist fragment (order=%s): %s", order_no, e)

			# track in memory as well
			collected[order_no] = word

			# delete processed message
			try:
				sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m['ReceiptHandle'])
			except Exception as e:
				logger.error("Failed to delete message (order=%s): %s", order_no, e)

			logger.info("Collected %d/%d fragments (order=%s)", len(collected), expected_count, order_no)

	logger.info("Finished collecting %d fragments", len(collected))
	conn.close()
	return collected


@task(log_prints=True)
def assemble_phrase_task(collected: Dict[int, str]) -> str:
	logger = get_run_logger()
	if not collected:
		raise RuntimeError("No fragments collected to assemble phrase")
	phrase = ' '.join(collected[i] for i in sorted(collected.keys()))
	logger.info("Assembled phrase: %s", phrase)
	return phrase


@task(log_prints=True)
def submit_solution_task(uvaid: str, phrase: str, platform: str = 'prefect') -> dict:
	logger = get_run_logger()
	logger.info("Submitting solution to submit queue: %s", SUBMIT_QUEUE_URL)
	sqs = boto3.client('sqs')
	message = f"DP2 submission from {uvaid} via {platform}"
	response = sqs.send_message(
		QueueUrl=SUBMIT_QUEUE_URL,
		MessageBody=message,
		MessageAttributes={
			'uvaid': {'DataType': 'String', 'StringValue': uvaid},
			'phrase': {'DataType': 'String', 'StringValue': phrase},
			'platform': {'DataType': 'String', 'StringValue': platform}
		}
	)
	logger.info("Submit response: %s", response)
	return response


@flow(name="dp2-prefect-flow")
def dp2_flow(uvaid: str,
			 seed_queue: bool = True,
			 queue_url: Optional[str] = None,
			 expected: int = 21,
			 wait_for_seed: bool = True):
	"""Top-level flow wiring the tasks together."""
	logger = get_run_logger()

	# Step 1: seed queue
	sqs_url = None
	if seed_queue:
		sqs_url = seed_queue_task(uvaid)
	else:
		sqs_url = queue_url or os.getenv('SCATTER_SQS_URL')

	if not sqs_url:
		raise RuntimeError('No SQS URL available: either seed_queue=True or provide queue_url/SCATTER_SQS_URL')

	# Step 2: wait for queue to be seeded
	if wait_for_seed:
		ok = wait_for_seed_task(sqs_url, expected)
		if not ok:
			logger.warning('Proceeding even though queue may not be fully seeded (approx counts)')

	# Step 3: collect messages, assemble and submit
	collected = collect_messages_task(sqs_url, expected)
	phrase = assemble_phrase_task(collected)
	submit_solution_task(uvaid, phrase, platform='prefect')


if __name__ == '__main__':
	import argparse

	p = argparse.ArgumentParser(description='Run the DP2 Prefect flow locally')
	p.add_argument('--uvaid', required=True, help='Your UVA computing id')
	p.add_argument('--no-seed', dest='seed', action='store_false', help='Do not call scatter API (use existing queue)')
	p.add_argument('--queue-url', help='Optional existing queue URL (overrides SCATTER_SQS_URL env var)')
	p.add_argument('--expected', type=int, default=21, help='Expected fragment count')
	p.add_argument('--no-wait', dest='wait_for_seed', action='store_false', help='Do not wait for queue seed attributes')
	args = p.parse_args()

	# Run the flow

	dp2_flow(uvaid=args.uvaid, seed_queue=args.seed, queue_url=args.queue_url, expected=args.expected, wait_for_seed=args.wait_for_seed)

