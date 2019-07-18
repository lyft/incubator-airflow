# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import logging
from multiprocessing import Process, JoinableQueue
from os.path import getsize
from tempfile import NamedTemporaryFile
from typing import Any
from uuid import uuid4

from boto.compat import json

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator


MAX_QUEUE_LEN = 1000


def _convert_item_to_json_str(item):
    return json.dumps(item) + '\n'


def _upload_file_to_s3(file_obj, bucket_name, s3_key_prefix):
    s3_client = AwsHook().get_client_type('s3')
    file_obj.seek(0)

    # TODO: add retries and handle failures.
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class S3Uploader(Process):
    """
    Reads items from the queue, do some transformation, write to disk, upload to s3.

    """

    def __init__(self, item_queue, transform_func, file_size, s3_bucket_name, s3_key_prefix):
        # type: (JoinableQueue, Any, int, str, str, str) -> None
        super(S3Uploader, self).__init__()
        self.item_queue = item_queue
        self.transform_func = transform_func
        self.file_size = file_size
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix

    def run(self):
        f = NamedTemporaryFile()
        try:
            while True:
                item = self.item_queue.get()
                try:
                    if item is None:
                        logging.debug('Got poisoned and die.')
                        break
                    f.write(self.transform_func(item))

                finally:
                    self.item_queue.task_done()

                # Upload the file to S3 if reach file size limit
                if getsize(f.name) >= self.file_size:
                    _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
                    f.close()
                    f = NamedTemporaryFile()
        finally:
            _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
            f.close()


class DynamoDBScanner(Process):
    """
    Scan DynamoDB table and put items in a queue.
    """

    def __init__(self, item_queue, table_name, total_segments, segment):
        # type: (JoinableQueue, str, int, int) -> None

        super(DynamoDBScanner, self).__init__()
        self.item_queue = item_queue
        self.table_name = table_name
        self.total_segments = total_segments
        self.segment = segment

    def run(self):
        dynamodb_client = AwsHook().get_client_type('dynamodb')
        scan_kwargs = {
            'TableName': self.table_name,
            'TotalSegments': self.total_segments,
            'Segment': self.segment,
        }

        while True:
            # TODO: add retries, handle failures.
            response = dynamodb_client.scan(**scan_kwargs)
            items = response['Items']
            for item in items:
                self.item_queue.put(item)

            if 'LastEvaluatedKey' not in response:
                # no more items to scan
                break

            last_evaluated_key = response['LastEvaluatedKey']
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key


class DynamoDBToS3Operator(BaseOperator):

    def __init__(self,
                 table_name,
                 s3_bucket_name,
                 file_size,
                 s3_key_prefix='',
                 num_s3_uploader=1,
                 num_dynamodb_scanner=1,
                 process_func=_convert_item_to_json_str,
                 max_queue_len=MAX_QUEUE_LEN,
                 *args, **kwargs):
        super(DynamoDBToS3Operator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.file_size = file_size
        self.process_func = process_func
        self.num_s3_uploader = num_s3_uploader
        self.num_dynamodb_scanner = num_dynamodb_scanner
        self.s3_uploaders = []
        self.dynamodb_scanners = []
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix
        self.max_queue_len = max_queue_len

    def execute(self, context):
        item_queue = JoinableQueue(maxsize=self.max_queue_len)
        try:
            for i in range(self.num_dynamodb_scanner):
                dynamodb_scanner = DynamoDBScanner(
                    item_queue=item_queue,
                    table_name=self.table_name,
                    total_segments=self.num_dynamodb_scanner,
                    segment=i,
                )
                dynamodb_scanner.start()
                self.dynamodb_scanners.append(dynamodb_scanner)

            for _ in range(self.num_s3_uploader):
                s3_uploader = S3Uploader(
                    item_queue=item_queue,
                    transform_func=self.process_func,
                    file_size=self.file_size,
                    s3_bucket_name=self.s3_bucket_name,
                    s3_key_prefix=self.s3_key_prefix,
                )
                s3_uploader.start()
                self.s3_uploaders.append(s3_uploader)

            # Wait until dynamodb scan finish.
            for dynamodb_scanner in self.dynamodb_scanners:
                dynamodb_scanner.join()

            logging.info('Sending poison pills to kill workers.')
            for _ in range(self.num_s3_uploader):
                item_queue.put(None)

            item_queue.join()
        except Exception:
            for s3_uploader in self.s3_uploaders:
                s3_uploader.terminate()
            for dynamodb_scanner in self.dynamodb_scanners:
                dynamodb_scanner.terminate()
            raise
        finally:
            for s3_uploader in self.s3_uploaders:
                s3_uploader.join()
            for dynamodb_scanner in self.dynamodb_scanners:
                dynamodb_scanner.join()
            # TODO: Cleanup the S3 files...

    def on_kill(self):
        for s3_uploader in self.s3_uploaders:
            s3_uploader.terminate()

        for dynamodb_scanner in self.dynamodb_scanners:
            dynamodb_scanner.terminate()
