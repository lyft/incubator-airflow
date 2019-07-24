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

from copy import copy
from os.path import getsize
from tempfile import NamedTemporaryFile
from uuid import uuid4

from boto.compat import json

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator


def _convert_item_to_json_str(item):
    return json.dumps(item) + '\n'


def _upload_file_to_s3(file_obj, bucket_name, s3_key_prefix):
    s3_client = AwsHook().get_client_type('s3')
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class DynamoDBToS3Operator(BaseOperator):

    def __init__(self,
                 s3_bucket_name,
                 file_size,
                 dynamodb_scan_kwargs,
                 s3_key_prefix='',
                 process_func=_convert_item_to_json_str,
                 *args, **kwargs):
        super(DynamoDBToS3Operator, self).__init__(*args, **kwargs)
        self.file_size = file_size
        self.process_func = process_func
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix
        self.dynamodb_scan_kwargs = dynamodb_scan_kwargs

    def execute(self, context):
        dynamodb_client = AwsHook().get_client_type('dynamodb')
        dynamodb_scan_kwargs = copy(self.dynamodb_scan_kwargs)
        f = NamedTemporaryFile()
        err = None
        try:
            while True:
                response = dynamodb_client.scan(**dynamodb_scan_kwargs)
                items = response['Items']
                for item in items:
                    f.write(self.process_func(item))
                if 'LastEvaluatedKey' not in response:
                    # no more items to scan
                    break

                last_evaluated_key = response['LastEvaluatedKey']
                dynamodb_scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
                # Upload the file to S3 if reach file size limit
                if getsize(f.name) >= self.file_size:
                    _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
                    f.close()
                    f = NamedTemporaryFile()
        except Exception as e:
            # TODO: clean up S3
            err = e
            pass
        finally:
            if err is None:
                _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
                f.close()

    def on_kill(self):
        # TODO: clean up S3.
        pass
