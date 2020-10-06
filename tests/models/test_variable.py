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

import unittest
from typing import Any, Optional

from cryptography.fernet import Fernet

from airflow import settings
from airflow.models import crypto, Variable
from airflow.models.variable import BaseVariable, resolve_variable_backend
from tests.test_utils.config import conf_vars


class CustomVariable(BaseVariable):

    @classmethod
    def get(
        cls,
        key,
        default_var=None,
        deserialize_json=False,
        session=None,
    ):
        # type: (str, Any, bool, Optional[Any]) -> Any
        return 'override'


class VariableTest(unittest.TestCase):
    def setUp(self):
        crypto._fernet = None

    def tearDown(self):
        crypto._fernet = None

    @conf_vars({('core', 'fernet_key'): ''})
    def test_variable_no_encryption(self):
        """
        Test variables without encryption
        """
        Variable.set('key', 'value')
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == 'key').one()
        self.assertFalse(test_var.is_encrypted)
        self.assertEqual(test_var.val, 'value')

    @conf_vars({('core', 'fernet_key'): Fernet.generate_key().decode()})
    def test_variable_with_encryption(self):
        """
        Test variables with encryption
        """
        Variable.set('key', 'value')
        session = settings.Session()
        test_var = session.query(Variable).filter(Variable.key == 'key').one()
        self.assertTrue(test_var.is_encrypted)
        self.assertEqual(test_var.val, 'value')

    def test_var_with_encryption_rotate_fernet_key(self):
        """
        Tests rotating encrypted variables.
        """
        key1 = Fernet.generate_key()
        key2 = Fernet.generate_key()

        with conf_vars({('core', 'fernet_key'): key1.decode()}):
            Variable.set('key', 'value')
            session = settings.Session()
            test_var = session.query(Variable).filter(Variable.key == 'key').one()
            self.assertTrue(test_var.is_encrypted)
            self.assertEqual(test_var.val, 'value')
            self.assertEqual(Fernet(key1).decrypt(test_var._val.encode()), b'value')

        # Test decrypt of old value with new key
        with conf_vars({('core', 'fernet_key'): ','.join([key2.decode(), key1.decode()])}):
            crypto._fernet = None
            self.assertEqual(test_var.val, 'value')

            # Test decrypt of new value with new key
            test_var.rotate_fernet_key()
            self.assertTrue(test_var.is_encrypted)
            self.assertEqual(test_var.val, 'value')
            self.assertEqual(Fernet(key2).decrypt(test_var._val.encode()), b'value')

    @conf_vars(
        {('core', 'variable_backend'): 'tests.models.test_variable.CustomVariable'}
    )
    def test_resolve_variable_backend(self):
        cls = resolve_variable_backend()
        assert issubclass(cls, CustomVariable)
        assert cls.get('') == 'override'

    @conf_vars(
        {('core', 'variable_backend'): ''}
    )
    def test_resolve_variable_backend_fallback_to_base_variable(self):
        cls = resolve_variable_backend()
        assert issubclass(cls, BaseVariable)
        assert cls.get('', default_var='default') == 'default'
