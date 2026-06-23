# Copyright 2025 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import unittest
from unittest.mock import MagicMock, patch

import ops_sunbeam.test_utils as test_utils
from ops.model import ActiveStatus, BlockedStatus
from unit import testbase

import relation_handlers
from relation_handlers import CephClientProviderHandler


class TestRelationHelpers(testbase.TestBaseCharm):
    PATCHES = [
        "gethostname",
    ]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(relation_handlers, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        with open("metadata.yaml", "r") as f:
            metadata = f.read()
        self.harness = test_utils.get_harness(
            testbase._MicroCephCharm,
            container_calls=self.container_calls,
            charm_config=config_data,
            charm_metadata=metadata,
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_collect_peer_data(self):
        self.harness.set_leader()
        rel_id = self.add_complete_peer_relation(self.harness)
        unit_name = self.harness.model.unit.name
        # set up some initial relation data
        self.harness.update_relation_data(
            rel_id,
            "microceph/0",
            {
                unit_name: "test-hostname",
            },
        )
        self.gethostname.return_value = "test-hostname"
        change_data = relation_handlers.collect_peer_data(self.harness.model)
        self.assertNotIn(unit_name, change_data)
        self.assertEqual(change_data["public-address"], "10.0.0.10")
        self.gethostname.return_value = "changed-hostname"
        # assert that collect_peer_data raises an exception
        with self.assertRaises(relation_handlers.HostnameChangeError):
            relation_handlers.collect_peer_data(self.harness.model)


class TestBrokerRequestStatus(testbase.TestBaseCharm):
    """LP #2147014: broker failures set blocked status; success clears it."""

    PATCHES = ["gethostname"]

    def setUp(self):
        """Setup MicroCeph Charm tests."""
        super().setUp(relation_handlers, self.PATCHES)
        with open("config.yaml", "r") as f:
            config_data = f.read()
        with open("metadata.yaml", "r") as f:
            metadata = f.read()
        self.harness = test_utils.get_harness(
            testbase._MicroCephCharm,
            container_calls=self.container_calls,
            charm_config=config_data,
            charm_metadata=metadata,
        )
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()
        # Reuse the charm's own ceph handler (instantiated during charm init)
        # rather than creating a duplicate, which the ops framework rejects.
        self.handler = next(
            h
            for h in self.harness.charm.relation_handlers
            if isinstance(h, CephClientProviderHandler) and h.relation_name == "ceph"
        )
        # Track the broker response without exercising the relation data bag.
        self.handler.interface.set_broker_response = MagicMock()
        self.harness.charm.get_ceph_info_from_configs = MagicMock(return_value={})

    def _event(self):
        event = MagicMock()
        event.broker_req = '{"api-version": 1, "ops": []}'
        event.client_unit_name = "gnocchi-0"
        event.client_app_name = "gnocchi"
        event.relation_id = 1
        event.relation_name = "ceph"
        event.broker_req_id = "req-1"
        return event

    @patch("relation_handlers.process_requests")
    def test_broker_failure_sets_blocked_and_defers(self, mock_process):
        """A failed broker request sets blocked status and defers for retry."""
        # process_requests is decorated to return a JSON-encoded response string.
        mock_process.return_value = json.dumps(
            {"exit-code": 1, "stderr": "TOO_MANY_PGS: limit exceeded"}
        )
        event = self._event()
        self.handler._on_process_request(event)

        status = self.handler.status.status
        self.assertIsInstance(status, BlockedStatus)
        self.assertIn("TOO_MANY_PGS", status.message)
        self.assertIn("gnocchi-0", status.message)
        # Defers so the request is retried once the operator resolves the issue.
        event.defer.assert_called_once()
        # Response not sent: marking processed would prevent the retry.
        self.handler.interface.set_broker_response.assert_not_called()

    @patch("relation_handlers.process_requests")
    def test_broker_success_sets_active_and_responds(self, mock_process):
        """A successful broker request clears status and sends the response."""
        mock_process.return_value = json.dumps({"exit-code": 0})
        event = self._event()
        self.handler._on_process_request(event)

        status = self.handler.status.status
        self.assertIsInstance(status, ActiveStatus)
        self.handler.interface.set_broker_response.assert_called_once()
        event.defer.assert_not_called()

    @patch("relation_handlers.process_requests")
    def test_broker_malformed_response_sets_blocked(self, mock_process):
        """A non-JSON broker response is treated as a failure and blocked."""
        mock_process.return_value = "not-json"
        event = self._event()
        self.handler._on_process_request(event)

        status = self.handler.status.status
        self.assertIsInstance(status, BlockedStatus)
        self.assertIn("malformed", status.message)
        event.defer.assert_called_once()
        self.handler.interface.set_broker_response.assert_not_called()


if __name__ == "__main__":
    unittest.main()
