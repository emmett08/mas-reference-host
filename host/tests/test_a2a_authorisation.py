"""A2A operation scopes must agree with durable submission/continuation dispatch."""
import hashlib
from pathlib import Path
import tempfile
import unittest

from mas_host import Outcome
from mas_host.bridges.a2a import A2ABridge
from mas_host.bridges.authentication import StaticTokenVerifier
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from test_bridge_server import ServerFixture
from test_runner import FunctionAdapter, topology


INVALID_TASK_IDS = (None, "", 7, True, [], {}, "task/invalid", "x" * 201)
ABSENT = object()
TOKEN = "local-a2a-authorisation-regression-token"


class PausedExecutor:
    def execute(self, _workflow, execution, **_options):
        return ExecutionResult("input_required", execution["input"])


def send(task_id=ABSENT):
    message = {"messageId": "scope-regression", "role": "ROLE_USER",
               "parts": [{"mediaType": "application/json",
                          "data": {"workflow": "echo", "input": {"value": 2}}}]}
    if task_id is not ABSENT:
        message["taskId"] = task_id
    return {"jsonrpc": "2.0", "id": "scope-regression", "method": "SendMessage",
            "params": {"message": message, "configuration": {"returnImmediately": True}}}


class A2AAuthorisationTests(unittest.TestCase):
    def service(self, directory):
        workflow = Workflow("echo/1", topology(),
            {actor: FunctionAdapter(lambda _context, request: Outcome(request.input))
             for actor in ("a", "b", "c")}, "a", "b", "echo",
            continue_input=lambda _previous, supplied: supplied)
        return DurableTaskService(Path(directory), {"echo": workflow},
                                  {"alice": {"echo"}}, executor=PausedExecutor())

    def test_narrow_scope_matrix_cannot_admit_a_different_operation(self):
        for scope in ("mas:submit", "mas:update", "mas:read", "mas:cancel"):
            with self.subTest(scope=scope), tempfile.TemporaryDirectory() as directory:
                service = self.service(directory)
                seed = service.submit("alice", "seed", "echo", {"value": 1})
                service.run_once()
                verifier = StaticTokenVerifier([{
                    "sha256": hashlib.sha256(TOKEN.encode()).hexdigest(),
                    "principal": "alice", "scopes": [scope]}])
                with ServerFixture(service=service, verifier=verifier,
                                   a2a_bridge=A2ABridge(service)) as server:
                    for task_id in (*INVALID_TASK_IDS, ABSENT, seed["task_id"]):
                        with self.subTest(task_id=task_id):
                            before = service.list_tasks("alice")
                            status, _, body = server.request("/a2a", send(task_id),
                                token=TOKEN, headers={"A2A-Version": "1.0"})
                            if task_id is ABSENT or task_id == seed["task_id"]:
                                required = "mas:submit" if task_id is ABSENT else "mas:update"
                                if scope != required:
                                    self.assertEqual(status, 403)
                                    self.assertEqual(body["error"]["code"], "insufficient_scope")
                                    self.assertEqual(service.list_tasks("alice"), before)
                                else:
                                    self.assertEqual(status, 200)
                                    self.assertEqual(body["result"]["task"]["status"]["state"],
                                                     "TASK_STATE_SUBMITTED")
                                    if task_id is ABSENT:
                                        self.assertEqual(len(service.list_tasks("alice")), len(before) + 1)
                                    else:
                                        self.assertEqual(body["result"]["task"]["id"], seed["task_id"])
                                        self.assertEqual(service.inspect("alice", seed["task_id"])["revision"], 1)
                                        self.assertEqual(len(service.list_tasks("alice")), len(before))
                            else:
                                self.assertEqual(status, 200)
                                self.assertEqual(body["error"]["code"], -32602)
                                self.assertEqual(body["id"], "scope-regression")
                                self.assertEqual(service.list_tasks("alice"), before)

    def test_direct_bridge_rejects_present_invalid_task_ids_before_submission(self):
        with tempfile.TemporaryDirectory() as directory:
            service = self.service(directory)
            bridge = A2ABridge(service)
            for task_id in INVALID_TASK_IDS:
                with self.subTest(task_id=task_id):
                    response = bridge.handle("alice", send(task_id))
                    self.assertEqual(response["error"]["code"], -32602)
                    self.assertEqual(service.list_tasks("alice"), [])

    def test_envelope_and_version_errors_precede_task_classification(self):
        cases = [("invalid_id", {"id": identifier}, "1.0", -32600, None)
                 for identifier in (None, False, True, {}, [])]
        cases += [
            ("missing_id", {"id": ABSENT}, "1.0", -32600, None),
            ("invalid_jsonrpc", {"jsonrpc": "1.0"}, "1.0", -32600, "scope-regression"),
            ("invalid_method", {"method": []}, "1.0", -32600, "scope-regression"),
            ("missing_version", {}, "", -32009, "scope-regression"),
            ("old_version", {}, "0.3", -32009, "scope-regression"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            service = self.service(directory)
            bridge = A2ABridge(service)
            verifier = StaticTokenVerifier([{
                "sha256": hashlib.sha256(TOKEN.encode()).hexdigest(),
                "principal": "alice", "scopes": ["mas:update"]}])
            with ServerFixture(service=service, verifier=verifier, a2a_bridge=bridge) as server:
                for label, changes, version, code, response_id in cases:
                    with self.subTest(case=label, changes=changes):
                        request = send(None) | changes
                        if request.get("id") is ABSENT:
                            request.pop("id")
                        headers = {"A2A-Version": version}
                        direct = bridge.handle("alice", request, headers)
                        status, _, response = server.request("/a2a", request,
                            token=TOKEN, headers=headers)
                        self.assertEqual(status, 200)
                        self.assertEqual(response, direct)
                        self.assertEqual(response["id"], response_id)
                        self.assertEqual(response["error"]["code"], code)
                        self.assertEqual(service.list_tasks("alice"), [])

    def test_application_input_rejections_are_protocol_parameter_errors(self):
        class RejectingService:
            def submit(self, *_args, **_kwargs):
                raise ServiceError(self.reason)

        service = RejectingService()
        with ServerFixture(service=service, a2a_bridge=A2ABridge(service)) as server:
            for reason in ("input_too_large", "input_not_encodable"):
                with self.subTest(reason=reason):
                    service.reason = reason
                    status, _, response = server.request("/a2a", send(),
                                                        headers={"A2A-Version": "1.0"})
                    self.assertEqual(status, 200)
                    self.assertEqual(response["id"], "scope-regression")
                    self.assertEqual(response["error"]["code"], -32602)
                    self.assertEqual(response["error"]["message"], reason)


if __name__ == "__main__":
    unittest.main()
