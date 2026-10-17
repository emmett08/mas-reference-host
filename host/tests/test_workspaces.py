"""Workspace authority, snapshot integrity, context selection and real exec."""
from dataclasses import FrozenInstanceError
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

from mas_host import RecoveryRequired, Runner, RunnerError
from mas_host.artifacts import ArtifactError, ArtifactRef, ArtifactStore
from mas_host.context_snapshots import ContextPolicy, TokenCounter, build_context
from mas_host.executable_worker import RegisteredProcessExecutor, WorkerSpec
from mas_host.journal import inspect_run
from mas_host.operations import DeploymentLedger, OperationsError, _file_lock
from mas_host.workspaces import (WorkspaceAdapter, WorkspaceError, WorkspaceExecutor,
    WorkspaceInput, WorkspaceManager, WorkspaceOutput, WorkspacePolicy, WorkspaceTool, WorkspaceTools)
from test_runner import topology


ROOT = Path(__file__).resolve().parents[2]
BINARIES = ROOT / "rust-protocol/target/release"
FIXTURE = Path(__file__).resolve().parent / "fixtures/workspace_worker.py"


class WorkspaceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = ArtifactStore(self.root / "artifacts", namespace="project", principal="alice")
        self.manager = WorkspaceManager(self.root / "workspaces", principal="alice", artifacts=self.store)
        self.policy = WorkspacePolicy("coding", "1", ("uppercase",))
        self.source = self.store.put_bytes(b"approved source", media_type="text/plain")

    def prepared(self, **kwargs):
        return self.manager.prepare("run-1", "attempt-1", self.policy,
            (WorkspaceInput("source.txt", self.source),), **kwargs)

    def tool(self, *, mode="normal", worker_file=FIXTURE, external=False):
        worker = WorkerSpec("uppercase", "1", (sys.executable, str(worker_file)),
            factory="workspace-fixture", configuration={"mode": mode, "outside": str(self.root / "outside")},
            cwd=ROOT, environment={"PYTHONPATH": str(ROOT / "host")},
            pinned_files={str(worker_file): hashlib.sha256(worker_file.read_bytes()).hexdigest()})
        return WorkspaceTool("uppercase", worker, reservation_microusd=1, external_effects=external)

    def runner(self, reference, tool=None):
        tools = WorkspaceTools((tool or self.tool(),))
        adapter = WorkspaceAdapter(self.manager, reference, self.policy,
            tools.resolve("uppercase", self.policy), (WorkspaceOutput("result.txt", "text/plain"),))
        self.registered_adapters = {actor: adapter for actor in ("a", "b", "c")}
        runner = Runner(topology(), self.registered_adapters, self.root / "run",
            mas_bin=BINARIES / "mas", gateway_bin=BINARIES / "mas-gateway",
            workflow_version="workspace-test/1",
            executor=WorkspaceExecutor(RegisteredProcessExecutor(BINARIES / "mas-gateway")))
        self.addCleanup(runner.close)
        return runner

    def test_deterministic_private_inputs_and_changed_manifest_rejected(self):
        reference = self.prepared()
        self.assertEqual(self.prepared().identity, reference.identity)
        source = self.manager.root / reference.workspace_id / "inputs/source.txt"
        self.assertEqual(source.read_bytes(), b"approved source")
        self.assertEqual(source.stat().st_mode & 0o777, 0o400)
        replacement = self.store.put_bytes(b"new source", media_type="text/plain")
        with self.assertRaisesRegex(WorkspaceError, "manifest changed"):
            self.manager.prepare("run-1", "attempt-1", self.policy, (WorkspaceInput("source.txt", replacement),))
        source.chmod(0o600)
        source.write_bytes(b"modified")
        with self.assertRaisesRegex(WorkspaceError, "snapshot changed"):
            self.manager.verify(reference)

    def test_cross_principal_inputs_and_workspace_operations_are_denied(self):
        bob = ArtifactStore(self.root / "artifacts", namespace="project", principal="bob")
        manager = WorkspaceManager(self.root / "workspaces", principal="bob", artifacts=bob)
        with self.assertRaises(ArtifactError):
            manager.prepare("run-1", "attempt-1", self.policy, (WorkspaceInput("source.txt", self.source),))
        reference = self.prepared()
        with self.assertRaisesRegex(WorkspaceError, "not owned"):
            manager.verify(reference)
        with self.assertRaisesRegex(WorkspaceError, "principal"):
            WorkspaceManager(self.root / "other", principal="bob", artifacts=self.store)

    def test_traversal_symlink_hardlink_and_untrusted_profiles_are_rejected(self):
        for path in ("../outside", "/absolute", "a/../../x", "a\\b", "a//b"):
            with self.subTest(path=path), self.assertRaises(WorkspaceError):
                WorkspaceInput(path, self.source)
        with self.assertRaises(WorkspaceError):
            WorkspacePolicy("unsafe", "1", ("uppercase",), profile="isolated-untrusted-code")
        self.root.joinpath("linked").symlink_to(self.manager.root, target_is_directory=True)
        with self.assertRaises(WorkspaceError):
            WorkspaceManager(self.root / "linked", principal="alice", artifacts=self.store)
        reference = self.prepared()
        source = self.manager.root / reference.workspace_id / "inputs/source.txt"
        source.unlink()
        outside = self.root / "outside"
        outside.write_bytes(b"approved source")
        os.link(outside, source)
        with self.assertRaisesRegex(WorkspaceError, "private regular"):
            self.manager.verify(reference)

    def test_tool_allowlist_and_external_effect_capability_cannot_be_expanded(self):
        tools = WorkspaceTools((self.tool(),))
        with self.assertRaisesRegex(WorkspaceError, "not authorised"):
            tools.resolve("request-selected-command", self.policy)
        with self.assertRaises(FrozenInstanceError):
            tools._tools = {}
        with self.assertRaisesRegex(WorkspaceError, "external effects"):
            WorkspaceTools((self.tool(external=True),)).resolve("uppercase", self.policy)

    def test_input_quotas_reject_before_workspace_materialisation(self):
        policy = WorkspacePolicy("tiny", "1", ("uppercase",), max_input_bytes=3)
        before = set(self.manager.root.iterdir())
        with self.assertRaisesRegex(WorkspaceError, "quota"):
            self.manager.prepare("run", "attempt", policy, (WorkspaceInput("source.txt", self.source),))
        self.assertEqual(set(self.manager.root.iterdir()), before)

    def test_cleanup_requires_resolution_and_a_quiescent_lease(self):
        reference = self.prepared()
        with self.assertRaisesRegex(WorkspaceError, "not been released"):
            self.manager.cleanup(reference)
        resolution = self.store.put_bytes(b"operator verified no outstanding effects", media_type="text/plain")
        with self.manager.lease(reference):
            with self.assertRaisesRegex(WorkspaceError, "busy"):
                self.manager.release(reference, resolution=resolution)
        self.manager.release(reference, resolution=resolution)
        with self.manager.lease(reference):
            with self.assertRaisesRegex(WorkspaceError, "busy"):
                self.manager.cleanup(reference)
        self.manager.cleanup(reference)
        self.assertFalse((self.manager.root / reference.workspace_id).exists())
        self.assertTrue(self.store.verify(self.source))
        with self.assertRaisesRegex(WorkspaceError, "retired"):
            self.prepared()

    def test_release_recovers_output_retention_after_publication_before_state_crash(self):
        reference = self.prepared()
        (self.manager.root / reference.workspace_id / "outputs/result.txt").write_bytes(b"result")
        with self.manager.lease(reference) as (directory, _):
            self.manager._state(directory, "running")
            published = self.manager._publish(reference, directory, self.policy,
                (WorkspaceOutput("result.txt", "text/plain"),))
            # Simulate process death after the retention link commits, before
            # execute() can persist its quiescent/unknown state and references.
        state = json.loads((self.manager.root / reference.workspace_id / "state.json").read_bytes())
        self.assertNotIn("artifacts", state)
        link = "workspace-output-" + reference.workspace_id[3:]
        self.assertEqual(self.store.status()["links"][link]["state"], "live")
        restarted = WorkspaceManager(self.manager.root, principal="alice", artifacts=self.store)
        resolution = self.store.put_bytes(b"operator resolved interrupted execution", media_type="text/plain")
        restarted.release(reference, resolution=resolution)
        self.assertEqual(self.store.status()["links"][link]["state"], "released")
        restarted.release(reference, resolution=resolution)
        restarted.cleanup(reference)
        self.assertTrue(self.store.verify(published[0]))

    def test_audit_only_workspace_rejects_creation_and_existing_manager_mutation(self):
        reference = self.prepared()
        resolution = self.store.put_bytes(b"resolved", media_type="text/plain")
        marker = self.root / ".restored-audit-only"
        marker.write_text("restored")
        before = sorted(str(path.relative_to(self.root)) for path in self.root.rglob("*"))
        for operation in (
                lambda: WorkspaceManager(self.root / "new-workspaces", principal="alice", artifacts=self.store),
                lambda: self.manager.prepare("new-run", "attempt", self.policy, ()),
                lambda: self.manager.release(reference, resolution=resolution),
                lambda: self.manager.cleanup(reference)):
            with self.subTest(operation=operation), self.assertRaisesRegex(WorkspaceError, "audit-only"):
                operation()
        self.assertEqual(sorted(str(path.relative_to(self.root)) for path in self.root.rglob("*")), before)

    def test_workspace_lease_holds_deployment_maintenance_barrier(self):
        reference = self.prepared()
        DeploymentLedger(self.root)
        with self.manager.lease(reference):
            with self.assertRaisesRegex(OperationsError, "busy"):
                with _file_lock(self.root / ".deployment.lock", True, nonblocking=True):
                    self.fail("maintenance acquired the workspace mutation barrier")
        with _file_lock(self.root / ".deployment.lock", True, nonblocking=True):
            pass

    def test_context_is_bounded_exact_authorised_and_recoverable(self):
        first = self.store.put_bytes(b"one two", media_type="text/plain")
        second = self.store.put_bytes(b"three four", media_type="text/plain")
        tokeniser = TokenCounter("test-words", "1", lambda text: len(text.split()))
        context = build_context(self.store, (first, second), policy=ContextPolicy(max_tokens=3), tokeniser=tokeniser)
        snapshot = json.loads(self.store.read_bytes(context))
        self.assertEqual(snapshot["text"], "one two")
        self.assertEqual(snapshot["tokens"], 2)
        self.assertEqual(snapshot["omitted"], [second.artifact_id])
        self.assertEqual(snapshot["omission_reason"], "token_limit")
        self.assertEqual(build_context(self.store, (first, second), policy=ContextPolicy(max_tokens=3),
                                      tokeniser=tokeniser), context)
        reference = self.prepared(context=context)
        self.assertEqual((self.manager.root / reference.workspace_id / "context.json").read_bytes(),
                         self.store.read_bytes(context))
        bob = ArtifactStore(self.root / "artifacts", namespace="project", principal="bob")
        with self.assertRaises(ArtifactError):
            build_context(bob, (first,), policy=ContextPolicy(), tokeniser=tokeniser)

    def test_context_tokeniser_and_budget_changes_change_snapshot_identity(self):
        counter = TokenCounter("words", "1", lambda text: len(text.split()))
        original = build_context(self.store, (self.source,), policy=ContextPolicy(), tokeniser=counter)
        changed = build_context(self.store, (self.source,), policy=ContextPolicy(max_tokens=3), tokeniser=counter)
        self.assertNotEqual(original.artifact_id, changed.artifact_id)
        with self.assertRaises(WorkspaceError):
            build_context(self.store, (self.source,), policy=ContextPolicy(),
                          tokeniser=TokenCounter("invalid", "1", lambda text: True))

    def test_real_workspace_tool_is_admitted_publishes_provenance_and_replays(self):
        reference = self.prepared()
        runner = self.runner(reference)
        outcome = runner.request("a", "b", "uppercase", {})
        self.assertEqual(outcome.output["result"]["cwd"], reference.workspace_id)
        published = ArtifactRef.from_dict(outcome.output["artifacts"][0])
        self.assertEqual(self.store.read_bytes(published), b"APPROVED SOURCE")
        self.assertEqual(published.input_sha256s, (self.source.sha256,))
        self.assertEqual(len(runner.status()["calls"]), 1)
        self.assertEqual(inspect_run(runner.run_dir)["invocations"][0]["reservation_microusd"], 1)
        output = self.manager.root / reference.workspace_id / "outputs/result.txt"
        output.unlink()
        run_dir, adapters = runner.run_dir, self.registered_adapters
        runner.close()
        with Runner.resume(run_dir, adapters, workflow_version="workspace-test/1",
                mas_bin=BINARIES / "mas", gateway_bin=BINARIES / "mas-gateway",
                executor=WorkspaceExecutor(RegisteredProcessExecutor(BINARIES / "mas-gateway"))) as resumed:
            self.assertEqual(resumed.request("a", "b", "uppercase", {}).output, outcome.output)
        self.assertFalse(output.exists(), "replay executed the tool again")

    def test_changed_tool_script_fails_before_any_effect_admission(self):
        script = self.root / "worker.py"
        script.write_bytes(FIXTURE.read_bytes())
        tool = self.tool(worker_file=script)
        script.write_bytes(script.read_bytes() + b"\n# changed\n")
        runner = self.runner(self.prepared(), tool)
        with self.assertRaises(RunnerError):
            runner.request("a", "b", "uppercase", {})
        self.assertEqual(runner.status()["calls"], [])

    def test_worker_input_mutation_and_symlink_outputs_never_publish(self):
        for mode in ("rewrite-input", "symlink"):
            with self.subTest(mode=mode):
                # Separate attempts and Rust runs avoid conflating test states.
                manager = WorkspaceManager(self.root / mode, principal="alice", artifacts=self.store)
                reference = manager.prepare("run", "attempt", self.policy,
                    (WorkspaceInput("source.txt", self.source),))
                adapter = WorkspaceAdapter(manager, reference, self.policy, self.tool(mode=mode),
                    (WorkspaceOutput("result.txt"),))
                with Runner(topology(), {actor: adapter for actor in ("a", "b", "c")}, self.root / (mode + "-run"),
                        mas_bin=BINARIES / "mas", gateway_bin=BINARIES / "mas-gateway",
                        executor=WorkspaceExecutor(RegisteredProcessExecutor(BINARIES / "mas-gateway"))) as runner:
                    with self.assertRaises(RecoveryRequired):
                        runner.request("a", "b", "uppercase", {})
                    self.assertEqual(inspect_run(runner.run_dir)["invocations"][0]["state"], "unknown")
                self.assertEqual(self.store.status()["artifacts"], [self.source.to_dict()])


if __name__ == "__main__":
    unittest.main()
