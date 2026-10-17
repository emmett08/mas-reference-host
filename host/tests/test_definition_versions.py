"""Version availability affects execution, never ownership of retained history."""
from dataclasses import replace
import hashlib
from pathlib import Path
import tempfile
import unittest

from mas_host.definitions import DefinitionRegistry, canonical
from mas_host.operations import DeploymentLedger, backup_state, restore_state
from mas_host.service_contracts import ServiceError
from mas_host.task_service import DurableTaskService, ExecutionResult, Workflow
from mas_host.workflow_engine import WorkflowEngine
from mas_host.workflow_plan import WorkflowPlan


class Adapter:
    def handle(self, _context, _request):
        raise AssertionError("version tests use the injected execution boundary")


def registered(version="1", **changes):
    return Workflow(version, {"agents": ["a", "b"], "application": "agent-task/1"},
                    {"a": Adapter(), "b": Adapter()}, "a", "b", "work", **changes)


def source(version="1", *, subworkflow=False):
    return {"format": "mas-workflow/1", "name": "plan", "version": version,
            "limits": {"max_steps": 20, "max_parallel": 2, "wall_seconds": 300,
                       "max_calls": 20, "max_reserved_microusd": 0},
            "root": {"kind": "subworkflow" if subworkflow else "task",
                     "workflow": "child" if subworkflow else "leaf"}}


class Executor:
    def __init__(self):
        self.calls = []

    def execute(self, workflow, execution, **_options):
        self.calls.append((workflow.version, execution["run_id"]))
        return ExecutionResult("completed", {"version": workflow.version})


class DefinitionVersionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        self.executor = Executor()
        self.original = registered()

    def service(self, workflows=None, *, grants=None, **options):
        return DurableTaskService(self.state, {"leaf": self.original} if workflows is None else workflows,
                                  {"owner": {"leaf"}} if grants is None else grants,
                                  executor=self.executor, **options)

    def engine(self, workflow=None, *, plans=None, workflows=None, **options):
        return WorkflowEngine(self.state, {"plan": source()} if plans is None else plans,
                              {"leaf": workflow or self.original} if workflows is None else workflows,
                              {"owner": {"plan"}}, **options)

    def test_new_identity_binds_verified_source_without_relabelling_legacy(self):
        descriptor = {"version": "1", "topology": self.original.topology,
                      "sender": "a", "recipient": "b", "operation": "work", "max_depth": 8,
                      "continuation": False, "classifier": False, "adapter_factory": False}
        legacy = hashlib.sha256(canonical(descriptor).encode()).hexdigest()
        self.assertNotEqual(self.original.identity, legacy)
        registry = self.service().definitions
        registry.remember("workflow", "leaf", legacy, descriptor=descriptor)
        self.assertEqual(registry.inspect("workflow", "leaf", legacy)["descriptor"], descriptor)
        self.assertEqual(registry.inspect("workflow", "leaf", legacy)["implementation_provenance"], "not_recorded")
        pinned = replace(self.original, implementation_digest=self.original.verified_implementation.digest)
        self.assertEqual(pinned.identity, self.original.identity)
        with self.assertRaisesRegex(ServiceError, "implementation digest mismatch"):
            replace(self.original, implementation_digest="a" * 64)
        with self.assertRaises(ServiceError):
            replace(self.original, implementation_digest="unverified-package-name")

    def test_current_alias_advances_while_old_requests_keep_their_identity(self):
        old_service = self.service()
        old = old_service.submit("owner", "old", "leaf", {"value": 1})
        new_service = self.service({"leaf": registered("2")})
        fresh = new_service.submit("owner", "new", "leaf", {})
        replay = new_service.submit("owner", "old", "leaf", {"value": 1})
        self.assertEqual(replay["task_id"], old["task_id"])
        with self.assertRaises(ServiceError) as error:
            new_service.submit("owner", "old", "leaf", {"value": 2})
        self.assertEqual(error.exception.code, "request_conflict")
        with self.assertRaises(ServiceError) as error:
            old_service.submit("owner", "stale-process", "leaf", {})
        self.assertEqual(error.exception.code, "workflow_changed")
        self.assertEqual(new_service.run_once()["task_id"], fresh["task_id"])
        self.assertEqual(new_service.inspect("owner", old["task_id"])["state"], "blocked")
        self.assertEqual(self.executor.calls[0][0], "2")

    def test_removed_old_name_does_not_block_unrelated_jobs_or_history(self):
        old = self.service().submit("owner", "old", "leaf", {})
        service = self.service({"other": registered("2")}, grants={"owner": {"leaf", "other"}})
        fresh = service.submit("owner", "new", "other", {})
        self.assertEqual(service.run_once()["task_id"], fresh["task_id"])
        self.assertEqual(service.inspect("owner", old["task_id"])["state"], "blocked")
        self.assertEqual(service.cancel("owner", old["task_id"])["state"], "cancelled")
        self.assertEqual(len(service.list_tasks("owner")), 2)
        with self.assertRaises(ServiceError):
            service.inspect("intruder", old["task_id"])

    def test_exact_retained_version_restores_blocked_assignment_without_new_run(self):
        service = self.service()
        old = service.submit("owner", "old", "leaf", {})
        with service.store.transaction(readonly=True) as db:
            assignment = dict(db.execute("SELECT * FROM executions").fetchone())
        absent = self.service({"leaf": registered("2")})
        self.assertEqual(absent.run_once()["state"], "blocked")
        restored = self.service({"leaf": registered("2")}, workflow_versions={"leaf": (self.original,)})
        self.assertEqual(restored.run_once()["result"], {"version": "1"})
        self.assertEqual(self.executor.calls, [("1", assignment["run_id"])])
        self.assertEqual(restored.inspect("owner", old["task_id"])["revision"], 0)

    def test_missing_code_after_execution_intent_preserves_uncertainty(self):
        service = self.service()
        old = service.submit("owner", "old", "leaf", {})
        with service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='running' WHERE id=?", (old["task_id"],))
            db.execute("UPDATE executions SET state='running' WHERE task_id=?", (old["task_id"],))
        absent = self.service({"leaf": registered("2")})
        self.assertEqual(absent.run_once()["state"], "unknown")
        self.assertEqual(absent.cancel("owner", old["task_id"])["state"], "unknown")
        restored = self.service({"leaf": registered("2")}, workflow_versions={"leaf": (self.original,)})
        self.assertIsNone(restored.run_once(), "returning code cannot authorise uncertain replay")
        self.assertEqual(self.executor.calls, [])

    def test_continuation_uses_original_version_not_new_alias(self):
        original = registered(continue_input=lambda prior, supplied: {"original": supplied["value"]})
        service = self.service({"leaf": original})
        old = service.submit("owner", "old", "leaf", {})
        with service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='input_required',result='{}' WHERE id=?", (old["task_id"],))
            db.execute("UPDATE executions SET state='input_required' WHERE task_id=?", (old["task_id"],))
        upgraded = self.service({"leaf": registered("2", continue_input=lambda prior, supplied: {"wrong": True})},
                                workflow_versions={"leaf": (original,)})
        continued = upgraded.update("owner", old["task_id"], "answer", {"value": 9})
        self.assertEqual(continued["revision"], 1)
        with upgraded.store.transaction(readonly=True) as db:
            row = db.execute("SELECT input FROM executions WHERE task_id=? AND revision=1", (old["task_id"],)).fetchone()
        self.assertEqual(row[0], '{"original":9}')

    def test_history_only_service_and_readonly_backup_need_no_adapters(self):
        service = self.service()
        old = service.submit("owner", "old", "leaf", {})
        service.run_once()
        backup_state(self.state, self.root / "backup")
        restore_state(self.root / "backup", self.root / "restored")
        restored = DurableTaskService(self.root / "restored", {}, {"owner": {"leaf"}}, executor=self.executor)
        self.assertEqual(restored.result("owner", old["task_id"]), {"version": "1"})
        self.assertEqual(restored.inspect("owner", old["task_id"])["capabilities"]["update"], False)
        self.assertEqual(restored.definitions.inspect("workflow", "leaf", self.original.identity)["implementation_provenance"], "verified_source_package")

    def test_legacy_backfill_never_invents_missing_descriptor(self):
        service = self.service()
        old = service.submit("owner", "old", "leaf", {})
        service.definitions.path.unlink()  # Simulate a pre-registry deployment.
        upgraded = self.service({"leaf": registered("2")})
        record = upgraded.definitions.inspect("workflow", "leaf", self.original.identity)
        self.assertIsNone(record["descriptor"])
        self.assertEqual(record["implementation_provenance"], "not_recorded")
        self.assertEqual(upgraded.inspect("owner", old["task_id"])["state"], "queued")
        restored = self.service({"leaf": registered("2")}, workflow_versions={"leaf": (self.original,)})
        self.assertEqual(restored.definitions.inspect("workflow", "leaf", self.original.identity)["descriptor"], self.original.descriptor)

    def test_registry_rejects_descriptor_under_a_different_identity(self):
        service = self.service()
        with self.assertRaises(ServiceError) as error:
            service.definitions.remember("workflow", "leaf", self.original.identity, descriptor=registered("2").descriptor)
        self.assertEqual(error.exception.code, "definition_identity_conflict")

    def test_retirement_requires_draining_and_preserves_history(self):
        service = self.service()
        old = service.submit("owner", "old", "leaf", {})
        upgraded = self.service({"leaf": registered("2")}, workflow_versions={"leaf": (self.original,)})
        with self.assertRaises(ServiceError) as error:
            upgraded.retire_definition("leaf", self.original.identity)
        self.assertEqual(error.exception.code, "definition_in_use")
        upgraded.cancel("owner", old["task_id"])
        self.assertTrue(upgraded.retire_definition("leaf", self.original.identity)["retired"])
        self.assertEqual(upgraded.inspect("owner", old["task_id"])["state"], "cancelled")

    def test_terminal_task_with_uncertain_execution_keeps_definition(self):
        service = self.service()
        old = service.submit('owner', 'old', 'leaf', {})
        upgraded = self.service({'leaf': registered('2')})
        with service.store.transaction() as db:
            db.execute("UPDATE tasks SET state='failed' WHERE id=?", (old['task_id'],))
            db.execute("UPDATE executions SET state='unknown' WHERE task_id=?", (old['task_id'],))
        with self.assertRaises(ServiceError) as active:
            upgraded.retire_definition('leaf', self.original.identity)
        self.assertEqual(active.exception.code, 'definition_in_use')

    def test_terminal_composition_with_uncertain_attempt_keeps_plan_and_leaf(self):
        engine = self.engine()
        old = engine.submit('owner', 'old', 'plan', {})
        with engine.store.transaction() as db:
            db.execute("UPDATE instances SET state='failed' WHERE id=?", (old['instance_id'],))
            db.execute("INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state) "
                "VALUES(?, 'root', 0, 'leaf', ?, 'retained-run', '{}', 30, 'unknown')",
                (old['instance_id'], self.original.identity))
        upgraded = self.engine(registered('2'), plans={'plan': source('2')})
        service = self.service({'leaf': registered('2')})
        for retire in (lambda: upgraded.retire_definition('plan', old['identity']),
                       lambda: service.retire_definition('leaf', self.original.identity)):
            with self.assertRaises(ServiceError) as active:
                retire()
            self.assertEqual(active.exception.code, 'definition_in_use')

    def test_composition_history_and_replay_survive_new_definition(self):
        engine = self.engine()
        first = engine.submit("owner", "old", "plan", {"value": 1})
        upgraded = self.engine(registered("2"))
        self.assertEqual(upgraded.inspect("owner", first["instance_id"])["identity"], first["identity"])
        self.assertEqual(upgraded.submit("owner", "old", "plan", {"value": 1})["instance_id"], first["instance_id"])
        self.assertEqual(upgraded.run("owner", first["instance_id"])["state"], "blocked")
        self.assertEqual(upgraded.cancel("owner", first["instance_id"])["state"], "cancelled")

    def test_persisted_subplan_source_resolves_with_retained_leaf_version(self):
        child = source(); child["name"] = "child"
        engine = self.engine(plans={"plan": source(subworkflow=True), "child": child})
        first = engine.submit("owner", "old", "plan", {})
        upgraded = self.engine(registered("2"), workflow_versions={"leaf": (self.original,)})
        with upgraded._execution_definition(upgraded._instance(first["instance_id"])) as resolved:
            self.assertEqual(resolved["root"]["kind"], "subworkflow")
            self.assertEqual(upgraded.evaluator.plans["child"].spec, child)
            self.assertEqual(upgraded._active_workflows["leaf"].version, "1")
        self.assertEqual(upgraded._active_workflows["leaf"].version, "2")

    def test_legacy_composition_with_no_matching_source_stays_inspectable(self):
        engine = self.engine()
        first = engine.submit("owner", "old", "plan", {})
        engine.definitions.path.unlink()
        upgraded = self.engine(registered("2"))
        record = upgraded.definitions.inspect("plan", "plan", first["identity"])
        self.assertIsNone(record["descriptor"])
        self.assertEqual(upgraded.run("owner", first["instance_id"])["state"], "blocked")
        self.assertEqual(upgraded.inspect("owner", first["instance_id"])["identity"], first["identity"])

    def test_leaf_retirement_respects_composition_references(self):
        engine = self.engine()
        engine.submit("owner", "old", "plan", {})
        service = self.service({"leaf": registered("2")}, workflow_versions={"leaf": (self.original,)})
        with self.assertRaises(ServiceError) as error:
            service.retire_definition("leaf", self.original.identity)
        self.assertEqual(error.exception.code, "definition_in_use")


if __name__ == "__main__":
    unittest.main()
