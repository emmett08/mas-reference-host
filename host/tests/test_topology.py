"""Composition preserves explicit routes and the existing Rust configuration."""
from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError
import itertools
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from mas_host.contracts import RunnerError
from mas_host.topology import (FORMAT, compile_topology, freeze_topology,
                               inspect_topology, topology_mermaid)


def composed():
    return {
        "format": FORMAT,
        "groups": {
            "coordinate": {"kind": "star", "agents": ["hub", "worker"], "hub": "hub"},
            "review": {"kind": "mesh", "agents": ["a", "b", "c"]},
            "validate": {"kind": "tree", "agents": ["root", "branch", "leaf"],
                         "parents": {"branch": "root", "leaf": "branch"}},
        },
        "links": [["coordinate/hub", "review/a"], ["review/a", "coordinate/hub"],
                  ["coordinate/hub", "validate/root"], ["validate/root", "coordinate/hub"]],
        "requests": [["coordinate/hub", "review/a"], ["coordinate/hub", "validate/root"]],
        "application": "agent-task/1",
        "max_calls": 17,
    }


class TopologyTests(unittest.TestCase):
    def test_star_mesh_tree_compose_without_implicit_cross_group_permissions(self):
        spec = compile_topology(composed())
        self.assertEqual(spec["agents"], ["coordinate/hub", "coordinate/worker", "review/a",
                                         "review/b", "review/c", "validate/branch",
                                         "validate/leaf", "validate/root"])
        expected = {("coordinate/hub", "coordinate/worker"),
                    ("coordinate/worker", "coordinate/hub"),
                    ("validate/root", "validate/branch"),
                    ("validate/branch", "validate/root"),
                    ("validate/branch", "validate/leaf"),
                    ("validate/leaf", "validate/branch")}
        expected.update(itertools.permutations(["review/a", "review/b", "review/c"], 2))
        expected.update(map(tuple, composed()["links"]))
        self.assertEqual(set(map(tuple, spec["edges"])), expected)
        self.assertEqual(spec["max_calls"], 17)
        self.assertEqual(spec["application"], "agent-task/1")

    def test_same_group_definition_reused_across_unrelated_domains(self):
        group = {"kind": "star", "agents": ["lead", "worker"], "hub": "lead"}
        for names in (("algebra", "geometry"), ("triage", "response"), ("design", "review")):
            with self.subTest(names=names):
                spec = {"format": FORMAT, "groups": {name: group for name in names}}
                compiled = compile_topology(spec)
                self.assertEqual(len(compiled["edges"]), 4)
                self.assertEqual(len(compiled["agents"]), 4)
                self.assertTrue(all(a.split("/")[0] == b.split("/")[0]
                                    for a, b in compiled["edges"]))

    def test_custom_group_preserves_directed_routes_and_isolated_agents(self):
        spec = {"format": FORMAT, "groups": {"pipeline": {
            "kind": "custom", "agents": ["source", "sink", "isolated"],
            "edges": [["source", "sink"]]}}}
        self.assertEqual(compile_topology(spec)["edges"], [["pipeline/source", "pipeline/sink"]])
        self.assertIn("pipeline/isolated", compile_topology(spec)["agents"])

    def test_single_agent_groups_are_valid(self):
        for kind, extra in (("star", {"hub": "a"}), ("mesh", {}),
                            ("tree", {"parents": {}}), ("custom", {"edges": []})):
            spec = {"format": FORMAT, "groups": {"solo": {"kind": kind, "agents": ["a"], **extra}}}
            self.assertEqual(compile_topology(spec)["edges"], [])

    def test_declarations_do_not_depend_on_insertion_order(self):
        first = composed()
        second = copy.deepcopy(first)
        second["groups"] = dict(reversed(list(second["groups"].items())))
        for group in second["groups"].values():
            group["agents"].reverse()
        second["links"].reverse()
        second["requests"].reverse()
        self.assertEqual(compile_topology(first), compile_topology(second))
        self.assertEqual(freeze_topology(first), freeze_topology(second))
        self.assertEqual(topology_mermaid(first), topology_mermaid(second))

    def test_flat_legacy_defaults_and_omitted_acl_preserve_meaning(self):
        self.assertEqual(compile_topology({"agents": ["b", "a"]}), {
            "agents": ["a", "b"], "edges": [["a", "b"], ["b", "a"]],
            "application": None, "max_messages": 100, "max_bytes": 100_000,
            "max_calls": 20, "wall_seconds": 3600, "mailbox_capacity": 100})
        self.assertEqual(compile_topology({"agents": ["a", "b"], "edges": []})["edges"], [])

    def test_flat_explicit_null_does_not_broaden_permissions(self):
        with self.assertRaises(RunnerError):
            compile_topology({"agents": ["a", "b"], "edges": None})

    def test_existing_flat_example_metadata_does_not_change_authority(self):
        from mas_host.demo import DEFAULT_TOPOLOGY
        original = copy.deepcopy(DEFAULT_TOPOLOGY)
        compiled = compile_topology(original)
        self.assertNotIn("name", compiled)
        self.assertNotIn("description", compiled)
        self.assertEqual(set(map(tuple, compiled["edges"])), set(map(tuple, original["edges"])))
        original.pop("name")
        original.pop("description")
        self.assertEqual(compile_topology(original), compiled)

    def test_frozen_configuration_and_source_are_independent(self):
        source = composed()
        frozen = freeze_topology(source)
        fingerprint = frozen.fingerprint
        source["groups"]["review"]["agents"].append("new")
        value = frozen.configuration
        value["agents"].append("intruder")
        value["edges"].clear()
        self.assertNotIn("intruder", frozen.configuration["agents"])
        self.assertNotIn("review/new", frozen.configuration["agents"])
        self.assertEqual(frozen.fingerprint, fingerprint)
        with self.assertRaises(FrozenInstanceError):
            frozen._canonical = "{}"

    def test_fingerprint_identifies_effective_permissions_and_budgets(self):
        spec = composed()
        compiled = compile_topology(spec)
        original = freeze_topology(spec).fingerprint
        self.assertEqual(original, freeze_topology(compiled).fingerprint)
        compiled["max_calls"] += 1
        self.assertNotEqual(original, freeze_topology(compiled).fingerprint)
        compiled = compile_topology(spec)
        compiled["edges"].pop()
        self.assertNotEqual(original, freeze_topology(compiled).fingerprint)

    def test_inspection_and_mermaid_describe_routes_without_executing_code(self):
        inspection = inspect_topology(composed())
        self.assertEqual(inspection["agent_count"], 8)
        self.assertEqual(inspection["edge_count"], 16)
        self.assertEqual(len(inspection["fingerprint"]), 64)
        mermaid = topology_mermaid(composed())
        self.assertTrue(mermaid.startswith("flowchart TD\n"))
        self.assertEqual(mermaid.count(" --> "), 16)
        self.assertIn('["coordinate/hub"]', mermaid)
        with self.assertRaises(RunnerError):
            topology_mermaid({"agents": ['a"]-->b["']})

    def test_requests_require_existing_forward_and_return_routes(self):
        for route in (("review/a", "coordinate/hub"), ("coordinate/hub", "review/a")):
            with self.subTest(route=route):
                spec = composed()
                spec["links"].remove(list(route))
                with self.assertRaisesRegex(RunnerError, "forward and return"):
                    compile_topology(spec)
        # A directed notification remains permitted when no request is declared.
        spec.pop("requests")
        self.assertEqual(len(compile_topology(spec)["edges"]), 15)

    def test_unknown_self_duplicate_and_intragroup_links_are_rejected(self):
        for link in (["review/a", "unknown/a"], ["review/a", "review/a"],
                     ["review/a", "review/b"], ["coordinate/hub", "review/a"]):
            with self.subTest(link=link):
                spec = composed()
                spec["links"].append(link)
                with self.assertRaises(RunnerError):
                    compile_topology(spec)

    def test_duplicate_actors_routes_and_requests_are_rejected(self):
        for spec in ({"agents": ["a", "a"]},
                     {"agents": ["a", "b"], "edges": [["a", "b"], ["a", "b"]]},
                     composed() | {"requests": [["review/a", "review/b"], ["review/a", "review/b"]]}):
            with self.subTest(spec=spec), self.assertRaises(RunnerError):
                compile_topology(spec)

    def test_unknown_fields_and_kinds_are_rejected(self):
        for spec in ({"agents": ["a"], "agentz": []}, composed() | {"format": "mas-topology/2"},
                     composed() | {"agents": ["a"]}, composed() | {"groups": {}},
                     {"format": FORMAT, "groups": {"g": {"kind": "code", "agents": ["a"]}}},
                     {"format": FORMAT, "groups": {"g": {"kind": "mesh", "agents": ["a"], "hub": "a"}}}):
            with self.subTest(spec=spec), self.assertRaises(RunnerError):
                compile_topology(spec)

    def test_tree_rejects_cycles_and_missing_or_invalid_parent_relations(self):
        for parents in ({"b": "c", "c": "b"}, {"b": "a"},
                        {"a": "b", "b": "c", "c": "a"}, {"b": "a", "c": "missing"},
                        {"b": "b", "c": "a"}, {"b": "a", "outsider": "a"}):
            spec = {"format": FORMAT, "groups": {
                "g": {"kind": "tree", "agents": ["a", "b", "c"], "parents": parents}}}
            with self.subTest(parents=parents), self.assertRaises(RunnerError):
                compile_topology(spec)

    def test_group_identity_separator_and_qualified_length_are_checked(self):
        for name, actor in (("bad/name", "a"), ("g", "bad/name"), ("g" * 128, "a"),
                            ("g", "a" * 128), ("é", "a"), ("g", "_a")):
            with self.subTest(name=name, actor=actor), self.assertRaises(RunnerError):
                compile_topology({"format": FORMAT, "groups": {
                    name: {"kind": "mesh", "agents": [actor]}}})
        actor = "g/" + "a" * 126
        self.assertEqual(compile_topology({"agents": [actor]})["agents"], [actor])

    def test_combined_membership_limit_checked_before_mesh_expansion(self):
        group = {"kind": "mesh", "agents": [f"a{i}" for i in range(16)]}
        spec = {"format": FORMAT, "groups": {"a": group, "b": copy.deepcopy(group)}}
        self.assertEqual(len(compile_topology(spec)["agents"]), 32)
        spec["groups"]["b"]["agents"].append("extra")
        with self.assertRaises(RunnerError):
            compile_topology(spec)

    def test_budgets_match_rust_integer_and_capacity_bounds(self):
        for key in ("max_messages", "max_bytes", "max_calls", "wall_seconds", "mailbox_capacity"):
            for invalid in (0, -1, True, 1.0, "1", None, 2**63):
                with self.subTest(key=key, invalid=invalid), self.assertRaises(RunnerError):
                    compile_topology({"agents": ["a"], key: invalid})
        with self.assertRaises(RunnerError):
            compile_topology({"agents": ["a"], "mailbox_capacity": 101})
        self.assertEqual(compile_topology({"agents": ["a"], "max_calls": 2**63 - 1})["max_calls"],
                         2**63 - 1)

    def test_input_and_output_have_bounded_json(self):
        cyclic = {}
        cyclic["self"] = cyclic
        for spec in (cyclic, {"agents": ["a"], "extra": "x" * 65_536},
                     {"agents": ["a"], "extra": object()}):
            with self.subTest(kind=type(spec)), self.assertRaises(RunnerError):
                compile_topology(spec)
        # Input fits but a dense graph of long names exceeds the output bound.
        with self.assertRaises(RunnerError):
            compile_topology({"agents": [f"a{i}" + "x" * 120 for i in range(32)]})

    def test_protocol_invalid_fields_and_malformed_sequences_are_rejected(self):
        for spec in ([], {"agents": ()}, {"agents": [1]}, {"agents": ["a"], "application": "math/1"},
                     {"agents": ["a", "b"], "edges": [["a", "b", "a"]]},
                     {"agents": ["a", "b"], "edges": [["a", {}]]},
                     {"format": FORMAT, "groups": {"g": {"kind": [], "agents": ["a"]}}}):
            with self.subTest(spec=spec), self.assertRaises(RunnerError):
                compile_topology(spec)


class TopologyRustTests(unittest.TestCase):
    def test_effective_configuration_is_accepted_unchanged_by_rust(self):
        release = Path(__file__).resolve().parents[2] / "rust-protocol/target/release/mas"
        binary = Path(os.environ.get("MAS_BIN") or shutil.which("mas") or str(release))
        if not binary.is_file():
            self.skipTest("Rust release binary unavailable")
        for spec in (composed(), {"agents": ["b", "a"], "application": "agent-task/1"},
                     {"agents": ["a"], "edges": [], "application": "agent-task/1"}):
            with self.subTest(spec=spec), tempfile.TemporaryDirectory() as directory:
                configuration = compile_topology(spec)
                path = Path(directory) / "topology.json"
                path.write_text(json.dumps(configuration))
                result = subprocess.run([str(binary), "init", str(Path(directory) / "state.sqlite"),
                                         "test-run", str(path)], capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(json.loads(result.stdout)["result"]["config"], configuration)


if __name__ == "__main__":
    unittest.main()
