"""Provider construction must preserve independent credentials and frozen identities."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from mas_host import acceptance, demo
from mas_host import provider_config as config
from mas_host.providers import MockProvider, ProviderError, provider_binding


class ProviderConfigurationTests(unittest.TestCase):
    def test_default_mock_is_offline_even_with_live_environment(self):
        settings = config.load_provider_configs(environ={
            "MAS_PLANNER_PROVIDER": "cursor", "MAS_REVIEWER_PROVIDER": "openai",
            "MAS_PLANNER_MODEL": "live-model", "CURSOR_API_KEY": "private-key"})
        with patch.object(config, "read_api_key", side_effect=AssertionError("offline mode read a key")):
            providers = config.create_providers(settings, environ={})
        self.assertEqual(config.selected_mode(settings), "mock")
        for role, provider in providers.items():
            self.assertEqual(provider_binding(provider), provider_binding(MockProvider(role)))

    def test_openai_defaults_and_role_allowances_preserved(self):
        settings = config.load_provider_configs("openai", environ={
            "MAS_CALL_RESERVATION_MICROUSD": "300", "MAS_REVIEWER_RESERVATION_MICROUSD": "700"})
        self.assertEqual(settings["planner"], config.ProviderConfig("planner", "openai", "gpt-4.1-mini", 300))
        self.assertEqual(settings["reviewer"], config.ProviderConfig("reviewer", "openai", "gpt-4o-mini", 700))

    def test_mixed_selection_requires_explicit_valid_roles_and_cursor_model(self):
        good = {"MAS_PLANNER_PROVIDER": "cursor", "MAS_PLANNER_MODEL": "cursor-test-model",
                "MAS_REVIEWER_PROVIDER": "openai"}
        for invalid in ({}, {k: v for k, v in good.items() if k != "MAS_PLANNER_MODEL"},
                        {**good, "MAS_REVIEWER_PROVIDER": "cloud"},
                        {**good, "MAS_PLANNER_MODEL": "model\nsecret"},
                        {**good, "MAS_CALL_RESERVATION_MICROUSD": "0"},
                        {**good, "MAS_CALL_RESERVATION_MICROUSD": "1.5"},
                        {**good, "MAS_CALL_RESERVATION_MICROUSD": "100000001"}):
            with self.subTest(invalid=invalid), self.assertRaises(ProviderError):
                config.load_provider_configs("mixed", environ=invalid)
        settings = config.load_provider_configs("mixed", environ=good)
        self.assertEqual(config.selected_mode(settings), "mixed")
        self.assertEqual(config.selected_models(settings), {
            "planner": "cursor-test-model", "reviewer": "gpt-4o-mini", "checker": "python"})

    def test_cursor_only_requires_no_openai_key(self):
        environment = {"MAS_PLANNER_MODEL": "cursor-one", "MAS_REVIEWER_MODEL": "cursor-two",
                       "CURSOR_API_KEY": "cursor-private-key"}
        settings = config.load_provider_configs("cursor", environ=environment)
        calls = []
        def construct(item, key):
            calls.append((item, key))
            return object()
        with patch.dict(config._FACTORIES, {"cursor": construct}):
            config.create_providers(settings, environ=environment)
        self.assertEqual([key for _, key in calls], ["cursor-private-key"] * 2)
        self.assertEqual(config.selected_mode(settings), "cursor")

    def test_mixed_credentials_are_never_exchanged(self):
        environment = {"MAS_PLANNER_PROVIDER": "cursor", "MAS_PLANNER_MODEL": "cursor-model",
                       "MAS_REVIEWER_PROVIDER": "openai", "CURSOR_API_KEY": "cursor-secret",
                       "OPENAI_API_KEY": "openai-secret"}
        settings = config.load_provider_configs("mixed", environ=environment)
        calls = []
        def construct(item, key):
            calls.append((item.provider, item.role, key))
            return object()
        with patch.dict(config._FACTORIES, {"cursor": construct, "openai": construct}):
            config.create_providers(settings, environ=environment)
        self.assertEqual(calls, [("cursor", "planner", "cursor-secret"), ("openai", "reviewer", "openai-secret")])
        self.assertNotIn("secret", repr(settings))

    def test_cursor_transport_is_explicit_and_applies_only_to_cursor(self):
        environment = {"MAS_PLANNER_PROVIDER": "cursor", "MAS_PLANNER_MODEL": "cursor-model",
                       "MAS_REVIEWER_PROVIDER": "openai",
                       "MAS_CURSOR_TRANSPORT": "environment-proxy-http1"}
        settings = config.load_provider_configs("mixed", environ=environment)
        self.assertEqual(settings["planner"].cursor_transport, "environment-proxy-http1")
        self.assertEqual(settings["reviewer"].cursor_transport, "sdk-default")
        with patch("mas_host.cursor_session.check_runtime"), \
                patch("mas_host.invoice_cursor.InvoiceCursorProvider") as provider:
            config._cursor(settings["planner"], "fixture-key")
        self.assertEqual(provider.call_args.kwargs["transport_policy"], "environment-proxy-http1")
        with self.assertRaises(ProviderError):
            config.load_provider_configs("mixed", environ={**environment, "MAS_CURSOR_TRANSPORT": "auto"})
        with self.assertRaises(ProviderError):
            config.ProviderConfig("planner", "openai", "model", 100, "environment-proxy-http1")
        # An unselected provider cannot make offline or OpenAI-only mode fail.
        for mode in ("mock", "openai"):
            settings = config.load_provider_configs(mode, environ={"MAS_CURSOR_TRANSPORT": "invalid"})
            self.assertTrue(all(item.cursor_transport == "sdk-default" for item in settings.values()))

    def test_all_credentials_validated_before_any_client_is_constructed(self):
        settings = config.load_provider_configs("mixed", environ={
            "MAS_PLANNER_PROVIDER": "cursor", "MAS_PLANNER_MODEL": "cursor-model",
            "MAS_REVIEWER_PROVIDER": "openai"})
        with patch.dict(config._FACTORIES, {"cursor": lambda *_: self.fail("client constructed"),
                                           "openai": lambda *_: self.fail("client constructed")}):
            with self.assertRaises(config.CredentialError):
                config.create_providers(settings, environ={"CURSOR_API_KEY": "a-key"})

    def test_secret_file_precedes_environment_and_failure_does_not_fall_back(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "key"
            environment = {"CURSOR_API_KEY_FILE": str(key), "CURSOR_API_KEY": "env-value"}
            with self.assertRaises(config.CredentialError):
                config.read_api_key("cursor", environ=environment)
            key.write_text("file-value\n")
            self.assertEqual(config.read_api_key("cursor", environ=environment), "file-value")
            for source in ("", "x" * 4097, "bad key", "bad\nheader"):
                key.write_text(source)
                with self.subTest(length=len(source)), self.assertRaises(config.CredentialError) as caught:
                    config.read_api_key("cursor", environ=environment)
                self.assertNotIn(str(key), str(caught.exception))

    def test_invalid_configuration_rejected_before_state_is_created(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            state = Path(directory) / "missing"
            report = acceptance.run_live_acceptance(state_dir=state, mode="cursor")
            self.assertEqual(report["status"], "blocked_provider_configuration")
            self.assertFalse(state.exists())

    def test_live_acceptance_uses_same_factory_and_reports_submission_unit(self):
        environment = {"MAS_PLANNER_PROVIDER": "cursor", "MAS_PLANNER_MODEL": "cursor-model",
                       "MAS_REVIEWER_PROVIDER": "openai"}
        providers = {role: MockProvider(role) for role in ("planner", "reviewer")}
        result = {"ok": True, "run_id": "fixture", "outcome": "completed", "result": {}, "status": {"calls": []}}
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, environment, clear=True), \
                patch.object(acceptance, "create_providers", return_value=providers), \
                patch.object(acceptance, "run_demo", return_value=result) as run:
            report = acceptance.run_live_acceptance(state_dir=directory, mode="mixed")
        self.assertTrue(report["ok"])
        self.assertEqual(report["providers"], {"planner": "cursor", "reviewer": "openai"})
        self.assertEqual(report["workload"]["maximum_provider_submissions"], 2)
        self.assertEqual(report["provider_internal_requests"], "not_measured")
        self.assertIs(run.call_args.kwargs["planner_provider"], providers["planner"])
        self.assertEqual(run.call_args.kwargs["mode"], "mixed")

    def test_demo_reports_effective_mixed_mode(self):
        environment = {"MAS_PLANNER_PROVIDER": "cursor", "MAS_PLANNER_MODEL": "cursor-model",
                       "MAS_REVIEWER_PROVIDER": "openai"}
        providers = {role: MockProvider(role) for role in ("planner", "reviewer")}
        output = io.StringIO()
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(demo, "create_providers", return_value=providers), \
                patch.object(demo, "run_demo", return_value={"ok": True}) as run, redirect_stdout(output):
            code = demo.main(["--mode", "mixed"])
        self.assertEqual(code, 0)
        self.assertEqual(run.call_args.kwargs["mode"], "mixed")
        self.assertTrue(json.loads(output.getvalue().splitlines()[-1])["ok"])


if __name__ == "__main__":
    unittest.main()
