# Trusted provider registration

`mas_host.provider_registry` lets application startup code register additional
structured-inference providers. JSON settings select an authorised provider ID;
they cannot select a Python import, executable, endpoint or credential. The
original `mock`, `openai` and `cursor` settings keep their provider descriptors.
`cursor` is an alias for the descriptor's existing `cursor-local` identity.

Each `ProviderRegistration` binds a provider ID, implementation version, factory,
capabilities and optional aliases. `ProviderRegistry.with_provider()` creates a
new immutable registry and rejects duplicate names or alias collisions. Register
only trusted factories; this API is not a sandbox for third-party Python code.

Pass the same registry to configuration loading and adapter creation:

```python
from mas_host.agent_config import load_agent_configs, create_agent_adapters
from mas_host.provider_registry import (
    DEFAULT_PROVIDER_REGISTRY, ProviderCapabilities, ProviderRegistration,
)

# This is an explicit import in trusted application code, not a path from JSON.
from my_application.providers import LocalInferenceProvider

def create_local(config, context):
    return LocalInferenceProvider(
        model=config.model,
        role=config.role,
        reservation_microusd=config.reservation_microusd,
        environ=context.environ,
    )

registry = DEFAULT_PROVIDER_REGISTRY.with_provider(ProviderRegistration(
    provider="application-local",
    implementation="application-local/1",
    factory=create_local,
    capabilities=ProviderCapabilities(accounting="none"),
))

configs = load_agent_configs({
    "specialist": {
        "provider": "application-local",
        "model": "application-model-v1",
        "reservation_microusd": 0,
        "instructions": "Classify the supplied text.",
        "operation": "classify/1",
    },
}, contracts=application_contracts, registry=registry)
adapters = create_agent_adapters(
    configs, registered_actors=registered_actors, registry=registry,
)
```

`LocalInferenceProvider` implements the existing `StructuredProvider` protocol:
`descriptor()`, `reservation_microusd` and
`infer(StructuredRequest, request_id=...) -> Invocation`. Its descriptor must
identify provider `application-local`, implementation `application-local/1`,
contract `structured-inference/1` and the requested model, role and reservation.
The host rejects a factory result whose binding disagrees with its registration.
`host/tests/test_provider_registry.py` contains an executable fourth provider
which classifies text and runs through the normal admitted adapter.

Factories construct configuration bindings without opening SDK sessions or
performing inference. They defer credentials, SDK loading and external effects
until `infer()` runs through `StructuredAgentAdapter`. Environment values and
fixture content are excluded from registry metadata. Provider descriptors remain
public: application providers must keep secrets out of them.

The capability descriptor covers structured-output dialects and enforcement,
tools, sessions, remote cancellation, reconciliation, streaming, output-token
limits, endpoint ownership, transport policies and allocation accounting. These
are capabilities exposed by the registered implementation, not a vendor feature
list. For example, the restricted Cursor adapter declares no tool, resumable
session, streaming, remote cancellation or reconciliation support.

An optional `required_capabilities` JSON array requests capabilities explicitly;
unsupported and unknown names fail during configuration loading. For example,
`["output_tokens"]` is accepted for the OpenAI registration and rejected for the
restricted Cursor registration. `structured_output_dialect` defaults to
`mas-integer-json-schema/1`, the host's bounded integer-JSON object contract with
local Draft 2020-12 schema validation. A registration's optional `validate_config`
callable can enforce a trusted model allow-list before any factory is invoked.

`accounting="none"` requires a zero reservation. The default
`accounting="configured-reservation"` requires a positive reservation bounded by
the existing host limit; this remains an allocation, not a provider invoice.

Use `registry.descriptor()` for public discovery. It returns copies of sorted
registrations and capability metadata. Registration identities bind their public
metadata; changing a registration between loading and construction fails before
factory invocation. Custom or modified registrations are also included in the
provider and adapter descriptors. Reloading configuration after a capability,
dialect or endpoint-policy change therefore changes the durable effect identity,
even if its implementation ID was mistakenly left unchanged. A change to factory behaviour requires a new explicit
implementation version. Existing built-in provider and adapter descriptors are
unchanged, preserving their recovery identities.
