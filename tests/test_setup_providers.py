from urllib.parse import urlsplit

import pytest

from elephant.setup_providers import MODEL_PROVIDERS


def test_catalog_has_unique_provider_ids_and_defaults():
    ids = [provider.id for provider in MODEL_PROVIDERS]
    assert ids[0] == "deepseek"
    assert set(ids) == {"deepseek", "openai", "openrouter", "qwen", "moonshot", "zai", "gemini", "groq", "xai"}
    assert len(ids) == len(set(ids))
    assert len({provider.endpoint for provider in MODEL_PROVIDERS}) == len(ids)
    for provider in MODEL_PROVIDERS:
        assert provider.default_model == provider.models[0]
        assert len(provider.models) == len(set(provider.models))
        assert all(model and model.strip() == model and " " not in model for model in provider.models)


@pytest.mark.parametrize("provider", MODEL_PROVIDERS, ids=lambda provider: provider.id)
def test_presets_use_full_https_chat_completions_api_without_embedded_credentials(provider):
    endpoint = urlsplit(provider.endpoint)
    assert endpoint.scheme == "https"
    assert endpoint.hostname and endpoint.port in {None, 443}
    assert endpoint.username is None and endpoint.password is None
    assert not endpoint.query and not endpoint.fragment
    assert endpoint.path.endswith("/chat/completions")
    assert urlsplit(provider.docs_url).scheme == "https"
