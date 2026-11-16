"""Curated API-key presets for the OpenAI-compatible Chat Completions transport.

These presets are starting points, not model discovery or a compatibility
guarantee. Account eligibility, model options and tool support may vary.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelProvider:
    id: str
    label: str
    endpoint: str
    models: tuple[str, ...]
    docs_url: str

    @property
    def default_model(self) -> str:
        return self.models[0]


MODEL_PROVIDERS = (
    ModelProvider(
        "deepseek", "DeepSeek", "https://api.deepseek.com/chat/completions",
        ("deepseek-flash", "deepseek-v4-pro"), "https://api-docs.deepseek.com/quick_start/pricing/",
    ),
    ModelProvider(
        "openai", "OpenAI", "https://api.openai.com/v1/chat/completions",
        ("gpt-5.4-mini", "gpt-5.4"), "https://developers.openai.com/api/docs/models/gpt-5.4-mini",
    ),
    ModelProvider(
        "openrouter", "OpenRouter", "https://openrouter.ai/api/v1/chat/completions",
        ("deepseek/deepseek-v4-flash-20260731", "openai/gpt-oss-120b"),
        "https://openrouter.ai/docs/api/api-reference/chat/send-chat-completion-request",
    ),
    # This fixed endpoint is for Beijing keys. Other regions and workspace-specific
    # domains are user-entered endpoints, not guessed from the supplied secret.
    ModelProvider(
        "qwen", "通义千问 Qwen · 阿里云北京", "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
        ("qwen-plus", "qwen3.8-max"), "https://www.alibabacloud.com/help/en/model-studio/compatibility-of-openai-with-dashscope",
    ),
    ModelProvider(
        "moonshot", "Kimi · Moonshot 国际站", "https://api.moonshot.ai/v1/chat/completions",
        ("kimi-k3.1-pro", "kimi-k3.1-pro-highspeed"), "https://platform.kimi.ai/docs/get-your-api-key",
    ),
    ModelProvider(
        "zai", "智谱 GLM · Z.AI 国际站", "https://api.z.ai/api/paas/v4/chat/completions",
        ("glm-5.2", "glm-5.3"), "https://docs.z.ai/api-reference/llm/chat-completion",
    ),
    ModelProvider(
        "gemini", "Google Gemini", "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
        ("gemini-3.5-flash-lite", "gemini-3.8-flash"), "https://ai.google.dev/gemini-api/docs/openai",
    ),
    ModelProvider(
        "groq", "Groq", "https://api.groq.com/openai/v1/chat/completions",
        ("openai/gpt-oss-120b", "openai/gpt-oss-20b"), "https://console.groq.com/docs/openai",
    ),
    ModelProvider(
        "xai", "xAI", "https://api.x.ai/v1/chat/completions",
        ("grok-4.7",), "https://docs.x.ai/developers/model-capabilities/legacy/chat-completions",
    ),
)
