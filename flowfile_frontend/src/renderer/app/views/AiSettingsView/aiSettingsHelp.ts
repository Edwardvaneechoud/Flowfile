import type { PageHelpContent } from "../../components/common/PageHelpModal/types";

export const aiSettingsHelp: PageHelpContent = {
  title: "AI",
  icon: "fa-solid fa-wand-magic-sparkles",
  sections: [
    {
      title: "Two tabs, one assistant",
      icon: "fa-solid fa-info-circle",
      description:
        "Everything the AI features need lives here. What you set applies to every AI surface: the chat drawer, ⌘K, next-node suggestions, schedule text and code generation.",
      features: [
        {
          icon: "fa-solid fa-plug",
          title: "Providers",
          description:
            "Where models come from. Bring your own API keys for Anthropic, OpenAI, Google, Groq, OpenRouter or a local Ollama server, or set up On-device AI: a small model that runs on this machine with no key or account.",
        },
        {
          icon: "fa-solid fa-wand-magic-sparkles",
          title: "Assistant",
          description:
            "Pick the default provider and model, optionally route simple tasks to a cheaper model, and choose how the agent plans and applies changes.",
        },
      ],
    },
    {
      title: "Where the model picker in the chat fits in",
      icon: "fa-solid fa-comments",
      description:
        "The chat drawer's model switcher changes the same default you set on the Assistant tab — it is a shortcut, not a separate setting.",
      features: [
        {
          icon: "fa-solid fa-layer-group",
          title: "Two tiers",
          description:
            "With the cheaper-model split on, chat, agent runs, ⌘K and code generation use the main model while schedule text and settings autocomplete use the simple one.",
        },
        {
          icon: "fa-solid fa-diagram-project",
          title: "Agent variant",
          description:
            "Live (REPL) applies each step to the canvas immediately; Staged reviews every step before staging a diff; Single-shot full hands a large model the whole tool catalog at once.",
        },
      ],
    },
    {
      title: "Quick Tips",
      icon: "fa-solid fa-lightbulb",
      tips: [
        {
          type: "success",
          title: "Settings are per device",
          description:
            "Model and agent choices are saved in this browser or desktop app. API keys are saved on the server for your account.",
        },
        {
          type: "warning",
          title: "On-device AI is for simple tasks",
          description:
            "It chats, explains and builds simple flows, but it cannot run the Agent (no tool calling) and gives rougher answers than a cloud model. Manage on its row spells out what to expect.",
        },
        {
          type: "success",
          title: "Env vars still work",
          description:
            "A provider shown as env key picks its key up from the server's environment (for example ANTHROPIC_API_KEY) — no key needs to be saved here.",
        },
      ],
    },
  ],
};
