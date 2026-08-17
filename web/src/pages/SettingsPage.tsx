import { useMemo, useState, type ReactNode } from "react";
import { useI18n, type Locale } from "../i18n";
import { useSettingsPreferences } from "../settingsPreferences";
import type { AccentPreference, MotionPreference } from "../settingsPreferencesModel";
import { Icon, type IconName } from "../workbench/Icons";

type SettingsSection = "general" | "appearance" | "about";

const SECTIONS: Array<{
  id: SettingsSection;
  icon: IconName;
  label: "settings.general" | "settings.appearance" | "settings.about";
}> = [
  { id: "general", icon: "settings", label: "settings.general" },
  { id: "appearance", icon: "monitor", label: "settings.appearance" },
  { id: "about", icon: "info", label: "settings.about" },
];

const ACCENTS: Array<{
  id: AccentPreference;
  label:
    | "settings.accent.pulse"
    | "settings.accent.blue"
    | "settings.accent.violet"
    | "settings.accent.green";
}> = [
  { id: "pulse", label: "settings.accent.pulse" },
  { id: "blue", label: "settings.accent.blue" },
  { id: "violet", label: "settings.accent.violet" },
  { id: "green", label: "settings.accent.green" },
];

const MOTION_OPTIONS: Array<{
  id: MotionPreference;
  label: "settings.motion.system" | "settings.motion.reduce";
}> = [
  { id: "system", label: "settings.motion.system" },
  { id: "reduce", label: "settings.motion.reduce" },
];

function SettingRow({
  title,
  description,
  children,
}: {
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <div className="pw-setting-row">
      <div className="pw-setting-copy">
        <strong>{title}</strong>
        <p>{description}</p>
      </div>
      <div className="pw-setting-control">{children}</div>
    </div>
  );
}

export function SettingsPage({ onClose }: { onClose: () => void }) {
  const { locale, setLocale, t } = useI18n();
  const { accent, motion, setAccent, setMotion } = useSettingsPreferences();
  const [section, setSection] = useState<SettingsSection>("general");
  const shortcutModifier = useMemo(
    () => (/Mac|iPhone|iPad|iPod/.test(navigator.platform) ? "⌘" : "Ctrl"),
    [],
  );

  return (
    <section className="pw-settings" aria-labelledby="settings-title">
      <header className="pw-settings-header">
        <div>
          <Icon name="settings" size={18} />
          <h1 id="settings-title">{t("settings.title")}</h1>
        </div>
        <button
          className="pw-icon-button"
          type="button"
          aria-label={t("settings.close")}
          title={t("settings.close")}
          onClick={onClose}
        >
          <Icon name="x" size={17} />
        </button>
      </header>

      <div className="pw-settings-layout">
        <nav className="pw-settings-nav" aria-label={t("settings.navigation")}>
          {SECTIONS.map((item) => (
            <button
              key={item.id}
              type="button"
              className={section === item.id ? "is-active" : ""}
              aria-current={section === item.id ? "page" : undefined}
              onClick={() => setSection(item.id)}
            >
              <Icon name={item.icon} size={16} />
              <span>{t(item.label)}</span>
            </button>
          ))}
        </nav>

        <div className="pw-settings-content">
          {section === "general" && (
            <div className="pw-settings-panel">
              <div className="pw-settings-intro">
                <h2>{t("settings.general")}</h2>
                <p>{t("settings.generalDescription")}</p>
              </div>
              <div className="pw-settings-group">
                <SettingRow
                  title={t("settings.language.label")}
                  description={t("settings.language.description")}
                >
                  <label className="pw-setting-select">
                    <Icon name="globe" size={15} />
                    <select
                      aria-label={t("settings.language.label")}
                      value={locale}
                      onChange={(event) => setLocale(event.target.value as Locale)}
                    >
                      <option value="en">{t("locale.english")}</option>
                      <option value="zh-CN">{t("locale.chinese")}</option>
                    </select>
                    <Icon name="chevronDown" size={13} />
                  </label>
                </SettingRow>
                <SettingRow
                  title={t("settings.storage.label")}
                  description={t("settings.storage.description")}
                >
                  <span className="pw-setting-value">
                    <Icon name="monitor" size={13} />
                    {t("settings.storage.localOnly")}
                  </span>
                </SettingRow>
                <SettingRow
                  title={t("settings.shortcut.label")}
                  description={t("settings.shortcut.description")}
                >
                  <kbd className="pw-setting-shortcut">{shortcutModifier}+,</kbd>
                </SettingRow>
              </div>
            </div>
          )}

          {section === "appearance" && (
            <div className="pw-settings-panel">
              <div className="pw-settings-intro">
                <h2>{t("settings.appearance")}</h2>
                <p>{t("settings.appearanceDescription")}</p>
              </div>
              <div className="pw-settings-group">
                <SettingRow
                  title={t("settings.theme.label")}
                  description={t("settings.theme.description")}
                >
                  <span className="pw-setting-value">{t("settings.theme.dark")}</span>
                </SettingRow>
                <SettingRow
                  title={t("settings.accent.label")}
                  description={t("settings.accent.description")}
                >
                  <div className="pw-accent-options" role="group" aria-label={t("settings.accent.label")}>
                    {ACCENTS.map((option) => (
                      <button
                        key={option.id}
                        type="button"
                        className={`is-${option.id}${accent === option.id ? " is-selected" : ""}`}
                        aria-label={t(option.label)}
                        aria-pressed={accent === option.id}
                        title={t(option.label)}
                        onClick={() => setAccent(option.id)}
                      >
                        <span />
                      </button>
                    ))}
                  </div>
                </SettingRow>
                <SettingRow
                  title={t("settings.motion.label")}
                  description={t("settings.motion.description")}
                >
                  <div className="pw-setting-segments" role="group" aria-label={t("settings.motion.label")}>
                    {MOTION_OPTIONS.map((option) => (
                      <button
                        key={option.id}
                        type="button"
                        className={motion === option.id ? "is-selected" : ""}
                        aria-pressed={motion === option.id}
                        onClick={() => setMotion(option.id)}
                      >
                        {t(option.label)}
                      </button>
                    ))}
                  </div>
                </SettingRow>
              </div>
            </div>
          )}

          {section === "about" && (
            <div className="pw-settings-panel">
              <div className="pw-settings-intro">
                <h2>{t("settings.about")}</h2>
                <p>{t("settings.aboutDescription")}</p>
              </div>
              <div className="pw-settings-group">
                <SettingRow
                  title={t("settings.product.label")}
                  description={t("settings.product.description")}
                >
                  <span className="pw-setting-value">{t("settings.product.name")}</span>
                </SettingRow>
                <SettingRow
                  title={t("settings.version.label")}
                  description={t("settings.version.description")}
                >
                  <code className="pw-setting-code">0.2.0-alpha.1</code>
                </SettingRow>
                <SettingRow
                  title={t("settings.localeState.label")}
                  description={t("settings.localeState.description")}
                >
                  <code className="pw-setting-code">{locale}</code>
                </SettingRow>
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
