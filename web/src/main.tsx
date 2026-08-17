import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { LocaleProvider } from "./i18n";
import { SettingsPreferencesProvider } from "./settingsPreferences";
import "./app.css";
import "./workbench.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <LocaleProvider>
      <SettingsPreferencesProvider>
        <App />
      </SettingsPreferencesProvider>
    </LocaleProvider>
  </React.StrictMode>,
);
