import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import "./index.css";
import { initAnalytics } from "./lib/analytics";

// Privacy-respecting analytics (Phase 89): cookie-less GoatCounter, live
// host only, all paths canonicalised so no LEI/query string is recorded.
initAnalytics();

const queryClient = new QueryClient();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </React.StrictMode>
);
