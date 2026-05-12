import { useEffect, useRef, useState } from "react";
import type { SourceInfo } from "../lib/api";

type ChipState = "idle" | "querying" | "done";

/**
 * Animated source-status grid shown while OpenCheck is querying.
 *
 * Each chip cycles: idle → querying (pulsing dot) → done (green).
 *
 * Because the backend returns all results in a single response there
 * is no real per-source progress signal, so we simulate staggered
 * completion to convey that multiple APIs are being queried in
 * parallel. The animation loops automatically if the real response
 * takes longer than one full cycle (~4 s for a 12-source set).
 */
export default function SearchLoadingGrid({
  sources,
}: {
  sources: SourceInfo[];
}) {
  const [states, setStates] = useState<ChipState[]>(() =>
    sources.map(() => "idle")
  );
  const [doneCount, setDoneCount] = useState(0);

  // Stable refs so the recursive cycle never captures stale state.
  const timersRef = useRef<ReturnType<typeof setTimeout>[]>([]);
  // Incremented on every new cycle and on unmount — lets stale
  // timers detect they belong to a cancelled run and bail out.
  const cycleRef = useRef(0);

  useEffect(() => {
    if (sources.length === 0) return;

    function runCycle(cycle: number) {
      const n = sources.length;
      const order = [...Array(n).keys()].sort(() => Math.random() - 0.5);

      setStates(Array<ChipState>(n).fill("idle"));
      setDoneCount(0);

      order.forEach((idx, rank) => {
        const startAt = rank * 290 + Math.random() * 100;
        const doneAt = startAt + 360 + Math.random() * 240;

        timersRef.current.push(
          setTimeout(() => {
            if (cycleRef.current !== cycle) return;
            setStates((prev) => {
              const next = [...prev] as ChipState[];
              next[idx] = "querying";
              return next;
            });
          }, startAt)
        );

        timersRef.current.push(
          setTimeout(() => {
            if (cycleRef.current !== cycle) return;
            setStates((prev) => {
              const next = [...prev] as ChipState[];
              next[idx] = "done";
              return next;
            });
            setDoneCount((d) => d + 1);
          }, doneAt)
        );
      });

      // After all chips complete + a brief pause, start a new cycle.
      const loopAt = (sources.length - 1) * 290 + 100 + 360 + 240 + 650;
      timersRef.current.push(
        setTimeout(() => {
          if (cycleRef.current !== cycle) return;
          const next = cycle + 1;
          cycleRef.current = next;
          runCycle(next);
        }, loopAt)
      );
    }

    timersRef.current.forEach(clearTimeout);
    timersRef.current = [];
    const initial = ++cycleRef.current;
    runCycle(initial);

    return () => {
      // Cancel all pending timers from this or any previous cycle.
      cycleRef.current++;
      timersRef.current.forEach(clearTimeout);
      timersRef.current = [];
    };
  }, [sources]);

  const n = sources.length;
  const progress = n > 0 ? (doneCount / n) * 100 : 0;
  const allDone = doneCount === n && n > 0;

  return (
    <div className="bg-white border border-oo-rule rounded-oo p-4 mb-6">
      {/* Counter row */}
      <div className="flex items-center gap-3 mb-2">
        <p className="text-[11px] text-oo-muted flex-1">
          {allDone ? `Queried ${n} sources` : `Querying ${n} sources…`}
        </p>
        <span className="text-[10px] font-mono text-oo-muted">
          {doneCount} / {n}
        </span>
      </div>

      {/* Progress bar */}
      <div className="h-0.5 bg-oo-rule rounded-full overflow-hidden mb-3">
        <div
          className="h-full rounded-full transition-all duration-300"
          style={{
            width: `${progress}%`,
            background: allDone ? "#25cb55" : "#3d30d4",
          }}
        />
      </div>

      {/* Source chips */}
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-1.5">
        {sources.map((src, i) => {
          const state = states[i] ?? "idle";
          return (
            <div
              key={src.id}
              className="flex items-center gap-1.5 px-2 py-1.5 rounded text-[10.5px] border overflow-hidden transition-colors duration-200"
              style={
                state === "idle"
                  ? {
                      background: "#f3f3f5",
                      color: "#757575",
                      borderColor: "#e5e5e5",
                    }
                  : state === "querying"
                  ? {
                      background: "#dceeff",
                      color: "#3d30d4",
                      borderColor: "#3d30d440",
                    }
                  : {
                      background: "#e8faf0",
                      color: "#1a7a38",
                      borderColor: "#25cb5540",
                    }
              }
            >
              <span
                className={`flex-shrink-0 rounded-full${state === "querying" ? " animate-pulse" : ""}`}
                style={{
                  width: 5,
                  height: 5,
                  background:
                    state === "idle"
                      ? "#e5e5e5"
                      : state === "querying"
                      ? "#3d30d4"
                      : "#25cb55",
                }}
              />
              <span className="truncate">{src.name}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
