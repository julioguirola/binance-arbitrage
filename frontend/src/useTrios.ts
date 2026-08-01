import { useEffect, useState } from "react";
import type { ServerMsg } from "./types";

export function useTrios() {
  const [rates, setRates] = useState<Map<string, number>>(new Map());
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let closed = false;
    let retry = 0;
    let timer: number | undefined;

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}/ws`);

      ws.onopen = () => {
        retry = 0;
        setConnected(true);
      };

      ws.onmessage = (ev) => {
        let msg: ServerMsg;
        try {
          msg = JSON.parse(ev.data) as ServerMsg;
        } catch {
          return;
        }
        if (msg.type === "snapshot") {
          setRates(new Map(msg.trios.map((t) => [t.route, t.rate])));
        } else {
          setRates((prev) => {
            const next = new Map(prev);
            next.set(msg.trio.route, msg.trio.rate);
            return next;
          });
        }
      };

      ws.onclose = () => {
        setConnected(false);
        if (!closed) scheduleReconnect();
      };

      ws.onerror = () => ws?.close();
    };

    const scheduleReconnect = () => {
      const delay = Math.min(1000 * 2 ** retry, 15000);
      retry += 1;
      timer = window.setTimeout(connect, delay);
    };

    connect();

    return () => {
      closed = true;
      window.clearTimeout(timer);
      ws?.close();
    };
  }, []);

  return { rates, connected };
}
