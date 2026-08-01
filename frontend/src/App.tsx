import { useTrios } from "./useTrios";

export default function App() {
  const { rates, connected } = useTrios();

  const entries = [...rates.entries()].sort((a, b) => a[0].localeCompare(b[0]));

  return (
    <div className="app">
      <header>
        <h1>Binance Arbitrage</h1>
        <span className={`status ${connected ? "ok" : "bad"}`}>
          {connected ? "conectado" : "reconectando..."}
        </span>
        <span className="count">{entries.length} trios</span>
      </header>
      <main className="grid">
        {entries.map(([route, rate]) => (
          <div key={route} className={`cell ${rate > 1 ? "profit" : ""}`}>
            <div className="route">{route}</div>
            <div className="rate">{rate.toFixed(6)}</div>
          </div>
        ))}
      </main>
    </div>
  );
}
