use std::time::{Duration, Instant};
use tokio::time::sleep;

async fn tarea(id: u32, duracion_ms: u64) -> u32 {
    println!("Tarea {id} empezó");
    sleep(Duration::from_millis(duracion_ms)).await;
    println!("Tarea {id} terminó");
    id * 10
}

#[tokio::main]
async fn main() {
    let inicio = Instant::now();

    tokio::spawn(tarea(1, 3000));
    tokio::spawn(tarea(2, 1000));
    tokio::spawn(tarea(3, 2000));

    sleep(Duration::from_hours(33333333333)).await;

    // println!("Resultados: {h1}, {h2}, {h3}");
    println!("Tiempo total: {:.2?}", inicio.elapsed());
    // ~3s, no 6s, porque las 3 tareas corrieron al mismo tiempo
}
