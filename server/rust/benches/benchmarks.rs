use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use roomer::{Conn, Hub, InMemoryMetrics, Message};
use std::sync::Arc;
use tokio::sync::mpsc;

fn bench_message_encode_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_framing");
    let payload = Bytes::from(vec![42u8; 1024]);
    let msg = Message::new("room_alpha", "chat_message", "user_dst", "user_src", payload);
    let raw = msg.encode();

    group.throughput(Throughput::Bytes(raw.len() as u64));

    group.bench_function("encode_1kb", |b| {
        b.iter(|| {
            black_box(msg.encode());
        });
    });

    group.bench_function("decode_1kb", |b| {
        b.iter(|| {
            black_box(Message::decode(black_box(raw.clone())).unwrap());
        });
    });

    group.finish();
}

fn bench_room_fanout_1000(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    let mut group = c.benchmark_group("room_fanout");

    let hub = Hub::new();
    let metrics = Arc::new(InMemoryMetrics::new());
    let (senders, mut receivers) = (0..1000)
        .map(|i| {
            let (tx, rx) = mpsc::channel(100);
            let conn = Conn::new(format!("user_{i}"), Default::default(), tx, metrics.clone());
            hub.join_room("broadcast_room", conn);
            (i, rx)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip::<_, _, Vec<_>, Vec<_>>();

    let _ = senders;

    // Drain background receivers task
    rt.spawn(async move {
        loop {
            for rx in &mut receivers {
                let _ = rx.try_recv();
            }
            tokio::task::yield_now().await;
        }
    });

    let msg = Message::new(
        "broadcast_room",
        "chat",
        "",
        "sender",
        Bytes::from_static(b"benchmark broadcast test message"),
    );

    group.bench_function("fanout_1000_connections", |b| {
        b.iter(|| {
            hub.broadcast_room(Some("sender"), black_box(msg.clone()));
        });
    });

    group.finish();
}

criterion_group!(benches, bench_message_encode_decode, bench_room_fanout_1000);
criterion_main!(benches);
