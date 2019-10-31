use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use redis_oxide::asyncresp::RedisValueCodec;
use redis_oxide::keys::{key_interact, KeyOps};
use redis_oxide::ops::{op_interact, translate};
use redis_oxide::types::{RedisValue, ReturnValue, State};
use std::sync::Arc;
use tokio::codec::Decoder;

fn bench_parsing(c: &mut Criterion) {
    let buf: String = std::iter::repeat("a").take(100).collect();
    let mut decoder = RedisValueCodec::default();
    let mut group = c.benchmark_group("decoding");
    group.throughput(Throughput::Bytes(buf.len() as u64 + 3));
    group.bench_function("simple_string", |b| {
        let _ = b.iter(|| {
            let mut buf = BytesMut::from(format!("+{}\r\n", buf));
            decoder
                .decode(black_box(&mut buf))
                .expect("parsing to work");
        });
    });
    group.finish();
}

fn bench_translate(c: &mut Criterion) {
    let value = RedisValue::Array(vec![
        RedisValue::SimpleString("set".as_bytes().to_vec()),
        RedisValue::SimpleString("foo".as_bytes().to_vec()),
        RedisValue::SimpleString(
            std::iter::repeat("a")
                .take(200)
                .collect::<String>()
                .as_bytes()
                .to_vec(),
        ),
    ]);
    let mut group = c.benchmark_group("translate");
    group.throughput(Throughput::Bytes(212));
    group.bench_function("translate", |b| {
        b.iter(|| translate(black_box(&value)));
    });
    group.finish();
}

fn bench_interact(c: &mut Criterion) {
    let s = Arc::new(State::default());
    c.bench_function("KeyOps::Set", |b| {
        b.iter(|| {
            async {
                let f = KeyOps::Set("foo".as_bytes().to_vec(), "bar".as_bytes().to_vec());
                key_interact(black_box(f), black_box(s.clone())).await;
            }
        });
    });
}

fn bench_full_life_cycle(c: &mut Criterion) {
    c.bench_function("full_life_cycle", |b| {
        b.iter(|| {
            async {
                let mut decoder = RedisValueCodec::default();
                let s = Arc::new(State::default());
                let scc = "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
                let mut buf = BytesMut::from(format!("{}", scc));
                let res = decoder
                    .decode(black_box(&mut buf))
                    .expect("parsing to work")
                    .unwrap();
                let op = translate(black_box(&res)).unwrap();
                let res = op_interact(black_box(op), black_box(s.clone())).await;
                assert_eq!(res, ReturnValue::Ok);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_parsing,
    bench_translate,
    bench_interact,
    bench_full_life_cycle
);
criterion_main!(benches);
