# redis-rs

A currently very WIP re-implementation of Redis in Rust. 

See [this blog series](https://blog.jverkamp.com/series/cloning-redis-in-rust/) for more information. 

# Running

To run the server:

```bash
$ RUST_LOG=debug cargo run --bin server
```

To run the client:

```bash
$ RUST_LOG=debug cargo run --bin client
```
