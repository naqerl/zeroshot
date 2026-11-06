//! The tailcat data path: a loopback proxy in front of a tailcat server, the transport choice,
//! and the pool configuration that selects it.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::json;

use super::super::pool::{Pool, Transport};
use super::super::tunnel::{
    ProxyHandle, SshTunnels, TailcatProxy, TailcatTunnels, TunnelSpec, Tunnels, choose_tunnels,
    read_tailcat_addr,
};
use super::pool::{add_request, pool_fixture};

/// A local echo server the fake proxy forwards to, standing in for the target's port 8080.
fn echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("echo bind");
    let address = listener.local_addr().expect("echo addr");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            std::thread::spawn(move || {
                let mut buffer = [0_u8; 1024];
                while let Ok(read) = stream.read(&mut buffer) {
                    if read == 0 || stream.write_all(&buffer[..read]).is_err() {
                        return;
                    }
                }
            });
        }
    });
    address
}

#[derive(Default)]
struct FakeState {
    target: Mutex<Option<SocketAddr>>,
    started: Mutex<Vec<(String, u16, u16)>>,
    alive: Mutex<Vec<Arc<AtomicBool>>>,
}

impl FakeState {
    fn kill(&self, index: usize) {
        if let Some(alive) = self.alive.lock().expect("alive").get(index) {
            alive.store(false, Ordering::SeqCst);
        }
    }
}

/// An in-process proxy: bind the loopback port and copy bytes to the echo server. This exercises
/// the listener, the forwarded bytes, and the liveness checks without any real tailcat traffic.
struct FakeProxy {
    state: Arc<FakeState>,
}

impl TailcatProxy for FakeProxy {
    fn start(
        &self,
        addr: &str,
        local_port: u16,
        remote_port: u16,
    ) -> Result<Box<dyn ProxyHandle>, super::super::error::HubError> {
        self.state.started.lock().expect("started").push((
            addr.to_owned(),
            local_port,
            remote_port,
        ));
        let target = self
            .state
            .target
            .lock()
            .expect("target")
            .expect("fake target");
        let listener = TcpListener::bind(("127.0.0.1", local_port)).map_err(|error| {
            super::super::error::HubError::storage("fake proxy could not bind", error)
        })?;
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let alive = Arc::new(AtomicBool::new(true));
        self.state
            .alive
            .lock()
            .expect("alive")
            .push(Arc::clone(&alive));
        let thread_alive = Arc::clone(&alive);
        let join = std::thread::spawn(move || {
            while thread_alive.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((incoming, _)) => {
                        std::thread::spawn(move || {
                            let Ok(outgoing) = TcpStream::connect(target) else {
                                return;
                            };
                            forward(incoming, outgoing);
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Box::new(FakeHandle {
            alive,
            join: Some(join),
        }))
    }
}

fn forward(incoming: TcpStream, outgoing: TcpStream) {
    let mut incoming_read = incoming.try_clone().expect("clone");
    let mut outgoing_write = outgoing.try_clone().expect("clone");
    let back = std::thread::spawn(move || {
        let _ = std::io::copy(&mut incoming_read, &mut outgoing_write);
        let _ = outgoing_write.shutdown(std::net::Shutdown::Write);
    });
    let mut outgoing_read = outgoing;
    let mut incoming_write = incoming;
    let _ = std::io::copy(&mut outgoing_read, &mut incoming_write);
    let _ = incoming_write.shutdown(std::net::Shutdown::Write);
    let _ = back.join();
}

struct FakeHandle {
    alive: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl ProxyHandle for FakeHandle {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::SeqCst)
            && self.join.as_ref().is_some_and(|join| !join.is_finished())
    }

    fn stop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn spec(local_port: u16, addr: Option<&str>) -> TunnelSpec {
    TunnelSpec {
        name: "zs-one".to_owned(),
        host: "203.0.113.10".to_owned(),
        local_port,
        key: std::path::PathBuf::from("/hub/id_ed25519"),
        known_hosts: std::path::PathBuf::from("/hub/pool/known_hosts"),
        tailcat_addr: addr.map(str::to_owned),
    }
}

fn wait_until_closed(tunnels: &TailcatTunnels, name: &str) {
    let started = Instant::now();
    while tunnels.is_open(name) {
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the dead proxy still reported open"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn tailcat_tunnels_forward_loopback_and_report_a_dead_proxy() {
    let echo = echo_server();
    let state = Arc::new(FakeState::default());
    *state.target.lock().expect("target") = Some(echo);
    let tunnels = TailcatTunnels::with_proxy(Box::new(FakeProxy {
        state: Arc::clone(&state),
    }));
    let port = super::super::tunnel::free_port().expect("free port");
    let spec = spec(port, Some("tcfakeaddress"));

    tunnels.open(&spec).expect("open");
    assert!(tunnels.is_open("zs-one"));
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("timeout");
    stream.write_all(b"hello tailcat").expect("write");
    let mut echoed = [0_u8; 13];
    stream.read_exact(&mut echoed).expect("echo");
    assert_eq!(&echoed, b"hello tailcat");

    // Opening twice keeps the running proxy, like SshTunnels.
    tunnels.open(&spec).expect("still open");
    assert_eq!(state.started.lock().expect("started").len(), 1);
    assert_eq!(
        state.started.lock().expect("started")[0],
        ("tcfakeaddress".to_owned(), port, 8080)
    );

    tunnels.close("zs-one");
    assert!(!tunnels.is_open("zs-one"));
    tunnels.close("zs-one");
    tunnels.open(&spec).expect("reopened");
    assert_eq!(state.started.lock().expect("started").len(), 2);

    // A proxy that dies is not open, even before anyone calls close.
    state.kill(1);
    wait_until_closed(&tunnels, "zs-one");
    tunnels.open(&spec).expect("reopened after death");
    assert!(tunnels.is_open("zs-one"));
}

#[test]
fn tailcat_tunnels_refuse_a_spec_without_an_address() {
    let tunnels = TailcatTunnels::with_proxy(Box::new(FakeProxy {
        state: Arc::new(FakeState::default()),
    }));
    let error = tunnels.open(&spec(40123, None)).expect_err("no address");
    assert!(error.to_string().contains("tailcat address"), "{error}");
    assert!(!tunnels.is_open("zs-one"));
}

#[test]
fn transport_selection_prefers_tailcat_only_with_a_readable_address() {
    let ssh: Arc<dyn Tunnels> = Arc::new(SshTunnels::default());
    let tailcat: Arc<dyn Tunnels> = Arc::new(TailcatTunnels::default());

    let (chosen, transport, addr) =
        choose_tunnels(Transport::Ssh, Some("tcfake".to_owned()), &ssh, &tailcat);
    assert!(Arc::ptr_eq(&chosen, &ssh));
    assert_eq!(transport, Transport::Ssh);
    assert_eq!(addr, None);

    let (chosen, transport, addr) = choose_tunnels(Transport::Tailcat, None, &ssh, &tailcat);
    assert!(Arc::ptr_eq(&chosen, &ssh));
    assert_eq!(transport, Transport::Ssh);
    assert_eq!(addr, None);

    let (chosen, transport, addr) = choose_tunnels(
        Transport::Tailcat,
        Some("tcfake".to_owned()),
        &ssh,
        &tailcat,
    );
    assert!(Arc::ptr_eq(&chosen, &tailcat));
    assert_eq!(transport, Transport::Tailcat);
    assert_eq!(addr.as_deref(), Some("tcfake"));
}

#[test]
fn a_pool_transport_defaults_to_ssh_and_rejects_unknown_values() {
    let pool = pool_fixture();
    let mut value = serde_json::to_value(&pool).expect("serialize");
    assert_eq!(value["transport"], json!("ssh"));
    value.as_object_mut().expect("object").remove("transport");
    let defaulted: Pool = serde_json::from_value(value.clone()).expect("default");
    assert_eq!(defaulted.transport, Transport::Ssh);

    value["transport"] = json!("tailcat");
    let tailcat: Pool = serde_json::from_value(value.clone()).expect("tailcat");
    assert_eq!(tailcat.transport, Transport::Tailcat);

    value["transport"] = json!("quic");
    let error = serde_json::from_value::<Pool>(value)
        .expect_err("unknown transport")
        .to_string();
    assert!(error.contains("transport"), "{error}");
    assert!(error.contains("quic"), "{error}");
}

#[test]
fn the_pool_api_accepts_a_tailcat_transport() {
    let directory = tempfile::tempdir().expect("tempdir");
    let request = add_request(directory.path(), 1);
    let mut value = serde_json::to_value(&request).expect("serialize");
    value["transport"] = json!("tailcat");
    let parsed: super::super::wire::AddPool =
        serde_json::from_value(value).expect("tailcat request");
    assert_eq!(parsed.transport, Transport::Tailcat);
    assert_eq!(
        Transport::parse("tailcat").expect("parse"),
        Transport::Tailcat
    );
    assert!(
        Transport::parse("quic")
            .expect_err("unknown")
            .to_string()
            .contains("transport")
    );
}

#[test]
fn a_tailcat_address_is_one_tc_token() {
    assert_eq!(read_tailcat_addr("tcABC\n").as_deref(), Some("tcABC"));
    assert_eq!(read_tailcat_addr("  tcABC  \n").as_deref(), Some("tcABC"));
    assert_eq!(read_tailcat_addr(""), None);
    assert_eq!(read_tailcat_addr("nope"), None);
    assert_eq!(read_tailcat_addr("tcABC tcDEF"), None);
    assert_eq!(read_tailcat_addr("tcABC\nsecond"), None);
}
