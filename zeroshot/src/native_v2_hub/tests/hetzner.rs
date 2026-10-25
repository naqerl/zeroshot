//! The Hetzner provisioner against a local mock of its API: no real API is ever called.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use axum::extract::Request;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use super::super::hetzner::{Hetzner, Token, classify};
use super::super::pool::{Resource, ResourceKind};
use super::super::provision::{Architecture, Progress, Provisioner, Refusal, ServerRequest};

const SECRET: &str = "test-token-not-real";

/// A scripted answer: status, JSON body (`None` sends plain text) and extra headers.
#[derive(Clone)]
struct Answer {
    status: u16,
    body: Option<Value>,
    headers: Vec<(&'static str, String)>,
}

fn answer(status: u16, body: Value) -> Answer {
    Answer {
        status,
        body: Some(body),
        headers: Vec::new(),
    }
}

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    uri: String,
    authorization: String,
    body: Value,
}

#[derive(Default)]
struct Mock {
    script: Mutex<BTreeMap<String, VecDeque<Answer>>>,
    seen: Mutex<Vec<Seen>>,
}

impl Mock {
    fn on(&self, key: &str, reply: Answer) {
        self.script
            .lock()
            .expect("lock")
            .entry(key.to_owned())
            .or_default()
            .push_back(reply);
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("lock").clone()
    }
}

async fn handle(State(mock): State<Arc<Mock>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let (method, uri, headers) = (parts.method, parts.uri, parts.headers);
    let body = axum::body::to_bytes(body, usize::MAX).await.expect("body");
    let path = uri.path().to_owned();
    mock.seen.lock().expect("lock").push(Seen {
        method: method.to_string(),
        uri: uri.to_string(),
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned(),
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    });
    let reply = mock
        .script
        .lock()
        .expect("lock")
        .get_mut(&format!("{method} {path}"))
        .and_then(VecDeque::pop_front)
        .unwrap_or_else(|| answer(404, json!({"error": {"code": "not_found"}})));
    let status = StatusCode::from_u16(reply.status).expect("status");
    let mut response = match reply.body {
        Some(body) => (status, axum::Json(body)).into_response(),
        None => (status, "not json").into_response(),
    };
    for (name, value) in reply.headers {
        response
            .headers_mut()
            .insert(name, value.parse().expect("header value"));
    }
    response
}

async fn serve() -> (Hetzner, Arc<Mock>) {
    let mock = Arc::new(Mock::default());
    let app = axum::Router::new()
        .fallback(handle)
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}/v1/", listener.local_addr().expect("addr"));
    tokio::spawn(async move { axum::serve(listener, app).await });
    let client = Hetzner::new(&base, Token::parse(SECRET).expect("token")).expect("client");
    (client, mock)
}

fn request() -> ServerRequest {
    ServerRequest {
        name: "zs-one".to_owned(),
        server_type: "cpx31".to_owned(),
        location: "fsn1".to_owned(),
        image: "docker-ce".to_owned(),
        user_data: "#cloud-config\n".to_owned(),
        labels: BTreeMap::from([("zeroshot/hub".to_owned(), "h1".to_owned())]),
        ssh_from: vec!["198.51.100.7/32".to_owned()],
    }
}

fn resource(kind: ResourceKind, id: u64) -> Resource {
    Resource {
        kind,
        id,
        name: "zs-one".to_owned(),
        labels: BTreeMap::new(),
    }
}

#[tokio::test]
async fn create_puts_an_ssh_only_firewall_in_front_of_the_labelled_server() {
    let (hetzner, mock) = serve().await;
    mock.on(
        "POST /v1/firewalls",
        answer(201, json!({"firewall": {"id": 11}})),
    );
    mock.on(
        "POST /v1/servers",
        answer(
            201,
            json!({"server": {"id": 22}, "action": {"id": 33}, "root_password": "ignored"}),
        ),
    );
    let created = hetzner.create(&request()).await.expect("created");
    assert_eq!(
        (created.server_id, created.firewall_id, created.action_id),
        (22, Some(11), Some(33))
    );
    let seen = mock.seen();
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter()
            .all(|call| call.authorization == format!("Bearer {SECRET}"))
    );
    let rule = &seen[0].body["rules"][0];
    assert_eq!(rule["port"], json!("22"));
    assert_eq!(rule["direction"], json!("in"));
    assert_eq!(rule["source_ips"], json!(["198.51.100.7/32"]));
    assert_eq!(seen[0].body["rules"].as_array().map(Vec::len), Some(1));
    let server = &seen[1].body;
    assert_eq!(server["name"], json!("zs-one"));
    assert_eq!(server["server_type"], json!("cpx31"));
    assert_eq!(server["firewalls"], json!([{"firewall": 11}]));
    assert_eq!(server["labels"]["zeroshot/hub"], json!("h1"));
    for call in &seen {
        assert!(!call.body.to_string().contains(SECRET));
    }
    assert!(!format!("{hetzner:?}").contains(SECRET));
}

#[tokio::test]
async fn a_refused_server_create_removes_its_firewall_and_reports_the_error() {
    let (hetzner, mock) = serve().await;
    mock.on(
        "POST /v1/firewalls",
        answer(201, json!({"firewall": {"id": 11}})),
    );
    mock.on(
        "POST /v1/servers",
        answer(
            403,
            json!({"error": {"code": "resource_limit_exceeded", "message": "server limit"}}),
        ),
    );
    mock.on(
        "DELETE /v1/firewalls/11",
        Answer {
            status: 204,
            body: None,
            headers: Vec::new(),
        },
    );
    let error = hetzner.create(&request()).await.expect_err("refused");
    assert_eq!(error.refusal, Refusal::Quota);
    let message = error.to_string();
    assert!(
        message.contains("HTTP 403 resource_limit_exceeded: server limit"),
        "{message}"
    );
    assert!(!message.contains(SECRET));
    assert_eq!(mock.seen()[2].method, "DELETE");

    mock.on(
        "POST /v1/firewalls",
        answer(201, json!({"firewall": {"id": 14}})),
    );
    mock.on(
        "POST /v1/servers",
        answer(
            412,
            json!({"error": {"code": "resource_unavailable", "message": "no room"}}),
        ),
    );
    mock.on(
        "DELETE /v1/firewalls/14",
        answer(
            409,
            json!({"error": {"code": "resource_in_use", "message": "attached"}}),
        ),
    );
    let error = hetzner.create(&request()).await.expect_err("no capacity");
    assert_eq!(error.refusal, Refusal::Capacity);
    assert!(
        error.message.contains("firewall 14 left for cleanup"),
        "{}",
        error.message
    );

    mock.on(
        "POST /v1/firewalls",
        answer(201, json!({"firewall": {"id": 12}})),
    );
    let missing = hetzner
        .create(&request())
        .await
        .expect_err("404 server create");
    assert!(missing.to_string().contains("POST /servers"), "{missing}");
    mock.on("POST /v1/firewalls", answer(201, json!({})));
    assert!(hetzner.create(&request()).await.is_err());
    mock.on(
        "POST /v1/firewalls",
        answer(201, json!({"firewall": {"id": 13}})),
    );
    mock.on("POST /v1/servers", answer(201, json!({"server": {}})));
    assert!(hetzner.create(&request()).await.is_err());
}

#[tokio::test]
async fn readiness_waits_for_the_create_action_and_a_running_server() {
    let (hetzner, mock) = serve().await;
    mock.on(
        "GET /v1/actions/33",
        answer(200, json!({"action": {"status": "running"}})),
    );
    assert_eq!(
        hetzner.wait_ready(22, Some(33)).await.expect("ready"),
        Progress::Pending
    );
    mock.on(
        "GET /v1/actions/33",
        answer(200, json!({"action": {"status": "success"}})),
    );
    mock.on(
        "GET /v1/servers/22",
        answer(200, json!({"server": {"status": "initializing"}})),
    );
    assert_eq!(
        hetzner.wait_ready(22, Some(33)).await.expect("ready"),
        Progress::Pending
    );
    mock.on(
        "GET /v1/servers/22",
        answer(
            200,
            json!({"server": {"status": "running", "public_net": {"ipv4": {"ip": "203.0.113.10"}}}}),
        ),
    );
    assert_eq!(
        hetzner.wait_ready(22, None).await.expect("ready"),
        Progress::Done(Some("203.0.113.10".to_owned()))
    );
    assert!(matches!(
        hetzner.wait_ready(22, None).await.expect("gone"),
        Progress::Failed(message) if message.contains("no longer exists")
    ));
    mock.on(
        "GET /v1/actions/34",
        answer(
            200,
            json!({"action": {"status": "error", "error": {"message": "no capacity"}}}),
        ),
    );
    assert_eq!(
        hetzner.wait_ready(22, Some(34)).await.expect("failed"),
        Progress::Failed("no capacity".to_owned())
    );
    assert!(matches!(
        hetzner.action(35).await.expect("unknown"),
        Progress::Failed(message) if message.contains("unknown")
    ));
}

#[tokio::test]
async fn destroy_returns_the_action_to_await_and_treats_404_as_gone() {
    let (hetzner, mock) = serve().await;
    mock.on(
        "DELETE /v1/servers/22",
        answer(200, json!({"action": {"id": 44}})),
    );
    assert_eq!(
        hetzner
            .destroy(&resource(ResourceKind::Server, 22))
            .await
            .expect("deleting"),
        Some(44)
    );
    assert_eq!(
        hetzner
            .destroy(&resource(ResourceKind::Server, 22))
            .await
            .expect("gone"),
        None
    );
    mock.on(
        "DELETE /v1/primary_ips/5",
        Answer {
            status: 204,
            body: None,
            headers: Vec::new(),
        },
    );
    assert_eq!(
        hetzner
            .destroy(&resource(ResourceKind::PrimaryIp, 5))
            .await
            .expect("deleted"),
        None
    );
    mock.on(
        "DELETE /v1/firewalls/11",
        answer(409, json!({"error": {"code": "resource_in_use"}})),
    );
    let error = hetzner
        .destroy(&resource(ResourceKind::Firewall, 11))
        .await
        .expect_err("still attached");
    assert!(error.to_string().contains("resource_in_use"), "{error}");
    mock.on(
        "DELETE /v1/firewalls/12",
        Answer {
            status: 500,
            body: None,
            headers: Vec::new(),
        },
    );
    let error = hetzner
        .destroy(&resource(ResourceKind::Firewall, 12))
        .await
        .expect_err("not json");
    assert!(error.to_string().contains("without JSON"), "{error}");
}

#[tokio::test]
async fn list_by_label_pages_through_every_resource_kind() {
    let (hetzner, mock) = serve().await;
    mock.on(
        "GET /v1/servers",
        answer(
            200,
            json!({"servers": [{"id": 1, "name": "zs-a", "labels": {"zeroshot/hub": "h1", "n": 2}}],
                   "meta": {"pagination": {"next_page": 2}}}),
        ),
    );
    mock.on(
        "GET /v1/servers",
        answer(
            200,
            json!({"servers": [{"id": 2}], "meta": {"pagination": {"next_page": null}}}),
        ),
    );
    mock.on(
        "GET /v1/primary_ips",
        answer(200, json!({"primary_ips": [{"id": 3}]})),
    );
    mock.on("GET /v1/firewalls", answer(200, json!({"firewalls": []})));
    let found = hetzner
        .list_by_label("zeroshot/hub=h1")
        .await
        .expect("listed");
    let ids = found
        .iter()
        .map(|resource| (resource.kind, resource.id))
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [
            (ResourceKind::Server, 1),
            (ResourceKind::Server, 2),
            (ResourceKind::PrimaryIp, 3)
        ]
    );
    assert_eq!(found[0].labels.len(), 1, "non-string labels are dropped");
    let seen = mock.seen();
    assert_eq!(
        seen[0].uri,
        "/v1/servers?label_selector=zeroshot%2Fhub%3Dh1&page=1&per_page=50"
    );
    assert!(seen[1].uri.contains("page=2"));

    mock.on(
        "GET /v1/servers",
        answer(200, json!({"servers": [{"name": "no id"}]})),
    );
    assert!(hetzner.list_by_label("a=b").await.is_err());
    assert!(
        hetzner.list_by_label("a=b").await.is_err(),
        "404 collection"
    );
}

#[tokio::test]
async fn a_spent_rate_limit_stops_requests_until_it_resets() {
    let (hetzner, mock) = serve().await;
    let reset = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs()
        + 600;
    mock.on(
        "GET /v1/actions/1",
        Answer {
            status: 429,
            body: Some(json!({"error": {"code": "rate_limit_exceeded"}})),
            headers: vec![
                ("ratelimit-remaining", "0".to_owned()),
                ("ratelimit-reset", reset.to_string()),
            ],
        },
    );
    let error = hetzner.action(1).await.expect_err("limited");
    assert!(error.to_string().contains("rate_limit_exceeded"), "{error}");
    mock.on(
        "POST /v1/firewalls",
        answer(201, json!({"firewall": {"id": 1}})),
    );
    let held = hetzner.create(&request()).await.expect_err("held back");
    assert_eq!(held.refusal, Refusal::RateLimited);
    assert_eq!(hetzner.rate_limit().remaining, Some(0));
    assert_eq!(hetzner.rate_limit().reset, Some(reset));
    let error = hetzner.action(1).await.expect_err("held back");
    assert!(error.to_string().contains("rate limit is spent"), "{error}");
    assert!(
        held.message.contains("rate limit is spent"),
        "{}",
        held.message
    );
    assert_eq!(mock.seen().len(), 1, "no request while the budget is spent");
}

#[tokio::test]
async fn an_unreachable_api_is_a_typed_provider_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let base = format!("http://{}/v1", listener.local_addr().expect("addr"));
    drop(listener);
    let hetzner = Hetzner::new(&base, Token::parse(SECRET).expect("token")).expect("client");
    let error = hetzner.action(1).await.expect_err("refused");
    assert!(error.to_string().contains("was not answered"), "{error}");
    assert!(!error.to_string().contains(SECRET));
    let failure = hetzner.create(&request()).await.expect_err("refused");
    assert_eq!(failure.refusal, Refusal::Transient);
}

#[test]
fn create_failures_are_classified_by_code_status_and_message() {
    let cases = [
        (412, "resource_unavailable", "", Refusal::Capacity),
        (422, "placement_error", "", Refusal::Capacity),
        (503, "unavailable", "", Refusal::Capacity),
        (503, "maintenance", "", Refusal::Capacity),
        (
            422,
            "invalid_input",
            "server type cpx31 is not available in location hel1",
            Refusal::Capacity,
        ),
        (
            422,
            "unsupported_error",
            "unsupported location for server type",
            Refusal::Capacity,
        ),
        (403, "resource_limit_exceeded", "", Refusal::Quota),
        (403, "server_limit_exceeded", "", Refusal::Quota),
        (403, "limit_exceeded", "", Refusal::Quota),
        (403, "forbidden", "", Refusal::Quota),
        (401, "unauthorized", "", Refusal::Quota),
        (403, "token_readonly", "", Refusal::Quota),
        (403, "", "", Refusal::Quota),
        (429, "rate_limit_exceeded", "", Refusal::RateLimited),
        (429, "", "", Refusal::RateLimited),
        (400, "invalid_input", "invalid image", Refusal::Invalid),
        (400, "json_error", "", Refusal::Invalid),
        (404, "not_found", "", Refusal::Invalid),
        (422, "invalid_server_type", "unknown type", Refusal::Invalid),
        (400, "", "", Refusal::Invalid),
        (500, "service_error", "", Refusal::Transient),
        (502, "", "", Refusal::Transient),
        (504, "timeout", "", Refusal::Transient),
        (409, "conflict", "", Refusal::Transient),
        (423, "locked", "", Refusal::Transient),
        (200, "", "", Refusal::Transient),
    ];
    for (status, code, message, expected) in cases {
        assert_eq!(
            classify(status, code, message),
            expected,
            "{status} {code} {message}"
        );
    }
}

#[tokio::test]
async fn the_catalog_joins_types_prices_and_datacenters_by_location() {
    let (hetzner, mock) = serve().await;
    let price = |location: &str, gross: &str| json!({"location": location, "price_hourly": {"net": "0", "gross": gross}});
    mock.on(
        "GET /v1/server_types",
        answer(
            200,
            json!({"server_types": [
                {"id": 1, "name": "cx23", "cores": 2, "memory": 4.0, "disk": 40,
                 "architecture": "x86", "deprecation": null,
                 "prices": [price("fsn1", "0.0071"), price("nbg1", "0.0071")]},
                {"id": 2, "name": "cax21", "cores": 4, "memory": 8.0, "disk": 80,
                 "architecture": "arm", "prices": []}
            ], "meta": {"pagination": {"next_page": 2}}}),
        ),
    );
    mock.on(
        "GET /v1/server_types",
        answer(
            200,
            json!({"server_types": [
                {"id": 3, "name": "cx11", "cores": 1, "memory": 2.0, "disk": 20,
                 "architecture": "x86", "deprecation": {"announced": "2024-01-01"}},
                {"name": "no id"}
            ]}),
        ),
    );
    mock.on(
        "GET /v1/datacenters",
        answer(
            200,
            json!({"datacenters": [
                {"location": {"name": "fsn1"},
                 "server_types": {"supported": [1, 2, 3], "available": [1]}},
                {"location": {"name": "fsn1"},
                 "server_types": {"supported": [1], "available": [2, 99]}},
                {"location": {"name": "nbg1"},
                 "server_types": {"supported": [1], "available": []}},
                {"name": "no location"}
            ]}),
        ),
    );
    let catalog = hetzner.catalog().await.expect("catalog");
    let names = catalog
        .types
        .iter()
        .map(|server_type| server_type.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["cx23", "cax21", "cx11"]);
    let cx23 = catalog.server_type("cx23").expect("cx23");
    assert_eq!((cx23.cores, cx23.memory_mb, cx23.disk_gb), (2, 4096, 40));
    assert_eq!(cx23.architecture, Architecture::X86);
    assert!(!cx23.deprecated);
    assert_eq!(cx23.hourly.get("nbg1").map(String::as_str), Some("0.0071"));
    assert_eq!(
        catalog.server_type("cax21").map(|t| t.architecture),
        Some(Architecture::Arm)
    );
    assert!(catalog.server_type("cx11").expect("cx11").deprecated);
    let fsn1 = catalog.location("fsn1").expect("fsn1");
    assert_eq!(
        fsn1.supported
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["cax21", "cx11", "cx23"]
    );
    assert_eq!(
        fsn1.available
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["cax21", "cx23"]
    );
    assert!(catalog.location("nbg1").expect("nbg1").available.is_empty());
    assert_eq!(catalog.locations.len(), 2);
    assert!(mock.seen()[1].uri.contains("page=2"));
    assert!(hetzner.catalog().await.is_err(), "404 collection");
}

#[test]
fn tokens_load_from_a_file_trimmed_and_never_print() {
    let directory = tempfile::tempdir().expect("tempdir");
    let file = directory.path().join("token");
    std::fs::write(&file, format!("{SECRET}\n")).expect("token file");
    let token = Token::load(Some(&file)).expect("loads").expect("present");
    assert_eq!(format!("{token:?}"), "Token(<redacted>)");
    assert!(Token::load(Some(&directory.path().join("absent"))).is_err());
    assert!(Token::parse("  ").is_err());
    assert!(Token::parse("two words").is_err());
}
