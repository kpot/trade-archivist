//! Checks basic client-server machinery, which implies sending requests in a format described
//! by some arbitrary type (`FakeAPIRequest` in this case) and verifying those requests
//! get answered.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use trade_archivist::api::FrameReader;

/// Fake request protocol
#[derive(Serialize, Deserialize, Debug, Clone)]
enum FakeAPIRequest<'a> {
    Request1 { pair: &'a str, extra: u32 },
    Request2(String),
}

/// A server handling requests described by the `FakeAPIRequest` API.
async fn archivist_test_server(
    listener: TcpListener,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
    data_limit: usize,
) {
    barrier.wait().await;
    if let Ok((mut socket, _)) = listener.accept().await {
        let mut state = FrameReader::new(Some(data_limit));
        while let Ok(Some(ref request)) = state.next::<FakeAPIRequest>(&mut socket).await {
            let response = match request {
                FakeAPIRequest::Request1 { pair, extra, .. } => {
                    assert_eq!(pair, &"ethusd");
                    *extra
                }
                FakeAPIRequest::Request2(_) => 0u32,
            };
            let response_buf = response.to_le_bytes();
            socket.write_all(&response_buf).await.unwrap();
        }
    }
}

/// Imitates a client connecting to the server and using `FakeAPIRequest` to make demands.
async fn archivist_test_client(port: u16, barrier: std::sync::Arc<tokio::sync::Barrier>) {
    barrier.wait().await;
    let mut connection = TcpStream::connect(("localhost", port)).await.unwrap();
    for _ in 0..2 {
        trade_archivist::api::send_frame(
            &mut connection,
            &FakeAPIRequest::Request1 {
                pair: "ethusd",
                extra: 42,
            },
        )
        .await
        .unwrap();
        let mut response_buf: [u8; 4] = [0; 4];
        connection.read_exact(&mut response_buf).await.unwrap();
        let response = u32::from_le_bytes(response_buf);
        assert_eq!(response, 42);
    }
}

/// Client sends a custom data structure containing 42, and checks that the server returns it back
#[tokio::main(flavor = "current_thread")]
async fn archivist_client_server_pair(data_limit: usize) {
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let server_listener = TcpListener::bind(("localhost", 0)).await.unwrap();
    let server_port = server_listener.local_addr().unwrap().port();
    let server = tokio::spawn(archivist_test_server(
        server_listener,
        barrier.clone(),
        data_limit,
    ));
    let client = tokio::spawn(archivist_test_client(server_port, barrier.clone()));
    server.await.unwrap();
    client.await.unwrap();
}

#[test]
pub fn test_archivist_client_server() {
    archivist_client_server_pair(1024);
}

#[test]
#[should_panic]
pub fn test_archivist_server_data_size_checks() {
    // If we try to send packages larger than expected by the server, it should break the connection
    // causing the client to throw an error.
    archivist_client_server_pair(5);
}
