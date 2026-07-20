use reticulum::identity::PrivateIdentity;
use rand_core::OsRng;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use socket2::{Domain, Type, Protocol, Socket};

/// Wire protocol — every packet is JSON-serialized and sent via UDP broadcast.
/// Recipients filter by `to_addr` where applicable.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum WirePacket {
    /// Periodic broadcast: "I'm here, my name is X, my address is Y"
    Announce { name: String, addr: String },

    /// Directed: "I'd like to chat with you"
    ChatRequest { from_name: String, from_addr: String, to_addr: String },

    /// Directed: "I accept your chat request"
    ChatAccept { from_name: String, from_addr: String, to_addr: String },

    /// Directed: "I decline your chat request"
    ChatDecline { from_name: String, from_addr: String, to_addr: String },

    /// Broadcast to contacts: a chat message
    Message { from_name: String, from_addr: String, text: String },

    /// Directed to a new member: "Here are all the people in the group"
    ContactList { from_addr: String, to_addr: String, contacts: Vec<(String, String)> },

    /// Broadcast to existing contacts: "Please add this new person"
    AddContact { from_addr: String, new_name: String, new_addr: String },
}

pub struct NetNode {
    pub addr_hex: String,
    pub sock: Arc<UdpSocket>,
    pub broadcast_addr: SocketAddr,
    pub my_name: String,
}

impl NetNode {
    pub async fn new(name: String, port: u16) -> Arc<Self> {
        // Generate a Reticulum cryptographic identity for this node
        let identity = PrivateIdentity::new_from_rand(OsRng);
        let addr_hash = *identity.address_hash();
        let addr_hex = addr_hash.to_hex_string();

        // Create UDP socket with SO_REUSEADDR + SO_REUSEPORT + SO_BROADCAST
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        let broadcast_addr: SocketAddr = format!("255.255.255.255:{}", port).parse().unwrap();

        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
            .expect("create socket");
        socket.set_reuse_address(true).expect("SO_REUSEADDR");
        #[cfg(unix)]
        socket.set_reuse_port(true).expect("SO_REUSEPORT");
        socket.set_broadcast(true).expect("SO_BROADCAST");
        socket.bind(&bind_addr.into()).expect("bind socket");
        socket.set_nonblocking(true).expect("nonblocking");
        let sock = UdpSocket::from_std(socket.into()).expect("tokio socket");

        log::info!("UDP bound to {} (broadcast {})", bind_addr, broadcast_addr);

        Arc::new(Self {
            addr_hex,
            sock: Arc::new(sock),
            broadcast_addr,
            my_name: name,
        })
    }

    pub async fn send(&self, packet: &WirePacket) {
        if let Ok(data) = serde_json::to_vec(packet) {
            let _ = self.sock.send_to(&data, self.broadcast_addr).await;
        }
    }

    /// Try to receive a packet (non-blocking, returns None if nothing available)
    pub async fn try_recv(&self) -> Option<WirePacket> {
        let mut buf = vec![0u8; 4096];
        match self.sock.try_recv(&mut buf) {
            Ok(n) => serde_json::from_slice::<WirePacket>(&buf[..n]).ok(),
            Err(_) => None,
        }
    }
}