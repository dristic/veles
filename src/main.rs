use std::{
    net::{UdpSocket, SocketAddr, IpAddr, Ipv4Addr},
};

use clap::{Parser, Subcommand};

use serde::{Serialize, Deserialize};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Client,
    Server,
}

fn main() {
    let args = Args::parse();

    match args.command.unwrap() {
        Command::Client => client(),
        Command::Server => server(),
    }
    .unwrap()
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Entity {
    x: f32,
    y: f32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct World(Vec<Entity>);

#[derive(Serialize, Deserialize, PartialEq, Debug)]
enum Message {
    Handshake,
    Heartbeat,
}

// TODO
// - handshake
// - storage of connections
// - reconnect handshake
// 

fn client() -> std::io::Result<()> {
    let world = World(vec![Entity { x: 0.0, y: 4.0 }, Entity { x: 10.0, y: 20.5 }]);

    let encoded: Vec<u8> = bincode::serialize(&world).unwrap();

    let socket = UdpSocket::bind("127.0.0.1:34254")?;

    let src = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
    socket.send_to(&encoded, &src)?;

    let mut buf = [0; 200];
    let (_amt, _src) = socket.recv_from(&mut buf)?;

    let decoded: World = bincode::deserialize(&buf[..]).unwrap();

    println!("Got {:?}", decoded);

    Ok(())
}

fn server() -> std::io::Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:8080")?;

    // Receives a single datagram message on the socket. If `buf` is too small to hold
    // the message, it will be cut off.
    let mut buf = [0; 200];
    let (_amt, src) = socket.recv_from(&mut buf)?;

    let mut decoded: World = bincode::deserialize(&buf[..]).unwrap();

    println!("Got {:?}", decoded);

    decoded.0[0].x = 200.0;

    let encoded: Vec<u8> = bincode::serialize(&decoded).unwrap();

    println!("Sending {:?}", encoded);
    socket.send_to(&encoded, &src)?;

    Ok(())
}
