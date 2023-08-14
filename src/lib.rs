use anyhow::Context;
use std::io::{BufRead, StdoutLock, Write};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub trait Node<S, Payload, InjectedPayload = ()>
where
    InjectedPayload: std::fmt::Debug,
{
    fn from_init(
        state: S,
        init: Init,
        inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub enum Event<Payload, InjectedPayload = ()>
where
    InjectedPayload: std::fmt::Debug,
{
    Message(Message<Payload>),
    Injected(InjectedPayload),
    EOF,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

impl<Payload: Serialize> Message<Payload> {
    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *output, self).context("serialize response to echo.")?;
        output.write_all(b"\n").context("Write trailing line.")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

pub fn main_loop<S, N, P, IP>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + std::fmt::Debug + Send + 'static,
    IP: Send + std::fmt::Debug + 'static,
    N: Node<S, P, IP>,
{
    let (tx, rx) = std::sync::mpsc::channel();

    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str::<Message<InitPayload>>(
        &stdin
            .next()
            .expect("Failed to read init message from stdin.")?,
    )
    .context("Init message could not be deserialized.")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("First message should be init.");
    };

    let mut node: N =
        Node::from_init(init_state, init, tx.clone()).context("Node initialization failed")?;

    let reply = Message {
        src: init_msg.dest,
        dest: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    serde_json::to_writer(&mut stdout, &reply).context("Serialize init_ok response to init.")?;
    stdout
        .write_all(b"\n")
        .context("Write trailing newline to init response.")?;

    let stdin_tx = tx.clone();

    drop(stdin);
    let jh = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let line = line.context("Maelstrom input from STDIN could not be read.")?;
            let input: Message<P> = serde_json::from_str(&line)
                .context("Maelstrom input from STDIN could not be deserialized.")?;
            let send_result = stdin_tx.send(Event::Message(input));
            if send_result.is_err() {
                return Ok::<_, anyhow::Error>(());
            };
        }
        let _ = tx.send(Event::EOF);
        Ok(())
    });

    for input in rx {
        node.step(input, &mut stdout)
            .context("Node step function failed.")?;
    }

    jh.join()
        .expect("thread panicked")
        .expect("thread returned err");

    Ok(())
}
