use anyhow::Context;
use std::io::{StdoutLock, Write};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
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

pub trait Node<S, Payload> {
    fn from_init(state: S, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned,
    N: Node<S, P>,
{
    let stdin = std::io::stdin();
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

    let mut node: N = Node::from_init(init_state, init).context("Node initialization failed")?;

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

    for line in stdin {
        let line = line.context("Maelstrom input from STDIN could not be read.")?;
        let input = serde_json::from_str::<Message<P>>(&line);
        let input = input.context("Maelstrom input from STDIN could not be deserialized.")?;
        node.step(input, &mut stdout)
            .context("Node step function failed.")?;
    }

    Ok(())
}
