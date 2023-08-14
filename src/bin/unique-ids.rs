use anyhow::Context;
use flyio_challenges::*;
use std::io::{StdoutLock, Write};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

struct UniqueNode {
    node_id: String,
    local_id: usize,
}

impl Node<(), Payload> for UniqueNode {
    fn from_init(
        _state: (),
        init: Init,
        _tx: std::sync::mpsc::Sender<Event<Payload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: init.node_id,
            local_id: 1,
        })
    }
    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("Invalid event injection");
        };
        match input.body.payload {
            Payload::Generate => {
                let guid = format!("{}-{}", self.node_id, self.local_id);

                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        id: Some(self.local_id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk { guid },
                    },
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to echo.")?;
                output.write_all(b"\n").context("Write trailing line.")?;
                self.local_id += 1;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueNode, _, _>(())
}
