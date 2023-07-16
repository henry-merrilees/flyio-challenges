use anyhow::Context;
use flyio_challenges::*;
use std::{
    collections::{HashMap, HashSet},
    io::{StdoutLock, Write},
};

use serde::{Deserialize, Serialize};

type Topology = HashMap<String, HashSet<String>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast { message: usize },
    BroadcastOk,
    Read,
    ReadOk { messages: HashSet<usize> },
    Topology { topology: Topology },
    TopologyOk,
}

struct BroadcastNode {
    node_id: String,
    local_id: usize,
    messages: HashSet<usize>,
    topology: Topology,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self> {
        Ok(Self {
            node_id: init.node_id,
            local_id: 1,
            messages: HashSet::new(),
            topology: Topology::new(),
        })
    }

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let response_payload = match input.body.payload {
            Payload::Broadcast { message } => {
                self.messages.insert(message);
                Some(Payload::BroadcastOk)
            }
            Payload::Read => Some(Payload::ReadOk {
                messages: self.messages.clone(),
            }),
            Payload::Topology { topology } => {
                for (node, new_neighbors) in topology.into_iter() {
                    if let Some(existing_neighbors) = self.topology.get_mut(&node) {
                        existing_neighbors.extend(new_neighbors);
                    } else {
                        self.topology.insert(node, new_neighbors);
                    }
                }
                Some(Payload::TopologyOk)
            }
            Payload::BroadcastOk => None,
            Payload::ReadOk { messages } => {
                self.messages.extend(messages);
                None
            }
            Payload::TopologyOk => None,
        };

        if let Some(payload) = response_payload {
            let reply = Message {
                src: input.dest, // Aruably this should be self.node_id regardless of whether we validate
                dest: input.src,
                body: Body {
                    id: Some(self.local_id),
                    in_reply_to: input.body.id,
                    payload,
                },
            };

            serde_json::to_writer(&mut *output, &reply).context("serialize response to echo.")?;
            output.write_all(b"\n").context("Write trailing line.")?;
            self.local_id += 1;
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
