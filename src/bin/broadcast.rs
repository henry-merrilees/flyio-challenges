use flyio_challenges::*;
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    time::Duration,
};

use rand::Rng;

use serde::{Deserialize, Serialize};

use anyhow::Context;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

#[allow(dead_code)]
struct BroadcastNode {
    node: String,
    id: usize,
    messages: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

#[derive(Debug)]
enum InjectedPayload {
    Gossip,
}

impl Node<(), Payload, InjectedPayload> for BroadcastNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(300));
                let send_result = tx.send(Event::Injected(InjectedPayload::Gossip));
                if send_result.is_err() {
                    // EOF reached, we can close
                    break;
                };
            }
        });

        Ok(Self {
            node: init.node_id,
            id: 1,
            messages: HashSet::new(),
            known: init
                .node_ids
                .into_iter()
                .map(|nid| (nid, HashSet::<usize>::new()))
                .collect(),
            neighborhood: Vec::new(),
        })
    }
    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let reply_payload = match input.body.payload {
                    Payload::Broadcast { message } => {
                        self.messages.insert(message);
                        Some(Payload::BroadcastOk)
                    }
                    Payload::Gossip { seen } => {
                        self.known
                            .get_mut(&input.src)
                            .expect("sender not initialized in known")
                            .extend(seen.clone());
                        self.messages.extend(seen);
                        None // No reply needed
                    }

                    Payload::Read => {
                        let messages = self.messages.clone();
                        Some(Payload::ReadOk { messages })
                    }
                    Payload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node)
                            .unwrap_or_else(|| panic!("no topology given for node {}", self.node));
                        Some(Payload::TopologyOk)
                    }
                    Payload::ReadOk { messages } => {
                        self.messages.extend(messages);
                        None
                    }
                    Payload::BroadcastOk | Payload::TopologyOk => None,
                };

                if let Some(payload) = reply_payload {
                    Message {
                        src: input.dest, // Aruably this should be self.node_id regardless of whether we validate
                        dest: input.src,
                        body: Body {
                            id: Some(self.id),
                            in_reply_to: input.body.id,
                            payload,
                        },
                    }
                    .send(output)?;
                    self.id += 1;
                }
            }
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known_to_n = &self.known[n];
                        let (already_known, mut notify_of): (HashSet<_>, HashSet<_>) = self
                            .messages
                            .iter()
                            .copied()
                            .partition(|m| !known_to_n.contains(m));

                        let mut rng = rand::thread_rng();

                        notify_of.extend(already_known.iter().filter(|_| {
                            rng.gen_ratio(
                                already_known.len().min(10) as u32,
                                already_known.len() as u32,
                            )
                        }));

                        let gossip = Message {
                            src: self.node.clone(),
                            dest: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::Gossip { seen: notify_of },
                            },
                        };
                        gossip
                            .send(output)
                            .with_context(|| format!("gossip to {:?}", n))?;
                    }
                }
            },
            Event::EOF => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _, _>(())
}
