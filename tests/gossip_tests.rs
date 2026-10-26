#![cfg(test)]

use crate::common::{self, Libp2pInstance, Protocol, build_config, build_tracing_subscriber};
use eth2_libp2p::types::{ForkContext, GossipEncoding, GossipKind, core_topics_to_subscribe};
use eth2_libp2p::{
    GossipTopic, NetworkConfig, NetworkEvent, PeerId, PubsubMessage, Topic, TopicHash,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, sleep};
use types::{
    config::Config,
    eip8025::containers::{ExecutionProofEnvelope, ProofData, SignedExecutionProofEnvelope},
    nonstandard::Phase,
    phase0::primitives::{ForkDigest, H256},
    preset::{Mainnet, Preset},
};

fn execution_proof_config(enable_execution_proof: bool) -> Arc<NetworkConfig> {
    let mut config = Arc::unwrap_or_clone(build_config(vec![], false, None));
    config.enable_execution_proof = enable_execution_proof;
    Arc::new(config)
}

fn topic_hash(kind: GossipKind, fork_digest: ForkDigest) -> TopicHash {
    Topic::from(GossipTopic::new(
        kind,
        GossipEncoding::default(),
        fork_digest,
    ))
    .hash()
}

// Subscribes as Grandine's `subscribe_to_core_topics` does.
fn subscribe_to_core_topics<P: Preset>(
    node: &mut Libp2pInstance<P>,
    chain_config: &Config,
    fork_digest: ForkDigest,
) {
    let topic_config = node.network_globals().as_topic_config();
    for kind in core_topics_to_subscribe(chain_config, Phase::Gloas, &topic_config) {
        assert!(node.subscribe(GossipTopic::new(
            kind,
            GossipEncoding::default(),
            fork_digest
        )));
    }
}

// The topics `observer` has seen `peer_id` subscribe to.
fn peer_topics<P: Preset>(observer: &Libp2pInstance<P>, peer_id: PeerId) -> HashSet<TopicHash> {
    observer
        .gossipsub()
        .all_peers()
        .filter(|(id, _)| **id == peer_id)
        .flat_map(|(_, topics)| topics.into_iter().cloned())
        .collect()
}

fn own_topics<P: Preset>(node: &Libp2pInstance<P>) -> HashSet<TopicHash> {
    node.gossipsub().topics().cloned().collect()
}

fn execution_proof() -> SignedExecutionProofEnvelope {
    SignedExecutionProofEnvelope {
        message: ExecutionProofEnvelope {
            proof_data: ProofData::try_from(vec![0xcd; 1024])
                .expect("proof data should be within bounds"),
            proof_type: 1,
            beacon_block_root: H256::repeat_byte(0xab),
        }
        .into(),
        validator_index: 42,
        signature: Default::default(),
    }
}

fn gloas_setup() -> (Arc<Config>, ForkDigest) {
    let chain_config = Arc::new(Config::mainnet().start_and_stay_in(Phase::Gloas));
    let fork_digest =
        ForkContext::dummy::<Mainnet>(&chain_config, Phase::Gloas).current_fork_digest();
    (chain_config, fork_digest)
}

// With the flag set on both nodes, a published proof reaches the peer as a typed message.
#[tokio::test]
async fn test_execution_proof_gossip_when_enabled() {
    build_tracing_subscriber("debug", false);

    let (chain_config, fork_digest) = gloas_setup();
    let (mut sender, mut receiver) = common::build_node_pair_with_configs::<Mainnet>(
        &chain_config,
        Phase::Gloas,
        Protocol::Tcp,
        execution_proof_config(true),
        execution_proof_config(true),
    )
    .await;

    subscribe_to_core_topics(&mut sender, &chain_config, fork_digest);
    subscribe_to_core_topics(&mut receiver, &chain_config, fork_digest);

    let execution_proof_topic = topic_hash(GossipKind::ExecutionProof, fork_digest);
    assert!(own_topics(&receiver).contains(&execution_proof_topic));
    let receiver_id = receiver.network_globals().local_peer_id();

    let message = PubsubMessage::<Mainnet>::ExecutionProof(Arc::new(execution_proof()));

    let sender_future = async {
        let mut tick = interval(Duration::from_millis(50));
        let mut published = false;
        loop {
            tokio::select! {
                _ = sender.next_event() => {}
                _ = tick.tick() => {}
            }
            // Publish once the sender has seen the receiver subscribe to the topic.
            if !published && peer_topics(&sender, receiver_id).contains(&execution_proof_topic) {
                sender.publish_with_digest(message.clone(), fork_digest);
                published = true;
            }
        }
    };

    let receiver_future = async {
        loop {
            if let NetworkEvent::PubsubMessage {
                topic,
                message: received,
                ..
            } = receiver.next_event().await
            {
                assert_eq!(topic, execution_proof_topic);
                assert_eq!(received, message);
                return;
            }
        }
    };

    tokio::select! {
        _ = sender_future => {}
        _ = receiver_future => {}
        _ = sleep(Duration::from_secs(30)) => {
            panic!("Future timed out");
        }
    }
}

// With the flag off on the receiver, it neither subscribes to nor whitelists the topic.
#[tokio::test]
async fn test_execution_proof_gossip_when_disabled_on_peer() {
    build_tracing_subscriber("debug", false);

    let (chain_config, fork_digest) = gloas_setup();
    let (mut sender, mut receiver) = common::build_node_pair_with_configs::<Mainnet>(
        &chain_config,
        Phase::Gloas,
        Protocol::Tcp,
        execution_proof_config(true),
        execution_proof_config(false),
    )
    .await;

    subscribe_to_core_topics(&mut sender, &chain_config, fork_digest);
    subscribe_to_core_topics(&mut receiver, &chain_config, fork_digest);

    let execution_proof_topic = topic_hash(GossipKind::ExecutionProof, fork_digest);
    let receiver_id = receiver.network_globals().local_peer_id();
    let receiver_topics = own_topics(&receiver);
    assert!(receiver_topics.contains(&topic_hash(GossipKind::BeaconBlock, fork_digest)));
    assert!(!receiver_topics.contains(&execution_proof_topic));
    assert!(
        !receiver
            .network_globals()
            .gossipsub_subscriptions
            .read()
            .iter()
            .any(|topic| *topic.kind() == GossipKind::ExecutionProof)
    );

    // Drive both nodes until the sender has seen every subscription the receiver holds.
    let sync_future = async {
        let mut tick = interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = sender.next_event() => {}
                _ = receiver.next_event() => {}
                _ = tick.tick() => {}
            }
            if peer_topics(&sender, receiver_id) == receiver_topics {
                return;
            }
        }
    };

    tokio::select! {
        _ = sync_future => {}
        _ = sleep(Duration::from_secs(30)) => {
            panic!("Future timed out");
        }
    }

    assert!(
        !sender
            .gossipsub()
            .all_peers()
            .any(|(_, topics)| topics.contains(&&execution_proof_topic))
    );

    // The receiver's whitelist does not admit the topic.
    let topic = Topic::from(GossipTopic::new(
        GossipKind::ExecutionProof,
        GossipEncoding::default(),
        fork_digest,
    ));
    assert!(receiver.gossipsub_mut().subscribe(&topic).is_err());
}
