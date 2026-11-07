use openengine_cluster_protocol::{GraphNode, MapNode, NodeName};

use super::NativeV2AdmissionError;
use crate::native_v2_delivery::DeliveryMode;

#[derive(Default)]
struct PossibleWriters<'a> {
    writer: Option<&'a NodeName>,
    delivery: Option<&'a NodeName>,
}

impl<'a> PossibleWriters<'a> {
    fn append(&mut self, other: Self) {
        self.writer = self.writer.or(other.writer);
        self.delivery = self.delivery.or(other.delivery);
    }
}

// Ordinary writers share the candidate. Trusted delivery must have exclusive mutation authority
// before committing or publishing; rejecting only the final receipt would be too late.
pub(super) fn validate_delivery_concurrency(
    node: &GraphNode,
) -> Result<(), NativeV2AdmissionError> {
    if max_delivery_nodes_per_path(node)? > 1 {
        return Err(NativeV2AdmissionError::MultipleDeliveriesOnPath);
    }
    possible_writers(node).map(|_| ())
}

pub(super) fn max_delivery_nodes_per_path(node: &GraphNode) -> Result<u64, NativeV2AdmissionError> {
    match node {
        GraphNode::Step(_) | GraphNode::Succeed(_) | GraphNode::Fail(_) => Ok(0),
        GraphNode::Verifier(verifier) => Ok(u64::from(
            DeliveryMode::from_worker(&verifier.worker).is_some()
                || verifier.worker.as_str()
                    == crate::native_v2_comment_delivery::COMMENT_DELIVERY_WORKER_REF,
        )),
        GraphNode::Seq(group) => group
            .children
            .as_slice()
            .iter()
            .try_fold(0, |total, child| {
                Ok(total + max_delivery_nodes_per_path(child)?)
            }),
        GraphNode::Choice(group) => group
            .branches
            .as_slice()
            .iter()
            .map(|branch| max_delivery_nodes_per_path(&branch.node))
            .chain(group.otherwise.as_deref().map(max_delivery_nodes_per_path))
            .try_fold(0, |maximum, count| Ok(maximum.max(count?))),
        GraphNode::Par(group) => group
            .branches
            .as_slice()
            .iter()
            .map(max_delivery_nodes_per_path)
            .try_fold(0, |maximum, count| Ok(maximum.max(count?))),
        GraphNode::Loop(group) => max_delivery_nodes_per_path(&group.body),
        GraphNode::Map(group) => max_delivery_nodes_per_path(&group.body),
    }
}

fn possible_writers(node: &GraphNode) -> Result<PossibleWriters<'_>, NativeV2AdmissionError> {
    match node {
        GraphNode::Step(step) => Ok(PossibleWriters {
            writer: Some(&step.name),
            delivery: None,
        }),
        GraphNode::Verifier(verifier) => {
            let delivery = (DeliveryMode::from_worker(&verifier.worker).is_some()
                || verifier.worker.as_str()
                    == crate::native_v2_comment_delivery::COMMENT_DELIVERY_WORKER_REF)
                .then_some(&verifier.name);
            Ok(PossibleWriters {
                writer: Some(&verifier.name),
                delivery,
            })
        }
        GraphNode::Seq(group) => collect_writers(group.children.as_slice().iter(), false),
        GraphNode::Choice(group) => collect_writers(
            group
                .branches
                .as_slice()
                .iter()
                .map(|branch| &branch.node)
                .chain(group.otherwise.as_deref()),
            false,
        ),
        GraphNode::Par(group) => collect_writers(group.branches.as_slice().iter(), true),
        GraphNode::Loop(group) => possible_writers(&group.body),
        GraphNode::Map(group) => map_writers(group),
        GraphNode::Succeed(_) | GraphNode::Fail(_) => Ok(PossibleWriters::default()),
    }
}

fn collect_writers<'a>(
    nodes: impl Iterator<Item = &'a GraphNode>,
    parallel: bool,
) -> Result<PossibleWriters<'a>, NativeV2AdmissionError> {
    let mut combined = PossibleWriters::default();
    for node in nodes {
        let next = possible_writers(node)?;
        if parallel {
            let overlap = combined
                .delivery
                .zip(next.writer)
                .or_else(|| next.delivery.zip(combined.writer));
            if let Some((delivery, writer)) = overlap {
                return Err(NativeV2AdmissionError::ConcurrentDelivery {
                    delivery: delivery.clone(),
                    writer: writer.clone(),
                });
            }
        }
        combined.append(next);
    }
    Ok(combined)
}

fn map_writers(group: &MapNode) -> Result<PossibleWriters<'_>, NativeV2AdmissionError> {
    let writers = possible_writers(&group.body)?;
    if group.max_items.get() > 1
        && let Some(delivery) = writers.delivery
    {
        return Err(NativeV2AdmissionError::ConcurrentMapDelivery {
            map: group.name.clone(),
            delivery: delivery.clone(),
        });
    }
    Ok(writers)
}
