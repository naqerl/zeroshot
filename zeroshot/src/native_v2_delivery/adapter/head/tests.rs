use super::*;

#[test]
fn uncertain_head_recovery_never_silently_adopts_changed_work() {
    for (outcome, head_changed, expected_diagnostic) in [
        (
            ForgeReconciliationOutcome::Adopted,
            false,
            "without its mutation receipt",
        ),
        (
            ForgeReconciliationOutcome::Unchanged,
            true,
            "changed remote head",
        ),
    ] {
        let recovered = recovered_outcome(outcome, head_changed);
        let ForgeReconciliationOutcome::NeedsWork(diagnostic) = recovered else {
            panic!("uncertain recovery must require review: {recovered:?}");
        };
        assert!(diagnostic.contains(expected_diagnostic), "{diagnostic}");
    }

    for outcome in [
        ForgeReconciliationOutcome::Unchanged,
        ForgeReconciliationOutcome::NeedsWork("existing repair".to_owned()),
        ForgeReconciliationOutcome::Refused("existing refusal".to_owned()),
    ] {
        assert_eq!(recovered_outcome(outcome.clone(), false), outcome);
    }
}
