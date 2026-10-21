//! Workload checkers for transactional tests (bank conservation, write skew).

/// A read of all accounts at one snapshot must sum to the constant total.
pub fn check_bank_snapshot(balances: &[i64], expected_total: i64) -> Result<(), String> {
    let s: i64 = balances.iter().sum();
    if s == expected_total {
        Ok(())
    } else {
        Err(format!("bank invariant violated: total {s} != {expected_total} ({balances:?})"))
    }
}

/// Write-skew detector for the classic "on-call doctors" test: two doctors
/// each check that the other is on call and then take themselves off.
/// Snapshot isolation allows both to commit; serializable must not.
/// `final_state` = (doctor_a_on_call, doctor_b_on_call).
pub fn write_skew_occurred(final_state: (bool, bool)) -> bool {
    !final_state.0 && !final_state.1
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bank_and_skew() {
        assert!(check_bank_snapshot(&[10, 20, 30], 60).is_ok());
        assert!(check_bank_snapshot(&[10, 20, 29], 60).is_err());
        assert!(write_skew_occurred((false, false)));
        assert!(!write_skew_occurred((true, false)));
    }
}
