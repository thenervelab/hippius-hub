//! Pure part-plan helpers — no I/O, unit-tested. The receiver derives the part
//! count the same way the client does (`ceil(size / part_size)`), so both
//! agree on which part numbers must exist before a blob can be finalized.

use std::collections::HashSet;

/// Clamp the client's requested part size into `[min, max]`. A `0` request
/// (or any below `min`) becomes `min`; anything above `max` is capped so a
/// hostile client cannot force an unbounded per-part buffer.
pub(crate) fn clamp_part_size(requested: u64, min: u64, max: u64) -> u64 {
    requested.clamp(min, max)
}

/// Number of parts covering `size` bytes at `part_size` each. Returns 0 for an
/// empty blob or a zero `part_size`; the initiate handler rejects both before
/// this is used to build a session.
pub(crate) fn num_parts(size: u64, part_size: u64) -> u32 {
    if size == 0 || part_size == 0 {
        return 0;
    }
    // `unwrap_or` (not the denied `unwrap`) saturates the u64->u32 narrowing.
    size.div_ceil(part_size).try_into().unwrap_or(u32::MAX)
}

/// The 1-based part numbers in `1..=num_parts` not present in `received`.
/// Empty means every part landed and the blob can be finalized.
pub(crate) fn missing_parts(num_parts: u32, received: &HashSet<u32>) -> Vec<u32> {
    (1..=num_parts).filter(|n| !received.contains(n)).collect()
}

#[cfg(test)]
mod tests {
    use super::{clamp_part_size, missing_parts, num_parts};
    use std::collections::HashSet;

    #[test]
    fn num_parts_boundaries() {
        assert_eq!(num_parts(0, 4), 0);
        assert_eq!(num_parts(8, 4), 2);
        assert_eq!(num_parts(10, 4), 3, "10 bytes at 4 -> 4+4+2");
        assert_eq!(num_parts(1000, 0), 0, "zero part_size does not divide-by-zero");
    }

    #[test]
    fn clamp_part_size_bounds() {
        assert_eq!(clamp_part_size(0, 10, 100), 10, "zero clamps up to min");
        assert_eq!(clamp_part_size(50, 10, 100), 50, "in-range passes through");
        assert_eq!(clamp_part_size(999, 10, 100), 100, "over-large caps at max");
    }

    #[test]
    fn missing_parts_reports_only_gaps() {
        let mut received = HashSet::new();
        received.insert(1);
        received.insert(3);
        assert_eq!(missing_parts(3, &received), vec![2]);
        received.insert(2);
        assert!(missing_parts(3, &received).is_empty(), "all parts present -> nothing missing");
    }
}
