/// A capability which an execution component may declare.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Capability {
    MarketOrder = 0,
    LimitOrder = 1,
    PartialFill = 2,
    PostOnly = 3,
    ReduceOnly = 4,
    Funding = 5,
    Margin = 6,
    TickExecution = 7,
    BarExecution = 8,
    HybridExecution = 9,
    StopMarket = 10,
    StopLimit = 11,
    Gtd = 12,
    Timer = 13,
    LiveProjection = 14,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CapabilityError {
    #[error("required execution capability is not supported")]
    Unsupported { capability: Capability },
}

pub fn validate_capabilities(
    descriptors: &[ModelDescriptor],
    required: CapabilitySet,
) -> Result<(), CapabilityError> {
    for bit in 0..64 {
        let mask = 1_u64 << bit;
        if required.bits() & mask == 0 {
            continue;
        }
        if descriptors
            .iter()
            .all(|descriptor| descriptor.capabilities.bits() & mask == 0)
        {
            let capability = match bit {
                0 => Capability::MarketOrder,
                1 => Capability::LimitOrder,
                2 => Capability::PartialFill,
                3 => Capability::PostOnly,
                4 => Capability::ReduceOnly,
                5 => Capability::Funding,
                6 => Capability::Margin,
                7 => Capability::TickExecution,
                8 => Capability::BarExecution,
                9 => Capability::HybridExecution,
                10 => Capability::StopMarket,
                11 => Capability::StopLimit,
                12 => Capability::Gtd,
                13 => Capability::Timer,
                14 => Capability::LiveProjection,
                _ => continue,
            };
            return Err(CapabilityError::Unsupported { capability });
        }
    }
    Ok(())
}

/// Allocation-free capability bitmap used during startup validation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CapabilitySet(u64);

impl CapabilitySet {
    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u64 {
        self.0
    }

    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | (1_u64 << capability as u8))
    }

    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & (1_u64 << capability as u8) != 0
    }

    pub const fn contains_all(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

/// Stable identity and capability declaration for a configured model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelDescriptor {
    pub id: &'static str,
    pub version: u32,
    pub capabilities: CapabilitySet,
}

impl ModelDescriptor {
    pub const fn new(id: &'static str, version: u32, capabilities: CapabilitySet) -> Self {
        Self {
            id,
            version,
            capabilities,
        }
    }

    pub const fn supports(self, capability: Capability) -> bool {
        self.capabilities.contains(capability)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_set_is_composable_and_stable() {
        let capabilities = CapabilitySet::empty()
            .with(Capability::MarketOrder)
            .with(Capability::TickExecution);
        let descriptor = ModelDescriptor::new("tick-l2", 1, capabilities);

        assert!(descriptor.supports(Capability::MarketOrder));
        assert!(descriptor.supports(Capability::TickExecution));
        assert!(!descriptor.supports(Capability::BarExecution));
        assert_eq!(capabilities.bits(), (1 << 0) | (1 << 7));
    }
}
