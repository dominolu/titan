//! Allocation probe for the V14 object access path (acceptance matrix item 14).
//!
//! The probe lives in its own test module because only one `#[global_allocator]` may exist per
//! test binary. It replays what the generated callbacks do — scan the frozen reference array,
//! read one payload, read one fee record — and asserts the count stays at zero after warm-up.

#[cfg(test)]
mod tests {
    use crate::object_table::{
        AccountObjectInput, CurrencyObjectInput, FeeObjectInput, ObjectTable, ObjectTableInput,
        SymbolObjectInput,
    };
    use crate::context::{
        FEE_FLAG_VALID, OBJECT_DOMAIN_SYMBOL, TitanObjectRef, TitanSymbolPayloadV1, Valuation,
        MarginMode, PositionMode,
    };
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::hint::black_box;
    use std::sync::Arc;
    use titan_domain_types::DecimalUnit;

    thread_local! {
        static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
    }

    struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let _ = ALLOCATIONS.try_with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            unsafe { System.dealloc(pointer, layout) };
        }
        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let _ = ALLOCATIONS.try_with(|count| {
                if let Some(value) = count.get() {
                    count.set(Some(value + 1));
                }
            });
            unsafe { System.realloc(pointer, layout, size) }
        }
    }

    #[global_allocator]
    static ALLOCATOR: CountingAllocator = CountingAllocator;

    fn unit(value: &str) -> DecimalUnit {
        value.parse().unwrap()
    }

    /// Mirrors the generated `ctx.symbol(0)` scan, including the guard conditions.
    fn symbol_scan(refs: &[TitanObjectRef]) -> (u64, u8) {
        let mut observed = (0u64, 0u8);
        for reference in refs {
            if reference.domain == OBJECT_DOMAIN_SYMBOL
                && reference.object_id == 0
                && reference.flags & 1 != 0
                && reference.schema_major == 1
                && reference.schema_minor >= 1
                && !reference.payload.is_null()
                && reference.payload as usize % 8 == 0
                && reference.payload_len >= size_of::<TitanSymbolPayloadV1>() as u64
            {
                let payload: TitanSymbolPayloadV1 =
                    unsafe { std::ptr::read_unaligned(reference.payload.cast()) };
                observed = match payload.price_tick.unit() {
                    Some(value) => (value.coefficient(), value.scale()),
                    None => (0, 0),
                };
            }
        }
        observed
    }

    fn table() -> ObjectTable {
        ObjectTable::build(ObjectTableInput {
            symbols: vec![SymbolObjectInput {
                local_asset_no: 0,
                asset_id: 1,
                native_symbol: Arc::from("TEST"),
                source_key: Arc::from("venue"),
                price_tick: unit("0.1"),
                quantity_lot: unit("0.01"),
                contract_multiplier: unit("1"),
                min_quantity: Some(unit("0.01")),
                min_notional_quote: Some(unit("5")),
                quote_currency: Arc::from("USDT"),
                margin_currency: Arc::from("USDT"),
                valuation: Valuation::Linear,
                tradable: true,
                synthetic: false,
                metadata_version: 1,
            }],
            accounts: vec![AccountObjectInput {
                local_account_no: 0,
                account_id: 1,
                quote_currency: Arc::from("USDT"),
                margin_currency: Arc::from("USDT"),
                position_mode: PositionMode::Net,
                margin_mode: MarginMode::Cross,
                margin_domain: 0,
                ready: true,
                synthetic: false,
                currencies: vec![CurrencyObjectInput {
                    currency_no: 1,
                    native_currency: Arc::from("USDT"),
                    amount_unit: unit("0.000001"),
                    quote: true,
                    margin: true,
                    settlement: true,
                }],
            }],
            pairs: vec![],
            fees: vec![FeeObjectInput {
                local_account_no: 0,
                maker_fee_bps: Some(-2),
                taker_fee_bps: Some(5),
                source_config: true,
                observed_at_ns: 1,
            }],
            metadata_snapshot_digest: [0; 32],
        })
        .expect("table admits the fixture")
    }

    #[test]
    fn object_access_is_allocation_free_after_build() {
        let table = table();
        let refs = table.refs();

        // Warm-up: first reads may touch lazily initialised machinery.
        black_box(symbol_scan(black_box(refs)));
        black_box(table.fee(0));
        black_box(table.fee(0).map(|fee| fee.flags & FEE_FLAG_VALID));

        ALLOCATIONS.with(|value| value.set(Some(0)));
        for _ in 0..1_000 {
            assert_eq!(black_box(symbol_scan(black_box(refs))), (1, 1));
            let fee = black_box(table.fee(0)).expect("fee object exists");
            assert_eq!(fee.maker_fee_bps, -2);
            assert_eq!(fee.flags & FEE_FLAG_VALID, FEE_FLAG_VALID);
        }
        let allocations = ALLOCATIONS.with(|value| value.replace(None).unwrap());
        assert_eq!(allocations, 0, "object access must not allocate");
    }
}
