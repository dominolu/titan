from __future__ import annotations

import math

import polars as pl

from .models import (
    IssueSeverity,
    ReportBundle,
    ReportConfig,
    ValidationIssue,
    ValidationResult,
    status_from_issues,
)


PORTFOLIO_REQUIRED = {
    "timestamp",
    "equity_gross",
    "equity_net",
    "fee",
    "rebate",
    "funding",
    "external_flow",
    "reporting_currency",
}

ACCOUNT_REQUIRED = {
    "timestamp",
    "view_kind",
    "venue_id",
    "currency_id",
    "balance",
    "fee",
    "rebate",
    "funding",
    "realized_pnl",
    "unrealized_pnl",
    "margin",
}

POSITION_REQUIRED = {
    "timestamp",
    "view_kind",
    "venue_id",
    "instrument_id",
    "currency_id",
    "quantity",
    "mark_price",
    "notional",
    "realized_pnl",
    "unrealized_pnl",
    "margin",
}

FILL_REQUIRED = {
    "timestamp",
    "exchange_timestamp",
    "fill_id",
    "order_id",
    "venue_id",
    "instrument_id",
    "side",
    "quantity",
    "price",
    "notional",
    "fee",
    "rebate",
    "currency",
    "liquidity",
}

ORDER_REQUIRED = {
    "timestamp",
    "exchange_timestamp",
    "event_sequence",
    "order_id",
    "venue_id",
    "instrument_id",
    "side",
    "status",
    "request",
    "quantity",
    "price",
    "executed_quantity",
    "reason",
}

ROUND_TRIP_REQUIRED = {
    "round_trip_id",
    "venue_id",
    "instrument_id",
    "entry_timestamp",
    "exit_timestamp",
    "side",
    "quantity",
    "entry_price",
    "exit_price",
    "gross_pnl",
    "fee",
    "rebate",
    "funding",
    "net_pnl",
    "currency",
}

FX_REQUIRED = {"timestamp", "currency", "reporting_currency", "rate"}

RISK_REQUIRED = {
    "timestamp",
    "event_id",
    "limit_id",
    "scope",
    "metric",
    "observed_value",
    "limit_value",
    "utilization",
    "breached",
}

MARK_REQUIRED = {
    "timestamp",
    "exchange_timestamp",
    "venue_id",
    "instrument_id",
    "mark_price",
    "source",
    "age_ns",
    "stale",
}


def _conversion_issues(
    frame: pl.DataFrame,
    table: str,
    config: ReportConfig,
    *,
    currency_field: str,
    amount_fields: set[str],
) -> list[ValidationIssue]:
    """Validate explicit row-level conversion while retaining source-currency values."""
    if currency_field not in frame.columns:
        return []
    currencies = frame[currency_field].drop_nulls().unique().to_list()
    foreign = [str(item) for item in currencies if str(item) != config.reporting_currency]
    if not foreign:
        return []
    required = {"fx_rate_to_reporting"}.union(
        f"{name}_reporting" for name in amount_fields.intersection(frame.columns)
    )
    missing = required.difference(frame.columns)
    if missing:
        return [
            _issue(
                f"{table}.fx_conversion_missing",
                f"foreign currencies {foreign!r} require conversion fields: {sorted(missing)}",
                IssueSeverity.ERROR,
                table=table,
                field=currency_field,
            )
        ]
    foreign_rows = frame.filter(pl.col(currency_field) != config.reporting_currency)
    issues: list[ValidationIssue] = []
    invalid_rates = foreign_rows.select(
        (
            pl.col("fx_rate_to_reporting").is_null()
            | ~pl.col("fx_rate_to_reporting").is_finite()
            | (pl.col("fx_rate_to_reporting") <= 0.0)
        ).sum()
    ).item()
    if invalid_rates:
        issues.append(
            _issue(
                f"{table}.fx_rate_unavailable",
                "foreign-currency rows lack a finite positive as-of FX rate",
                IssueSeverity.ERROR,
                table=table,
                field="fx_rate_to_reporting",
                count=int(invalid_rates),
            )
        )
    for name in amount_fields.intersection(frame.columns):
        converted = f"{name}_reporting"
        missing_values = foreign_rows.filter(pl.col(name).is_not_null()).select(
            pl.col(converted).is_null().sum()
        ).item()
        if missing_values:
            issues.append(
                _issue(
                    f"{table}.fx_value_unavailable",
                    f"{table}.{converted} is missing for foreign-currency source values",
                    IssueSeverity.ERROR,
                    table=table,
                    field=converted,
                    count=int(missing_values),
                )
            )
        if not frame.schema[name].is_numeric() or not frame.schema[converted].is_numeric():
            continue
        expected = pl.col(name) * pl.col("fx_rate_to_reporting")
        tolerance = pl.max_horizontal(
            pl.lit(1e-9),
            expected.abs() * 1e-10,
        )
        inconsistent = foreign_rows.filter(pl.col(name).is_not_null()).select(
            ((pl.col(converted) - expected).abs() > tolerance).sum()
        ).item()
        if inconsistent:
            issues.append(
                _issue(
                    f"{table}.fx_value_inconsistent",
                    f"{table}.{converted} does not equal source value times FX rate",
                    IssueSeverity.ERROR,
                    table=table,
                    field=converted,
                    count=int(inconsistent),
                )
            )
    return issues


def _issue(
    code: str,
    message: str,
    severity: IssueSeverity,
    *,
    table: str | None = None,
    field: str | None = None,
    count: int | None = None,
) -> ValidationIssue:
    return ValidationIssue(code, message, severity, table, field, count)


def _numeric_quality(
    frame: pl.DataFrame,
    table: str,
    fields: set[str],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for name in fields.intersection(frame.columns):
        if frame[name].null_count() == len(frame):
            continue
        if not frame.schema[name].is_numeric():
            issues.append(
                _issue(
                    "schema.non_numeric",
                    f"{table}.{name} must be numeric",
                    IssueSeverity.ERROR,
                    table=table,
                    field=name,
                )
            )
            continue
        invalid = frame.select(
            ((pl.col(name).is_not_null()) & (~pl.col(name).is_finite())).sum()
        ).item()
        if invalid:
            issues.append(
                _issue(
                    "data.non_finite",
                    f"{table}.{name} contains non-finite values",
                    IssueSeverity.ERROR,
                    table=table,
                    field=name,
                    count=int(invalid),
                )
            )
    return issues


def _required_value_issues(
    frame: pl.DataFrame,
    table: str,
    fields: set[str],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    for name in fields.intersection(frame.columns):
        nulls = frame[name].null_count()
        if nulls:
            issues.append(
                _issue(
                    f"{table}.required_value_null",
                    f"required field {table}.{name} contains null values",
                    IssueSeverity.ERROR,
                    table=table,
                    field=name,
                    count=nulls,
                )
            )
    return issues


def _reporting_view(frame: pl.DataFrame) -> pl.DataFrame | None:
    """Select the local-delivered ledger view used by portfolio snapshots."""
    if "view_kind" not in frame.columns:
        return None
    kinds = [str(item) for item in frame["view_kind"].drop_nulls().unique().to_list()]
    for preferred in ("local_delivered", "local_delivery"):
        if preferred in kinds:
            return frame.filter(pl.col("view_kind") == preferred)
    if len(kinds) == 1:
        return frame
    return None


def _timestamp_quality(
    frame: pl.DataFrame,
    table: str,
    *,
    allow_duplicates: bool = False,
    field: str = "timestamp",
    require_monotonic: bool = True,
) -> list[ValidationIssue]:
    if field not in frame.columns:
        return [
            _issue(
                "schema.timestamp_missing",
                f"{table}.{field} is required",
                IssueSeverity.ERROR,
                table=table,
                field=field,
            )
        ]
    issues: list[ValidationIssue] = []
    if not isinstance(frame.schema[field], pl.Datetime):
        issues.append(
            _issue(
                "schema.timestamp_type",
                f"{table}.{field} must be a Polars Datetime",
                IssueSeverity.ERROR,
                table=table,
                field=field,
            )
        )
        return issues
    nulls = frame[field].null_count()
    if nulls:
        issues.append(
            _issue(
                "data.timestamp_null",
                f"{table}.{field} contains null values",
                IssueSeverity.ERROR,
                table=table,
                field=field,
                count=nulls,
            )
        )
        return issues
    if len(frame) > 1 and require_monotonic:
        out_of_order = frame.select((pl.col(field).diff() < pl.duration()).sum()).item()
        if out_of_order:
            issues.append(
                _issue(
                    "data.timestamp_not_monotonic",
                    f"{table}.{field} is not monotonic",
                    IssueSeverity.ERROR,
                    table=table,
                    field=field,
                    count=int(out_of_order),
                )
            )
        duplicates = frame.select(pl.col(field).is_duplicated().sum()).item()
        if duplicates and not allow_duplicates:
            issues.append(
                _issue(
                    "data.timestamp_duplicate",
                    f"{table}.{field} contains duplicate values",
                    IssueSeverity.WARNING,
                    table=table,
                    field=field,
                    count=int(duplicates),
                )
            )
    return issues


def validate_bundle(bundle: ReportBundle, config: ReportConfig) -> ValidationResult:
    issues: list[ValidationIssue] = []
    portfolio = bundle.portfolio_snapshots
    if portfolio.is_empty():
        issues.append(
            _issue(
                "portfolio.empty",
                "portfolio_snapshots must contain at least one row",
                IssueSeverity.ERROR,
                table="portfolio_snapshots",
            )
        )
        return ValidationResult(status_from_issues(issues), tuple(issues))

    missing = PORTFOLIO_REQUIRED.difference(portfolio.columns)
    if missing:
        issues.append(
            _issue(
                "portfolio.required_fields",
                f"portfolio_snapshots is missing required fields: {sorted(missing)}",
                IssueSeverity.ERROR,
                table="portfolio_snapshots",
            )
        )
    issues.extend(_timestamp_quality(portfolio, "portfolio_snapshots"))
    issues.extend(
        _numeric_quality(
            portfolio,
            "portfolio_snapshots",
            {
                "equity_gross",
                "equity_net",
                "cash",
                "realized_pnl",
                "unrealized_pnl",
                "fee",
                "rebate",
                "funding",
                "external_flow",
                "gross_exposure",
                "net_exposure",
                "margin",
                "leverage",
                "num_trades",
                "trading_volume",
                "trading_value",
            },
        )
    )
    if missing:
        return ValidationResult(status_from_issues(issues), tuple(issues))

    for name in (
        "equity_gross",
        "equity_net",
        "fee",
        "rebate",
        "funding",
        "external_flow",
        "reporting_currency",
    ):
        nulls = portfolio[name].null_count()
        if nulls:
            issues.append(
                _issue(
                    "portfolio.required_value_null",
                    f"required field portfolio_snapshots.{name} contains null values",
                    IssueSeverity.ERROR,
                    table="portfolio_snapshots",
                    field=name,
                    count=nulls,
                )
            )

    currencies = portfolio["reporting_currency"].drop_nulls().unique().to_list()
    if currencies != [config.reporting_currency]:
        issues.append(
            _issue(
                "portfolio.currency_mismatch",
                f"expected reporting currency {config.reporting_currency!r}, got {currencies!r}",
                IssueSeverity.ERROR,
                table="portfolio_snapshots",
                field="reporting_currency",
            )
        )

    non_positive = portfolio.select((pl.col("equity_net") <= 0.0).sum()).item()
    if non_positive:
        issues.append(
            _issue(
                "portfolio.non_positive_equity",
                "equity_net must stay positive for percentage-return analytics",
                IssueSeverity.ERROR,
                table="portfolio_snapshots",
                field="equity_net",
                count=int(non_positive),
            )
        )

    expected_net = (
        pl.col("equity_gross")
        - pl.col("fee")
        + pl.col("rebate")
        + pl.col("funding")
    )
    scale = max(
        config.initial_capital,
        abs(float(portfolio["equity_net"].drop_nulls().max() or config.initial_capital)),
    )
    tolerance = max(1e-9, scale * 1e-10)
    reconciliation_errors = portfolio.select(
        ((pl.col("equity_net") - expected_net).abs() > tolerance).sum()
    ).item()
    if reconciliation_errors:
        issues.append(
            _issue(
                "portfolio.reconciliation",
                "equity_net does not reconcile with equity_gross - fee + rebate + funding",
                IssueSeverity.ERROR,
                table="portfolio_snapshots",
                field="equity_net",
                count=int(reconciliation_errors),
            )
        )

    for name in ("fee", "rebate", "num_trades", "trading_volume", "trading_value"):
        if name not in portfolio.columns or portfolio[name].null_count() == len(portfolio):
            continue
        decreases = portfolio.select(
            (pl.col(name).fill_null(strategy="forward").diff() < -tolerance).sum()
        ).item()
        if decreases:
            issues.append(
                _issue(
                    "portfolio.cumulative_decrease",
                    f"cumulative field {name} decreases",
                    IssueSeverity.ERROR,
                    table="portfolio_snapshots",
                    field=name,
                    count=int(decreases),
                )
            )

    optional_contracts = {
        "account_snapshots": (
            ACCOUNT_REQUIRED,
            {"balance", "fee", "rebate", "funding", "realized_pnl", "unrealized_pnl", "margin"},
        ),
        "position_snapshots": (
            POSITION_REQUIRED,
            {"quantity", "mark_price", "notional", "realized_pnl", "unrealized_pnl", "margin"},
        ),
    }
    for table_name, (required_fields, numeric_fields) in optional_contracts.items():
        value = getattr(bundle, table_name)
        if value is None:
            issues.append(
                _issue(
                    f"{table_name}.unavailable",
                    f"{table_name} is unavailable; related sections will be skipped",
                    IssueSeverity.WARNING,
                    table=table_name,
                )
            )
        elif value.is_empty():
            issues.append(
                _issue(
                    f"{table_name}.empty",
                    f"{table_name} is empty; related sections will be skipped",
                    IssueSeverity.WARNING,
                    table=table_name,
                )
            )
        else:
            table_missing = required_fields.difference(value.columns)
            if table_missing:
                issues.append(
                    _issue(
                        f"{table_name}.required_fields",
                        f"{table_name} is missing required fields: {sorted(table_missing)}",
                        IssueSeverity.ERROR,
                        table=table_name,
                    )
                )
            for name in required_fields.intersection(value.columns):
                if name in {"realized_pnl", "unrealized_pnl", "margin"}:
                    continue
                nulls = value[name].null_count()
                if nulls:
                    issues.append(
                        _issue(
                            f"{table_name}.required_value_null",
                            f"required field {table_name}.{name} contains null values",
                            IssueSeverity.ERROR,
                            table=table_name,
                            field=name,
                            count=nulls,
                        )
                    )
            issues.extend(_timestamp_quality(value, table_name, allow_duplicates=True))
            issues.extend(_numeric_quality(value, table_name, numeric_fields))
            issues.extend(
                _conversion_issues(
                    value,
                    table_name,
                    config,
                    currency_field="currency_id",
                    amount_fields=numeric_fields.difference({"quantity"}),
                )
            )

    accounts = bundle.account_snapshots
    reporting_accounts = (
        _reporting_view(accounts)
        if accounts is not None and not accounts.is_empty()
        else accounts
    )
    if accounts is not None and not accounts.is_empty() and reporting_accounts is None:
        issues.append(
            _issue(
                "account_snapshots.view_kind_ambiguous",
                "account reconciliation requires one view or a local_delivered view",
                IssueSeverity.ERROR,
                table="account_snapshots",
                field="view_kind",
            )
        )
    if (
        reporting_accounts is not None
        and not reporting_accounts.is_empty()
        and ACCOUNT_REQUIRED.issubset(reporting_accounts.columns)
        and "cash" in portfolio.columns
        and portfolio["cash"].null_count() < len(portfolio)
    ):
        account_balance = (
            "balance_reporting"
            if "balance_reporting" in reporting_accounts.columns
            else "balance"
        )
        account_cash = reporting_accounts.group_by("timestamp").agg(
            pl.col(account_balance).sum().alias("account_cash")
        )
        cash_check = portfolio.select("timestamp", "cash").drop_nulls("cash").join(
            account_cash, on="timestamp", how="inner"
        )
        mismatches = cash_check.select(
            ((pl.col("cash") - pl.col("account_cash")).abs() > tolerance).sum()
        ).item()
        if mismatches:
            issues.append(
                _issue(
                    "account_snapshots.cash_reconciliation",
                    "portfolio cash does not reconcile with account balances",
                    IssueSeverity.ERROR,
                    table="account_snapshots",
                    field="balance",
                    count=int(mismatches),
                )
            )

    positions = bundle.position_snapshots
    reporting_positions = (
        _reporting_view(positions)
        if positions is not None and not positions.is_empty()
        else positions
    )
    if positions is not None and not positions.is_empty() and reporting_positions is None:
        issues.append(
            _issue(
                "position_snapshots.view_kind_ambiguous",
                "position reconciliation requires one view or a local_delivered view",
                IssueSeverity.ERROR,
                table="position_snapshots",
                field="view_kind",
            )
        )
    if (
        reporting_positions is not None
        and not reporting_positions.is_empty()
        and POSITION_REQUIRED.issubset(reporting_positions.columns)
    ):
        position_notional = (
            "notional_reporting"
            if "notional_reporting" in reporting_positions.columns
            else "notional"
        )
        position_exposure = reporting_positions.group_by("timestamp").agg(
            pl.col(position_notional).abs().sum().alias("position_gross_exposure"),
            pl.col(position_notional).sum().alias("position_net_exposure"),
        )
        exposure_check = portfolio.select(
            "timestamp", "gross_exposure", "net_exposure"
        ).join(position_exposure, on="timestamp", how="inner")
        for portfolio_field, position_field in (
            ("gross_exposure", "position_gross_exposure"),
            ("net_exposure", "position_net_exposure"),
        ):
            comparable = exposure_check.drop_nulls([portfolio_field, position_field])
            mismatches = comparable.select(
                ((pl.col(portfolio_field) - pl.col(position_field)).abs() > tolerance).sum()
            ).item()
            if mismatches:
                issues.append(
                    _issue(
                        "position_snapshots.exposure_reconciliation",
                        f"portfolio {portfolio_field} does not reconcile with position notionals",
                        IssueSeverity.ERROR,
                        table="position_snapshots",
                        field=position_field,
                        count=int(mismatches),
                    )
                )

    facts = (
        (
            "fill_events",
            bundle.fill_events,
            FILL_REQUIRED,
            {"quantity", "price", "notional", "fee", "rebate"},
        ),
        (
            "order_events",
            bundle.order_events,
            ORDER_REQUIRED,
            {"quantity", "price", "executed_quantity"},
        ),
    )
    for table_name, frame, required_fields, numeric_fields in facts:
        if frame is None:
            issues.append(
                _issue(
                    f"{table_name}.unavailable",
                    f"{table_name} is unavailable; exact execution analysis will be skipped",
                    IssueSeverity.WARNING,
                    table=table_name,
                )
            )
            continue
        if frame.is_empty():
            continue
        fact_missing = required_fields.difference(frame.columns)
        if fact_missing:
            issues.append(
                _issue(
                    f"{table_name}.required_fields",
                    f"{table_name} is missing required fields: {sorted(fact_missing)}",
                    IssueSeverity.ERROR,
                    table=table_name,
                )
            )
        issues.extend(_timestamp_quality(frame, table_name, allow_duplicates=True))
        issues.extend(_numeric_quality(frame, table_name, numeric_fields))
        if fact_missing:
            continue
        for name in required_fields:
            nulls = frame[name].null_count()
            if nulls:
                issues.append(
                    _issue(
                        f"{table_name}.required_value_null",
                        f"required field {table_name}.{name} contains null values",
                        IssueSeverity.ERROR,
                        table=table_name,
                        field=name,
                        count=nulls,
                    )
                )
        if table_name == "fill_events":
            issues.extend(
                _conversion_issues(
                    frame,
                    table_name,
                    config,
                    currency_field="currency",
                    amount_fields={"price", "notional", "fee", "rebate"},
                )
            )

    fills = bundle.fill_events
    if fills is not None and fills.is_empty() and (bundle.metadata.fill_count or 0) > 0:
        issues.append(
            _issue(
                "fill_events.counter_mismatch",
                f"fill table is empty but engine fill_count is {bundle.metadata.fill_count}",
                IssueSeverity.ERROR,
                table="fill_events",
                count=int(bundle.metadata.fill_count or 0),
            )
        )
    if fills is not None and not fills.is_empty() and FILL_REQUIRED.issubset(fills.columns):
        duplicate_fills = fills.select(pl.col("fill_id").is_duplicated().sum()).item()
        if duplicate_fills:
            issues.append(
                _issue(
                    "fill_events.duplicate_fill_id",
                    "fill_id must uniquely identify each partial or complete fill",
                    IssueSeverity.ERROR,
                    table="fill_events",
                    field="fill_id",
                    count=int(duplicate_fills),
                )
            )
        invalid_fills = fills.select(
            ((pl.col("quantity") <= 0.0) | (pl.col("price") <= 0.0)).sum()
        ).item()
        if invalid_fills:
            issues.append(
                _issue(
                    "fill_events.invalid_execution",
                    "fill quantity and price must be positive",
                    IssueSeverity.ERROR,
                    table="fill_events",
                    count=int(invalid_fills),
                )
            )
        if bundle.metadata.fill_count is not None and len(fills) != bundle.metadata.fill_count:
            issues.append(
                _issue(
                    "fill_events.counter_mismatch",
                    f"fill table has {len(fills)} rows but engine fill_count is {bundle.metadata.fill_count}",
                    IssueSeverity.ERROR,
                    table="fill_events",
                    count=abs(len(fills) - bundle.metadata.fill_count),
                )
            )

    orders = bundle.order_events
    if orders is not None and orders.is_empty() and (bundle.metadata.order_count or 0) > 0:
        issues.append(
            _issue(
                "order_events.counter_mismatch",
                f"order table is empty but engine order_count is {bundle.metadata.order_count}",
                IssueSeverity.ERROR,
                table="order_events",
                count=int(bundle.metadata.order_count or 0),
            )
        )
    if orders is not None and not orders.is_empty() and ORDER_REQUIRED.issubset(orders.columns):
        duplicate_events = orders.select(
            pl.struct("venue_id", "order_id", "event_sequence").is_duplicated().sum()
        ).item()
        if duplicate_events:
            issues.append(
                _issue(
                    "order_events.duplicate_event",
                    "venue_id/order_id/event_sequence must uniquely identify an order event",
                    IssueSeverity.ERROR,
                    table="order_events",
                    count=int(duplicate_events),
                )
            )
        order_count = orders.select(pl.struct("venue_id", "order_id").n_unique()).item()
        if bundle.metadata.order_count is not None and order_count != bundle.metadata.order_count:
            issues.append(
                _issue(
                    "order_events.counter_mismatch",
                    f"order table has {order_count} unique orders but engine order_count is {bundle.metadata.order_count}",
                    IssueSeverity.ERROR,
                    table="order_events",
                    count=abs(order_count - bundle.metadata.order_count),
                )
            )

    round_trips = bundle.round_trip_events
    if (
        round_trips is not None
        and round_trips.is_empty()
        and (bundle.metadata.round_trip_count or 0) > 0
    ):
        issues.append(
            _issue(
                "round_trip_events.counter_mismatch",
                "round-trip table is empty but engine metadata reports completed round trips",
                IssueSeverity.ERROR,
                table="round_trip_events",
                count=int(bundle.metadata.round_trip_count or 0),
            )
        )
    if round_trips is not None and not round_trips.is_empty():
        missing_round_trips = ROUND_TRIP_REQUIRED.difference(round_trips.columns)
        if missing_round_trips:
            issues.append(
                _issue(
                    "round_trip_events.required_fields",
                    f"round_trip_events is missing required fields: {sorted(missing_round_trips)}",
                    IssueSeverity.ERROR,
                    table="round_trip_events",
                )
            )
        else:
            issues.extend(
                _timestamp_quality(
                    round_trips,
                    "round_trip_events",
                    field="entry_timestamp",
                    require_monotonic=False,
                )
            )
            issues.extend(
                _timestamp_quality(
                    round_trips,
                    "round_trip_events",
                    field="exit_timestamp",
                    require_monotonic=False,
                )
            )
            issues.extend(
                _required_value_issues(
                    round_trips,
                    "round_trip_events",
                    ROUND_TRIP_REQUIRED,
                )
            )
            round_trip_numeric = {
                "quantity",
                "entry_price",
                "exit_price",
                "gross_pnl",
                "fee",
                "rebate",
                "funding",
                "net_pnl",
            }
            issues.extend(
                _numeric_quality(
                    round_trips,
                    "round_trip_events",
                    round_trip_numeric,
                )
            )
            issues.extend(
                _conversion_issues(
                    round_trips,
                    "round_trip_events",
                    config,
                    currency_field="currency",
                    amount_fields={
                        "gross_pnl",
                        "fee",
                        "rebate",
                        "funding",
                        "net_pnl",
                    },
                )
            )
            numeric_schema_valid = all(
                round_trips.schema[name].is_numeric() for name in round_trip_numeric
            )
            timestamp_schema_valid = all(
                isinstance(round_trips.schema[name], pl.Datetime)
                for name in ("entry_timestamp", "exit_timestamp")
            )
            if timestamp_schema_valid:
                invalid_intervals = round_trips.select(
                    (pl.col("exit_timestamp") < pl.col("entry_timestamp")).sum()
                ).item()
                if invalid_intervals:
                    issues.append(
                        _issue(
                            "round_trip_events.invalid_interval",
                            "round-trip exit_timestamp must not precede entry_timestamp",
                            IssueSeverity.ERROR,
                            table="round_trip_events",
                            count=int(invalid_intervals),
                        )
                    )
            if numeric_schema_valid:
                invalid_execution = round_trips.select(
                    (
                        (pl.col("quantity") <= 0.0)
                        | (pl.col("entry_price") <= 0.0)
                        | (pl.col("exit_price") <= 0.0)
                        | (pl.col("fee") < 0.0)
                        | (pl.col("rebate") < 0.0)
                    ).sum()
                ).item()
                if invalid_execution:
                    issues.append(
                        _issue(
                            "round_trip_events.invalid_execution",
                            "quantity/prices must be positive and fee/rebate non-negative",
                            IssueSeverity.ERROR,
                            table="round_trip_events",
                            count=int(invalid_execution),
                        )
                    )
                expected_net_pnl = (
                    pl.col("gross_pnl")
                    - pl.col("fee")
                    + pl.col("rebate")
                    + pl.col("funding")
                )
                pnl_scale = pl.max_horizontal(
                    pl.lit(1e-9), expected_net_pnl.abs() * 1e-10
                )
                pnl_mismatches = round_trips.select(
                    ((pl.col("net_pnl") - expected_net_pnl).abs() > pnl_scale).sum()
                ).item()
                if pnl_mismatches:
                    issues.append(
                        _issue(
                            "round_trip_events.pnl_reconciliation",
                            "net_pnl must equal gross_pnl - fee + rebate + funding",
                            IssueSeverity.ERROR,
                            table="round_trip_events",
                            field="net_pnl",
                            count=int(pnl_mismatches),
                        )
                    )
            duplicate_round_trips = round_trips.select(
                pl.col("round_trip_id").is_duplicated().sum()
            ).item()
            if duplicate_round_trips:
                issues.append(
                    _issue(
                        "round_trip_events.duplicate_id",
                        "round_trip_id must be unique",
                        IssueSeverity.ERROR,
                        table="round_trip_events",
                        count=int(duplicate_round_trips),
                    )
                )
            if (
                bundle.metadata.round_trip_count is not None
                and len(round_trips) != bundle.metadata.round_trip_count
            ):
                issues.append(
                    _issue(
                        "round_trip_events.counter_mismatch",
                        "round-trip table count differs from engine metadata",
                        IssueSeverity.ERROR,
                        table="round_trip_events",
                        count=abs(len(round_trips) - bundle.metadata.round_trip_count),
                    )
                )

    if bundle.fx_marks is not None:
        fx_marks = bundle.fx_marks
        missing_fx = FX_REQUIRED.difference(fx_marks.columns)
        if missing_fx:
            issues.append(
                _issue(
                    "fx_marks.required_fields",
                    f"fx_marks is missing required fields: {sorted(missing_fx)}",
                    IssueSeverity.ERROR,
                    table="fx_marks",
                )
            )
        elif not fx_marks.is_empty():
            issues.extend(_required_value_issues(fx_marks, "fx_marks", FX_REQUIRED))
            issues.extend(_timestamp_quality(fx_marks, "fx_marks", allow_duplicates=True))
            issues.extend(_numeric_quality(fx_marks, "fx_marks", {"rate"}))
            duplicate_marks = fx_marks.select(
                pl.struct("timestamp", "currency", "reporting_currency")
                .is_duplicated()
                .sum()
            ).item()
            if duplicate_marks:
                issues.append(
                    _issue(
                        "fx_marks.duplicate",
                        "timestamp/currency/reporting_currency must uniquely identify an FX mark",
                        IssueSeverity.ERROR,
                        table="fx_marks",
                        count=int(duplicate_marks),
                    )
                )
            invalid_rates = fx_marks.select(
                (
                    pl.col("rate").is_null()
                    | ~pl.col("rate").is_finite()
                    | (pl.col("rate") <= 0.0)
                ).sum()
            ).item()
            if invalid_rates:
                issues.append(
                    _issue(
                        "fx_marks.invalid_rate",
                        "FX rates must be finite and positive",
                        IssueSeverity.ERROR,
                        table="fx_marks",
                        field="rate",
                        count=int(invalid_rates),
                    )
                )
            targets = fx_marks["reporting_currency"].drop_nulls().unique().to_list()
            if targets != [config.reporting_currency]:
                issues.append(
                    _issue(
                        "fx_marks.reporting_currency_mismatch",
                        f"FX marks must target {config.reporting_currency!r}, got {targets!r}",
                        IssueSeverity.ERROR,
                        table="fx_marks",
                        field="reporting_currency",
                    )
                )

    if bundle.risk_events is not None and not bundle.risk_events.is_empty():
        risk_events = bundle.risk_events
        missing_risk = RISK_REQUIRED.difference(risk_events.columns)
        if missing_risk:
            issues.append(
                _issue(
                    "risk_events.required_fields",
                    f"risk_events is missing required fields: {sorted(missing_risk)}",
                    IssueSeverity.ERROR,
                    table="risk_events",
                )
            )
        else:
            issues.extend(_required_value_issues(risk_events, "risk_events", RISK_REQUIRED))
            issues.extend(_timestamp_quality(risk_events, "risk_events", allow_duplicates=True))
            issues.extend(
                _numeric_quality(
                    risk_events,
                    "risk_events",
                    {"observed_value", "limit_value", "utilization"},
                )
            )
            duplicate_events = risk_events.select(
                pl.col("event_id").is_duplicated().sum()
            ).item()
            if duplicate_events:
                issues.append(
                    _issue(
                        "risk_events.duplicate_event_id",
                        "risk event_id must be unique within a run",
                        IssueSeverity.ERROR,
                        table="risk_events",
                        field="event_id",
                        count=int(duplicate_events),
                    )
                )
            invalid_limits = risk_events.select(
                (
                    (pl.col("limit_value") <= 0.0)
                    | (pl.col("utilization") < 0.0)
                ).sum()
            ).item()
            if invalid_limits:
                issues.append(
                    _issue(
                        "risk_events.invalid_limit",
                        "risk limits must be positive and utilization non-negative",
                        IssueSeverity.ERROR,
                        table="risk_events",
                        count=int(invalid_limits),
                    )
                )
            if risk_events.schema["breached"] != pl.Boolean:
                issues.append(
                    _issue(
                        "risk_events.breached_type",
                        "risk_events.breached must be Boolean",
                        IssueSeverity.ERROR,
                        table="risk_events",
                        field="breached",
                    )
                )
            else:
                breach_count = risk_events.select(pl.col("breached").sum()).item()
                if breach_count:
                    issues.append(
                        _issue(
                            "risk_events.limit_breach",
                            "one or more risk-limit breaches occurred",
                            IssueSeverity.WARNING,
                            table="risk_events",
                            field="breached",
                            count=int(breach_count),
                        )
                    )

    if bundle.market_marks is not None and not bundle.market_marks.is_empty():
        market_marks = bundle.market_marks
        missing_marks = MARK_REQUIRED.difference(market_marks.columns)
        if missing_marks:
            issues.append(
                _issue(
                    "market_marks.required_fields",
                    f"market_marks is missing required fields: {sorted(missing_marks)}",
                    IssueSeverity.ERROR,
                    table="market_marks",
                )
            )
        else:
            issues.extend(_required_value_issues(market_marks, "market_marks", MARK_REQUIRED))
            issues.extend(_timestamp_quality(market_marks, "market_marks", allow_duplicates=True))
            issues.extend(
                _numeric_quality(market_marks, "market_marks", {"mark_price", "age_ns"})
            )
            invalid_marks = market_marks.select(
                ((pl.col("mark_price") <= 0.0) | (pl.col("age_ns") < 0)).sum()
            ).item()
            if invalid_marks:
                issues.append(
                    _issue(
                        "market_marks.invalid_mark",
                        "mark price must be positive and age_ns non-negative",
                        IssueSeverity.ERROR,
                        table="market_marks",
                        count=int(invalid_marks),
                    )
                )
            if market_marks.schema["stale"] != pl.Boolean:
                issues.append(
                    _issue(
                        "market_marks.stale_type",
                        "market_marks.stale must be Boolean",
                        IssueSeverity.ERROR,
                        table="market_marks",
                        field="stale",
                    )
                )
            else:
                if config.max_mark_age_ns is not None:
                    inconsistent = market_marks.select(
                        (
                            pl.col("stale")
                            != (pl.col("age_ns") > config.max_mark_age_ns)
                        ).sum()
                    ).item()
                    if inconsistent:
                        issues.append(
                            _issue(
                                "market_marks.stale_inconsistent",
                                "stale flag disagrees with configured max_mark_age_ns",
                                IssueSeverity.ERROR,
                                table="market_marks",
                                field="stale",
                                count=int(inconsistent),
                            )
                        )
                stale_count = market_marks.select(pl.col("stale").sum()).item()
                if stale_count:
                    issues.append(
                        _issue(
                            "market_marks.stale",
                            "one or more stale valuation marks occurred",
                            IssueSeverity.WARNING,
                            table="market_marks",
                            field="stale",
                            count=int(stale_count),
                        )
                    )

    if bundle.benchmark is not None:
        benchmark = bundle.benchmark
        issues.extend(_timestamp_quality(benchmark, "benchmark"))
        missing_benchmark = {
            "timestamp",
            "benchmark_id",
            "equity_or_return",
            "value_kind",
            "currency",
            "timezone",
            "source",
            "frequency",
        }.difference(benchmark.columns)
        if missing_benchmark:
            issues.append(
                _issue(
                    "benchmark.required_fields",
                    f"benchmark is missing required fields: {sorted(missing_benchmark)}",
                    IssueSeverity.ERROR,
                    table="benchmark",
                )
            )
        elif benchmark.is_empty():
            issues.append(
                _issue(
                    "benchmark.empty",
                    "benchmark is empty; benchmark metrics will be skipped",
                    IssueSeverity.WARNING,
                    table="benchmark",
                )
            )
        else:
            issues.extend(
                _numeric_quality(benchmark, "benchmark", {"equity_or_return"})
            )
            kinds = benchmark["value_kind"].drop_nulls().unique().to_list()
            if len(kinds) != 1 or kinds[0] not in {"return", "equity"}:
                issues.append(
                    _issue(
                        "benchmark.value_kind",
                        "benchmark.value_kind must contain exactly one of: return, equity",
                        IssueSeverity.ERROR,
                        table="benchmark",
                        field="value_kind",
                    )
                )
            currencies = benchmark["currency"].drop_nulls().unique().to_list()
            if currencies != [config.reporting_currency]:
                issues.append(
                    _issue(
                        "benchmark.currency_mismatch",
                        f"benchmark currency must be {config.reporting_currency!r}, got {currencies!r}",
                        IssueSeverity.ERROR,
                        table="benchmark",
                        field="currency",
                    )
                )
            zones = benchmark["timezone"].drop_nulls().unique().to_list()
            if len(zones) != 1 or not zones[0]:
                issues.append(
                    _issue(
                        "benchmark.timezone",
                        "benchmark.timezone must contain exactly one non-empty timezone",
                        IssueSeverity.ERROR,
                        table="benchmark",
                        field="timezone",
                    )
                )

    if not math.isclose(bundle.metadata.initial_capital, config.initial_capital):
        issues.append(
            _issue(
                "metadata.initial_capital_mismatch",
                "metadata initial_capital differs from ReportConfig",
                IssueSeverity.ERROR,
                table="run_metadata",
                field="initial_capital",
            )
        )
    for warning in bundle.metadata.warnings:
        issues.append(
            _issue("engine.warning", warning, IssueSeverity.WARNING, table="run_metadata")
        )
    for downgrade in bundle.metadata.capability_downgrades:
        issues.append(
            _issue(
                "engine.capability_downgrade",
                downgrade,
                IssueSeverity.WARNING,
                table="run_metadata",
            )
        )
    return ValidationResult(status_from_issues(issues), tuple(issues))
