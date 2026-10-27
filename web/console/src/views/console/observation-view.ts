import { object, rows } from "./run-view";

// Public field meanings, never private recovery paths or state layout hashes.
const gridFields: Record<string, [string, string]> = {
  phase: ["enum", "state"],
  anchor_ticks: ["decimal", "price_tick"],
  completed_pairs: ["uint64", "count"],
  paired_lots: ["uint64", "quantity_lot"],
  paired_gross_tick_lots: ["decimal", "tick_lot"],
  expected_inventory_lots: ["int64", "quantity_lot"]
};
export function gridObservationCompatible(value: unknown): boolean {
  const observation = object(value);
  if (
    ![1, 2].includes(Number(observation.protocol_version)) ||
    observation.schema_id !== "titan.grid" ||
    Number(observation.schema_version) !== 1
  )
    return false;
  const schema = object(observation.schema);
  if (
    schema.schema_id !== observation.schema_id ||
    Number(schema.schema_version) !== 1
  )
    return false;
  const fields = rows(schema.fields);
  return Object.entries(gridFields).every(([name, [kind, unit]]) => {
    const matching = fields.filter(field => field.name === name);
    return (
      matching.length === 1 &&
      matching[0].kind === kind &&
      matching[0].unit === unit
    );
  });
}
