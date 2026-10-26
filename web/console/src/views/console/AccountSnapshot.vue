<script setup lang="ts">
import { computed } from "vue";
import { rows, raw, scaled, pretty, object, orderStates } from "./run-view";
const props = defineProps<{ image: unknown; stale: boolean }>();
const image = computed(() => object(props.image));
const instrumentFor = (row: any) =>
  rows(image.value.instruments).find(
    unit => raw(unit.asset_id) === raw(row.asset_id)
  );
const sides: Record<string, string> = {
  "0": "未指定",
  "1": "买入",
  "2": "卖出"
};
const directions: Record<string, string> = {
  "0": "NET",
  "1": "LONG",
  "2": "SHORT"
};
</script>
<template>
  <section class="console-card">
    <div class="card-heading">
      <h2>账户订单快照</h2>
      <span class="subtle"
        >{{ stale ? "最后确认值" : "所有已知状态" }} · 保留原生 ticks /
        lots</span
      >
    </div>
    <div class="table-scroll">
      <table class="data-table">
        <thead>
          <tr>
            <th>品种 / 资产</th>
            <th>方向 / 仓位方向</th>
            <th>价格</th>
            <th>基础币数量 / 已成交</th>
            <th>状态 / 只减仓</th>
            <th>原始精度</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(order, index) in rows(image.orders)" :key="index">
            <td>
              {{ instrumentFor(order)?.native_symbol ?? raw(order.asset_id) }}
            </td>
            <td>
              {{ sides[raw(order.side)] ?? raw(order.side) }} /
              {{
                directions[raw(order.position_side)] ?? raw(order.position_side)
              }}
            </td>
            <td class="number">
              {{
                raw(scaled(order.price_ticks, instrumentFor(order)?.price_tick))
              }}
            </td>
            <td class="number">
              {{
                raw(
                  scaled(
                    order.quantity_lots,
                    instrumentFor(order)?.quantity_lot
                  )
                )
              }}
              /
              {{
                raw(
                  scaled(
                    order.filled_quantity_lots,
                    instrumentFor(order)?.quantity_lot
                  )
                )
              }}
            </td>
            <td>
              {{ orderStates[raw(order.status)] ?? raw(order.status) }} /
              {{ raw(order.reduce_only) }}
            </td>
            <td>
              {{ raw(order.price_ticks) }} ticks ·
              {{ raw(order.quantity_lots) }} lots<span class="cell-note"
                >累计成交 {{ raw(order.filled_quantity_lots) }} lots</span
              >
            </td>
          </tr>
          <tr v-if="!rows(image.orders).length">
            <td colspan="6" class="compact-empty">
              来源中没有订单记录，缺失不作为交易所无挂单的证明。
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <details class="secondary-details">
      <summary>全部订单字段</summary>
      <pre>{{ pretty(image.orders) }}</pre>
    </details>
  </section>
  <section class="console-card section-gap">
    <div class="card-heading">
      <h2>账户持仓与余额</h2>
      <span class="subtle">不合并 LONG / SHORT</span>
    </div>
    <div class="table-scroll">
      <table class="data-table">
        <thead>
          <tr>
            <th>品种 / 资产</th>
            <th>持仓方向</th>
            <th>基础币数量</th>
            <th>开仓价</th>
            <th>原始数量</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(position, index) in rows(image.positions)" :key="index">
            <td>
              {{
                instrumentFor(position)?.native_symbol ?? raw(position.asset_id)
              }}
            </td>
            <td>
              {{
                directions[raw(position.position_side)] ??
                raw(position.position_side)
              }}
            </td>
            <td>
              {{
                raw(
                  scaled(
                    position.quantity_lots,
                    instrumentFor(position)?.quantity_lot
                  )
                )
              }}
            </td>
            <td>
              {{
                raw(
                  scaled(
                    position.entry_price_ticks,
                    instrumentFor(position)?.price_tick
                  )
                )
              }}
            </td>
            <td>{{ raw(position.quantity_lots) }} lots</td>
          </tr>
          <tr v-if="!rows(image.positions).length">
            <td colspan="5" class="compact-empty">
              该来源没有已记录的持仓项；不能据此确认当前仓位为零。
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <details class="secondary-details">
      <summary>余额、币种及全部持仓字段</summary>
      <pre>{{
        pretty({
          positions: image.positions,
          balances: image.balances,
          currencies: image.currencies,
          instruments: image.instruments
        })
      }}</pre>
    </details>
  </section>
</template>
