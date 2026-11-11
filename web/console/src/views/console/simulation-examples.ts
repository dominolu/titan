// Authored fictional fixtures for the explicitly labelled, read-only example view.
// They never become engine resources, account facts or execution requests.
export type SimulationBacktest = {
  id: string;
  name: string;
  status: string;
  description: string;
  equity: number[];
  trades: number;
  winRate: number;
  fees: number;
  maxExposure: number;
};
export type SimulationRun = {
  id: string;
  account: string;
  instruments: string;
  status: string;
  uptime: string;
  fills: number;
  latency: number;
  equity: number[];
  exposure: number;
  exposureLimit: number;
  positions: {
    symbol: string;
    side: string;
    quantity: string;
    entry: string;
    mark: string;
    pnl: string;
  }[];
  orders: {
    id: string;
    symbol: string;
    side: string;
    price: string;
    quantity: string;
    status: string;
  }[];
  alerts: { level: string; time: string; text: string }[];
};
export type SimulationStrategy = {
  source: "synthetic";
  parameters: Record<string, string>;
  backtests: SimulationBacktest[];
  run: SimulationRun;
};

export const simulationExamples: Record<string, SimulationStrategy> = {
  grid: {
    source: "synthetic",
    parameters: {
      交易品种: "BTC / USDT",
      网格区间: "60,000–68,000 USDT",
      网格数量: "20",
      单格数量: "0.002 BTC",
      最大敞口: "5,000 USDT"
    },
    backtests: [
      {
        id: "demo-grid-range",
        name: "震荡行情 · 标准网格",
        status: "模拟完成",
        description: "价格在网格区间内反复波动，展示逐步累积收益的示例。",
        equity: [10000, 10058, 10031, 10145, 10122, 10254, 10308, 10386],
        trades: 186,
        winRate: 68.3,
        fees: 28.6,
        maxExposure: 4200
      },
      {
        id: "demo-grid-trend",
        name: "上涨行情 · 区间上移",
        status: "模拟完成",
        description: "趋势行情下网格逐步退出，收益与成交次数用于展示不同场景。",
        equity: [10000, 10076, 10120, 10064, 10215, 10284, 10418, 10542],
        trades: 124,
        winRate: 63.7,
        fees: 22.4,
        maxExposure: 4650
      },
      {
        id: "demo-grid-stress",
        name: "急跌行情 · 风险终止",
        status: "模拟风险终止",
        description: "模拟价格跌破保护线后终止，保留亏损与回撤示例。",
        equity: [10000, 10048, 9952, 9814, 9636, 9685, 9704, 9718],
        trades: 82,
        winRate: 42.7,
        fees: 18.2,
        maxExposure: 4980
      }
    ],
    run: {
      id: "demo-grid-live-01",
      account: "做市研究账户（示例）",
      instruments: "BTC / USDT",
      status: "模拟运行中",
      uptime: "2 小时 18 分钟",
      fills: 128,
      latency: 24,
      equity: [20000, 20018, 20004, 20037, 20044, 20031, 20068, 20086.4],
      exposure: 4500,
      exposureLimit: 5000,
      positions: [
        {
          symbol: "BTC / USDT",
          side: "多",
          quantity: "0.020 BTC",
          entry: "63,240.00",
          mark: "63,360.00",
          pnl: "2.40"
        }
      ],
      orders: [
        {
          id: "DEMO-GRID-001",
          symbol: "BTC / USDT",
          side: "买入",
          price: "63,200.00",
          quantity: "0.002 BTC",
          status: "模拟已挂单"
        },
        {
          id: "DEMO-GRID-002",
          symbol: "BTC / USDT",
          side: "卖出",
          price: "63,520.00",
          quantity: "0.002 BTC",
          status: "模拟已挂单"
        }
      ],
      alerts: [
        {
          level: "提示",
          time: "20:00:00",
          text: "模拟网格正常报价，当前敞口上界占预算 90%。"
        }
      ]
    }
  },
  pair_arb: {
    source: "synthetic",
    parameters: {
      交易组合: "BTC / USDT · ETH / USDT",
      入场阈值: "Z-score 2.0",
      退出阈值: "Z-score 0.4",
      单腿预算: "3,000 USDT",
      最大敞口: "8,000 USDT"
    },
    backtests: [
      {
        id: "demo-pair-range",
        name: "价差回归 · 基准场景",
        status: "模拟完成",
        description: "模拟价差偏离后回归，展示双腿交易的结果。",
        equity: [10000, 10026, 10011, 10098, 10084, 10156, 10206, 10248],
        trades: 72,
        winRate: 66.7,
        fees: 35.8,
        maxExposure: 5840
      },
      {
        id: "demo-pair-trend",
        name: "高波动 · 多次回归",
        status: "模拟完成",
        description: "模拟较高波动下多次价差回归，增加成交与费用。",
        equity: [10000, 9984, 10052, 10124, 10076, 10218, 10264, 10312],
        trades: 94,
        winRate: 62.8,
        fees: 46.2,
        maxExposure: 6200
      },
      {
        id: "demo-pair-stress",
        name: "相关性走弱 · 保护退出",
        status: "模拟风险终止",
        description: "模拟组合相关性下降与保护退出，展示不利市场情形。",
        equity: [10000, 10018, 9964, 9882, 9745, 9796, 9814, 9836],
        trades: 48,
        winRate: 39.6,
        fees: 27.4,
        maxExposure: 6980
      }
    ],
    run: {
      id: "demo-pair-live-01",
      account: "套利研究账户（示例）",
      instruments: "BTC / USDT · ETH / USDT",
      status: "模拟已暂停",
      uptime: "1 小时 42 分钟",
      fills: 46,
      latency: 31,
      equity: [15000, 15022, 15016, 15038, 15082, 15064, 15114, 15124.8],
      exposure: 5980,
      exposureLimit: 8000,
      positions: [
        {
          symbol: "BTC / USDT",
          side: "多",
          quantity: "0.030 BTC",
          entry: "63,100.00",
          mark: "63,360.00",
          pnl: "7.80"
        },
        {
          symbol: "ETH / USDT",
          side: "空",
          quantity: "0.600 ETH",
          entry: "3,460.00",
          mark: "3,450.00",
          pnl: "6.00"
        }
      ],
      orders: [],
      alerts: [
        {
          level: "预警",
          time: "19:58:30",
          text: "模拟价差偏离保护阈值，策略暂停新增订单，保留组合持仓供检查。"
        }
      ]
    }
  },
  dual_ma: {
    source: "synthetic",
    parameters: {
      交易品种: "BTC / USDT",
      快线周期: "10",
      慢线周期: "30",
      K线周期: "5 分钟",
      最大敞口: "6,000 USDT"
    },
    backtests: [
      {
        id: "demo-ma-trend",
        name: "趋势行情 · 双均线跟随",
        status: "模拟完成",
        description: "模拟趋势行情的均线交叉，展示较少交易与较长持仓。",
        equity: [10000, 10124, 10088, 10316, 10482, 10406, 10712, 10924],
        trades: 34,
        winRate: 58.8,
        fees: 16.9,
        maxExposure: 5600
      },
      {
        id: "demo-ma-range",
        name: "震荡行情 · 反复交叉",
        status: "模拟完成",
        description: "模拟窄幅震荡中的反复信号，展示交易成本与小幅亏损。",
        equity: [10000, 9978, 10016, 9942, 9986, 9904, 9938, 9926],
        trades: 86,
        winRate: 43.0,
        fees: 39.7,
        maxExposure: 4800
      },
      {
        id: "demo-ma-stress",
        name: "趋势反转 · 止损保护",
        status: "模拟风险终止",
        description: "模拟趋势快速反转后的止损，展示此前盈利回吐。",
        equity: [10000, 10082, 10146, 10324, 10268, 10038, 9854, 9812],
        trades: 28,
        winRate: 46.4,
        fees: 15.4,
        maxExposure: 5920
      }
    ],
    run: {
      id: "demo-ma-live-01",
      account: "网格研究账户（示例）",
      instruments: "BTC / USDT",
      status: "模拟已停止",
      uptime: "4 小时 06 分钟",
      fills: 64,
      latency: 18,
      equity: [12000, 12028, 12012, 12076, 12110, 12098, 12142, 12184.5],
      exposure: 0,
      exposureLimit: 6000,
      positions: [],
      orders: [],
      alerts: [
        {
          level: "提示",
          time: "19:50:00",
          text: "模拟运行已结束，示例持仓与挂单均为零。"
        }
      ]
    }
  }
};

export function simulationForStrategy(id: string, name: string) {
  return name.endsWith("（示例）") ? simulationExamples[id] : undefined;
}

export function simulationMetrics(equity: number[]) {
  const initial = equity[0],
    final = equity[equity.length - 1];
  let peak = initial,
    drawdown = 0;
  for (const value of equity) {
    peak = Math.max(peak, value);
    drawdown = Math.max(drawdown, ((peak - value) / peak) * 100);
  }
  return {
    initial,
    final,
    pnl: final - initial,
    returnPct: ((final - initial) / initial) * 100,
    drawdown
  };
}
