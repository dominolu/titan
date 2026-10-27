const Layout = () => import("@/views/console/ConsoleLayout.vue");
export default {
  path: "/",
  name: "Home",
  component: Layout,
  redirect: "/runs",
  meta: { icon: "ep/monitor", title: "Titan 管理", rank: 0 },
  children: [
    {
      path: "/welcome",
      name: "Welcome",
      component: () => import("@/views/console/LegacyStrategyRoute.vue"),
      meta: { title: "策略参数", icon: "ep/set-up" }
    },
    {
      path: "/versions",
      alias: "/artifacts",
      name: "StrategyVersions",
      component: () => import("@/views/console/LegacyStrategyRoute.vue"),
      meta: { title: "策略版本", icon: "ep/box" }
    },
    {
      path: "/backtests",
      name: "Backtests",
      component: () => import("@/views/console/LegacyStrategyRoute.vue"),
      meta: { title: "历史回测", icon: "ep/data-line" }
    },
    {
      path: "/strategies",
      alias: "/research",
      name: "Strategies",
      component: () => import("@/views/console/StrategyList.vue"),
      meta: { title: "策略", icon: "ep/set-up" }
    },
    {
      path: "/strategies/:strategyId",
      alias: "/research/:strategyId",
      name: "StrategyDetail",
      component: () => import("@/views/console/StrategyDetail.vue"),
      meta: { title: "策略", showLink: false }
    },
    {
      path: "/runs",
      name: "Runs",
      component: () => import("@/views/console/RunsOverview.vue"),
      meta: { title: "运行总览", icon: "ep/video-play" }
    },
    {
      path: "/runs/:id",
      name: "RunDetail",
      component: () => import("@/views/console/RunDetail.vue"),
      meta: { title: "策略详情", showLink: false }
    },
    {
      path: "/accounts",
      name: "Accounts",
      component: () => import("@/views/console/AccountRisk.vue"),
      meta: { title: "账户风险", icon: "ep/warning" }
    }
  ]
} satisfies RouteConfigsTable;
