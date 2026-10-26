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
      component: () => import("@/views/console/LegacyResearchRoute.vue"),
      meta: { title: "策略参数", icon: "ep/set-up" }
    },
    {
      path: "/artifacts",
      name: "Artifacts",
      component: () => import("@/views/console/LegacyResearchRoute.vue"),
      meta: { title: "构建制品", icon: "ep/box" }
    },
    {
      path: "/backtests",
      name: "Backtests",
      component: () => import("@/views/console/LegacyResearchRoute.vue"),
      meta: { title: "历史回测", icon: "ep/data-line" }
    },
    {
      path: "/research",
      name: "Research",
      component: () => import("@/views/console/ResearchIndex.vue"),
      meta: { title: "策略研究", icon: "ep/set-up" }
    },
    {
      path: "/research/:researchId",
      name: "ResearchWorkspace",
      component: () => import("@/views/console/ResearchWorkspace.vue"),
      meta: { title: "策略研究", showLink: false }
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
