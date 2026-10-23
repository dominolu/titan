const Layout = () => import("@/layout/index.vue");
const Console = () => import("@/views/console/index.vue");
export default {
  path: "/",
  name: "Home",
  component: Layout,
  redirect: "/welcome",
  meta: { icon: "ep/monitor", title: "Titan 管理", rank: 0 },
  children: [
    {
      path: "/welcome",
      name: "Welcome",
      component: Console,
      meta: { title: "策略参数", icon: "ep/set-up" }
    },
    {
      path: "/artifacts",
      name: "Artifacts",
      component: Console,
      meta: { title: "构建制品", icon: "ep/box" }
    },
    {
      path: "/backtests",
      name: "Backtests",
      component: Console,
      meta: { title: "历史回测", icon: "ep/data-line" }
    },
    {
      path: "/runs",
      name: "Runs",
      component: Console,
      meta: { title: "运行详情", icon: "ep/video-play" }
    },
    {
      path: "/accounts",
      name: "Accounts",
      component: Console,
      meta: { title: "账户风险", icon: "ep/warning" }
    }
  ]
} satisfies RouteConfigsTable;
