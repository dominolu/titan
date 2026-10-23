import {createRequire} from "node:module";
import assert from "node:assert/strict";
import {writeFile} from "node:fs/promises";
const require=createRequire(new URL("../web/console/package.json",import.meta.url));
const {chromium,expect}=require("@playwright/test");
const [origin,fault,output]=process.argv.slice(2);
const browser=await chromium.launch({headless:true,executablePath:process.env.TITAN_BROWSER_EXECUTABLE});
const context=await browser.newContext({viewport:{width:1500,height:1050}}),page=await context.newPage(),errors=[];
page.on("pageerror",e=>errors.push(e.message));
async function login(name){await page.goto(origin+"/#/login");await page.locator("input[autocomplete='username']").fill(name);await page.locator("input[autocomplete='current-password']").fill(process.env.TITAN_BROWSER_PASSWORD);await page.getByRole("button",{name:"登录",exact:true}).click();await page.waitForURL("**/#/welcome");}
try{
  await login("operator");await page.goto(origin+"/#/accounts");
  await page.getByText("只读账户观测",{exact:true}).waitFor();await page.getByText("当前订单",{exact:true}).click();
  await page.getByRole("columnheader",{name:"累计成交 lots"}).waitFor();
  await page.getByText("权益与费用（保留原币种，不合并估值）",{exact:true}).click();await expect(page.locator(".observation")).toContainText("123.456772");
  await page.locator(".source-line .el-select").click();await page.getByRole("option",{name:"订单",exact:true}).click();await expect(page.locator(".observation")).toContainText('"scope": "account"');
  await page.getByRole("button",{name:"宿主日志",exact:true}).click();await page.getByText("synthetic worker log",{exact:true}).waitFor();await page.locator(".el-dialog__headerbtn").click();
  await page.getByRole("button",{name:"确认已知悉",exact:true}).click();await page.getByText("已知悉：operator",{exact:true}).waitFor();
  await page.waitForTimeout(1600);await page.reload();await page.getByText("已知悉：operator",{exact:true}).waitFor();
  await page.request.get(fault+"/fault/stale");await page.getByText("观测已过期或账户快照不可用；下方保留最后有效事实，不能据此确认当前仓位。",{exact:true}).waitFor();
  await page.screenshot({path:output+"/stale-account.png",fullPage:true,animations:"disabled"});
  await page.request.get(fault+"/fault/recover");await page.getByRole("button",{name:"确认已知悉",exact:true}).waitFor();
  await page.goto(origin+"/#/runs");await page.getByRole("heading",{name:"运行详情",exact:true}).waitFor();
  const business=page.locator(".el-collapse-item__header").filter({hasText:"网格业务状态 / checkpoint"});
  if(await business.getAttribute("aria-expanded")!=="true")await business.click();await page.getByText("当前来源未运行策略",{exact:true}).waitFor();
  await page.screenshot({path:output+"/observation.png",fullPage:true,animations:"disabled"});
  await page.getByText("operator",{exact:true}).click();await page.getByText("退出系统",{exact:true}).click();await page.waitForURL("**/#/login");
  await login("reader");await page.goto(origin+"/#/accounts");await expect(page.getByRole("button",{name:"确认已知悉",exact:true})).toBeDisabled();
  const session=await(await page.request.get(origin+"/api/v1/session")).json();
  const denied=await page.request.post(origin+"/api/v1/resources/observe-alert/acknowledge",{headers:{Origin:origin,"x-csrf-token":session.csrf_token},data:{expected_revision:"0",condition_identity:"two"}});assert.equal(denied.status(),403);
  assert.deepEqual(errors,[]);await writeFile(output+"/browser.json",JSON.stringify({passed:true,pageErrors:errors},null,2));
}catch(error){await page.screenshot({path:output+"/failure.png",fullPage:true,animations:"disabled"});await writeFile(output+"/failure.txt",await page.locator("body").innerText());throw error;}finally{await browser.close();}
