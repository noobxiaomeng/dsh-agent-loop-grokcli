/** GrokDesk 桥的浏览器模块（dsh.client）：占据侧栏品牌插槽——
 *  sidebar.brand.mark = noobxiaomeng 的像素鲸鱼标，sidebar.brand.name = "GrokDesk by noobxiaomeng"。
 *  格式照抄官方 @deepseek-ai/dsh-client-ui-brand-official/lib/client.js：
 *  window.__ModuleLoader__.load({ id, factory })，React 由宿主 require 提供，图标 base64 内嵌。 */
window.__ModuleLoader__.load({
	id: "dsh-agent-loop-grokcli",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
		const react_jsx_runtime = require("react/jsx-runtime");
		const jsx = react_jsx_runtime.jsx;
		const primitives = require("@deepseek-ai/dsh-client-ui-primitives");
		const MenuItemButton = primitives.MenuItemButton;
		const BRAND_MARK_URI = "data:image/png;base64,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";

		function GrokDeskBrandMark({ size }) {
			return jsx("img", {
				src: BRAND_MARK_URI,
				alt: "GrokDesk",
				draggable: false,
				style: { width: size, height: size, display: "block", flex: "none", objectFit: "contain", userSelect: "none" },
			});
		}

		function GrokDeskBrandName() {
			// 容器 .brandName 是 18px/600 + min-width:0（可收缩）：不 nowrap 时窄侧栏下署名会
			// 折行并把品牌行布局撑乱（实测 1280px 视口复现）。单行 nowrap + 显式 16px 字号
			// 保证整串放得下（~165px < 可用 ~184px），溢出兜底由外层 .brand 的 overflow hidden 管。
			return jsx("span", {
				style: { display: "inline-flex", alignItems: "baseline", gap: "0.32em", whiteSpace: "nowrap", fontSize: "16px", fontWeight: 600, letterSpacing: "0.01em" },
				children: [
					jsx("span", { key: "name", children: "GrokDesk" }),
					jsx("span", { key: "by", style: { fontWeight: 400, opacity: 0.55, fontSize: "13px" }, children: "by noobxiaomeng" }),
				],
			});
		}

		function GrokDeskDeleteMenuItem({ sessionId, useMenuOpenState }) {
			const setMenuOpen = useMenuOpenState ? useMenuOpenState()[1] : () => {};
			return jsx(MenuItemButton, {
				onSelect: () => {
					setMenuOpen(false);
					if (!window.confirm("永久删除这个会话？磁盘文件将一并删除，不可恢复。")) return;
					fetch("/grokdesk/delete-session", {
						method: "POST",
						headers: { "content-type": "application/json" },
						body: JSON.stringify({ sessionId }),
					})
						.then(r => r.json())
						.then(d => {
							if (!d || !d.ok) { window.alert("删除失败: " + ((d && d.error) || "未知错误")); return; }
							// 删除的可能是当前打开的会话（dsh 对外部归档当前会话无善后，页面会僵死）
							// ——重载页面落到健康的默认会话，一刀切最稳。
							window.location.reload();
						})
						.catch(e => window.alert("删除请求失败: " + e));
				},
				children: "永久删除",
			});
		}

		/** Required service: the UI slot registry. */
		const inject = ["slots"];

		/** 照官方品牌包的 declaration-aware 注册集写法（嵌套 inject 保证 mark/name 成对出现）。 */
		function apply(ctx) {
			ctx.slots.inject("sidebar.brand.mark", () => ctx.slots.inject("sidebar.brand.name", function* () {
				yield ctx.slots.register({ name: "sidebar.brand.mark", priority: -10 }, GrokDeskBrandMark);
				yield ctx.slots.register({ name: "sidebar.brand.name", priority: -10 }, GrokDeskBrandName);
			}));
			// 会话「...」菜单的「永久删除」项（order 450 落在官方 archive 之后）：owner 提供
			// sessionId 与菜单开合 hook；删除执行走桥的 /grokdesk/delete-session（archive 推送
			// 让列表即时隐藏 + 物理删除 + 账目 prune，全官方事件链）。
			ctx.slots.inject("sidebar.workspaces.session.menu.item", function* () {
				yield ctx.slots.register({ name: "sidebar.workspaces.session.menu.item", id: "grokdesk-delete", order: 450 }, GrokDeskDeleteMenuItem);
			});
		}

		exports.apply = apply;
		exports.inject = inject;
		return module.exports;
	}
});
