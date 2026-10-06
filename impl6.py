p = 'src/model-pinning-proxy.ts'
t = open(p, encoding='utf-8', newline='').read()
old = '''              const extra: string[] = [];
              if (ev.type === "response.reasoning_summary_text.delta" && typeof ev.delta === "string" && ev.delta.length > 0) {
                const openTag = thinkLive ? "" : "<think>";
                thinkLive = true;
                const fake = { type: "response.output_text.delta", delta: openTag + ev.delta, item_id: ev.item_id ?? "rs_live", output_index: 0, sequence_number: -1 };
                extra.push("data: " + JSON.stringify(fake) + "\n");
              } else if (ev.type === "response.output_text.delta" && typeof ev.delta === "string" && thinkLive && ev.delta.length > 0) {
                thinkLive = false;
                const fake = { type: "response.output_text.delta", delta: "</think>" + ev.delta, item_id: ev.item_id ?? "msg_live", output_index: 0, sequence_number: -1 };
                extra.push("data: " + JSON.stringify(fake) + "\n");
              }'''
new = '''              const extra: string[] = [];
              const seq = typeof (ev as { sequence_number?: unknown }).sequence_number === "number" ? (ev as { sequence_number: number }).sequence_number : 0;
              if (ev.type === "response.reasoning_summary_text.delta" && typeof ev.delta === "string" && ev.delta.length > 0) {
                const openTag = thinkLive ? "" : "<think>";
                thinkLive = true;
                const fake = { content_index: 0, delta: openTag + ev.delta, item_id: ev.item_id ?? "rs_live", output_index: 0, sequence_number: seq, type: "response.output_text.delta" };
                extra.push("event: response.output_text.delta\ndata: " + JSON.stringify(fake) + "\n");
              } else if (ev.type === "response.output_text.delta" && typeof ev.delta === "string" && thinkLive && ev.delta.length > 0) {
                thinkLive = false;
                const fake = { content_index: 0, delta: "</think>" + ev.delta, item_id: ev.item_id ?? "msg_live", output_index: 0, sequence_number: seq, type: "response.output_text.delta" };
                extra.push("event: response.output_text.delta\ndata: " + JSON.stringify(fake) + "\n");
              }'''
assert old in t, 'fake block not found'
t = t.replace(old, new)
open(p, 'w', encoding='utf-8', newline='').write(t)
print('fake 事件形状已修正')
