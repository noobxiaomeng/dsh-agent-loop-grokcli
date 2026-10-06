p = 'src/model-pinning-proxy.ts'
t = open(p, encoding='utf-8', newline='').read()
print('当前 fake 形态片段:')
i = t.find('const fake')
print(t[i:i+240])
# 无条件确保：每个 fake 对象在 type 前有 content_index
n = 0
for pat in ['output_index: 0,', 'output_index: 0, sequence_number']:
    pass
# 直接替换：把 'fake = { type:' 形态或已有字段序列统一重写两处 fake 定义
import re
def add_ci(m):
    global n
    n += 1
    return 'const fake = { content_index: 0, ' + m.group(1)
t2 = re.sub(r'const fake = \{ (?!content_index)', add_ci, t)
# 去掉可能重复的 content_index（若原来已有）
t2 = t2.replace('content_index: 0, content_index: 0,', 'content_index: 0,')
open(p, 'w', encoding='utf-8', newline='').write(t2)
print('补 content_index:', n, '处')
