# -*- coding: utf-8 -*-
"""通用工具诱导层：让不支持原生 function calling 的网页反代渠道也能调工具。

用法（适配层内）：
    prompt, tools_ctx = inject_tools(openai_body, prompt)
    ... 发送 prompt 给上游，得到 full_text ...
    tool_calls, text_out = extract_tool_calls(full_text, tools_ctx)

原理：把 tools 定义转成指令注入 prompt，要求模型用
<tool_call>{"name": "fn", "arguments": {...}}</tool_call> 格式输出；
适配层再解析还原成 OpenAI 标准 tool_calls 字段。
"""
import json
import re

TOOL_CALL_RE = re.compile(r"<tool_call>\s*(\{.*?\})\s*(?:</tool_call>|$)", re.S)

# 上游流式渠道偶尔夹带的内部标记(如 minimax 的 "<]minimax[>")会污染标签闭合
_INTERNAL_MARK_RE = re.compile(r"<\]\]\w+\[>|\]<\]|\[>\[</t|<t->\[|\]->\]")


def _strip_internal_marks(text: str) -> str:
    return _INTERNAL_MARK_RE.sub("", text)

_INSTR_TMPL = """你可以调用以下工具（函数）。当需要调用工具时，必须且只能输出如下格式（可以多个，每个一行），不要输出任何其他内容：
<tool_call>{"name": "函数名", "arguments": {"参数1": "值1"}}</tool_call>

可用工具列表：
{tool_descs}

规则：
- 只有确实需要调用工具才能回答时才调用；不需要时正常直接回答，不要输出 <tool_call>。
- arguments 必须是合法 JSON 对象，字段与参数 schema 完全一致。
- 不要把工具调用写成代码块或加其他解释文字。
- 重要：你自身没有文件读写能力，禁止使用任何内置工具或沙箱。一切文件操作只能通过输出 <tool_call> 让外部系统执行。需要写长代码（如整个HTML文件）时，把完整代码作为 content 参数值放进 <tool_call> 的 JSON 里一次性输出。"""


def build_tool_choice_hint(openai_body: dict) -> str:
    """tool_choice 强制时的额外提示。"""
    tc = openai_body.get("tool_choice")
    if tc == "required":
        return "\n（本次必须调用至少一个工具再回答。）"
    if isinstance(tc, dict):
        fn = (tc.get("function") or {}).get("name")
        if fn:
            return f"\n（本次必须调用工具 {fn}。）"
    return ""


def _build_instruction(tools) -> str:
    descs = []
    for t in tools:
        fn = t.get("function") or t
        name = fn.get("name") or t.get("name") or ""
        desc = fn.get("description") or ""
        params = fn.get("parameters") or {}
        try:
            params_s = json.dumps(params, ensure_ascii=False)
        except Exception:
            params_s = "{}"
        descs.append(f"- {name}: {desc}\n  参数schema: {params_s}")
    return _INSTR_TMPL.replace("{tool_descs}", "\n".join(descs))


def inject_tools(openai_body: dict, prompt: str):
    """把 openai_body 里的 tools 注入 prompt。返回 (新prompt, tools列表或None)。"""
    tools = openai_body.get("tools") or []
    if not tools:
        return prompt, None
    instruction = _build_instruction(tools) + build_tool_choice_hint(openai_body)
    return f"{instruction}\n\n---\n\n{prompt}", tools


def _salvage_truncated(text: str):
    """<tool_call> 的 JSON 被截断时的补救: 提取 name, 修复 arguments。"""
    seg = text.split("<tool_call>", 1)[1]
    import re as _r
    m = _r.search(r'"name"\s*:\s*"([^"]+)"', seg)
    if not m:
        return []
    name = m.group(1)
    args = {}
    ma = _r.search(r'"arguments"\s*:\s*(\{.*)', seg, _r.S)
    if ma:
        raw = ma.group(1).strip().rstrip("}")
        # 逐字符尝试闭合
        for fix in (raw + "}", raw + '"}', raw + '"}'):
            try:
                v = json.loads(fix)
                if isinstance(v, dict):
                    args = v
                    break
            except Exception:
                continue
        else:
            # 提取 "key": "value" 对
            for k, v in _r.findall(r'"(\w+)"\s*:\s*"([^"]*)"', ma.group(1)):
                args[k] = v
    return [json.dumps({"name": name, "arguments": args}, ensure_ascii=False)]


def extract_tool_calls(text: str, tools):
    """从模型输出解析 <tool_call> 并还原标准结构。
    返回 (tool_calls列表或None, 去掉调用块后的正文)。"""
    if not tools or not text:
        return None, text
    text = _strip_internal_marks(text)
    calls = TOOL_CALL_RE.findall(text)
    if not calls and "<tool_call>" in text:
        # JSON 截断的补救: 提取 name 字段, arguments 尽力修复
        calls = _salvage_truncated(text)
    if not calls:
        return None, text
    tool_calls = []
    for i, raw in enumerate(calls):
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        args = obj.get("arguments")
        if isinstance(args, str):
            try:
                args = json.loads(args)
            except Exception:
                args = {"_raw": args}
        tool_calls.append({
            "id": f"call_gw_{i}",
            "type": "function",
            "function": {"name": obj.get("name", ""),
                         "arguments": json.dumps(args or {}, ensure_ascii=False)},
        })
    if not tool_calls:
        return None, text
    remainder = TOOL_CALL_RE.sub("", text).strip()
    return tool_calls, remainder
