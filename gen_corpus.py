"""合成中文测试语料：正样本(精确重复/近重复) + 硬负样本(高度相似但不同主题)。

每个 base 生成 3 篇:
  orig       原始
  exact      精确复制(仅空白差异)
  near       近重复: 少量字符改写/增删, 字符级 3-gram Jaccard 约 0.85+
硬负样本(neg): 取另一个 base 的句子重组, 与某个 orig 共享约一半句子 -> 相似但不应判重。
所有标签随数据返回, 便于算 precision/recall。
"""
from __future__ import annotations

import random
import string
from dataclasses import dataclass

_TOPICS = [
    "营收", "毛利率", "用户增长", "供应链", "现金流", "研发投入", "市场份额", "客单价",
    "库存周转", "复购率", "获客成本", "履约时效", "补贴", "渠道下沉", "品牌心智",
]
_UNITS = ["亿元", "万元", "个百分点", "万人", "%", "天", "次/月"]

_SENT_TEMPLATES = [
    "报告显示, 公司{t}在本季度同比增长{num}{u}, 高于行业平均水平。",
    "管理层表示, {t}的改善主要来自组织效率提升与{t2}的持续优化。",
    "在华东地区, {t}环比提升{num}{u}, 但{t2}仍存在结构性压力。",
    "分析师指出, 若{t}维持当前节奏, 全年{t2}有望达到{num}{u}。",
    "从渠道结构看, 线上占比提升带来{t}改善, 同时压低了{t2}。",
    "风险提示方面, {t}的波动与{t2}的不确定性需要持续跟踪。",
    "公司计划在下半年加大{t}投入, 预计影响{t2}{num}{u}左右。",
    "与去年同期相比, {t}的变化幅度为{num}{u}, 符合此前指引。",
]
_CHARS = "的一是在不了有和人这中大为上个国我以要他时来用们生到作地于出就分对成会可主发年动同工也能下过子说产种面而方后多定行学法所民得经十三之进着等部度家电力里如水化高自二理起小物现实加量都两体制机当使点从业本去把性好应开它合还因由其些然前外天政四日那社义事平形相全表间样与关各重新线内数正心反你明看原又么利比或但质气第向道命此变条只没结解问意建月公无系军很情者最立代想已通并提直题党程展五果料象员革位入常文总次品式活设及管特件长求老头基资边流路级少图山统接知较将组见计别她手角期根论运农指几九区强放决西被干做必战先回则任取据处队南给色光门即保治北造百规热领七海口东导器压志世金增争济阶油思术极交受联什认六共权收证改清己美再采转更单风切打白教速花带安场身车例真务具万每目至达走积示议声报斗完类八离华名确才科张信马节话米整空元况今集温传土许步群广石记需段研界拉林律叫且究观越织装影算低持音众书布复容儿须际商非验连断深难近矿千周委素技备半办青省列习响约支般史感劳便团往酸历市克何除消构府称太准精值号率族维划选标写存候毛亲快效斯院查江型眼王按格养易置派层片始却专状育厂京识适属圆包火住调满县局照参红细引听该铁价严"
if not _CHARS:  # pragma: no cover - 保持字符串非空
    _CHARS = string.ascii_letters


def _num(rng: random.Random) -> str:
    return f"{rng.uniform(0.5, 48.0):.1f}"


def _sentence(rng: random.Random) -> str:
    tpl = rng.choice(_SENT_TEMPLATES)
    t = rng.choice(_TOPICS)
    t2 = rng.choice([x for x in _TOPICS if x != t])
    return tpl.format(t=t, t2=t2, num=_num(rng), u=rng.choice(_UNITS))


def _doc(rng: random.Random, n_sent: int) -> str:
    sents = [_sentence(rng) for _ in range(n_sent)]
    return "".join(sents)


def _mutate(rng: random.Random, text: str, ratio: float = 0.02) -> str:
    """字符级轻微改写：替换 / 插入 / 删除，模拟转述、OCR、转录误差。"""
    chars = list(text)
    n_ops = max(1, int(len(chars) * ratio))
    for _ in range(n_ops):
        op = rng.random()
        i = rng.randrange(len(chars))
        if op < 0.45:
            chars[i] = rng.choice(_CHARS)
        elif op < 0.75 and len(chars) > 20:
            del chars[i]
        else:
            chars.insert(i, rng.choice(_CHARS))
    return "".join(chars)


@dataclass
class Doc:
    doc_id: int
    text: str
    base_id: int
    kind: str  # orig | exact | near | neg


def build_corpus(n_docs: int = 20000, seed: int = 7, near_ratio: float = 0.01) -> list[Doc]:
    """生成 n_docs 篇文档；1/3 orig + 1/3 exact + 1/3 near，其中一部分近重复换成硬负样本。"""
    rng = random.Random(seed)
    n_bases = max(1, n_docs // 3)
    bases: list[str] = [_doc(rng, rng.randint(6, 12)) for _ in range(n_bases)]
    docs: list[Doc] = []
    did = 0
    for b, base in enumerate(bases):
        docs.append(Doc(did, base, b, "orig")); did += 1
        docs.append(Doc(did, f"{base}\n", b, "exact")); did += 1  # 仅空白差异
        if b % 10 == 0:  # 10% 的近重复槽位换成负样本：同模板不同内容的新文档（base_id=-1）
            docs.append(Doc(did, _doc(rng, rng.randint(6, 12)), -1, "neg")); did += 1
        elif b % 10 == 5:  # 5% 换成「边界样本」：换掉 1 句 + 打乱语序，衡量 trade-off
            mine = [x for x in base.split("。") if x]
            other = [x for x in bases[(b + 1) % n_bases].split("。") if x]
            k = rng.randrange(len(mine))
            mine[k] = rng.choice(other)
            if len(mine) > 3:
                i, j = rng.sample(range(len(mine)), 2)
                mine[i], mine[j] = mine[j], mine[i]
            docs.append(Doc(did, "。".join(mine) + "。", b, "border")); did += 1
        else:
            docs.append(Doc(did, _mutate(rng, base, near_ratio), b, "near")); did += 1
    rng.shuffle(docs)
    # 洗牌后重编 doc_id，保证与原顺序无关
    return [Doc(i, d.text, d.base_id, d.kind) for i, d in enumerate(docs)]


if __name__ == "__main__":
    import collections
    cs = build_corpus(3000)
    print(collections.Counter(d.kind for d in cs))
    print(cs[0])
