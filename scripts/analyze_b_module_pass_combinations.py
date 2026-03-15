#!/usr/bin/env python3
# 枚举 B 模块「总分≥70」的所有池分数组合（加权融合 + 共振），供文档分析用
# 公式：base = 0.7*s1 + 0.2*s2 + 0.1*s3 (s1>=s2>=s3)；若至少 2 池≥40 则 +10；pass = min(100, base 或 base+10) >= 70

def main():
    w1, w2, w3 = 0.7, 0.2, 0.1
    threshold = 70
    resonance_min = 40
    resonance_bonus = 10

    # 离散化步长 5 分，便于表格展示
    step = 5
    vals = list(range(0, 101, step))

    no_resonance = []   # 无共振通过
    with_resonance = []  # 靠共振加成通过

    for s1 in vals:
        for s2 in vals:
            if s2 > s1:
                continue
            for s3 in vals:
                if s3 > s2:
                    continue
                base = w1 * s1 + w2 * s2 + w3 * s3
                count_ge_40 = sum(1 for x in (s1, s2, s3) if x >= resonance_min)
                has_resonance = count_ge_40 >= 2
                total = min(100.0, base + (resonance_bonus if has_resonance else 0))
                if total < threshold:
                    continue
                row = (s1, s2, s3, round(base, 1), has_resonance, round(total, 1))
                if has_resonance and base < threshold and base + resonance_bonus >= threshold:
                    with_resonance.append(row)
                else:
                    no_resonance.append(row)

    # 去重：no_resonance 里可能有与 with_resonance 重叠的（同一组既无共振也>=70）
    no_res = [(s1,s2,s3,b,r,t) for s1,s2,s3,b,r,t in no_resonance if b >= threshold]
    with_res = sorted(with_resonance, key=lambda x: (-x[3], -x[0], -x[1], -x[2]))

    print("======== 无共振即通过（base = 0.7*s1+0.2*s2+0.1*s3 >= 70）========\n")
    print("s1(最高)  s2(次高)  s3(第三)  base   总分")
    print("-" * 50)
    for s1, s2, s3, base, _, total in sorted(no_res, key=lambda x: (-x[0], -x[1], -x[2]))[:80]:
        print("%6d   %6d   %6d   %6.1f  %6.1f" % (s1, s2, s3, base, total))
    if len(no_res) > 80:
        print("... 共 %d 组" % len(no_res))

    print("\n======== 靠共振加成才通过（base<70 且 base+10>=70，且至少 2 池≥40）========\n")
    print("s1(最高)  s2(次高)  s3(第三)  base   +10后  说明")
    print("-" * 60)
    for s1, s2, s3, base, _, total in with_res[:60]:
        print("%6d   %6d   %6d   %6.1f  %6.1f  两池≥40" % (s1, s2, s3, base, total))
    if len(with_res) > 60:
        print("... 共 %d 组" % len(with_res))

    # 边界与典型
    print("\n======== 边界与典型组合 ========")
    # 单池 100
    print("单池满分: (100,0,0) -> base=70, 总分=70 (刚好过)")
    # 两池 70
    print("两池 70: (70,70,0) -> base=63, 无共振; 需共振: (70,70,0) 两池≥40, base+10=73 过")
    b7070 = 0.7*70 + 0.2*70 + 0.1*0
    print("         base=%.1f, +10=%.1f -> %s" % (b7070, b7070+10, "通过" if b7070+10>=70 else "不通过"))
    # 两池 40 能否过
    b4040 = 0.7*40 + 0.2*40 + 0.1*0
    print("两池 40: (40,40,0) -> base=36, +10=46 < 70 不通过")
    # 最低过线组合（无共振）
    print("无共振通过最低: s1=100 且 s2=s3=0 -> 70")
    # 有共振最低
    print("有共振通过: base>=60 且至少 2 池≥40; 例如 (86,40,0)->60.2+10=70.2")

if __name__ == "__main__":
    main()
