# 锁顺序与死锁预防 — 业界调研

## Topic

Java 并发编程中的锁顺序（Lock Ordering）和死锁预防（Deadlock Prevention）最佳实践。

## Sources

1. **CERT/SEI — LCK07-J**: Avoid deadlock by requesting and releasing locks in the same order — https://wiki.sei.cmu.edu/confluence/display/java/LCK07-J
2. **Brian Goetz, "Java Concurrency in Practice" (2006), Chapter 10: Avoiding Liveness Hazards** — ISBN 0-321-34960-1
3. **Oracle Java Tutorials — Deadlock** — https://docs.oracle.com/javase/tutorial/essential/concurrency/deadlock.html

## Industry Consensus

1. **一致的锁顺序（Lock Hierarchy）**：所有线程必须以相同的全局顺序获取锁，打破循环等待条件。CERT LCK07-J 和 Goetz Chapter 10 均将其作为首要预防手段。
2. **最小化锁范围**：仅在访问共享可变状态的最小代码段持有锁。Goetz 称之为 "open calls" — 持有锁时不调用可能获取其他锁的外部方法。
3. **避免嵌套锁**：最简单的死锁预防是任何时刻只持有一把锁。不可避免时，必须强制执行严格的获取顺序。

## Common Pitfalls

1. **持有锁时调用外部方法** — 被调方法可能获取另一把锁，造成调用者不可见的顺序违规。
2. **不同代码路径的锁顺序不一致** — 两个方法都获取锁 A 和 B，但顺序相反。
3. **过宽的 synchronized 块** — 同步整个方法而非仅需保护的代码行，增加竞争并扩大嵌套锁的窗口。

## Recommendation

- **使用 open calls 模式**：持有锁 A 时需要调用触及锁 B 的逻辑时，重构为：(1) 在锁 A 内计算，(2) 释放锁 A，(3) 执行获取锁 B 的调用。彻底消除嵌套。
- **当嵌套不可避免时**，使用自然排序（如 `System.identityHashCode()` + 全局 tie-breaking 锁）或 `ReentrantLock.tryLock()` 带退避。
