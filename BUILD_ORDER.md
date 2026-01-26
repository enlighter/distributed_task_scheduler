
---

## What to implement first (pragmatic build order)

1. **Migrations + repo layer**
   - create schema
   - basic CRUD for tasks/deps

2. **Submit endpoint**
   - insert task + deps in one transaction
   - compute/set `remaining_deps`

3. **Claim loop**
   - `BEGIN IMMEDIATE` claim runnable tasks
   - set RUNNING + lease

4. **Worker execution + completion**
   - mark COMPLETED
   - decrement dependents `remaining_deps`

5. **Recovery**
   - requeue stale RUNNING tasks

6. **Tests**
   - dependency gating works
   - concurrency limit respected
   - crash recovery requeues tasks

This order minimizes the “it runs but it’s wrong” risk.

---

If you follow the structure above, the repo reads like a small serious system rather than a single-file script. You’ll also make your own life easier when you start adding things like retries, failure propagation, or a “cancel task” feature.
