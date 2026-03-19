# NM-13: Team Tree Release Checklist + Rollout Notes

## Scope
Enable Team Tree view in Network Marketing without breaking existing Pipeline/Kanban workflows.

---

## Release Notes (User-facing)

### What’s new
- Network Marketing now supports two view modes:
  - **Pipeline** (existing Kanban workflow)
  - **Team Tree** (hierarchy view for sponsor/downline structure)
- View mode preference persists per organization.
- Team Tree now supports add-member creation under:
  - Root (direct-to-me)
  - Existing member (parent-child link)

### Compatibility guarantees
- Existing Pipeline records continue to work.
- Legacy/mixed stage values are normalized at read-time to supported stages.
- Legacy `next_step` data is mapped to `nextStep` when needed.
- Invalid self-parent linkage is guarded and treated as root.

### Known constraints
- If no team members exist, Team Tree renders an empty-state prompt (no crash).
- Task-thread posting from this agent may require auth refresh (operational concern, not product runtime).

---

## Rollout Plan

### 1) Pre-deploy
- [ ] Confirm backend + frontend changes are merged to release branch.
- [ ] Confirm migrations are present and ordered correctly.
- [ ] Run local verification:
  - [ ] `npm --prefix frontend run -s build`
  - [ ] `npm --prefix frontend run -s test -- --coverage.enabled=false src/lib/control-center.test.ts`

### 2) Deploy
- [ ] Deploy backend API first.
- [ ] Run DB migrations.
- [ ] Deploy frontend.
- [ ] Invalidate relevant caches/CDN assets (if configured).

### 3) Post-deploy verification (smoke)
- [ ] Open `/control-center/network_marketing`.
- [ ] Toggle `Pipeline <-> Team Tree` successfully.
- [ ] In Team Tree, add member under root.
- [ ] In Team Tree, add member under existing member.
- [ ] Refresh page and confirm hierarchy persists.
- [ ] Switch back to Pipeline and confirm cards/stages still display.

### 4) Monitoring window (first 24h)
- [ ] Error logs: no spike in control-center/API 4xx/5xx.
- [ ] Frontend logs: no increase in render/runtime exceptions on network_marketing module.
- [ ] User support queue: no priority incidents for view switching/data loss.

---

## Verification Checklist (Acceptance-focused)

### Deploy verification
- [ ] Frontend build passes in CI.
- [ ] Backend health endpoint returns healthy.
- [ ] Control center config endpoint includes `network_marketing_view_mode`.

### Data integrity checks
- [ ] Existing records with legacy stages still load and map into current stages.
- [ ] `next_step` and `nextStep` both render as next-step value.
- [ ] Records with invalid self-parent do not break tree rendering.
- [ ] Records with no parent map to root.
- [ ] No existing Pipeline record loss after release.

### User-facing checks
- [ ] Team Tree empty-state appears when no members are present.
- [ ] Add-member flow enforces valid parent selection.
- [ ] New member appears in expected tree position immediately after creation.
- [ ] Pipeline UX remains unchanged when Pipeline mode is active.

---

## Rollback Plan

### Trigger conditions
Rollback if any of these occur:
- P1/P2 incident caused by Network Marketing view switch.
- Data corruption or missing records in Pipeline view.
- Repeated server/client exceptions tied to Team Tree rendering or creation flow.

### Rollback steps
1. Re-deploy previous stable frontend release.
2. Re-deploy previous stable backend release.
3. If needed, force config fallback to `network_marketing_view_mode = "pipeline"`.
4. Validate Pipeline page load and record operations (create/move/delete/promote).
5. Communicate temporary Team Tree suspension to users.

### Post-rollback checks
- [ ] Pipeline fully functional.
- [ ] No record loss.
- [ ] Incident summary captured with root cause + corrective actions.

---

## Operator Notes
- Prefer phased rollout (internal users first, then wider cohort).
- Keep release window staffed for quick triage.
- If any migration uncertainty remains, hold rollout and run dry-run in staging snapshot first.
