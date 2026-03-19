# Mission Control Manual

## Podcast Pipeline Ops + Handoff Runbook

This runbook documents the exact implementation paths, runtime folders, and execution flow for the podcasts pipeline.

## Exact implementation paths

### Backend API + models + schemas
- `backend/app/api/control_center.py`
- `backend/app/models/control_center.py`
- `backend/app/schemas/control_center.py`

### Backend services
- `backend/app/services/podcast_ingest.py`
- `backend/app/services/podcast_transcription.py`
- `backend/app/services/podcast_summary.py`
- `backend/app/services/podcast_classification.py`
- `backend/app/services/podcast_storage.py`

### Frontend
- `frontend/src/lib/control-center.ts`
- `frontend/src/app/control-center/[module]/page.tsx`
- `frontend/src/components/control-center/ModuleWorkspace.tsx`

## Required runtime config

### Auth/config
- `AUTH_MODE=local` (common local setup)
- `LOCAL_AUTH_TOKEN=<minimum 50 chars>` in `.env`
- `NEXT_PUBLIC_API_URL=<reachable backend URL>` for frontend calls

### Optional runtime dependencies
- Current transcription writes placeholder transcript text.
- A speech-to-text engine can be plugged into `podcast_transcription.py` later.

## Storage layout under `~/.openclaw/podcasts`

Created automatically by pipeline services:

```text
~/.openclaw/podcasts/
  inbox/                # uploaded source audio (.mp3/.m4a)
  transcripts/          # transcript artifacts and ad-hoc audio uploads for transcribe action
  summaries/            # generated summary text artifacts
  completed/
    motivational/
    teaching/
    self-confidence-mindset/
    general/
    habits-productivity/
```

## API flow (control-center)

1. **Ingest**
   - `POST /api/v1/control-center/podcasts/ingest`
   - Validates upload extension (`.mp3`, `.m4a`) and stores file to `~/.openclaw/podcasts/inbox`.
   - Creates `ControlCenterRecord` with podcast status fields.

2. **Transcribe**
   - `POST /api/v1/control-center/records/{record_id}/transcribe`
   - Stores audio + transcript artifacts in `~/.openclaw/podcasts/transcripts`.
   - Updates `transcript_status`, `transcript_path`.

3. **Summarize**
   - `POST /api/v1/control-center/records/{record_id}/summarize`
   - Requires completed transcript.
   - Writes summary artifact to `~/.openclaw/podcasts/summaries`.
   - Updates `summary_status`, `summary_path`.

4. **Classify + route completed artifacts**
   - `POST /api/v1/control-center/records/{record_id}/classify`
   - Classifies into category and moves artifacts to `~/.openclaw/podcasts/completed/<category>`.

5. **Task linkage**
   - `POST /api/v1/control-center/records/{record_id}/promote`
   - Creates linked task and sets `linked_task_id` on the record.

## Podcast record fields (data payload)

Expected fields in podcast `data` payload:
- `source_filename`
- `source_format`
- `ingest_status`
- `transcript_status`
- `summary_status`
- `task_extraction_status`
- `category`
- `transcript_path`
- `summary_path`
- `extracted_actions_count`

Additional operational fields may also be present:
- `source_path`, `audio_path`, `size_bytes`, `content_type`, `summary_error`, `transcript_error`.

## Frontend operator flow

In **Control Center → Podcasts** module:
- Upload audio from Add Record panel.
- Track ingest/transcript/summary/task extraction statuses per row.
- Trigger per-record transcribe + summarize actions.
- View summary path and linked task id.
- Promote any record to a board task via board id input.

## Validation commands

```bash
# Backend syntax check
cd backend
.venv/bin/python -m py_compile \
  app/api/control_center.py \
  app/models/control_center.py \
  app/schemas/control_center.py \
  app/services/podcast_ingest.py \
  app/services/podcast_transcription.py \
  app/services/podcast_summary.py \
  app/services/podcast_classification.py \
  app/services/podcast_storage.py

# Frontend type check
cd ../frontend
npx tsc --noEmit
```

## Handoff checklist

- Confirm all paths above exist and are committed.
- Run backend `py_compile` and frontend `npx tsc --noEmit`.
- Verify local folder creation under `~/.openclaw/podcasts` after ingest/transcribe/summarize.
- If agents cannot post task comments/status updates (`401 Unauthorized`), re-provision write-capable agent auth before handoff closure.
