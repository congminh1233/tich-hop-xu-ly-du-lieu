CREATE SCHEMA IF NOT EXISTS serving;
CREATE TABLE IF NOT EXISTS serving.experiments (
 experiment_id text PRIMARY KEY,
 source_path text NOT NULL,
 script_version text NOT NULL,
 snapshot_id text NOT NULL,
 manifest jsonb NOT NULL,
 summary jsonb NOT NULL,
 row_count bigint NOT NULL CHECK (row_count >= 0),
 score_digest text NOT NULL,
 loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS serving.scores (
 experiment_id text NOT NULL REFERENCES serving.experiments(experiment_id),
 subject_kind text NOT NULL CHECK (subject_kind IN ('host','iot')),
 subject_id text NOT NULL,
 window_start bigint NOT NULL CHECK (window_start % 60 = 0),
 family text NOT NULL,
 feature_group text NOT NULL,
 split text NOT NULL CHECK (split IN ('train','validation','test')),
 score double precision NOT NULL,
 threshold double precision NOT NULL,
 prediction smallint NOT NULL CHECK (prediction IN (0,1)),
 label_observed smallint CHECK (label_observed IN (0,1)),
 payload text NOT NULL CHECK (jsonb_typeof(payload::jsonb) = 'object'),
 PRIMARY KEY (experiment_id, subject_kind, subject_id, window_start, family, feature_group)
);
CREATE INDEX IF NOT EXISTS scores_filter_idx ON serving.scores
 (experiment_id,subject_id,family,feature_group,split,window_start);
CREATE TABLE IF NOT EXISTS serving.documents (
 document_id text PRIMARY KEY,
 source_path text NOT NULL,
 body jsonb NOT NULL,
 imported_at timestamptz NOT NULL DEFAULT now()
);
