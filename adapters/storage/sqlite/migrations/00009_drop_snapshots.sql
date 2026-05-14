-- +goose Up

DROP TABLE IF EXISTS snapshots;

-- +goose Down

SELECT 1;
