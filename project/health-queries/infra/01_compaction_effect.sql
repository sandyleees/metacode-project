-- Silver MOR Compaction 효과 확인 (주간 수동 실행)
-- 실행: aws athena start-query-execution --query-string "$(cat infra/01_compaction_effect.sql)"
-- 비용: $files 메타테이블 — 사실상 무료
--
-- 해석:
--   content=0(data) vs content=1(position_delete) 비율이 높을수록 Compaction 필요
--   Silver는 MOR 모드 — MERGE마다 delete file이 쌓임
--   Compaction(rewrite_data_files) 전후 position_delete file_count 감소 확인

SELECT
    CASE content
        WHEN 0 THEN 'data'
        WHEN 1 THEN 'position_delete'
        WHEN 2 THEN 'equality_delete'
        ELSE CAST(content AS VARCHAR)
    END                                               AS file_type,
    COUNT(*)                                          AS file_count,
    ROUND(SUM(file_size_in_bytes) / 1048576.0, 2)    AS total_mb,
    ROUND(AVG(file_size_in_bytes) / 1048576.0, 2)    AS avg_file_mb,
    SUM(record_count)                                 AS total_records
FROM silver."processed_events$files"
GROUP BY content
ORDER BY content;
