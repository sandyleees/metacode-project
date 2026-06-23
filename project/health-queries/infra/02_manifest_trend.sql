-- Silver/Gold manifest 수 확인 (월간 수동 실행, rewrite_manifests 전후 비교)
-- 실행: aws athena start-query-execution --query-string "$(cat infra/02_manifest_trend.sql)"
-- 비용: $manifests 메타테이블 — 사실상 무료
--
-- 해석:
--   manifest_count 과다 시 쿼리 플래닝 오버헤드 증가
--   Silver MOR은 MERGE마다 manifest가 추가되므로 월간 rewrite_manifests 실행 후 감소 확인
--   monthly DAG 실행 전후 이 쿼리 결과를 비교해 효과 측정

SELECT
    'silver'              AS layer,
    COUNT(*)              AS manifest_count,
    SUM(added_data_files_count)   AS added_files,
    SUM(deleted_data_files_count) AS deleted_files
FROM silver."processed_events$manifests"
UNION ALL
SELECT
    'gold',
    COUNT(*),
    SUM(added_data_files_count),
    SUM(deleted_data_files_count)
FROM gold."campaign_summary$manifests";
