WITH repo_contributors AS (
  -- Filtrar y contar los contribuidores para cada año
  SELECT
    '2018' AS year,
    repo.id AS repo_id,
    COUNT(DISTINCT actor.id) AS contributor_count
  FROM
    `githubarchive.year.2018`
  WHERE
    type = 'CreateEvent'
    AND public = TRUE
  GROUP BY
    repo_id

  UNION ALL

  SELECT
    '2019' AS year,
    repo.id AS repo_id,
    COUNT(DISTINCT actor.id) AS contributor_count
  FROM
    `githubarchive.year.2019`
  WHERE
    type = 'CreateEvent'
    AND public = TRUE
  GROUP BY
    repo_id

  UNION ALL

  SELECT
    '2020' AS year,
    repo.id AS repo_id,
    COUNT(DISTINCT actor.id) AS contributor_count
  FROM
    `githubarchive.year.2020`
  WHERE
    type = 'CreateEvent'
    AND public = TRUE
  GROUP BY
    repo_id

  UNION ALL

  SELECT
    '2021' AS year,
    repo.id AS repo_id,
    COUNT(DISTINCT actor.id) AS contributor_count
  FROM
    `githubarchive.year.2021`
  WHERE
    type = 'CreateEvent'
    AND public = TRUE
  GROUP BY
    repo_id

  UNION ALL

  SELECT
    '2022' AS year,
    repo.id AS repo_id,
    COUNT(DISTINCT actor.id) AS contributor_count
  FROM
    `githubarchive.year.2022`
  WHERE
    type = 'CreateEvent'
    AND public = TRUE
  GROUP BY
    repo_id

  UNION ALL

  SELECT
    '2023' AS year,
    repo.id AS repo_id,
    COUNT(DISTINCT actor.id) AS contributor_count
  FROM
    `githubarchive.year.2023`
  WHERE
    type = 'CreateEvent'
    AND public = TRUE
  GROUP BY
    repo_id
)

-- Contar repositorios públicos de software con hasta 3 contribuidores por año
SELECT
  year,
  COUNT(*) AS repo_count
FROM
  repo_contributors
WHERE
  contributor_count <= 3
GROUP BY
  year
ORDER BY
  year;
