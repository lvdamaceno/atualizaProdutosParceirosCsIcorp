SELECT
    CODPARC
FROM
    TGFPAR
WHERE
    DTALTER >= DATEADD(MINUTE, {tempo}, GETDATE())