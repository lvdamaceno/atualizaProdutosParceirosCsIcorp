SELECT
    CODPROD
FROM
    TGFPRO
WHERE
    DTALTER >= DATEADD(MINUTE, {tempo}, GETDATE())

UNION

SELECT
    CODPROD
FROM
    TGFITE ITE
INNER JOIN
    TGFCAB CAB ON ITE.NUNOTA = CAB.NUNOTA
WHERE
    ITE.DTALTER >= DATEADD(MINUTE, {tempo}, GETDATE())

UNION

SELECT
    LTRIM(SUBSTRING(CHAVE,CHARINDEX('CODPROD=', CHAVE) + 8,CHARINDEX('CODLOCAL=', CHAVE) - (CHARINDEX('CODPROD=', CHAVE) + 8))) AS CODPROD
FROM
    TSILGT
WHERE
    NOMETAB = 'TGFEXC'
    AND DHACAO >= DATEADD(MINUTE, {tempo}, GETDATE())