SELECT CAST (DATA: id AS NUMBER(18, 0)) AS organization_id,
    CAST (DATA: accountId AS NUMBER(18, 0)) AS account_id,
    CAST (
        IFF(
            REGEXP_LIKE(
                DATA: financeIdentifier,
                '^([[:alnum:]]+)001[[:alnum:]]{15}$'
            ),
            REGEXP_SUBSTR(
                DATA: financeIdentifier,
                '^([[:alnum:]]+)001[[:alnum:]]{15}$',
                1,
                1,
                'e',
                1
            ),
            LEFT(DATA: financeIdentifier, 120)
        ) AS VARCHAR(120)
    ) AS subscription_id,
    CAST (DATA: type AS VARCHAR(40)) as type
FROM {{ source('bpi','organization') }}
WHERE DATA: financeIdentifier is not NULL
    AND DATA: financeIdentifier <> ''