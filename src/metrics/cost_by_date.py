import psycopg2
import os

def creatview_cost_by_date():

    conn = psycopg2.connect(
        dbname="azurecost",
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host="airflow-postgres",
        port="5432"
    )
    cur = conn.cursor()

    sql = """
    CREATE OR REPLACE VIEW vw_cost_by_date AS
    WITH max_usagedate_per_day AS (
    SELECT
        usagedate::date AS day,
        MAX(usagedate) AS max_usagedate
    FROM
        costresources
    GROUP BY
        usagedate::date
    ),
    filtered AS (
    SELECT
        usagedate,
        pretaxcost
    FROM
        costresources t
    INNER JOIN
        max_usagedate_per_day m
    ON
        t.usagedate::date = m.day AND t.usagedate = m.max_usagedate
    )
    SELECT
    usagedate::date AS usagedate,
    SUM(pretaxcost) AS pretaxcost_sum
    FROM
    filtered
    GROUP BY
    usagedate::date
    ORDER BY
    usagedate;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()