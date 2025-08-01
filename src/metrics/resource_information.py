import psycopg2
import os

def creatview_resource_information():

    conn = psycopg2.connect(
        dbname="azurecost",
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host="airflow-postgres",
        port="5432"
    )
    cur = conn.cursor()

    sql = """
    CREATE OR REPLACE VIEW vw_resource_information AS
    WITH max_usagedate_per_id AS (
    SELECT
        id,
        MAX(usagedate) AS max_usagedate
    FROM
        costresources
    GROUP BY
        id
    ),
    tabela_cost_filtered AS (
    SELECT t2.*
    FROM costresources t2
    INNER JOIN max_usagedate_per_id m
        ON t2.id = m.id AND t2.usagedate = m.max_usagedate
    )
    SELECT
    t1.resourcename,
    t1.statusrecourse,
    t2f.pct_change,
    t1.tendenciacusto,
    t2f.previsaoproxima
    FROM
    resources t1
    LEFT JOIN
    tabela_cost_filtered t2f
    ON
    t1.id = t2f.id;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()