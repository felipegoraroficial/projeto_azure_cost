import psycopg2
import os

def creatview_resourcename_totals():

    conn = psycopg2.connect(
        dbname="azurecost",
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host="airflow-postgres",
        port="5432"
    )
    cur = conn.cursor()

    sql = """
    CREATE OR REPLACE VIEW vw_resourcename_totais AS
    WITH last_per_day AS (
    SELECT Id, PreTaxCost, UsageDate
    FROM (
        SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY Id, DATE(UsageDate)
                    ORDER BY UsageDate DESC
                ) as rn
        FROM costresources 
    ) t
    WHERE rn = 1
    )

    SELECT r.ResourceName, SUM(l.PreTaxCost) as Total_PreTaxCost
    FROM last_per_day l
    LEFT JOIN resources r
    ON l.Id = r.Id
    GROUP BY r.ResourceName
    ORDER BY r.ResourceName;
    """

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()