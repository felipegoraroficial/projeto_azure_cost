import psycopg2
import os

def creatview_resourcegroup_totals():

    conn = psycopg2.connect(
        dbname="azurecost",
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host="airflow-postgres",
        port="5432"
    )
    cur = conn.cursor()

    sql = """
    CREATE OR REPLACE VIEW vw_resourcegroup_totais AS
    WITH last_per_day AS (
    SELECT "id", "pretaxcost", "usagedate"
    FROM (
        SELECT *, 
                ROW_NUMBER() OVER (
                    PARTITION BY "id", DATE("usagedate")
                    ORDER BY "usagedate" DESC
                ) as rn
        FROM costresources
    ) t
    WHERE rn = 1
    )
    SELECT r."resourcegroup", SUM(l."pretaxcost") as Total_PreTaxCost
    FROM last_per_day l
    LEFT JOIN resources r
    ON l."id" = r."id"
    GROUP BY r."resourcegroup"
    ORDER BY r."resourcegroup";
    """

    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()