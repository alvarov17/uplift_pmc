import psycopg2
from datetime import datetime, timedelta
import pandas as pd

from .helpers import make_dataframe
from .core import obtener_recomendaciones_implementadas, tph_dict



def main():
    host = "10.18.18.247"
    user = "aasag_dch"
    password = "SAGChuqui2020"
    dbname = "det_pmc_output-data_prod"

    process_data_conn = psycopg2.connect(
        host=host, user=user, password=password, dbname="det_pmc_process-data_prod")

    output_data_conn = psycopg2.connect(
        host=host, user=user, password=password, dbname=dbname)

    output_data_conn.autocommit = True

    resultados = pd.DataFrame(columns=['TimeStamp', 'caso1MUN', 'caso2MUN', 'caso1M12', 'caso2M12', 'caso1M11',
                                       'caso2M11', 'caso1M10', 'caso2M10', 'caso1M9', 'caso2M9', 'caso1M8',
                                       'caso2M8', 'caso1M7', 'caso2M7', 'caso1M6', 'caso2M6', 'caso1M5',
                                       'caso2M5', 'caso1M4', 'caso2M4', 'caso1M3', 'caso2M3', 'caso1M2', 'caso2M2',
                                       'caso1M1', 'caso2M1'])

    fi = (datetime.now() + timedelta(hours=-2 * 24 - 3)
          ).strftime("%Y-%m-%d %H:%M")

    ft = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d %H:%M")

    query = """
        SELECT
            mcmc.created_at as turno,
            ri.created_at
        FROM (
            SELECT
                created_at
            FROM
                "mcmc_recommendations_ALL_VIEW"
            GROUP BY
                created_at) mcmc
            LEFT JOIN (
                SELECT
                    created_at
                FROM
                    recom_implemented
                GROUP BY
                    created_at) ri ON mcmc.created_at = ri.created_at
            WHERE mcmc.created_at >= '{fi}'
        ORDER BY
            mcmc.created_at ASC;
    """.format(fi=fi)

    df = make_dataframe(conn=output_data_conn, query=query)

    df = df.resample('4H', on='turno').last().drop(columns=['turno'])
    df.reset_index(level=0, inplace=True)

    for index, (turno, created_at) in df.iterrows():
        caso1 = False
        caso2 = False

        if created_at is pd.NaT:
            #df = pd.DataFrame([[0] * len(resultados.columns)], columns=resultados.columns)
            continue

        recomendaciones_implementadas = obtener_recomendaciones_implementadas(conn_output=output_data_conn, created_at=created_at)

        for index, recomendacion in recomendaciones_implementadas.iterrows():

            query_limites = """
                SELECT
                t."timestamp",
                l.tag,
                t."value",
                hl."value" AS hl
            FROM
                public.limits_tags l
                LEFT JOIN input_tags t ON t.tag = l.tag
                LEFT JOIN input_tags ll ON ll.tag = l.tagll
                    AND t. "timestamp" = ll. "timestamp"
                LEFT JOIN input_tags hl ON hl.tag = l.taghl
                    AND t. "timestamp" = hl. "timestamp"
            WHERE
                t.tag = '{tag}' and
                t. "timestamp" BETWEEN '{created_at}'::timestamp - interval '20m'
                AND '{created_at}'::timestamp + interval '2h';
                  """.format(tag=tph_dict[recomendacion['n_molino']], created_at=created_at)

            datos_x_minuto = make_dataframe(conn=process_data_conn, query=query_limites)

            bloques = datos_x_minuto.resample('15min', on='timestamp', label='left').last().drop(columns=['timestamp'])\
                .rename_axis('bloque').rename(columns={'value':'promedio_tph', 'hl':'promedio_hl'})

            bloques.reset_index(level=0, inplace=True)

            promedio_tph_anterior = 0
            promedio_hl_anterior = 0
            res = 0

            for index, (bloque, tag, promedio_tph, promedio_hl) in bloques.iterrows():

                try:
                    bloque_siguiente = bloques.iloc[index + 1]['bloque']
                except IndexError:
                    break

                if index == 0:
                    promedio_tph_anterior = promedio_tph
                    promedio_hl_anterior = promedio_hl
                    continue

                datos_de_bloque = datos_x_minuto.query('timestamp >= @bloque and timestamp <= @bloque_siguiente')

                if promedio_tph_anterior >= promedio_hl_anterior * .9:
                    delta = promedio_hl_anterior - promedio_tph
                    res += delta
                else:
                    delta = promedio_tph_anterior - promedio_tph
                    res += delta

                import ipdb; ipdb.set_trace()

                promedio_tph_anterior = promedio_tph
                promedio_hl_anterior = promedio_hl

            print(res)





            #for index, row in promedios.iterrows():
            #pd.merge(left=recomendaciones, right=datos_x_minuto, on='timestamp')


if __name__ == '__main__':
    main()
