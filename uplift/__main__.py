import psycopg2
from datetime import datetime, timedelta
import pandas as pd

from .helpers import make_dataframe, exec_query
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

    #fi = (datetime.now() + timedelta(hours=-6 * 24 - 3)
    #      ).strftime("%Y-%m-%d %H:%M")

    fi = '2020-08-17 00:00'

    ft = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d %H:%M")

    exec_query(conn=output_data_conn, query="""delete from uplift where \"TimeStamp\" >= '{fi}'""".format(fi=fi))

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
            continue

        recomendaciones_implementadas = obtener_recomendaciones_implementadas(conn_output=output_data_conn, created_at=created_at)

        for index_recomendacion, recomendacion in recomendaciones_implementadas.iterrows():

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
                t. "timestamp" BETWEEN '{updated_at}'::timestamp - interval '20m'
                AND '{updated_at}'::timestamp + interval '4h';
                  """.format(tag=tph_dict[recomendacion['n_molino']], updated_at=recomendacion['updated_at'])

            datos_x_minuto = make_dataframe(conn=process_data_conn, query=query_limites)

            bloques = datos_x_minuto.resample('15min', on='timestamp', label='left').last().drop(columns=['timestamp'])\
                .rename_axis('bloque').rename(columns={'value':'promedio_tph', 'hl':'promedio_hl'})

            bloques.reset_index(level=0, inplace=True)

            promedio_tph_inicial = 0
            promedio_hl_inicial = 0
            resultado_caso_1 = 0
            resultado_caso_2 = 0

            for index, (bloque, tag, promedio_tph, promedio_hl) in bloques.iterrows():

                if index == 0:
                    promedio_tph_inicial = promedio_tph
                    promedio_hl_inicial = promedio_hl
                    continue

                if promedio_tph_inicial >= promedio_hl_inicial * .9:
                    if recomendacion['description'] == 'Tonelaje':
                        caso1 = True
                        delta = (promedio_tph_inicial - promedio_tph) / 60
                        if delta > 0:
                            resultado_caso_1 += delta
                        else:
                            resultado_caso_1 += 0
                else:
                    caso2 = True
                    delta = (promedio_tph_inicial - promedio_tph) / 60
                    if delta > 0:
                        resultado_caso_2 += delta
                    else:
                        resultado_caso_2 += 0

            for index, row in tph_dict.items():
                if index_recomendacion != 0:
                    turno_anterior = turno
                else:
                    turno_anterior = ''

                key_caso1 = f'caso1M{index}'
                key_caso2 = f'caso2M{index}'

                if index == 13:
                    key_caso1 = 'caso1MUN'
                    key_caso2 = 'caso2MUN'

                if recomendacion['n_molino'] == index:
                    if caso1 == True:
                        #resultado = resultados.append(pd.Series({'TimeStamp': turno, key_caso1: resultado_caso_1}), ignore_index=True)
                        sql = f"insert into uplift (\"TimeStamp\", \"{key_caso1}\") values ('{turno}', {resultado_caso_1})"
                        if turno == turno_anterior:
                            sql = f"UPDATE uplift set \"{key_caso1}\" = {resultado_caso_1}"
                    else:
                        resultado = resultados.append(pd.Series({'TimeStamp': turno, key_caso2: resultado_caso_1}), ignore_index=True)
                        sql = f"insert into uplift (\"TimeStamp\", \"{key_caso2}\") values ('{turno}', {resultado_caso_2})"
                        if turno == turno_anterior:
                            sql = f"UPDATE uplift set \"{key_caso2}\" = {resultado_caso_2}"

                    exec_query(conn=output_data_conn, query=sql)
                    break





            #for index, row in promedios.iterrows():
            #pd.merge(left=recomendaciones, right=datos_x_minuto, on='timestamp')


if __name__ == '__main__':
    main()
