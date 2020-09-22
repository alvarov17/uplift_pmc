import psycopg2
from datetime import datetime, timedelta
import pandas as pd

from .helpers import make_dataframe, exec_query
from .core import obtener_recomendaciones_implementadas, tph_dict, molinos_dict
from sqlalchemy import create_engine

import argparse

parser = argparse.ArgumentParser(description="Obtiene uplift obtenido por el recomendador")
parser.add_argument('-fi', '--fecha_inicio',
                    metavar='fecha_inicio',
                    type=str,
                    default=(datetime.now() + timedelta(hours=-2 * 24)
                             ).strftime("%Y-%m-%d %H:%M"))

parser.add_argument('--baseline_m1_m8',
                    metavar='baseline',
                    type=int,
                    default=170)
parser.add_argument('--baseline_m9_m12',
                    metavar='baseline',
                    type=int,
                    default=220)
parser.add_argument('--baseline_mun',
                    metavar='baseline',
                    type=int,
                    default=850)

args = parser.parse_args()


def main():
    host = "10.18.18.247"
    user = "aasag_dch"
    password = "SAGChuqui2020"
    dbname = "det_pmc_output-data_prod"

    process_data_conn = psycopg2.connect(
        host=host, user=user, password=password, dbname="det_pmc_process-data_prod")

    output_data_conn = psycopg2.connect(
        host=host, user=user, password=password, dbname=dbname)

    sql_alchemy_output = create_engine(f"postgresql://{user}:{password}@{host}/{dbname}")

    output_data_conn.autocommit = True


    ft = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d %H:%M")

    exec_query(conn=output_data_conn, query="""delete from uplift where \"TimeStamp\" >= '{fi}'""".format(fi=args.fecha_inicio))

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
    """.format(fi=args.fecha_inicio)

    df = make_dataframe(conn=output_data_conn, query=query)

    df = df.resample('4H', on='turno').last().drop(columns=['turno'])
    df.reset_index(level=0, inplace=True)

    base_line_m1_m8 = args.baseline_m1_m8
    base_line_m9_m12 = args.baseline_m9_m12
    base_line_mun = args.baseline_mun

    for index, (turno, created_at) in df.iterrows():

        resultado = 0

        if created_at is pd.NaT:
            exec_query(conn=output_data_conn, query=f"insert into uplift (\"TimeStamp\") values ('{turno}')")
            continue

        recomendaciones_implementadas = obtener_recomendaciones_implementadas(conn_output=output_data_conn, created_at=created_at)

        resultados = pd.Series({
            'TimeStamp': 0, 'caso1MUN': 0, 'caso2MUN': 0, 'caso1M12': 0, 'caso2M12': 0,
            'caso1M11': 0, 'caso2M11': 0, 'caso1M10': 0, 'caso2M10': 0, 'caso1M9': 0,
            'caso2M9': 0, 'caso1M8': 0, 'caso2M8': 0, 'caso1M7': 0, 'caso2M7': 0,
            'caso1M6': 0, 'caso2M6': 0, 'caso1M5': 0, 'caso2M5': 0, 'caso1M4': 0,
            'caso2M4': 0, 'caso1M3': 0, 'caso2M3': 0, 'caso1M2': 0, 'caso2M2': 0,
            'caso1M1': 0, 'caso2M1': 0,
        })

        resultados.loc["TimeStamp"] = turno

        for index_recomendacion, recomendacion in recomendaciones_implementadas.iterrows():

            molino = molinos_dict[recomendacion.n_molino]

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

            if promedio_tph_inicial >= promedio_hl_inicial * .9:
                caso = 1
            else:
                caso = 2

            for index, (bloque, tag, promedio_tph, promedio_hl) in bloques.iterrows():

                if index == 0:
                    if molino == 'M1' or molino == 'M2' or molino == 'M3' \
                            or molino == 'M4' or molino == 'M5' or molino == 'M6' \
                            or molino == 'M7' or molino == 'M8':
                        promedio_tph_inicial = base_line_m1_m8
                    elif molino == 'M9' or molino == 'M10' or molino == 'M11' or molino == 'M12':
                        promedio_tph_inicial = base_line_m9_m12
                    elif molino == 'MUN':
                        promedio_tph_inicial = base_line_mun
                    continue

                if caso == 1:
                    delta = (promedio_tph - promedio_tph_inicial) / 60
                    if delta > 0:
                        resultado += delta
                    else:
                        resultado += 0
                    resultados.loc[f"caso{caso}{molino}"] = resultado + resultados.loc[f"caso{caso}{molino}"]
                elif caso == 2:
                    delta = (promedio_tph - promedio_tph_inicial) / 60
                    if delta > 0:
                        resultado += delta
                    else:
                        resultado += 0
                    resultados.loc[f"caso{caso}{molino}"] = resultado + resultados.loc[f"caso{caso}{molino}"]

        resultados = pd.DataFrame(columns=resultados.keys(), data=[resultados.values])

        resultados.to_sql(name='uplift', con=sql_alchemy_output, index=False,  if_exists='append')


if __name__ == '__main__':
    main()
