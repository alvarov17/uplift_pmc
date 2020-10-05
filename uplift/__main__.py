import psycopg2
from datetime import datetime, timedelta
import pandas as pd

from .helpers import exec_query
from .core import obtener_recomendaciones_implementadas, tph_dict, molinos_dict, obtener_tph_turno,\
    obtener_bloque_inicial, obtener_horarios_recomendaciones
from sqlalchemy import create_engine

import argparse

parser = argparse.ArgumentParser(description="Obtiene uplift obtenido por el recomendador")
parser.add_argument('-fi', '--fecha_inicio',
                    metavar='fecha_inicio',
                    type=str,
                    default=(datetime.now() + timedelta(hours=-2 * 24)
                             ).strftime("%Y-%m-%d %H:%M"))
parser.add_argument('-fib', '--fecha_inicio_bloque_0',
                    metavar='fecha_inicio_primer_bloque',
                    type=int,
                    default=-15)

parser.add_argument('-tb', '--tamaño_bloque',
                    metavar='tamaño de los bloques',
                    type=int,
                    default=15)

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

    exec_query(conn=output_data_conn, query="""delete from uplift where \"TimeStamp\" >= '{fi}'""".format(fi=args.fecha_inicio))

    df = obtener_horarios_recomendaciones(output_data_conn=output_data_conn, fecha_inicio=args.fecha_inicio)
    df = df.resample('4H', on='turno').last().drop(columns=['turno'])
    df.reset_index(level=0, inplace=True)

    for index, (turno, created_at) in df.iterrows():

        try:
            siguiente_turno = df.iloc[index + 1]['turno']
        except IndexError:
            exit()

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

        for n_molino, molino in molinos_dict.items():

            recomendaciones = recomendaciones_implementadas.query('n_molino == @n_molino')

            if len(recomendaciones) == 0:
                continue

            updated_at = recomendaciones[recomendaciones['updated_at'] == recomendaciones['updated_at'].min()].iloc[0]['updated_at']

            (bloque_inicial, vacio) = obtener_bloque_inicial(conn_process=process_data_conn,
                                                    fecha_inicio=created_at,
                                                    hora_turno=created_at,
                                                    tag=tph_dict[n_molino])

            if vacio:
                continue

            (bloques, vacio) = obtener_tph_turno(conn_process=process_data_conn,
                                               fi=created_at,
                                               ft=siguiente_turno,
                                               tag=tph_dict[n_molino],
                                               tamaño_bloque=args.tamaño_bloque)

            if vacio:
                continue

            promedio_tph_inicial = bloque_inicial.agg('mean')['value']
            promedio_hl_inicial = bloque_inicial.agg('mean')['hl']

            if promedio_tph_inicial >= promedio_hl_inicial * .9:
                caso = 1
            else:
                caso = 2

            fn = lambda row: (row.promedio_tph - promedio_tph_inicial) * args.tamaño_bloque / 60

            res = bloques.apply(fn, axis=1)

            resultado = res.sum()

            if caso == 1:
                resultados.loc[f"caso{caso}{molino}"] += resultado
            elif caso == 2:
                resultados.loc[f"caso{caso}{molino}"] += resultado


        resultados = pd.DataFrame(columns=resultados.keys(), data=[resultados.values])

        resultados.to_sql(name='uplift', con=sql_alchemy_output, index=False,  if_exists='append')


if __name__ == '__main__':
    main()
