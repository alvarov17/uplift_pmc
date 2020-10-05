from .helpers import make_dataframe

def obtener_recomendaciones_implementadas(conn_output, created_at):
    query_recomendaciones = """
        SELECT
            id,
            updated_at,
            val_actual,
            description,
            n_molino
        FROM
            recom_implemented
        WHERE
            entre_limites is not TRUE
            AND created_at = '{created_at}';
        """.format(created_at=created_at)

    recomendaciones = make_dataframe(conn=conn_output, query=query_recomendaciones)
    return recomendaciones


def obtener_bloque_inicial(conn_process, fecha_inicio, hora_turno, tag):
    sql = """
            SELECT
                t."timestamp",
                l.tag,
                CASE WHEN t.tag != 'MU:280_WIC_8778'
                AND t."value" > 150 then t."value"
                WHEN t.tag = 'MU:280_WIC_8778' 
                AND t."value" > 750 then t."value"
                ELSE NULL end as "value",
                hl. "value" AS hl
            FROM
                public.limits_tags l
                LEFT JOIN input_tags t ON t.tag = l.tag
                LEFT JOIN input_tags ll ON ll.tag = l.tagll
                    AND t. "timestamp" = ll. "timestamp"
                LEFT JOIN input_tags hl ON hl.tag = l.taghl
                    AND t. "timestamp" = hl. "timestamp"
            WHERE
            t.tag = '{tag}' and
            t. "timestamp" BETWEEN '{fecha_inicio}'::timestamp - interval '15m'
            AND '{fecha_inicio}'::timestamp;
              """.format(tag=tag, fecha_inicio=fecha_inicio, hora_turno=hora_turno)
    df = make_dataframe(conn=conn_process, query=sql)
    df.dropna(inplace=True)

    if df.empty:
        vacio = True
        return df, vacio
    else:
        vacio = False

    df = df.resample('15min', on='timestamp').mean()

    df.reset_index(level=0, inplace=True)

    return df, vacio


def obtener_tph_turno(conn_process, fi, ft, tag, tamaño_bloque):
    sql = """
        SELECT
         t."timestamp",
            l.tag,
            CASE WHEN t.tag != 'MU:280_WIC_8778'
            AND t."value" > 100 then t."value"
            WHEN t.tag = 'MU:280_WIC_8778' 
            AND t."value" > 500 then t."value"
            ELSE NULL end as "value",
            hl. "value" AS hl
        FROM
            public.limits_tags l
            LEFT JOIN input_tags t ON t.tag = l.tag
            LEFT JOIN input_tags ll ON ll.tag = l.tagll
                AND t. "timestamp" = ll. "timestamp"
            LEFT JOIN input_tags hl ON hl.tag = l.taghl
                AND t. "timestamp" = hl. "timestamp"
        WHERE
            t.tag = '{tag}' and
            t. "timestamp" BETWEEN '{fi}'::timestamp
            AND '{fi}'::timestamp + interval '1h';
              """.format(tag=tag, fi=fi, ft=ft)

    df = make_dataframe(conn=conn_process, query=sql)
    df.dropna(inplace=True)

    if df.empty:
        vacio = True
        return df, vacio
    else:
        vacio = False

    df.resample('15min', on='timestamp').mean()
    bloques = df.resample(f"{tamaño_bloque}min", on='timestamp', label='left').mean().rename_axis('bloque').rename(columns={'value': 'promedio_tph', 'hl': 'promedio_hl'})
    bloques.reset_index(level=0, inplace=True)
    bloques.dropna(inplace=True)

    return bloques, vacio


def obtener_horarios_recomendaciones(output_data_conn, fecha_inicio):
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
        """.format(fi=fecha_inicio)

    df = make_dataframe(conn=output_data_conn, query=query)
    return df

tph_dict = {
    13: 'MU:280_WIC_8778',
    12: 'MOL:WIC44N_MINERAL',
    11: 'MOL:WIC44M_MINERAL',
    10: 'MOL:WIC44L_MINERAL',
    9: 'MOL:WIC44K_MINERAL',
    8: 'MOL:WIC44J_MINERAL',
    7: 'MOL:WIC44G_MINERAL',
    6: 'MOL:WIC44F_MINERAL',
    5: 'MOL:WIC44E_MINERAL',
    4: 'MOL:WIC44D_MINERAL',
    3: 'MOL:WIC44C_MINERAL',
    2: 'MOL:WIC44B_MINERAL',
    1: 'MOL:WIC44A_MINERAL'
}

molinos_dict = {
    13: 'MUN',
    12: 'M12',
    11: 'M11',
    10: 'M10',
    9: 'M9',
    8: 'M8',
    7: 'M7',
    6: 'M6',
    5: 'M5',
    4: 'M4',
    3: 'M3',
    2: 'M2',
    1: 'M1'
}