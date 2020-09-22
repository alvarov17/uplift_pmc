from .helpers import make_dataframe

def obtener_recomendaciones_implementadas(conn_output, created_at):
    query_recomendaciones = """
        SELECT id, updated_at , val_actual, description, n_molino 
        from recom_implemented
        WHERE
        created_at = '{created_at}';
        """.format(created_at=created_at)

    recomendaciones = make_dataframe(conn=conn_output, query=query_recomendaciones)
    return recomendaciones


def obtener_datos_por_minutos(conn_process, created_at):
    pass

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