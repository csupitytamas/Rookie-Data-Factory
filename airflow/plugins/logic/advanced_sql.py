
import pandas as pd

import sqlalchemy as sa



def run_secure_sql_wrapper(user_sql, actual_source_table, engine):

    """

    Biztonságos SQL Wrapper (Sandwich módszer).

    1. CTE: Elrejti a valódi táblanevet az 'input_data' alias mögé.

    2. SUBQUERY: A felhasználó kódját bezárja, így csak SELECT futhat.

    """

   

    # 1. Tisztítás: Pontosvessző eltávolítása a végéről (hogy ne lehessen láncolni)

    clean_user_sql = user_sql.strip().rstrip(';')



    # 2. A SQL összeállítása (Sandwich)

    # A felhasználó kódja a FROM (...) zárójelébe kerül.

    wrapper_sql = f"""WITH input_data AS ( SELECT * FROM "{actual_source_table}") SELECT * FROM ({clean_user_sql}) AS user_query_wrapper"""



    print(f"[CUSTOM SQL] Executing Secured Wrapper:\n{wrapper_sql}")



    # 3. Futtatás

    try:

        with engine.connect() as conn:

            # Pandas read_sql használata a biztonságos lekérdezéssel

            return pd.read_sql(sa.text(wrapper_sql), conn)

    except Exception as e:

        print(f"[CUSTOM SQL ERROR] Hiba a felhasználói SQL futtatásakor: {e}")

        # Itt érdemes lehet egy barátságosabb hibaüzenetet visszaadni a UI-nak

        raise Exception(f"SQL Error: A megadott lekérdezés hibás. Részletek: {e}")

